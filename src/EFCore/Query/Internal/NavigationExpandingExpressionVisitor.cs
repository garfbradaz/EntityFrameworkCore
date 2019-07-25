// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices.ComTypes;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Microsoft.EntityFrameworkCore.Query.Internal
{
    public partial class NavigationExpandingExpressionVisitor : ExpressionVisitor
    {
        private readonly IModel _model;
        private readonly PendingSelectorExpandingExpressionVisitor _pendingSelectorExpandingExpressionVisitor;
        private readonly ReducingExpressionVisitor _reducingExpressionVisitor;
        private readonly ISet<string> _parameterNames = new HashSet<string>();
        private static readonly MethodInfo _enumerableToListMethodInfo = typeof(Enumerable).GetTypeInfo()
            .GetDeclaredMethods(nameof(Enumerable.ToList))
            .Single(mi => mi.GetParameters().Length == 1);
        public NavigationExpandingExpressionVisitor(QueryCompilationContext queryCompilationContext)
        {
            _model = queryCompilationContext.Model;
            _pendingSelectorExpandingExpressionVisitor = new PendingSelectorExpandingExpressionVisitor(this);
            _reducingExpressionVisitor = new ReducingExpressionVisitor();
        }

        private string GetParameterName(string prefix)
        {
            var uniqueName = prefix;
            var index = 0;
            while (_parameterNames.Contains(uniqueName))
            {
                uniqueName = $"{prefix}{index++}";
            }

            _parameterNames.Add(uniqueName);
            return uniqueName;
        }

        public Expression Expand(Expression query)
        {
            var result = Visit(query);
            result = _pendingSelectorExpandingExpressionVisitor.Visit(result);
            result = new IncludeApplyingExpressionVisitor(this).Visit(result);

            return Reduce(result);
        }

        protected override Expression VisitConstant(ConstantExpression constantExpression)
        {
            if (constantExpression.IsEntityQueryable())
            {
                var entityType = _model.FindEntityType(((IQueryable)constantExpression.Value).ElementType);
                var entityReference = new EntityReference(entityType);
                PopulateEagerLoadedNavigations(entityReference.IncludePaths);

                var currentTree = new NavigationTreeExpression(entityReference);
                var parameterName = GetParameterName((entityType.ShortName()[0] + "").ToLower());

                return new NavigationExpansionExpression(constantExpression, currentTree, currentTree, parameterName);
            }

            return base.VisitConstant(constantExpression);
        }

        private static void PopulateEagerLoadedNavigations(IncludeTreeNode includeTreeNode)
        {
            var entityType = includeTreeNode.EntityType;
            var outboundNavigations
                = entityType.GetNavigations()
                    .Concat(entityType.GetDerivedNavigations())
                    .Where(n => n.IsEagerLoaded());

            foreach (var navigation in outboundNavigations)
            {
                var addedIncludeTreeNode = includeTreeNode.AddNavigation(navigation);
                PopulateEagerLoadedNavigations(addedIncludeTreeNode);
            }
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            return extensionExpression is NavigationExpansionExpression
                ? extensionExpression
                : base.VisitExtension(extensionExpression);
        }

        protected override Expression VisitMember(MemberExpression memberExpression)
        {
            var innerExpression = Visit(memberExpression.Expression);

            // Convert CollectionNavigation.Count to subquery.Count()
            if (innerExpression is MaterializeCollectionNavigationExpression materializeCollectionNavigation
                && memberExpression.Member.Name == nameof(List<int>.Count))
            {
                return Visit(Expression.Call(
                    QueryableMethodProvider.CountWithoutPredicateMethodInfo.MakeGenericMethod(
                        materializeCollectionNavigation.Subquery.Type.TryGetSequenceType()),
                    materializeCollectionNavigation.Subquery));
            }

            var updatedExpression = (Expression)memberExpression.Update(innerExpression);
            if (innerExpression is NavigationExpansionExpression navigationExpansionExpression
                && navigationExpansionExpression.CardinalityReducingGenericMethodInfo != null)
            {
                // This is FirstOrDefault.Member
                // due to SubqueryMemberPushdown, this may be collection navigation which was not pushed down
                var expandedExpression = new ExpandingExpressionVisitor(this, navigationExpansionExpression).Visit(updatedExpression);
                if (expandedExpression != updatedExpression)
                {
                    updatedExpression = Visit(expandedExpression);
                }
            }

            return updatedExpression;
        }

        protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Method.DeclaringType == typeof(Queryable)
                || methodCallExpression.Method.DeclaringType == typeof(QueryableExtensions)
                || methodCallExpression.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions))
            {
                var firstArgument = Visit(methodCallExpression.Arguments[0]);
                if (firstArgument is NavigationExpansionExpression source)
                {
                    if (source.PendingOrderings.Any()
                        && !string.Equals(methodCallExpression.Method.Name, nameof(Queryable.ThenBy))
                        && !string.Equals(methodCallExpression.Method.Name, nameof(Queryable.ThenByDescending)))
                    {
                        ApplyPendingOrderings(source);
                    }

                    switch (methodCallExpression.Method.Name)
                    {
                        case nameof(Queryable.AsQueryable):
                            return source;

                        case nameof(Queryable.All):
                        case nameof(Queryable.Any):
                        case nameof(Queryable.Count):
                        case nameof(Queryable.LongCount):
                            return ProcessAllAnyCountLongCount(
                                source,
                                methodCallExpression.Method.GetGenericMethodDefinition(),
                                methodCallExpression.Arguments.Count == 2
                                    ? methodCallExpression.Arguments[1].UnwrapLambdaFromQuote()
                                    : null);

                        case nameof(Queryable.Average):
                        case nameof(Queryable.Max):
                        case nameof(Queryable.Min):
                        case nameof(Queryable.Sum):
                            return ProcessAverageMaxMinSum(
                                source,
                                methodCallExpression.Method.IsGenericMethod
                                    ? methodCallExpression.Method.GetGenericMethodDefinition()
                                    : methodCallExpression.Method,
                                methodCallExpression.Arguments.Count == 2
                                    ? methodCallExpression.Arguments[1].UnwrapLambdaFromQuote()
                                    : null);

                        case nameof(Queryable.Distinct):
                        case nameof(Queryable.Skip):
                        case nameof(Queryable.Take):
                            return ProcessDistinctSkipTake(
                                source,
                                methodCallExpression.Method.GetGenericMethodDefinition(),
                                methodCallExpression.Arguments.Count == 2
                                    ? methodCallExpression.Arguments[1]
                                    : null);

                        case nameof(Queryable.Contains):
                            return ProcessContains(
                                source,
                                methodCallExpression.Arguments[1]);

                        case nameof(Queryable.First):
                        case nameof(Queryable.FirstOrDefault):
                        case nameof(Queryable.Single):
                        case nameof(Queryable.SingleOrDefault):
                        case nameof(Queryable.Last):
                        case nameof(Queryable.LastOrDefault):
                            return ProcessFirstSingleLastOrDefault(
                                source,
                                methodCallExpression.Method.GetGenericMethodDefinition(),
                                methodCallExpression.Arguments.Count == 2
                                    ? methodCallExpression.Arguments[1].UnwrapLambdaFromQuote()
                                    : null,
                                methodCallExpression.Type);

                        case nameof(Queryable.Join):
                            {
                                var secondArgument = Visit(methodCallExpression.Arguments[1]);
                                if (secondArgument is NavigationExpansionExpression innerSource)
                                {
                                    return ProcessJoin(
                                        source,
                                        innerSource,
                                        methodCallExpression.Arguments[2].UnwrapLambdaFromQuote(),
                                        methodCallExpression.Arguments[3].UnwrapLambdaFromQuote(),
                                        methodCallExpression.Arguments[4].UnwrapLambdaFromQuote());
                                }
                            }
                            break;

                        case nameof(QueryableExtensions.LeftJoin):
                            {
                                var secondArgument = Visit(methodCallExpression.Arguments[1]);
                                if (secondArgument is NavigationExpansionExpression innerSource)
                                {
                                    return ProcessLeftJoin(
                                        source,
                                        innerSource,
                                        methodCallExpression.Arguments[2].UnwrapLambdaFromQuote(),
                                        methodCallExpression.Arguments[3].UnwrapLambdaFromQuote(),
                                        methodCallExpression.Arguments[4].UnwrapLambdaFromQuote());
                                }
                            }
                            break;

                        case nameof(Queryable.SelectMany):
                            return ProcessSelectMany(
                                source,
                                methodCallExpression.Method.GetGenericMethodDefinition(),
                                methodCallExpression.Arguments[1].UnwrapLambdaFromQuote(),
                                methodCallExpression.Arguments.Count == 3
                                    ? methodCallExpression.Arguments[2].UnwrapLambdaFromQuote()
                                    : null);

                        case nameof(Queryable.Concat):
                        case nameof(Queryable.Except):
                        case nameof(Queryable.Intersect):
                        case nameof(Queryable.Union):
                            {
                                var secondArgument = Visit(methodCallExpression.Arguments[1]);
                                if (secondArgument is NavigationExpansionExpression innerSource)
                                {
                                    return ProcessSetOperation(
                                        source,
                                        methodCallExpression.Method.GetGenericMethodDefinition(),
                                        innerSource);
                                }
                            }
                            break;

                        case nameof(Queryable.Cast):
                        case nameof(Queryable.OfType):
                            return ProcessCastOfType(
                                source,
                                methodCallExpression.Method.GetGenericMethodDefinition(),
                                methodCallExpression.Type.TryGetSequenceType());



                        case nameof(EntityFrameworkQueryableExtensions.Include):
                        case nameof(EntityFrameworkQueryableExtensions.ThenInclude):
                            return ProcessInclude(
                                source,
                                methodCallExpression.Arguments[1],
                                string.Equals(methodCallExpression.Method.Name,
                                  nameof(EntityFrameworkQueryableExtensions.ThenInclude)));

                        case nameof(Queryable.GroupJoin):
                        case nameof(Queryable.GroupBy):
                        case nameof(Queryable.DefaultIfEmpty):
                            throw new NotImplementedException(methodCallExpression.Method.Name);

                        case nameof(Queryable.OrderBy):
                        case nameof(Queryable.OrderByDescending):
                            return ProcessOrderByThenBy(
                                source,
                                methodCallExpression.Method.GetGenericMethodDefinition(),
                                methodCallExpression.Arguments[1].UnwrapLambdaFromQuote(),
                                thenBy: false);

                        case nameof(Queryable.ThenBy):
                        case nameof(Queryable.ThenByDescending):
                            return ProcessOrderByThenBy(
                                source,
                                methodCallExpression.Method.GetGenericMethodDefinition(),
                                methodCallExpression.Arguments[1].UnwrapLambdaFromQuote(),
                                thenBy: true);


                        case nameof(Queryable.Select):
                            return ProcessSelect(
                                source,
                                methodCallExpression.Arguments[1].UnwrapLambdaFromQuote());

                        case nameof(Queryable.Where):
                            return ProcessWhere(
                                source,
                                methodCallExpression.Arguments[1].UnwrapLambdaFromQuote());

                        default:
                            throw new NotImplementedException("Unhandled method");
                    }
                }
                else if (firstArgument is MaterializeCollectionNavigationExpression materializeCollectionNavigationExpression
                    && methodCallExpression.Method.Name == nameof(Queryable.AsQueryable))
                {
                    return materializeCollectionNavigationExpression.Subquery;
                }

                throw new NotImplementedException("NonNavSource");
            }

            if (methodCallExpression.Method.IsGenericMethod
                && methodCallExpression.Method.GetGenericMethodDefinition() == _enumerableToListMethodInfo)
            {
                var argument = Visit(methodCallExpression.Arguments[0]);
                if (argument is MaterializeCollectionNavigationExpression materializeCollectionNavigationExpression)
                {
                    argument = materializeCollectionNavigationExpression.Subquery;
                }

                return methodCallExpression.Update(null, new[] { argument });
            }

            return ProcessUnknownMethod(methodCallExpression);
        }

        private Expression ProcessUnknownMethod(MethodCallExpression methodCallExpression)
        {
            var queryableElementType = methodCallExpression.Type.TryGetElementType(typeof(IQueryable<>));
            if (queryableElementType != null
                && methodCallExpression.Object == null
                && methodCallExpression.Arguments.All(a => a.GetLambdaOrNull() == null)
                && methodCallExpression.Method.IsGenericMethod
                && methodCallExpression.Method.GetGenericArguments().Length == 1
                && methodCallExpression.Method.GetGenericArguments()[0] == queryableElementType
                && methodCallExpression.Arguments.Count > 0
                && methodCallExpression.Arguments.Skip(1).All(e => e.Type.TryGetElementType(typeof(IQueryable<>)) == null))
            {
                var firstArgumet = Visit(methodCallExpression.Arguments[0]);
                if (firstArgumet is NavigationExpansionExpression source
                    && source.Type == methodCallExpression.Type)
                {
                    source = (NavigationExpansionExpression)_pendingSelectorExpandingExpressionVisitor.Visit(source);
                    var newStructure = SnapshotExpression(source.PendingSelector);
                    var queryable = Reduce(source);

                    var result = Expression.Call(
                            methodCallExpression.Method.GetGenericMethodDefinition().MakeGenericMethod(queryableElementType),
                            new[] { queryable }.Concat(methodCallExpression.Arguments.Skip(1).Select(e => Visit(e))));

                    var navigationTree = new NavigationTreeExpression(newStructure);
                    var parameterName = GetParameterName("e");

                    return new NavigationExpansionExpression(result, navigationTree, navigationTree, parameterName);
                }
            }

            return base.VisitMethodCall(methodCallExpression);
        }

        private Expression ProcessInclude(
            NavigationExpansionExpression source, Expression expression, bool thenInclude)
        {
            if (source.PendingSelector is NavigationTreeExpression navigationTree
                && navigationTree.Value is EntityReference entityReferece)
            {
                if (expression is ConstantExpression includeConstant
                    && includeConstant.Value is string navigationChain)
                {
                    var navigationPaths = navigationChain.Split(new[] { "." }, StringSplitOptions.None);
                    var includeTreeNodes = new Queue<IncludeTreeNode>();
                    includeTreeNodes.Enqueue(entityReferece.IncludePaths);
                    foreach (var navigationName in navigationPaths)
                    {
                        var nodesToProcess = includeTreeNodes.Count;
                        while (nodesToProcess-- > 0)
                        {
                            var currentNode = includeTreeNodes.Dequeue();
                            foreach (var navigation in FindNavigations(currentNode.EntityType, navigationName))
                            {
                                var addedNode = currentNode.AddNavigation(navigation);
                                // This is to add eager Loaded navigations when owner type is included.
                                PopulateEagerLoadedNavigations(addedNode);
                                includeTreeNodes.Enqueue(addedNode);
                            }
                        }

                        if (includeTreeNodes.Count == 0)
                        {
                            throw new InvalidOperationException("Invalid include path: '" + navigationChain +
                                "' - couldn't find navigation for: '" + navigationName + "'");
                        }
                    }
                }
                else
                {
                    var currentIncludeTreeNode = thenInclude
                        ? entityReferece.LastIncludeTreeNode
                        : entityReferece.IncludePaths;
                    var includeLambda = expression.UnwrapLambdaFromQuote();
                    var lastIncludeTree = PopulateIncludeTree(currentIncludeTreeNode, includeLambda.Body);
                    if (lastIncludeTree == null)
                    {
                        throw new InvalidOperationException("Lambda expression used inside Include is not valid.");
                    }
                    entityReferece.SetLastInclude(lastIncludeTree);
                }

                return source;
            }

            throw new InvalidOperationException("Include has been used on non entity queryable.");
        }

        private IncludeTreeNode PopulateIncludeTree(IncludeTreeNode includeTreeNode, Expression expression)
        {
            switch (expression)
            {
                case ParameterExpression _:
                    return includeTreeNode;

                case MemberExpression memberExpression:

                    var innerExpression = memberExpression.Expression;
                    Type convertedType = null;
                    if (innerExpression is UnaryExpression unaryExpression
                        && (unaryExpression.NodeType == ExpressionType.Convert
                            || unaryExpression.NodeType == ExpressionType.ConvertChecked
                            || unaryExpression.NodeType == ExpressionType.TypeAs))
                    {
                        convertedType = unaryExpression.Type;
                        innerExpression = unaryExpression.Operand;
                    }

                    var innerIncludeTreeNode = PopulateIncludeTree(includeTreeNode, innerExpression);
                    var entityType = innerIncludeTreeNode.EntityType;
                    if (convertedType != null)
                    {
                        entityType = entityType.GetTypesInHierarchy().FirstOrDefault(et => et.ClrType == convertedType);
                        if (entityType == null)
                        {
                            throw new InvalidOperationException("Invalid include.");
                        }
                    }

                    var navigation = entityType.FindNavigation(memberExpression.Member);
                    if (navigation != null)
                    {
                        // This is to add eager Loaded navigations when owner type is included.
                        var addedNode = innerIncludeTreeNode.AddNavigation(navigation);
                        PopulateEagerLoadedNavigations(addedNode);
                        return addedNode;
                    }
                    break;
            }

            return null;
        }

        private IEnumerable<INavigation> FindNavigations(IEntityType entityType, string navigationName)
        {
            var navigation = entityType.FindNavigation(navigationName);
            if (navigation != null)
            {
                yield return navigation;
            }
            else
            {
                foreach (var derivedNavigation in entityType.GetDerivedTypes()
                    .Select(et => et.FindDeclaredNavigation(navigationName)).Where(n => n != null))
                {
                    yield return derivedNavigation;
                }
            }
        }

        private Expression ProcessSelectMany(
            NavigationExpansionExpression source,
            MethodInfo genericMethod,
            LambdaExpression collectionSelector,
            LambdaExpression resultSelector)
        {
            var collectionSelectorBody = ExpandNavigationsInLambdaExpression(source, collectionSelector);
            if (collectionSelectorBody is NavigationExpansionExpression collectionSource)
            {
                collectionSource = (NavigationExpansionExpression)_pendingSelectorExpandingExpressionVisitor.Visit(collectionSource);
                var innerTree = new NavigationTreeExpression(SnapshotExpression(collectionSource.PendingSelector));
                collectionSelector = GenerateLambda(collectionSource, source.CurrentParameter);
                var collectionElementType = collectionSelector.ReturnType.TryGetSequenceType();

                // Collection selector body is IQueryable, we need to adjust the type to IEnumerable, to match the SelectMany signature
                // therefore the delegate type is specified explicitly
                var collectionSelectorLambdaType = typeof(Func<,>).MakeGenericType(
                    source.SourceElementType,
                    typeof(IEnumerable<>).MakeGenericType(collectionElementType));

                collectionSelector = Expression.Lambda(
                    collectionSelectorLambdaType,
                    collectionSelector.Body,
                    collectionSelector.Parameters[0]);

                var transparentIdentifierType = TransparentIdentifierType.Create(
                    source.SourceElementType, collectionElementType);

                var transparentIdentifierOuterMemberInfo = transparentIdentifierType.GetTypeInfo().GetDeclaredField("Outer");
                var transparentIdentifierInnerMemberInfo = transparentIdentifierType.GetTypeInfo().GetDeclaredField("Inner");

                var collectionElementParameter = Expression.Parameter(collectionElementType, "c");

                var newResultSelector = Expression.Lambda(
                    Expression.New(
                        transparentIdentifierType.GetTypeInfo().GetConstructors().Single(),
                        new[] { source.CurrentParameter, collectionElementParameter },
                        new[] { transparentIdentifierOuterMemberInfo, transparentIdentifierInnerMemberInfo }),
                    source.CurrentParameter,
                    collectionElementParameter);

                var newSource = Expression.Call(
                    genericMethod.MakeGenericMethod(
                        source.SourceElementType, collectionElementType, newResultSelector.ReturnType),
                    source.Source,
                    Expression.Quote(collectionSelector),
                    Expression.Quote(newResultSelector));


                var currentTree = new NavigationTreeNode(source.CurrentTree, innerTree);
                var pendingSelector = new ReplacingExpressionVisitor(
                    new Dictionary<Expression, Expression>
                    {
                    { resultSelector.Parameters[0], source.PendingSelector },
                    { resultSelector.Parameters[1], innerTree }
                    }).Visit(resultSelector.Body);
                var parameterName = GetParameterName("ti");

                return new NavigationExpansionExpression(newSource, currentTree, pendingSelector, parameterName);
            }

            throw new InvalidOperationException("SelectMany's collectionSelector was not NavigationExpansionExpression");
        }

        private void ApplyPendingOrderings(NavigationExpansionExpression source)
        {
            if (source.PendingOrderings.Any())
            {
                foreach (var (orderingMethod, keySelector) in source.PendingOrderings)
                {
                    var keySelectorLambda = GenerateLambda(keySelector, source.CurrentParameter);

                    source.UpdateSource(Expression.Call(
                        orderingMethod.MakeGenericMethod(source.SourceElementType, keySelectorLambda.ReturnType),
                        source.Source,
                        keySelectorLambda));
                }

                source.ClearPendingOrderings();
            }
        }

        private bool CompareIncludes(Expression outer, Expression inner)
        {
            if (outer is EntityReference outerEntityReference
                && inner is EntityReference innerEntityReference)
            {
                return outerEntityReference.EntityType == innerEntityReference.EntityType
                    && outerEntityReference.IncludePaths.Equals(innerEntityReference.IncludePaths);
            }

            if (outer is NewExpression outerNewExpression
                && inner is NewExpression innerNewExpression)
            {
                if (outerNewExpression.Arguments.Count != innerNewExpression.Arguments.Count)
                {
                    return false;
                }

                for (var i = 0; i < outerNewExpression.Arguments.Count; i++)
                {
                    if (!CompareIncludes(outerNewExpression.Arguments[i], innerNewExpression.Arguments[i]))
                    {
                        return false;
                    }
                }

                return true;
            }

            return outer is DefaultExpression outerDefaultExpression
                && inner is DefaultExpression innerDefaultExpression
                && outerDefaultExpression.Type == innerDefaultExpression.Type;
        }

        private Expression ProcessSetOperation(
            NavigationExpansionExpression outerSource,
            MethodInfo genericMethod,
            NavigationExpansionExpression innerSource)
        {
            outerSource = (NavigationExpansionExpression)_pendingSelectorExpandingExpressionVisitor.Visit(outerSource);
            var outerTreeStructure = SnapshotExpression(outerSource.PendingSelector);

            innerSource = (NavigationExpansionExpression)_pendingSelectorExpandingExpressionVisitor.Visit(innerSource);
            var innerTreeStructure = SnapshotExpression(innerSource.PendingSelector);

            if (!CompareIncludes(outerTreeStructure, innerTreeStructure))
            {
                throw new InvalidOperationException(CoreStrings.SetOperationWithDifferentIncludesInOperands);
            }

            var outerQueryable = Reduce(outerSource);
            var innerQueryable = Reduce(innerSource);

            var outerType = outerQueryable.Type.TryGetSequenceType();
            var innerType = innerQueryable.Type.TryGetSequenceType();

            var result = Expression.Call(
                genericMethod.MakeGenericMethod(outerType.IsAssignableFrom(innerType) ? outerType : innerType),
                outerQueryable,
                innerQueryable);
            var navigationTree = new NavigationTreeExpression(
                outerType.IsAssignableFrom(innerType) ? outerTreeStructure : innerTreeStructure);
            var parameterName = GetParameterName("e");

            return new NavigationExpansionExpression(result, navigationTree, navigationTree, parameterName);
        }

        private Expression ProcessJoin(
            NavigationExpansionExpression outerSource,
            NavigationExpansionExpression innerSource,
            LambdaExpression outerKeySelector,
            LambdaExpression innerKeySelector,
            LambdaExpression resultSelector)
        {
            if (innerSource.PendingOrderings.Any())
            {
                ApplyPendingOrderings(innerSource);
            }

            var outerKey = ExpandNavigationsInLambdaExpression(outerSource, outerKeySelector);
            var innerKey = ExpandNavigationsInLambdaExpression(innerSource, innerKeySelector);

            outerKeySelector = GenerateLambda(outerKey, outerSource.CurrentParameter);
            innerKeySelector = GenerateLambda(innerKey, innerSource.CurrentParameter);

            var transparentIdentifierType = TransparentIdentifierType.Create(
                outerSource.SourceElementType, innerSource.SourceElementType);

            var transparentIdentifierOuterMemberInfo = transparentIdentifierType.GetTypeInfo().GetDeclaredField("Outer");
            var transparentIdentifierInnerMemberInfo = transparentIdentifierType.GetTypeInfo().GetDeclaredField("Inner");

            var newResultSelector = Expression.Lambda(
                Expression.New(
                    transparentIdentifierType.GetTypeInfo().GetConstructors().Single(),
                    new[] { outerSource.CurrentParameter, innerSource.CurrentParameter },
                    new[] { transparentIdentifierOuterMemberInfo, transparentIdentifierInnerMemberInfo }),
                outerSource.CurrentParameter,
                innerSource.CurrentParameter);

            var source = Expression.Call(
                QueryableMethodProvider.JoinMethodInfo.MakeGenericMethod(
                    outerSource.SourceElementType, innerSource.SourceElementType, outerKeySelector.ReturnType, newResultSelector.ReturnType),
                outerSource.Source,
                innerSource.Source,
                Expression.Quote(outerKeySelector),
                Expression.Quote(innerKeySelector),
                Expression.Quote(newResultSelector));

            var currentTree = new NavigationTreeNode(outerSource.CurrentTree, innerSource.CurrentTree);
            var pendingSelector = new ReplacingExpressionVisitor(
                new Dictionary<Expression, Expression>
                {
                    { resultSelector.Parameters[0], outerSource.PendingSelector },
                    { resultSelector.Parameters[1], innerSource.PendingSelector }
                }).Visit(resultSelector.Body);
            var parameterName = GetParameterName("ti");

            return new NavigationExpansionExpression(source, currentTree, pendingSelector, parameterName);
        }

        private Expression ProcessLeftJoin(
            NavigationExpansionExpression outerSource,
            NavigationExpansionExpression innerSource,
            LambdaExpression outerKeySelector,
            LambdaExpression innerKeySelector,
            LambdaExpression resultSelector)
        {
            if (innerSource.PendingOrderings.Any())
            {
                ApplyPendingOrderings(innerSource);
            }

            var outerKey = ExpandNavigationsInLambdaExpression(outerSource, outerKeySelector);
            var innerKey = ExpandNavigationsInLambdaExpression(innerSource, innerKeySelector);

            outerKeySelector = GenerateLambda(outerKey, outerSource.CurrentParameter);
            innerKeySelector = GenerateLambda(innerKey, innerSource.CurrentParameter);

            var transparentIdentifierType = TransparentIdentifierType.Create(
                outerSource.SourceElementType, innerSource.SourceElementType);

            var transparentIdentifierOuterMemberInfo = transparentIdentifierType.GetTypeInfo().GetDeclaredField("Outer");
            var transparentIdentifierInnerMemberInfo = transparentIdentifierType.GetTypeInfo().GetDeclaredField("Inner");

            var newResultSelector = Expression.Lambda(
                Expression.New(
                    transparentIdentifierType.GetTypeInfo().GetConstructors().Single(),
                    new[] { outerSource.CurrentParameter, innerSource.CurrentParameter },
                    new[] { transparentIdentifierOuterMemberInfo, transparentIdentifierInnerMemberInfo }),
                outerSource.CurrentParameter,
                innerSource.CurrentParameter);

            var source = Expression.Call(
                QueryableExtensions.LeftJoinMethodInfo.MakeGenericMethod(
                    outerSource.SourceElementType, innerSource.SourceElementType, outerKeySelector.ReturnType, newResultSelector.ReturnType),
                outerSource.Source,
                innerSource.Source,
                Expression.Quote(outerKeySelector),
                Expression.Quote(innerKeySelector),
                Expression.Quote(newResultSelector));

            var currentTree = new NavigationTreeNode(outerSource.CurrentTree, innerSource.CurrentTree);
            var pendingSelector = new ReplacingExpressionVisitor(
                new Dictionary<Expression, Expression>
                {
                    { resultSelector.Parameters[0], outerSource.PendingSelector },
                    { resultSelector.Parameters[1], innerSource.PendingSelector }
                }).Visit(resultSelector.Body);
            var parameterName = GetParameterName("ti");

            return new NavigationExpansionExpression(source, currentTree, pendingSelector, parameterName);
        }

        private Expression ProcessCastOfType(
            NavigationExpansionExpression source,
            MethodInfo genericMethod,
            Type castType)
        {
            source = (NavigationExpansionExpression)_pendingSelectorExpandingExpressionVisitor.Visit(source);
            var newStructure = SnapshotExpression(source.PendingSelector);
            var queryable = Reduce(source);

            var result = Expression.Call(genericMethod.MakeGenericMethod(castType), queryable);

            if (newStructure is EntityReference entityReference)
            {
                var castEntityType = entityReference.EntityType.GetTypesInHierarchy().FirstOrDefault(et => et.ClrType == castType);
                if (castEntityType != null)
                {
                    var newEntityReference = new EntityReference(castEntityType);
                    if (entityReference.IsOptional)
                    {
                        newEntityReference.MarkAsOptional();
                    }
                    newEntityReference.SetIncludePaths(entityReference.IncludePaths);

                    // Prune includes for sibling types
                    var siblingNavigations = newEntityReference.IncludePaths.Keys
                        .Where(n => !castEntityType.IsAssignableFrom(n.DeclaringEntityType)
                            && !n.DeclaringEntityType.IsAssignableFrom(castEntityType)).ToList();

                    foreach (var navigation in siblingNavigations)
                    {
                        newEntityReference.IncludePaths.Remove(navigation);
                    }

                    newStructure = newEntityReference;
                }
            }
            else
            {
                newStructure = Expression.Default(castType);
            }

            var navigationTree = new NavigationTreeExpression(newStructure);
            var parameterName = GetParameterName("e");

            return new NavigationExpansionExpression(result, navigationTree, navigationTree, parameterName);
        }

        private Expression ProcessAllAnyCountLongCount(
            NavigationExpansionExpression source,
            MethodInfo genericMethod,
            LambdaExpression predicate)
        {
            if (predicate != null)
            {
                var predicateBody = ExpandNavigationsInLambdaExpression(source, predicate);

                return Expression.Call(
                    genericMethod.GetGenericMethodDefinition().MakeGenericMethod(source.SourceElementType),
                    source.Source,
                    Expression.Quote(GenerateLambda(predicateBody, source.CurrentParameter)));
            }

            return Expression.Call(
                genericMethod.MakeGenericMethod(source.SourceElementType),
                source.Source);
        }

        private Expression ProcessAverageMaxMinSum(
            NavigationExpansionExpression source,
            MethodInfo method,
            LambdaExpression selector)
        {
            if (selector != null)
            {
                source = (NavigationExpansionExpression)ProcessSelect(source, selector);
                source = (NavigationExpansionExpression)_pendingSelectorExpandingExpressionVisitor.Visit(source);

                var selectorLambda = GenerateLambda(source.PendingSelector, source.CurrentParameter);
                if (method.GetGenericArguments().Length == 2)
                {
                    // Min/Max with selector has 2 generic parameters
                    method = method.MakeGenericMethod(source.SourceElementType, selectorLambda.ReturnType);
                }
                else
                {
                    method = method.MakeGenericMethod(source.SourceElementType);
                }

                return Expression.Call(method, source.Source, selectorLambda);
            }

            source = (NavigationExpansionExpression)_pendingSelectorExpandingExpressionVisitor.Visit(source);
            var queryable = Reduce(source);

            if (method.GetGenericArguments().Length == 1)
            {
                // Min/Max without selector has 1 generic parameters
                method = method.MakeGenericMethod(queryable.Type.TryGetSequenceType());
            }

            return Expression.Call(method, queryable);
        }

        private Expression SnapshotExpression(Expression selector)
        {
            switch (selector)
            {
                case EntityReference entityReference:
                    return entityReference.Clone();

                case NavigationTreeExpression navigationTreeExpression:
                    return SnapshotExpression(navigationTreeExpression.Value);

                case NewExpression newExpression:
                    {
                        var arguments = new Expression[newExpression.Arguments.Count];
                        for (var i = 0; i < newExpression.Arguments.Count; i++)
                        {
                            arguments[i] = newExpression.Arguments[i] is NewExpression
                                || newExpression.Arguments[i] is NavigationTreeExpression
                                ? SnapshotExpression(newExpression.Arguments[i])
                                : Expression.Default(newExpression.Arguments[i].Type);
                        }

                        return newExpression.Update(arguments);
                    }

                default:
                    return Expression.Default(selector.Type);
            }
        }

        private static readonly IDictionary<MethodInfo, MethodInfo> _predicateLessMethodInfo = new Dictionary<MethodInfo, MethodInfo>
        {
            { QueryableMethodProvider.FirstWithPredicateMethodInfo, QueryableMethodProvider.FirstWithoutPredicateMethodInfo },
            { QueryableMethodProvider.FirstOrDefaultWithPredicateMethodInfo, QueryableMethodProvider.FirstOrDefaultWithoutPredicateMethodInfo },
            { QueryableMethodProvider.SingleWithPredicateMethodInfo, QueryableMethodProvider.SingleWithoutPredicateMethodInfo },
            { QueryableMethodProvider.SingleOrDefaultWithPredicateMethodInfo, QueryableMethodProvider.SingleOrDefaultWithoutPredicateMethodInfo },
            { QueryableMethodProvider.LastWithPredicateMethodInfo, QueryableMethodProvider.LastWithoutPredicateMethodInfo },
            { QueryableMethodProvider.LastOrDefaultWithPredicateMethodInfo, QueryableMethodProvider.LastOrDefaultWithoutPredicateMethodInfo },
        };

        private Expression ProcessFirstSingleLastOrDefault(
            NavigationExpansionExpression source,
            MethodInfo genericMethod,
            LambdaExpression predicate,
            Type returnType)
        {
            if (predicate != null)
            {
                var predicateBody = ExpandNavigationsInLambdaExpression(source, predicate);

                source.UpdateSource(Expression.Call(
                    QueryableMethodProvider.WhereMethodInfo.MakeGenericMethod(source.SourceElementType),
                    source.Source,
                    Expression.Quote(GenerateLambda(predicateBody, source.CurrentParameter))));

                genericMethod = _predicateLessMethodInfo[genericMethod];
            }

            if (returnType == typeof(object))
            {
                source.ApplySelector(Expression.Convert(source.PendingSelector, returnType));
            }

            source.ConvertToSingleResult(genericMethod);

            return source;
        }

        private Expression ProcessOrderByThenBy(
            NavigationExpansionExpression source,
            MethodInfo genericMethod,
            LambdaExpression keySelector,
            bool thenBy)
        {
            var keySelectorBody = ExpandNavigationsInLambdaExpression(source, keySelector);

            if (thenBy)
            {
                source.AppendPendingOrdering(genericMethod, keySelectorBody);
            }
            else
            {
                source.AddPendingOrdering(genericMethod, keySelectorBody);
            }

            return source;
        }

        private Expression ProcessSelect(
            NavigationExpansionExpression source,
            LambdaExpression selector)
        {
            var selectorBody = ReplacingExpressionVisitor.Replace(
                selector.Parameters[0],
                source.PendingSelector,
                selector.Body);

            source.ApplySelector(selectorBody);

            return source;
        }

        private Expression ProcessContains(
            NavigationExpansionExpression source,
            Expression item)
        {
            source = (NavigationExpansionExpression)_pendingSelectorExpandingExpressionVisitor.Visit(source);
            var queryable = Reduce(source);

            return Expression.Call(
                QueryableMethodProvider.ContainsMethodInfo.MakeGenericMethod(queryable.Type.TryGetSequenceType()),
                queryable,
                item);
        }

        private Expression ProcessDistinctSkipTake(
            NavigationExpansionExpression source,
            MethodInfo genericMethod,
            Expression count)
        {
            source = (NavigationExpansionExpression)_pendingSelectorExpandingExpressionVisitor.Visit(source);
            var newStructure = SnapshotExpression(source.PendingSelector);
            var queryable = Reduce(source);

            var result = count == null
                ? Expression.Call(
                    genericMethod.MakeGenericMethod(queryable.Type.TryGetSequenceType()),
                    queryable)
                : Expression.Call(
                    genericMethod.MakeGenericMethod(queryable.Type.TryGetSequenceType()),
                    queryable,
                    count);

            var navigationTree = new NavigationTreeExpression(newStructure);
            var parameterName = GetParameterName("e");

            return new NavigationExpansionExpression(result, navigationTree, navigationTree, parameterName);
        }

        private Expression ProcessWhere(
            NavigationExpansionExpression source,
            LambdaExpression predicate)
        {
            var predicateBody = ExpandNavigationsInLambdaExpression(source, predicate);

            source.UpdateSource(Expression.Call(
                QueryableMethodProvider.WhereMethodInfo.MakeGenericMethod(source.SourceElementType),
                source.Source,
                Expression.Quote(GenerateLambda(predicateBody, source.CurrentParameter))));

            return source;
        }

        private LambdaExpression GenerateLambda(Expression body, ParameterExpression currentParameter)
        {
            return Expression.Lambda(Reduce(body), currentParameter);
        }

        private Expression Reduce(Expression source)
        {
            return _reducingExpressionVisitor.Visit(source);
        }

        private Expression ExpandNavigationsInExpression(NavigationExpansionExpression source, Expression expression)
        {
            expression = new ExpandingExpressionVisitor(this, source).Visit(expression);
            expression = Visit(expression);

            return expression;
        }

        private Expression ExpandNavigationsInLambdaExpression(
            NavigationExpansionExpression source,
            LambdaExpression lambdaExpression)
        {
            var lambdaBody = ReplacingExpressionVisitor.Replace(
                lambdaExpression.Parameters[0],
                source.PendingSelector,
                lambdaExpression.Body);

            return ExpandNavigationsInExpression(source, lambdaBody);
        }

        private static Expression CreateKeyAccessExpression(
                Expression target, IReadOnlyList<IProperty> properties)
                => properties.Count == 1
                    ? target.CreateEFPropertyExpression(properties[0])
                    : Expression.New(
                        AnonymousObject.AnonymousObjectCtor,
                        Expression.NewArrayInit(
                            typeof(object),
                            properties
                                .Select(p => Expression.Convert(target.CreateEFPropertyExpression(p), typeof(object)))
                                .ToArray()));
    }
}
