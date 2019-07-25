// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace Microsoft.EntityFrameworkCore.Query.Internal
{
    public partial class NavigationExpandingExpressionVisitor
    {
        private class ExpandingExpressionVisitor : ExpressionVisitor
        {
            private readonly NavigationExpandingExpressionVisitor _navigationExpandingExpressionVisitor;
            private readonly NavigationExpansionExpression _source;

            public ExpandingExpressionVisitor(
                NavigationExpandingExpressionVisitor navigationExpandingExpressionVisitor,
                NavigationExpansionExpression source)
            {
                _navigationExpandingExpressionVisitor = navigationExpandingExpressionVisitor;
                _source = source;
            }

            protected override Expression VisitExtension(Expression expression)
            {
                switch (expression)
                {
                    case NavigationExpansionExpression _:
                    case NavigationTreeExpression _:
                        return expression;

                    default:
                        return base.VisitExtension(expression);
                }
            }

            private EntityReference UnwrapEntityReference(Expression expression)
            {
                switch (expression)
                {
                    case EntityReference entityReference:
                        return entityReference;

                    case NavigationTreeExpression navigationTreeExpression:
                        return UnwrapEntityReference(navigationTreeExpression.Value);

                    case NavigationExpansionExpression navigationExpansionExpression
                        when navigationExpansionExpression.CardinalityReducingGenericMethodInfo != null:
                        return UnwrapEntityReference(navigationExpansionExpression.PendingSelector);

                    case MemberExpression memberExpression:
                        {
                            Type convertedType = null;
                            var innerExpression = memberExpression.Expression;
                            if (innerExpression is UnaryExpression unaryExpression
                                && unaryExpression.NodeType == ExpressionType.Convert)
                            {
                                innerExpression = unaryExpression.Operand;
                                if (unaryExpression.Type != typeof(object))
                                {
                                    convertedType = unaryExpression.Type;
                                }
                            }

                            var entityReference = UnwrapEntityReference(innerExpression);
                            if (entityReference != null)
                            {
                                var entityType = entityReference.EntityType;
                                if (convertedType != null)
                                {
                                    entityType = entityType.GetTypesInHierarchy().FirstOrDefault(et => et.ClrType == convertedType);
                                    if (entityType == null)
                                    {
                                        return null;
                                    }
                                }

                                var navigation = entityType.FindNavigation(memberExpression.Member);
                                if (navigation != null && navigation.ForeignKey.IsOwnership)
                                {
                                    var result = new EntityReference(navigation.GetTargetType());
                                    if (entityReference.IncludePaths.TryGetValue(navigation, out var includeTreeNode))
                                    {
                                        result.SetIncludePaths(includeTreeNode);
                                    }

                                    return result;
                                }
                            }
                        }

                        return null;


                    default:
                        return null;
                }
            }

            protected override Expression VisitMember(MemberExpression memberExpression)
            {
                var innerExpression = Visit(memberExpression.Expression);
                var expansion = TryExpandNavigation(innerExpression, MemberIdentity.Create(memberExpression.Member));
                if (expansion != null)
                {
                    return expansion;
                }

                var updatedExpression = memberExpression.Update(innerExpression);

                return innerExpression is NavigationTreeExpression
                    && innerExpression != memberExpression.Expression
                    // Inner was converted to NavigationTreeExpression which indicate it was expanded
                    // So visit updatedExpression to expand the navigation chain if any
                    ? Visit(updatedExpression)
                    : updatedExpression;
            }

            protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
            {
                if (methodCallExpression.TryGetEFPropertyArguments(out var source, out var navigationName))
                {
                    source = Visit(source);
                    var expansion = TryExpandNavigation(source, MemberIdentity.Create(navigationName));
                    if (expansion != null)
                    {
                        return expansion;
                    }
                }

                return base.VisitMethodCall(methodCallExpression);
            }

            private Expression TryExpandNavigation(Expression root, MemberIdentity memberIdentity)
            {
                Type convertedType = null;
                var innerExpression = root;
                if (innerExpression is UnaryExpression unaryExpression
                    && unaryExpression.NodeType == ExpressionType.Convert)
                {
                    innerExpression = unaryExpression.Operand;
                    if (unaryExpression.Type != typeof(object))
                    {
                        convertedType = unaryExpression.Type;
                    }
                }

                if (UnwrapEntityReference(innerExpression) is EntityReference entityReference)
                {
                    var entityType = entityReference.EntityType;
                    if (convertedType != null)
                    {
                        entityType = entityType.GetTypesInHierarchy().FirstOrDefault(et => et.ClrType == convertedType);
                        if (entityType == null)
                        {
                            return null;
                        }
                    }

                    var navigation = memberIdentity.MemberInfo != null
                        ? entityType.FindNavigation(memberIdentity.MemberInfo)
                        : entityType.FindNavigation(memberIdentity.Name);
                    if (navigation != null && !navigation.ForeignKey.IsOwnership)
                    {
                        if (entityReference.NavigationMap.TryGetValue(navigation, out var expansion))
                        {
                            return expansion;
                        }

                        var innerQueryableType = navigation.GetTargetType().ClrType;
                        var innerQueryable = NullAsyncQueryProvider.Instance.CreateEntityQueryableExpression(innerQueryableType);
                        var innerSource = (NavigationExpansionExpression)_navigationExpandingExpressionVisitor.Visit(innerQueryable);
                        if (entityReference.IncludePaths.ContainsKey(navigation))
                        {
                            var innerIncludeTreeNode = entityReference.IncludePaths[navigation];
                            var innerEntityReference = (EntityReference)((NavigationTreeExpression)innerSource.PendingSelector).Value;
                            innerEntityReference.SetIncludePaths(innerIncludeTreeNode);
                        }

                        var innerParameter = Expression.Parameter(innerSource.SourceElementType, "i");
                        var outerKey = CreateKeyAccessExpression(root,
                            navigation.IsDependentToPrincipal()
                                ? navigation.ForeignKey.Properties
                                : navigation.ForeignKey.PrincipalKey.Properties);

                        var innerKey = CreateKeyAccessExpression(innerParameter,
                            navigation.IsDependentToPrincipal()
                                ? navigation.ForeignKey.PrincipalKey.Properties
                                : navigation.ForeignKey.Properties);

                        if (outerKey.Type != innerKey.Type)
                        {
                            if (!outerKey.Type.IsNullableType())
                            {
                                outerKey = Expression.Convert(outerKey, outerKey.Type.MakeNullable());
                            }

                            if (!innerKey.Type.IsNullableType())
                            {
                                innerKey = Expression.Convert(innerKey, innerKey.Type.MakeNullable());
                            }
                        }

                        if (navigation.IsCollection())
                        {
                            var correlationPredicate = _navigationExpandingExpressionVisitor.ExpandNavigationsInLambdaExpression(
                                innerSource,
                                Expression.Lambda(Expression.Equal(outerKey, innerKey), innerParameter));

                            var subquery = Expression.Call(
                                    QueryableMethodProvider.WhereMethodInfo.MakeGenericMethod(innerSource.SourceElementType),
                                    innerSource,
                                    Expression.Quote(
                                        Expression.Lambda(correlationPredicate, innerSource.CurrentParameter)));

                            return new MaterializeCollectionNavigationExpression(subquery, navigation);
                        }
                        else
                        {
                            var outerKeySelector = _navigationExpandingExpressionVisitor.GenerateLambda(
                                outerKey, _source.CurrentParameter);
                            var innerKeySelector = _navigationExpandingExpressionVisitor.GenerateLambda(
                                _navigationExpandingExpressionVisitor.ExpandNavigationsInLambdaExpression(
                                    innerSource,
                                    Expression.Lambda(innerKey, innerParameter)),
                                innerSource.CurrentParameter);

                            var resultSelectorOuterParameter = Expression.Parameter(_source.SourceElementType, "o");
                            var resultSelectorInnerParameter = Expression.Parameter(innerQueryableType, "i");
                            var resultType = TransparentIdentifierType.Create(_source.SourceElementType, innerQueryableType);

                            var transparentIdentifierOuterMemberInfo = resultType.GetTypeInfo().GetDeclaredField("Outer");
                            var transparentIdentifierInnerMemberInfo = resultType.GetTypeInfo().GetDeclaredField("Inner");

                            var resultSelector = Expression.Lambda(
                                Expression.New(
                                    resultType.GetTypeInfo().GetConstructors().Single(),
                                    new[] { resultSelectorOuterParameter, resultSelectorInnerParameter },
                                    new[] { transparentIdentifierOuterMemberInfo, transparentIdentifierInnerMemberInfo }),
                                resultSelectorOuterParameter,
                                resultSelectorInnerParameter);

                            var innerJoin = !entityReference.IsOptional && convertedType == null
                                && navigation.IsDependentToPrincipal() && navigation.ForeignKey.IsRequired;

                            if (!innerJoin)
                            {
                                var innerEntityReference = (EntityReference)((NavigationTreeExpression)innerSource.PendingSelector).Value;
                                innerEntityReference.MarkAsOptional();
                            }

                            _source.UpdateSource(Expression.Call(
                                (innerJoin
                                    ? QueryableMethodProvider.JoinMethodInfo
                                    : QueryableExtensions.LeftJoinMethodInfo).MakeGenericMethod(
                                        _source.SourceElementType,
                                        innerQueryableType,
                                        outerKeySelector.ReturnType,
                                        resultSelector.ReturnType),
                                _source.Source,
                                innerQueryable,
                                outerKeySelector,
                                innerKeySelector,
                                resultSelector));

                            entityReference.NavigationMap[navigation] = (NavigationTreeExpression)innerSource.PendingSelector;

                            _source.UpdateCurrentTree(new NavigationTreeNode(_source.CurrentTree, innerSource.CurrentTree));

                            return innerSource.PendingSelector;
                        }
                    }
                }

                return null;
            }
        }

        private class IncludeApplyingExpressionVisitor : ExpressionVisitor
        {
            private readonly NavigationExpandingExpressionVisitor _visitor;

            public IncludeApplyingExpressionVisitor(NavigationExpandingExpressionVisitor visitor)
            {
                _visitor = visitor;
            }

            public override Expression Visit(Expression expression)
            {
                if (expression is NavigationExpansionExpression navigationExpansionExpression)
                {
                    var innerVisitor = new IncludeExpandingExpressionVisitor(_visitor, navigationExpansionExpression);
                    var pendingSelector = innerVisitor.Visit(navigationExpansionExpression.PendingSelector);
                    pendingSelector = _visitor.Visit(pendingSelector);
                    pendingSelector = Visit(pendingSelector);

                    navigationExpansionExpression.ApplySelector(pendingSelector);
                }

                return base.Visit(expression);
            }
        }

        private class IncludeExpandingExpressionVisitor : ExpressionVisitor
        {
            private readonly NavigationExpandingExpressionVisitor _navigationExpandingExpressionVisitor;
            private readonly NavigationExpansionExpression _source;

            public IncludeExpandingExpressionVisitor(
                NavigationExpandingExpressionVisitor navigationExpandingExpressionVisitor,
                NavigationExpansionExpression source)
            {
                _navigationExpandingExpressionVisitor = navigationExpandingExpressionVisitor;
                _source = source;
            }

            public override Expression Visit(Expression expression)
            {
                switch (expression)
                {
                    case NavigationTreeExpression navigationTreeExpression:
                        if (navigationTreeExpression.Value is EntityReference entityReference)
                        {
                            return ExpandIncludes(navigationTreeExpression, entityReference);
                        }

                        if (navigationTreeExpression.Value is NewExpression newExpression)
                        {
                            if (ReconstructAnonymousType(navigationTreeExpression, newExpression, out var replacement))
                            {
                                return replacement;
                            }
                        }
                        break;
                }

                return base.Visit(expression);
            }

            private Expression UnwrapEntityReference(Expression expression)
            {
                switch (expression)
                {
                    case NavigationTreeExpression navigationTreeExpression:
                        return navigationTreeExpression.Value;

                    default:
                        return expression;
                }
            }

            protected override Expression VisitMember(MemberExpression memberExpression)
            {
                if (UnwrapEntityReference(memberExpression.Expression) is EntityReference)
                {
                    // If it matches then it is property access. All navigation accesses are already expanded.
                    return memberExpression;
                }

                return base.VisitMember(memberExpression);
            }

            protected override Expression VisitNew(NewExpression newExpression)
            {
                var arguments = new Expression[newExpression.Arguments.Count];
                for (var i = 0; i < newExpression.Arguments.Count; i++)
                {
                    var argument = newExpression.Arguments[i];
                    arguments[i] = argument is EntityReference entityReference
                        ? ExpandIncludes(argument, entityReference)
                        : Visit(argument);
                }

                return newExpression.Update(arguments);
            }

            private bool ReconstructAnonymousType(Expression currentRoot, NewExpression newExpression, out Expression replacement)
            {
                replacement = null;
                var changed = false;
                if (newExpression.Arguments.Count > 0
                    && newExpression.Members == null)
                {
                    return changed;
                }

                var arguments = new Expression[newExpression.Arguments.Count];
                for (var i = 0; i < newExpression.Arguments.Count; i++)
                {
                    var argument = newExpression.Arguments[i];
                    var newRoot = Expression.MakeMemberAccess(currentRoot, newExpression.Members[i]);
                    if (argument is EntityReference entityReference)
                    {
                        changed = true;
                        arguments[i] = ExpandIncludes(newRoot, entityReference);
                    }
                    else if (argument is NewExpression innerNewExpression)
                    {
                        if (ReconstructAnonymousType(newRoot, innerNewExpression, out var innerReplacement))
                        {
                            changed = true;
                            arguments[i] = innerReplacement;
                        }
                        else
                        {
                            arguments[i] = newRoot;
                        }
                    }
                    else
                    {
                        arguments[i] = newRoot;
                    }
                }

                if (changed)
                {
                    replacement = newExpression.Update(arguments);
                }

                return changed;
            }

            protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
            {
                if (methodCallExpression.TryGetEFPropertyArguments(out var _, out var __))
                {
                    // If it matches then it is property access. All navigation accesses are already expanded.
                    return methodCallExpression;
                }

                return base.VisitMethodCall(methodCallExpression);
            }


            private Expression ExpandIncludes(Expression root, EntityReference entityReference)
            {
                var result = root;
                var convertedRoot = root;
                foreach (var kvp in entityReference.IncludePaths)
                {
                    var navigation = kvp.Key;
                    var converted = false;
                    if (entityReference.EntityType != navigation.DeclaringEntityType
                        && entityReference.EntityType.IsAssignableFrom(navigation.DeclaringEntityType))
                    {
                        converted = true;
                        convertedRoot = Expression.Convert(root, navigation.DeclaringEntityType.ClrType);
                    }
                    Expression included;
                    if (navigation.ForeignKey.IsOwnership)
                    {
                        included = Expression.Call(
                            EF.PropertyMethod.MakeGenericMethod(navigation.GetTargetType().ClrType),
                            convertedRoot,
                            Expression.Constant(navigation.Name));
                    }
                    else
                    {
                        included = ExpandNavigation(convertedRoot, entityReference, navigation, converted);
                        // Collection will expand it's include when reducing the navigationExpansionExpression
                        if (!navigation.IsCollection())
                        {
                            var navigationTreeExpression = (NavigationTreeExpression)included;
                            var innerEntityReference = (EntityReference)navigationTreeExpression.Value;
                            included = ExpandIncludes(navigationTreeExpression, innerEntityReference);
                        }
                    }

                    result = new IncludeExpression(result, included, navigation);
                }

                return result;
            }

            private Expression ExpandNavigation(Expression root, EntityReference entityReference, INavigation navigation, bool convertedRoot)
            {
                if (entityReference.NavigationMap.TryGetValue(navigation, out var expansion))
                {
                    return expansion;
                }

                var innerQueryableType = navigation.GetTargetType().ClrType;
                var innerQueryable = NullAsyncQueryProvider.Instance.CreateEntityQueryableExpression(innerQueryableType);
                var innerSource = (NavigationExpansionExpression)_navigationExpandingExpressionVisitor.Visit(innerQueryable);
                var innerIncludeTreeNode = entityReference.IncludePaths[navigation];
                var innerEntityReference = (EntityReference)((NavigationTreeExpression)innerSource.PendingSelector).Value;
                innerEntityReference.SetIncludePaths(innerIncludeTreeNode);

                var innerParameter = Expression.Parameter(innerSource.SourceElementType, "i");
                var outerKey = CreateKeyAccessExpression(root,
                    navigation.IsDependentToPrincipal()
                        ? navigation.ForeignKey.Properties
                        : navigation.ForeignKey.PrincipalKey.Properties);

                var innerKey = CreateKeyAccessExpression(innerParameter,
                    navigation.IsDependentToPrincipal()
                        ? navigation.ForeignKey.PrincipalKey.Properties
                        : navigation.ForeignKey.Properties);

                if (outerKey.Type != innerKey.Type)
                {
                    if (!outerKey.Type.IsNullableType())
                    {
                        outerKey = Expression.Convert(outerKey, outerKey.Type.MakeNullable());
                    }

                    if (!innerKey.Type.IsNullableType())
                    {
                        innerKey = Expression.Convert(innerKey, innerKey.Type.MakeNullable());
                    }
                }

                if (navigation.IsCollection())
                {
                    var correlationPredicate = _navigationExpandingExpressionVisitor.ExpandNavigationsInLambdaExpression(
                        innerSource,
                        Expression.Lambda(Expression.Equal(outerKey, innerKey), innerParameter));

                    var subquery = Expression.Call(
                            QueryableMethodProvider.WhereMethodInfo.MakeGenericMethod(innerSource.SourceElementType),
                            innerSource,
                            Expression.Quote(
                                Expression.Lambda(correlationPredicate, innerSource.CurrentParameter)));

                    return new MaterializeCollectionNavigationExpression(subquery, navigation);
                }
                else
                {
                    var outerKeySelector = _navigationExpandingExpressionVisitor.GenerateLambda(
                        outerKey, _source.CurrentParameter);
                    var innerKeySelector = _navigationExpandingExpressionVisitor.GenerateLambda(
                        _navigationExpandingExpressionVisitor.ExpandNavigationsInLambdaExpression(
                            innerSource,
                            Expression.Lambda(innerKey, innerParameter)),
                        innerSource.CurrentParameter);

                    var resultSelectorOuterParameter = Expression.Parameter(_source.SourceElementType, "o");
                    var resultSelectorInnerParameter = Expression.Parameter(innerQueryableType, "i");
                    var resultType = TransparentIdentifierType.Create(_source.SourceElementType, innerQueryableType);

                    var transparentIdentifierOuterMemberInfo = resultType.GetTypeInfo().GetDeclaredField("Outer");
                    var transparentIdentifierInnerMemberInfo = resultType.GetTypeInfo().GetDeclaredField("Inner");

                    var resultSelector = Expression.Lambda(
                        Expression.New(
                            resultType.GetTypeInfo().GetConstructors().Single(),
                            new[] { resultSelectorOuterParameter, resultSelectorInnerParameter },
                            new[] { transparentIdentifierOuterMemberInfo, transparentIdentifierInnerMemberInfo }),
                        resultSelectorOuterParameter,
                        resultSelectorInnerParameter);

                    var innerJoin = !entityReference.IsOptional && !convertedRoot
                                && navigation.IsDependentToPrincipal() && navigation.ForeignKey.IsRequired;

                    if (!innerJoin)
                    {
                        innerEntityReference.MarkAsOptional();
                    }

                    _source.UpdateSource(Expression.Call(
                        (innerJoin
                            ? QueryableMethodProvider.JoinMethodInfo
                            : QueryableExtensions.LeftJoinMethodInfo).MakeGenericMethod(
                                _source.SourceElementType,
                                innerQueryableType,
                                outerKeySelector.ReturnType,
                                resultSelector.ReturnType),
                        _source.Source,
                        innerQueryable,
                        outerKeySelector,
                        innerKeySelector,
                        resultSelector));

                    entityReference.NavigationMap[navigation] = (NavigationTreeExpression)innerSource.PendingSelector;

                    _source.UpdateCurrentTree(new NavigationTreeNode(_source.CurrentTree, innerSource.CurrentTree));

                    return innerSource.PendingSelector;
                }
            }
        }

        private class PendingSelectorExpandingExpressionVisitor : ExpressionVisitor
        {
            private readonly NavigationExpandingExpressionVisitor _visitor;

            public PendingSelectorExpandingExpressionVisitor(NavigationExpandingExpressionVisitor visitor)
            {
                _visitor = visitor;
            }

            public override Expression Visit(Expression expression)
            {
                if (expression is NavigationExpansionExpression navigationExpansionExpression)
                {
                    _visitor.ApplyPendingOrderings(navigationExpansionExpression);

                    var pendingSelector = _visitor.ExpandNavigationsInExpression(
                        navigationExpansionExpression, navigationExpansionExpression.PendingSelector);

                    pendingSelector = Visit(pendingSelector);

                    navigationExpansionExpression.ApplySelector(pendingSelector);

                    return navigationExpansionExpression;
                }

                return base.Visit(expression);
            }
        }

        private class ReducingExpressionVisitor : ExpressionVisitor
        {
            public override Expression Visit(Expression expression)
            {
                switch (expression)
                {
                    case NavigationTreeExpression navigationTreeExpression:
                        return navigationTreeExpression.GetExpression();

                    case NavigationExpansionExpression navigationExpansionExpression:
                        {
                            var pendingSelector = Visit(navigationExpansionExpression.PendingSelector);
                            Expression result;
                            if (pendingSelector == navigationExpansionExpression.CurrentParameter)
                            {
                                // identity projection
                                result = navigationExpansionExpression.Source;
                            }
                            else
                            {
                                var selectorLambda = Expression.Lambda(pendingSelector, navigationExpansionExpression.CurrentParameter);

                                result = Expression.Call(
                                    QueryableMethodProvider.SelectMethodInfo.MakeGenericMethod(
                                        navigationExpansionExpression.SourceElementType,
                                        selectorLambda.ReturnType),
                                    navigationExpansionExpression.Source,
                                    Expression.Quote(selectorLambda));
                            }

                            if (navigationExpansionExpression.CardinalityReducingGenericMethodInfo != null)
                            {
                                result = Expression.Call(
                                    navigationExpansionExpression.CardinalityReducingGenericMethodInfo.MakeGenericMethod(
                                        result.Type.TryGetSequenceType()),
                                    result);
                            }

                            return result;
                        }

                    default:
                        return base.Visit(expression);
                }
            }
        }
    }
}
