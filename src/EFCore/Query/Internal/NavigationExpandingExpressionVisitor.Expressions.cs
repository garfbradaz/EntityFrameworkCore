// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Microsoft.EntityFrameworkCore.Query.Internal
{
    public partial class NavigationExpandingExpressionVisitor
    {
        public class NavigationExpansionExpression : Expression
        {
            private readonly List<(MethodInfo OrderingMethod, Expression KeySelector)> _pendingOrderings
                = new List<(MethodInfo OrderingMethod, Expression KeySelector)>();
            private readonly string _parameterName;
            private NavigationTreeNode _currentTree;

            public NavigationExpansionExpression(
                Expression source, NavigationTreeNode currentTree, Expression pendingSelector, string parameterName)
            {
                Source = source;
                _parameterName = parameterName;
                CurrentTree = currentTree;
                PendingSelector = pendingSelector;
            }

            public Expression Source { get; private set; }
            public ParameterExpression CurrentParameter => CurrentTree.CurrentParameter;
            public NavigationTreeNode CurrentTree
            {
                get => _currentTree;
                private set
                {
                    _currentTree = value;
                    _currentTree.SetParameter(_parameterName);
                }
            }
            public Expression PendingSelector { get; private set; }
            public MethodInfo CardinalityReducingGenericMethodInfo { get; private set; }
            public Type SourceElementType => CurrentParameter.Type;
            public IReadOnlyList<(MethodInfo OrderingMethod, Expression KeySelector)> PendingOrderings => _pendingOrderings;

            public void UpdateSource(Expression source)
            {
                Source = source;
            }

            public void UpdateCurrentTree(NavigationTreeNode currentTree)
            {
                CurrentTree = currentTree;
            }

            public void ApplySelector(Expression selector)
            {
                PendingSelector = selector;
            }

            public void AddPendingOrdering(MethodInfo orderingMethod, Expression keySelector)
            {
                _pendingOrderings.Clear();
                _pendingOrderings.Add((orderingMethod, keySelector));
            }
            public void AppendPendingOrdering(MethodInfo orderingMethod, Expression keySelector)
            {
                _pendingOrderings.Add((orderingMethod, keySelector));
            }

            public void ClearPendingOrderings()
            {
                _pendingOrderings.Clear();
            }

            public void ConvertToSingleResult(MethodInfo genericMethod)
            {
                CardinalityReducingGenericMethodInfo = genericMethod;
            }

            protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

            public override ExpressionType NodeType => ExpressionType.Extension;
            public override Type Type => CardinalityReducingGenericMethodInfo == null
                ? typeof(IQueryable<>).MakeGenericType(PendingSelector.Type)
                : PendingSelector.Type;
        }

        public class NavigationTreeExpression : NavigationTreeNode
        {
            public NavigationTreeExpression(Expression value)
                : base(null, null)
            {
                Value = value;
            }
            public Expression Value { get; private set; }
            protected override Expression VisitChildren(ExpressionVisitor visitor)
            {
                Value = visitor.Visit(Value);

                return this;
            }
            public override Type Type => Value.Type;
        }

        public class EntityReference : Expression
        {
            public EntityReference(IEntityType entityType)
            {
                EntityType = entityType;
                IncludePaths = new IncludeTreeNode(entityType, this);
            }

            public IEntityType EntityType { get; }
            public IDictionary<INavigation, NavigationTreeExpression> NavigationMap { get; }
                = new Dictionary<INavigation, NavigationTreeExpression>();

            public IncludeTreeNode IncludePaths { get; private set; }
            public IncludeTreeNode LastIncludeTreeNode { get; private set; }

            protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

            public void SetIncludePaths(IncludeTreeNode includePaths)
            {
                IncludePaths = includePaths;
                includePaths.SetEntityReference(this);
            }

            public EntityReference Clone()
            {
                var result = new EntityReference(EntityType);
                result.IsOptional = IsOptional;
                result.IncludePaths = IncludePaths.Clone(result);

                return result;
            }

            public void SetLastInclude(IncludeTreeNode lastIncludeTree)
            {
                LastIncludeTreeNode = lastIncludeTree;
            }

            public void MarkAsOptional()
            {
                IsOptional = true;
            }

            public bool IsOptional { get; private set; }

            public override ExpressionType NodeType => ExpressionType.Extension;
            public override Type Type => EntityType.ClrType;
        }

        public class NavigationTreeNode : Expression
        {
            private NavigationTreeNode _parent;

            public NavigationTreeNode(NavigationTreeNode left, NavigationTreeNode right)
            {
                Left = left;
                Right = right;
                if (left != null)
                {
                    Left.Parent = this;
                    Right.Parent = this;
                }
            }

            public NavigationTreeNode Parent
            {
                get => _parent;
                private set
                {
                    _parent = value;
                    CurrentParameter = null;
                }
            }
            public NavigationTreeNode Left { get; }
            public NavigationTreeNode Right { get; }
            public ParameterExpression CurrentParameter { get; private set; }

            protected override Expression VisitChildren(ExpressionVisitor visitor) => throw new InvalidOperationException();

            public void SetParameter(string parameterName)
            {
                CurrentParameter = Parameter(Type, parameterName);
            }

            public override ExpressionType NodeType => ExpressionType.Extension;
            public override Type Type => TransparentIdentifierType.Create(Left.Type, Right.Type);
            public virtual Expression GetExpression()
            {
                if (Parent == null)
                {
                    return CurrentParameter;
                }

                var parentExperssion = Parent.GetExpression();
                return Parent.Left == this
                    ? MakeMemberAccess(parentExperssion, parentExperssion.Type.GetTypeInfo().GetMember("Outer")[0])
                    : MakeMemberAccess(parentExperssion, parentExperssion.Type.GetTypeInfo().GetMember("Inner")[0]);
            }
        }

        public class IncludeTreeNode : Dictionary<INavigation, IncludeTreeNode>
        {
            private EntityReference _entityReference;
            public IEntityType EntityType { get; private set; }

            public IncludeTreeNode(IEntityType entityType, EntityReference entityReference)
            {
                EntityType = entityType;
                _entityReference = entityReference;
            }

            public IncludeTreeNode AddNavigation(INavigation navigation)
            {
                if (TryGetValue(navigation, out var existingValue))
                {
                    return existingValue;
                }

                if (_entityReference != null
                    && _entityReference.NavigationMap.TryGetValue(navigation, out var navigationTree))
                {
                    var entityReference = (EntityReference)navigationTree.Value;
                    this[navigation] = entityReference.IncludePaths;
                }
                else
                {
                    this[navigation] = new IncludeTreeNode(navigation.GetTargetType(), null);
                }

                return this[navigation];
            }

            public IncludeTreeNode Clone(EntityReference entityReference)
            {
                var result = new IncludeTreeNode(EntityType, entityReference);
                foreach (var kvp in this)
                {
                    result[kvp.Key] = kvp.Value.Clone(kvp.Value._entityReference);
                }

                return result;
            }

            public override bool Equals(object obj)
            => obj != null
            && (ReferenceEquals(this, obj)
                || obj is IncludeTreeNode includeTreeNode
                    && Equals(includeTreeNode));

            private bool Equals(IncludeTreeNode includeTreeNode)
            {
                if (Count != includeTreeNode.Count)
                {
                    return false;
                }

                foreach (var kvp in this)
                {
                    if (!includeTreeNode.TryGetValue(kvp.Key, out var otherIncludeTreeNode)
                        || !kvp.Value.Equals(otherIncludeTreeNode))
                    {
                        return false;
                    }
                }

                return true;
            }

            public override int GetHashCode() => HashCode.Combine(base.GetHashCode(), EntityType);

            public void SetEntityReference(EntityReference entityReference)
            {
                _entityReference = entityReference;
                EntityType = entityReference.EntityType;
            }
        }
    }
}
