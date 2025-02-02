// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Diagnostics.Internal;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Internal;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Microsoft.EntityFrameworkCore.Metadata.Conventions
{
    public class NonNullableNavigationConventionTest
    {
        [ConditionalFact]
        public void Non_nullability_does_not_override_configuration_from_explicit_source()
        {
            var dependentEntityTypeBuilder = CreateInternalEntityTypeBuilder<Post>();
            var principalEntityTypeBuilder = dependentEntityTypeBuilder.ModelBuilder.Entity(typeof(Blog), ConfigurationSource.Convention);

            var relationshipBuilder = dependentEntityTypeBuilder.HasRelationship(
                principalEntityTypeBuilder.Metadata,
                nameof(Post.Blog),
                nameof(Blog.Posts),
                ConfigurationSource.Convention);

            var navigation = dependentEntityTypeBuilder.Metadata.FindNavigation(nameof(Post.Blog));

            relationshipBuilder.IsRequired(false, ConfigurationSource.Explicit);

            Assert.False(relationshipBuilder.Metadata.IsRequired);

            RunConvention(relationshipBuilder, navigation);

            Assert.False(relationshipBuilder.Metadata.IsRequired);
            Assert.Contains(principalEntityTypeBuilder.Metadata.GetNavigations(), nav => nav.Name == nameof(Blog.Posts));
            Assert.Contains(dependentEntityTypeBuilder.Metadata.GetNavigations(), nav => nav.Name == nameof(Post.Blog));
        }

        [ConditionalFact]
        public void Non_nullability_does_not_override_configuration_from_data_annotation()
        {
            var dependentEntityTypeBuilder = CreateInternalEntityTypeBuilder<Post>();
            var principalEntityTypeBuilder = dependentEntityTypeBuilder.ModelBuilder.Entity(typeof(Blog), ConfigurationSource.Convention);

            var relationshipBuilder = dependentEntityTypeBuilder.HasRelationship(
                principalEntityTypeBuilder.Metadata,
                nameof(Post.Blog),
                nameof(Blog.Posts),
                ConfigurationSource.Convention);

            var navigation = dependentEntityTypeBuilder.Metadata.FindNavigation(nameof(Post.Blog));

            relationshipBuilder.IsRequired(false, ConfigurationSource.DataAnnotation);

            Assert.False(relationshipBuilder.Metadata.IsRequired);

            RunConvention(relationshipBuilder, navigation);

            Assert.False(relationshipBuilder.Metadata.IsRequired);
            Assert.Contains(principalEntityTypeBuilder.Metadata.GetNavigations(), nav => nav.Name == nameof(Blog.Posts));
            Assert.Contains(dependentEntityTypeBuilder.Metadata.GetNavigations(), nav => nav.Name == nameof(Post.Blog));
        }

        [ConditionalFact]
        public void Non_nullability_does_not_set_is_required_for_collection_navigation()
        {
            var dependentEntityTypeBuilder = CreateInternalEntityTypeBuilder<Dependent>();
            var principalEntityTypeBuilder =
                dependentEntityTypeBuilder.ModelBuilder.Entity(typeof(Principal), ConfigurationSource.Convention);

            var relationshipBuilder = principalEntityTypeBuilder.HasRelationship(
                dependentEntityTypeBuilder.Metadata,
                nameof(Principal.Dependents),
                nameof(Dependent.Principal),
                ConfigurationSource.Convention);

            var navigation = principalEntityTypeBuilder.Metadata.FindNavigation(nameof(Principal.Dependents));

            Assert.False(relationshipBuilder.Metadata.IsRequired);

            RunConvention(relationshipBuilder, navigation);

            Assert.False(relationshipBuilder.Metadata.IsRequired);

            Assert.Empty(ListLoggerFactory.Log);
        }

        [ConditionalFact]
        public void Non_nullability_does_not_set_is_required_for_navigation_to_dependent()
        {
            var dependentEntityTypeBuilder = CreateInternalEntityTypeBuilder<Dependent>();
            var principalEntityTypeBuilder =
                dependentEntityTypeBuilder.ModelBuilder.Entity(typeof(Principal), ConfigurationSource.Convention);

            var relationshipBuilder = dependentEntityTypeBuilder.HasRelationship(
                    principalEntityTypeBuilder.Metadata,
                    nameof(Dependent.Principal),
                    nameof(Principal.Dependent),
                    ConfigurationSource.Convention)
                .HasEntityTypes
                    (principalEntityTypeBuilder.Metadata, dependentEntityTypeBuilder.Metadata, ConfigurationSource.Explicit);

            var navigation = principalEntityTypeBuilder.Metadata.FindNavigation(nameof(Principal.Dependent));

            Assert.False(relationshipBuilder.Metadata.IsRequired);

            RunConvention(relationshipBuilder, navigation);

            Assert.False(relationshipBuilder.Metadata.IsRequired);
        }

        [ConditionalFact]
        public void Non_nullability_inverts_when_navigation_to_dependent()
        {
            var dependentEntityTypeBuilder = CreateInternalEntityTypeBuilder<Dependent>();
            var principalEntityTypeBuilder =
                dependentEntityTypeBuilder.ModelBuilder.Entity(typeof(Principal), ConfigurationSource.Convention);

            var relationshipBuilder = dependentEntityTypeBuilder.HasRelationship(
                principalEntityTypeBuilder.Metadata,
                nameof(Dependent.Principal),
                nameof(Principal.Dependent),
                ConfigurationSource.Convention);

            Assert.Equal(nameof(Dependent), relationshipBuilder.Metadata.DeclaringEntityType.DisplayName());
            Assert.False(relationshipBuilder.Metadata.IsRequired);

            var navigation = principalEntityTypeBuilder.Metadata.FindNavigation(nameof(Principal.Dependent));

            navigation = RunConvention(relationshipBuilder, navigation);

            Assert.Equal(nameof(Principal), navigation.ForeignKey.DeclaringEntityType.DisplayName());
            Assert.True(navigation.ForeignKey.IsRequired);


            var logEntry = ListLoggerFactory.Log.Single();
            Assert.Equal(LogLevel.Debug, logEntry.Level);
            Assert.Equal(
                CoreResources.LogNonNullableOnDependent(new TestLogger<TestLoggingDefinitions>()).GenerateMessage(
                    nameof(Principal.Dependent), nameof(Principal)), logEntry.Message);
        }

        [ConditionalFact]
        public void Non_nullability_sets_is_required_with_conventional_builder()
        {
            var modelBuilder = CreateModelBuilder();
            var model = (Model)modelBuilder.Model;
            modelBuilder.Entity<BlogDetails>();

            Assert.True(
                model.FindEntityType(typeof(BlogDetails)).GetForeignKeys().Single(fk => fk.PrincipalEntityType?.ClrType == typeof(Blog))
                    .IsRequired);
        }

        [ConditionalFact]
        public void Non_nullability_can_be_specified_on_both_navigations()
        {
            var modelBuilder = CreateModelBuilder();
            var model = (Model)modelBuilder.Model;
            modelBuilder.Entity<BlogDetails>().HasOne(b => b.Blog).WithOne(b => b.BlogDetails);

            var logEntry = ListLoggerFactory.Log.Single();

            Assert.Equal(LogLevel.Debug, logEntry.Level);
            Assert.Equal(
                CoreResources.LogNonNullableReferenceOnBothNavigations(new TestLogger<TestLoggingDefinitions>()).GenerateMessage(
                    nameof(Blog), nameof(Blog.BlogDetails), nameof(BlogDetails), nameof(BlogDetails.Blog)), logEntry.Message);
        }

        private Navigation RunConvention(InternalRelationshipBuilder relationshipBuilder, Navigation navigation)
        {
            var context = new ConventionContext<IConventionNavigation>(
                relationshipBuilder.Metadata.DeclaringEntityType.Model.ConventionDispatcher);
            CreateNotNullNavigationConvention().ProcessNavigationAdded(relationshipBuilder, navigation, context);
            return context.ShouldStopProcessing() ? (Navigation)context.Result : navigation;
        }

        private NonNullableNavigationConvention CreateNotNullNavigationConvention()
            => new NonNullableNavigationConvention(CreateDependencies());

        public ListLoggerFactory ListLoggerFactory { get; }
            = new ListLoggerFactory(l => l == DbLoggerCategory.Model.Name);

        private InternalEntityTypeBuilder CreateInternalEntityTypeBuilder<T>()
        {
            var conventionSet = new ConventionSet();
            conventionSet.EntityTypeAddedConventions.Add(
                new PropertyDiscoveryConvention(CreateDependencies()));

            conventionSet.EntityTypeAddedConventions.Add(new KeyDiscoveryConvention(CreateDependencies()));

            var modelBuilder = new InternalModelBuilder(new Model(conventionSet));

            return modelBuilder.Entity(typeof(T), ConfigurationSource.Explicit);
        }

        private ModelBuilder CreateModelBuilder()
        {
            var dependencies = CreateDependencies();

            return new ModelBuilder(
                new RuntimeConventionSetBuilder(
                        new ProviderConventionSetBuilder(dependencies),
                        Enumerable.Empty<IConventionSetPlugin>())
                    .CreateConventionSet());
        }

        private ProviderConventionSetBuilderDependencies CreateDependencies()
            => InMemoryTestHelpers.Instance.CreateContextServices().GetRequiredService<ProviderConventionSetBuilderDependencies>()
                .With(CreateLogger());

        private DiagnosticsLogger<DbLoggerCategory.Model> CreateLogger()
        {
            ListLoggerFactory.Clear();
            var options = new LoggingOptions();
            options.Initialize(new DbContextOptionsBuilder().EnableSensitiveDataLogging(false).Options);
            var modelLogger = new DiagnosticsLogger<DbLoggerCategory.Model>(
                ListLoggerFactory,
                options,
                new DiagnosticListener("Fake"),
                new TestLoggingDefinitions());
            return modelLogger;
        }

#nullable enable
#pragma warning disable CS8618

        private class Blog
        {
            public int Id { get; set; }

            [NotMapped]
            public BlogDetails BlogDetails { get; set; }

            public ICollection<Post> Posts { get; set; }
        }

        private class BlogDetails
        {
            public int Id { get; set; }

            public Blog Blog { get; set; }

            private Post Post { get; set; }
        }

        private class Post
        {
            public int Id { get; set; }

            public Blog Blog { get; set; }
        }

        private class Principal
        {
            public static readonly PropertyInfo DependentIdProperty = typeof(Principal).GetProperty("DependentId")!;

            public int Id { get; set; }

            public int DependentId { get; set; }

            [ForeignKey("PrincipalFk")]
            public ICollection<Dependent> Dependents { get; set; }

            public Dependent Dependent { get; set; }
        }

        private class Dependent
        {
            public static readonly PropertyInfo PrincipalIdProperty = typeof(Dependent).GetProperty("PrincipalId")!;

            public int Id { get; set; }

            public int PrincipalId { get; set; }

            public int PrincipalFk { get; set; }

            [ForeignKey("AnotherPrincipal")]
            public int PrincipalAnotherFk { get; set; }

            [ForeignKey("PrincipalFk")]
            [InverseProperty("Dependent")]
            public Principal? Principal { get; set; }

            public Principal? AnotherPrincipal { get; set; }

            [ForeignKey("PrincipalId, PrincipalFk")]
            public Principal? CompositePrincipal { get; set; }
        }
#pragma warning restore CS8618
#nullable disable
    }
}
