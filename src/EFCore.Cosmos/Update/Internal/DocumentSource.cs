// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Cosmos.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Cosmos.Storage.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Update;
using Newtonsoft.Json.Linq;

namespace Microsoft.EntityFrameworkCore.Cosmos.Update.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class DocumentSource
    {
        private readonly string _collectionId;
        private readonly CosmosDatabaseWrapper _database;
        private readonly IProperty _idProperty;

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        public DocumentSource(IEntityType entityType, CosmosDatabaseWrapper database)
        {
            _collectionId = entityType.GetCosmosContainer();
            _database = database;
            _idProperty = entityType.FindProperty(StoreKeyConvention.IdPropertyName);
        }

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        public virtual string GetCollectionId()
            => _collectionId;

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        public virtual string GetId(IUpdateEntry entry)
            => entry.GetCurrentValue<string>(_idProperty);

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        public virtual JObject CreateDocument(IUpdateEntry entry)
        {
            var document = new JObject();
            foreach (var property in entry.EntityType.GetProperties())
            {
                var storeName = property.GetCosmosPropertyName();
                if (storeName.Length != 0)
                {
                    document[storeName] = ConvertPropertyValue(property, entry.GetCurrentValue(property));
                }
                else if (entry.HasTemporaryValue(property))
                {
                    ((InternalEntityEntry)entry)[property] = entry.GetCurrentValue(property);
                }
            }

            foreach (var ownedNavigation in entry.EntityType.GetNavigations())
            {
                var fk = ownedNavigation.ForeignKey;
                if (!fk.IsOwnership
                    || ownedNavigation.IsDependentToPrincipal()
                    || fk.DeclaringEntityType.IsDocumentRoot())
                {
                    continue;
                }

                var nestedValue = entry.GetCurrentValue(ownedNavigation);
                var nestedPropertyName = fk.DeclaringEntityType.GetCosmosContainingPropertyName();
                if (nestedValue == null)
                {
                    document[nestedPropertyName] = null;
                }
                else if (fk.IsUnique)
                {
                    var dependentEntry = ((InternalEntityEntry)entry).StateManager.TryGetEntry(nestedValue, fk.DeclaringEntityType);
                    document[nestedPropertyName] = _database.GetDocumentSource(dependentEntry.EntityType).CreateDocument(dependentEntry);
                }
                else
                {
                    var array = new JArray();
                    foreach (var dependent in (IEnumerable)nestedValue)
                    {
                        var dependentEntry = ((InternalEntityEntry)entry).StateManager.TryGetEntry(dependent, fk.DeclaringEntityType);
                        array.Add(_database.GetDocumentSource(dependentEntry.EntityType).CreateDocument(dependentEntry));
                    }

                    document[nestedPropertyName] = array;
                }
            }

            return document;
        }

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        public virtual JObject UpdateDocument(JObject document, IUpdateEntry entry)
        {
            var anyPropertyUpdated = false;
            foreach (var property in entry.EntityType.GetProperties())
            {
                if (entry.EntityState == EntityState.Added
                    || entry.IsModified(property))
                {
                    var storeName = property.GetCosmosPropertyName();
                    if (storeName.Length != 0)
                    {
                        document[storeName] = ConvertPropertyValue(property, entry.GetCurrentValue(property));
                        anyPropertyUpdated = true;
                    }
                    else if (entry.HasTemporaryValue(property))
                    {
                        ((InternalEntityEntry)entry)[property] = entry.GetCurrentValue(property);
                    }
                }
            }

            foreach (var ownedNavigation in entry.EntityType.GetNavigations())
            {
                var fk = ownedNavigation.ForeignKey;
                if (!fk.IsOwnership
                    || ownedNavigation.IsDependentToPrincipal()
                    || fk.DeclaringEntityType.IsDocumentRoot())
                {
                    continue;
                }

                var nestedDocumentSource = _database.GetDocumentSource(fk.DeclaringEntityType);
                var nestedValue = entry.GetCurrentValue(ownedNavigation);
                var nestedPropertyName = fk.DeclaringEntityType.GetCosmosContainingPropertyName();
                if (nestedValue == null)
                {
                    if (document[nestedPropertyName] != null)
                    {
                        document[nestedPropertyName] = null;
                        anyPropertyUpdated = true;
                    }
                }
                else if (fk.IsUnique)
                {
                    var nestedEntry = ((InternalEntityEntry)entry).StateManager.TryGetEntry(nestedValue, fk.DeclaringEntityType);
                    if (nestedEntry == null)
                    {
                        return document;
                    }

                    if (document[nestedPropertyName] is JObject nestedDocument)
                    {
                        nestedDocument = nestedDocumentSource.UpdateDocument(nestedDocument, nestedEntry);
                    }
                    else
                    {
                        nestedDocument = nestedDocumentSource.CreateDocument(nestedEntry);
                    }

                    if (nestedDocument != null)
                    {
                        document[nestedPropertyName] = nestedDocument;
                        anyPropertyUpdated = true;
                    }
                }
                else
                {
                    var array = new JArray();
                    foreach (var dependent in (IEnumerable)nestedValue)
                    {
                        var dependentEntry = ((InternalEntityEntry)entry).StateManager.TryGetEntry(dependent, fk.DeclaringEntityType);
                        if (dependentEntry == null)
                        {
                            continue;
                        }

                        array.Add(_database.GetDocumentSource(dependentEntry.EntityType).CreateDocument(dependentEntry));
                    }

                    document[nestedPropertyName] = array;
                    anyPropertyUpdated = true;
                }
            }

            return anyPropertyUpdated ? document : null;
        }

        private static JToken ConvertPropertyValue(IProperty property, object value)
        {
            if (value == null)
            {
                return null;
            }

            var converter = property.GetTypeMapping().Converter;
            if (converter != null)
            {
                value = converter.ConvertToProvider(value);
            }

            return (value as JToken) ?? JToken.FromObject(value, CosmosClientWrapper.Serializer);
        }
    }
}
