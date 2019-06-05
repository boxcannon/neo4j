/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexLimitation;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.internal.kernel.api.exceptions.schema.MisconfiguredIndexException;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.util.FeatureToggles;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.ValueCategory;

import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory.getConfiguredSpaceFillingCurveConfiguration;

/**
 * Native index able to handle all value types in a single {@link GBPTree}. Single-key as well as composite-key is supported.
 *
 * A composite index query have one predicate per slot / column.
 * The predicate comes in the form of an index query. Any of "exact", "range" or "exist".
 * Other index providers have support for exact predicate on all columns or exists predicate on all columns (full scan).
 * This index provider have some additional capabilities. It can combine the slot predicates under the following rules:
 * a. Exact can only follow another Exact or be in first slot.
 * b. Range can only follow Exact or be in first slot.
 *
 * We use the following notation for the predicates:
 * x: exact predicate
 * -: exists predicate
 * >: range predicate (this could be ranges with zero or one open end)
 *
 * With an index on 5 slots as en example we can build several different composite queries:
 *     p1 p2 p3 p4 p5 (order is important)
 * 1:  x  x  x  x  x
 * 2:  -  -  -  -  -
 * 3:  x  -  -  -  -
 * 4:  x  x  x  x  -
 * 5:  >  -  -  -  -
 * 6:  x  >  -  -  -
 * 7:  x  x  x  x  >
 * 8:  >  x  -  -  - (not allowed!)
 * 9:  >  >  -  -  - (not allowed!)
 * 10: -  x  -  -  - (not allowed!)
 * 11: -  >  -  -  - (not allowed!)
 *
 * 1: Exact match on all slots. Supported by all index providers.
 * 2: Exists scan on all slots. Supported by all index providers.
 * 3: Exact match on first column and exists on the rest.
 * 4: Exact match on all columns but the last.
 * 5: Range on first column and exists on rest.
 * 6: Exact on first, range on second and exists on rest.
 * 7: Exact on all but last column. Range on last.
 * 8: Not allowed because Exact can only follow another Exact.
 * 9: Not allowed because range can only follow Exact.
 * 10: Not allowed because Exact can only follow another Exact.
 * 11: Not allowed because range can only follow Exact.
 *
 * WHY?
 * In short, we only allow "restrictive" predicates (exact or range) if they help us restrict the scan range.
 * Let's take query 11 as example
 * p1 p2 p3 p4 p5
 * -  >  -  -  -
 * Index is sorted first by p1, then p2, etc.
 * Because we have a complete scan on p1 the range predicate on p2 can not restrict the range of the index we need to scan.
 * We COULD allow this query and do filter during scan instead and take the extra cost into account when planning queries.
 * As of writing this, there is no such filtering implementation.
 */
public class GenericNativeIndexProvider extends NativeIndexProvider<GenericKey,NativeIndexValue,GenericLayout>
{
    public static final String KEY = NATIVE_BTREE10.providerKey();
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( KEY, NATIVE_BTREE10.providerVersion() );
    public static final IndexCapability CAPABILITY = new GenericIndexCapability();
    public static final String BLOCK_BASED_POPULATION_NAME = "blockBasedPopulation";
    // todo turn OFF by default before releasing next patch. For now ON by default to test it.
    private final boolean blockBasedPopulation = FeatureToggles.flag( GenericNativeIndexPopulator.class, BLOCK_BASED_POPULATION_NAME, true );

    /**
     * Cache of all setting for various specific CRS's found in the config at instantiation of this provider.
     * The config is read once and all relevant CRS configs cached here.
     */
    private final ConfiguredSpaceFillingCurveSettingsCache configuredSettings;

    /**
     * A space filling curve configuration used when reading spatial index values.
     */
    private final SpaceFillingCurveConfiguration configuration;
    private final boolean archiveFailedIndex;

    public GenericNativeIndexProvider( IndexDirectoryStructure.Factory directoryStructureFactory, PageCache pageCache, FileSystemAbstraction fs,
            Monitor monitor, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly, Config config )
    {
        super( DESCRIPTOR, directoryStructureFactory, pageCache, fs, monitor, recoveryCleanupWorkCollector, readOnly );

        this.configuredSettings = new ConfiguredSpaceFillingCurveSettingsCache( config );
        this.configuration = getConfiguredSpaceFillingCurveConfiguration( config );
        this.archiveFailedIndex = config.get( GraphDatabaseSettings.archive_failed_index );
    }

    @Override
    public <T extends org.neo4j.internal.schema.IndexDescriptor> T bless( T index ) throws MisconfiguredIndexException
    {
        SchemaDescriptor sinfulSchema = index.schema();
        IndexConfig indexConfig = sinfulSchema.getIndexConfig();
        for ( CoordinateReferenceSystem crs : CoordinateReferenceSystem.all() )
        {
            SpaceFillingCurveSettings spaceFillingCurveSettings = configuredSettings.forCRS( crs );
            indexConfig = SpatialIndexConfig.addSpatialConfig( indexConfig, crs, spaceFillingCurveSettings );
        }
        SchemaDescriptor blessedSchema = sinfulSchema.withIndexConfig( indexConfig );
        index = (T) index.withSchemaDescriptor( blessedSchema );
        return super.bless( index );
    }

    @Override
    GenericLayout layout( StorageIndexReference descriptor, File storeFile )
    {
        int numberOfSlots = descriptor.schema().getPropertyIds().length;
        IndexConfig indexConfig = descriptor.schema().getIndexConfig();
        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> settings = SpatialIndexConfig.extractSpatialConfig( indexConfig );
        return new GenericLayout( numberOfSlots, new IndexSpecificSpaceFillingCurveSettings( settings ) );
    }

    @Override
    protected IndexPopulator newIndexPopulator( IndexFiles indexFiles, GenericLayout layout, StorageIndexReference descriptor, ByteBufferFactory bufferFactory )
    {
        if ( blockBasedPopulation )
        {
            return new GenericBlockBasedIndexPopulator( pageCache, fs, indexFiles, layout, monitor, descriptor, layout.getSpaceFillingCurveSettings(),
                    configuration, archiveFailedIndex, bufferFactory );
        }
        return new WorkSyncedNativeIndexPopulator<>(
                new GenericNativeIndexPopulator( pageCache, fs, indexFiles, layout, monitor, descriptor, layout.getSpaceFillingCurveSettings(), configuration,
                        archiveFailedIndex ) );
    }

    @Override
    protected IndexAccessor newIndexAccessor( IndexFiles indexFiles, GenericLayout layout, StorageIndexReference descriptor )
    {
        return new GenericNativeIndexAccessor( pageCache, fs, indexFiles, layout, recoveryCleanupWorkCollector, monitor, descriptor,
                layout.getSpaceFillingCurveSettings(), configuration );
    }

    @Override
    public IndexCapability getCapability( StorageIndexReference descriptor )
    {
        return CAPABILITY;
    }

    private static class GenericIndexCapability implements IndexCapability
    {
        private final IndexLimitation[] limitations = {IndexLimitation.SLOW_CONTAINS};

        @Override
        public IndexOrder[] orderCapability( ValueCategory... valueCategories )
        {
            if ( supportOrdering( valueCategories ) )
            {
                return IndexCapability.ORDER_BOTH;
            }
            return IndexCapability.ORDER_NONE;
        }

        @Override
        public IndexValueCapability valueCapability( ValueCategory... valueCategories )
        {
            return IndexValueCapability.YES;
        }

        private boolean supportOrdering( ValueCategory[] valueCategories )
        {
            for ( ValueCategory valueCategory : valueCategories )
            {
                if ( valueCategory == ValueCategory.GEOMETRY ||
                     valueCategory == ValueCategory.GEOMETRY_ARRAY ||
                     valueCategory == ValueCategory.UNKNOWN )
                {
                    return false;
                }
            }
            return true;
        }

        @Override
        public IndexLimitation[] limitations()
        {
            return limitations;
        }
    }
}