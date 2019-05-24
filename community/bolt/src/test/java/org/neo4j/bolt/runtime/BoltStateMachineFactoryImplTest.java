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
package org.neo4j.bolt.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.v1.BoltProtocolV1;
import org.neo4j.bolt.v1.runtime.BoltStateMachineV1;
import org.neo4j.bolt.v2.BoltProtocolV2;
import org.neo4j.bolt.v3.BoltStateMachineV3;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BoltStateMachineFactoryImplTest
{
    private static final String CUSTOM_DB_NAME = "customDbName";
    private static final SystemNanoClock CLOCK = Clocks.nanoClock();
    private static final BoltChannel CHANNEL = BoltTestUtil.newTestBoltChannel();

    @ParameterizedTest( name = "V{0}" )
    @ValueSource( longs = {BoltProtocolV1.VERSION, BoltProtocolV2.VERSION} )
    void shouldCreateBoltStateMachinesV1( long protocolVersion )
    {
        BoltStateMachineFactoryImpl factory = newBoltFactory();

        BoltStateMachine boltStateMachine = factory.newStateMachine( protocolVersion, CHANNEL );

        assertNotNull( boltStateMachine );
        assertThat( boltStateMachine, instanceOf( BoltStateMachineV1.class ) );
    }

    @Test
    void shouldCreateBoltStateMachinesV3()
    {
        BoltStateMachineFactoryImpl factory = newBoltFactory();

        BoltStateMachine boltStateMachine = factory.newStateMachine( 3L, CHANNEL );

        assertNotNull( boltStateMachine );
        assertThat( boltStateMachine, instanceOf( BoltStateMachineV3.class ) );
    }

    @ParameterizedTest( name = "V{0}" )
    @ValueSource( longs = {999, -1} )
    void shouldThrowExceptionIfVersionIsUnknown( long protocolVersion )
    {
        BoltStateMachineFactoryImpl factory = newBoltFactory();

        IllegalArgumentException error = assertThrows( IllegalArgumentException.class, () -> factory.newStateMachine( protocolVersion, CHANNEL ) );
        assertThat( error.getMessage(), startsWith( "Failed to create a state machine for protocol version" ) );
    }

    private static BoltStateMachineFactoryImpl newBoltFactory()
    {
        return newBoltFactory( newDbMock() );
    }

    private static BoltStateMachineFactoryImpl newBoltFactory( DatabaseManager<?> databaseManager )
    {
        Config config = Config.defaults( GraphDatabaseSettings.default_database, CUSTOM_DB_NAME );
        return new BoltStateMachineFactoryImpl( databaseManager, mock( Authentication.class ), CLOCK, config, NullLogService.getInstance() );
    }

    private static DatabaseManager<?> newDbMock()
    {
        StandaloneDatabaseContext db = mock( StandaloneDatabaseContext.class );
        Dependencies dependencies = mock( Dependencies.class );
        when( db.dependencies() ).thenReturn( dependencies );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        when( queryService.getDependencyResolver() ).thenReturn( dependencies );
        when( dependencies.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( queryService );
        @SuppressWarnings( "unchecked" )
        DatabaseManager<StandaloneDatabaseContext> databaseManager = (DatabaseManager<StandaloneDatabaseContext>) mock( DatabaseManager.class );
        when( databaseManager.getDatabaseContext( new DatabaseId( CUSTOM_DB_NAME ) ) ).thenReturn( Optional.of( db ) );
        return databaseManager;
    }
}