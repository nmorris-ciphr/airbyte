/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.snowflake.db

import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.component.TableOperationsClient
import io.airbyte.cdk.load.schema.model.TableName
import io.airbyte.cdk.load.table.BaseDirectLoadInitialStatusGatherer
import io.airbyte.cdk.load.table.directload.DirectLoadInitialStatus
import io.airbyte.cdk.load.table.directload.DirectLoadTableStatus
import io.airbyte.integrations.destination.snowflake.client.SnowflakeAirbyteClient
import io.airbyte.integrations.destination.snowflake.client.TableStatusEntry
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

/**
 * Resolves every stream's initial table status from a handful of batched SHOW TABLES queries
 * instead of one query per final and temp table. Tables whose batch fell back are resolved with the
 * per-table [TableOperationsClient.countTable] lookup, so the result is the same as the base
 * implementation's for the same database state.
 */
@Singleton
class SnowflakeDirectLoadDatabaseInitialStatusGatherer(
    private val tableOperationsClient: TableOperationsClient,
    private val catalog: DestinationCatalog,
    private val snowflakeClient: SnowflakeAirbyteClient,
) :
    BaseDirectLoadInitialStatusGatherer(
        tableOperationsClient,
        catalog,
    ) {
    override suspend fun gatherInitialStatus(): Map<DestinationStream, DirectLoadInitialStatus> {
        val batched = snowflakeClient.batchGatherTableStatus(catalog.streams)

        val map =
            ConcurrentHashMap<DestinationStream, DirectLoadInitialStatus>(catalog.streams.size)
        coroutineScope {
            catalog.streams.forEach { s ->
                launch {
                    val tableNames = s.tableSchema.tableNames
                    map[s] =
                        DirectLoadInitialStatus(
                            realTable = getTableStatus(tableNames.finalTableName!!, batched),
                            tempTable = getTableStatus(tableNames.tempTableName!!, batched),
                        )
                }
            }
        }
        return map
    }

    private suspend fun getTableStatus(
        tableName: TableName,
        batched: Map<TableName, TableStatusEntry?>,
    ): DirectLoadTableStatus? {
        val numberOfRecords: Long? =
            if (batched.containsKey(tableName)) {
                batched[tableName]?.rows
            } else {
                // The batch for this table fell back; use the per-table lookup.
                tableOperationsClient.countTable(tableName)
            }
        return when (numberOfRecords) {
            // Missing table
            null -> null
            // Empty Table
            0L -> DirectLoadTableStatus(isEmpty = true)
            // Non-empty Table
            else -> DirectLoadTableStatus(isEmpty = false)
        }
    }
}
