/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.snowflake.db

import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.schema.model.TableName
import io.airbyte.cdk.load.table.DatabaseInitialStatusGatherer
import io.airbyte.cdk.load.table.directload.DirectLoadInitialStatus
import io.airbyte.cdk.load.table.directload.DirectLoadTableStatus
import io.airbyte.integrations.destination.snowflake.client.SnowflakeAirbyteClient
import io.airbyte.integrations.destination.snowflake.client.TableStatusEntry
import jakarta.inject.Singleton

/** Resolve every stream from complete batched metadata; discovery failures stop the sync. */
@Singleton
class SnowflakeDirectLoadDatabaseInitialStatusGatherer(
    private val catalog: DestinationCatalog,
    private val snowflakeClient: SnowflakeAirbyteClient,
) : DatabaseInitialStatusGatherer<DirectLoadInitialStatus> {
    override suspend fun gatherInitialStatus(): Map<DestinationStream, DirectLoadInitialStatus> {
        val batched = snowflakeClient.batchGatherTableStatus(catalog.streams)
        return catalog.streams.associateWith { stream ->
            val names = stream.tableSchema.tableNames
            DirectLoadInitialStatus(
                realTable = getTableStatus(names.finalTableName!!, batched),
                tempTable = getTableStatus(names.tempTableName!!, batched),
            )
        }
    }

    private fun getTableStatus(
        tableName: TableName,
        batched: Map<TableName, TableStatusEntry?>,
    ): DirectLoadTableStatus? {
        check(batched.containsKey(tableName)) {
            "Batch discovery did not resolve table ${tableName.toPrettyString()}"
        }
        return batched[tableName]?.let { DirectLoadTableStatus(isEmpty = it.rows == 0L) }
    }
}
