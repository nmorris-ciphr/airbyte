/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.snowflake.db

import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.schema.model.ColumnSchema
import io.airbyte.cdk.load.schema.model.StreamTableSchema
import io.airbyte.cdk.load.schema.model.TableName
import io.airbyte.cdk.load.schema.model.TableNames
import io.airbyte.cdk.load.table.BaseDirectLoadInitialStatusGatherer
import io.airbyte.cdk.load.table.DefaultTempTableNameGenerator
import io.airbyte.cdk.load.table.directload.DirectLoadInitialStatus
import io.airbyte.cdk.load.table.directload.DirectLoadTableStatus
import io.airbyte.integrations.destination.snowflake.client.SHOW_TABLES_ROW_CAP
import io.airbyte.integrations.destination.snowflake.client.SnowflakeAirbyteClient
import io.airbyte.integrations.destination.snowflake.schema.SnowflakeColumnManager
import io.airbyte.integrations.destination.snowflake.spec.SnowflakeConfiguration
import io.airbyte.integrations.destination.snowflake.sql.SnowflakeDirectLoadSqlGenerator
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Exercises the batched initial status gatherer against a fake Snowflake at the JDBC level. The
 * fake answers both the batched SHOW TABLES queries and the legacy per-table `show tables like ?`
 * queries from the same table listing, so batched and per-table results can be compared directly.
 */
internal class SnowflakeDirectLoadDatabaseInitialStatusGathererTest {

    private data class FakeTable(val name: String, val rows: Long, val comment: String? = null)

    private val internalNamespace = "airbyte_internal"
    private val tempTableNameGenerator = DefaultTempTableNameGenerator(internalNamespace)

    private lateinit var dataSource: DataSource
    private lateinit var sqlGenerator: SnowflakeDirectLoadSqlGenerator
    private lateinit var client: SnowflakeAirbyteClient

    /** namespace -> tables in that schema */
    private val database = mutableMapOf<String, MutableList<FakeTable>>()
    private val executedQueries: MutableList<String> = Collections.synchronizedList(mutableListOf())
    private var failingQuery: (String) -> Boolean = { false }

    @BeforeEach
    fun setup() {
        database.clear()
        executedQueries.clear()
        failingQuery = { false }

        sqlGenerator = mockk(relaxed = true)
        every { sqlGenerator.showTablesInSchema(any()) } answers { "SHOW_IN|${firstArg<String>()}" }
        every { sqlGenerator.showTablesStartsWith(any(), any()) } answers
            {
                "SHOW_SW|${secondArg<String>()}|${firstArg<String>()}"
            }

        val connection = mockk<Connection>(relaxed = true)
        every { connection.createStatement() } answers { batchedStatement() }
        every { connection.prepareStatement(any()) } answers { perTableStatement(firstArg()) }
        dataSource = mockk()
        every { dataSource.connection } returns connection

        val configuration =
            mockk<SnowflakeConfiguration>(relaxed = true) {
                every { database } returns "test_database"
            }
        client =
            SnowflakeAirbyteClient(
                dataSource,
                sqlGenerator,
                configuration,
                mockk<SnowflakeColumnManager>(relaxed = true),
            )
    }

    private fun resultSetOf(tables: List<FakeTable>): ResultSet {
        val index = AtomicInteger(-1)
        return mockk(relaxed = true) {
            every { next() } answers { index.incrementAndGet() < tables.size }
            every { getString("name") } answers { tables[index.get()].name }
            every { getLong("rows") } answers { tables[index.get()].rows }
            every { getString("comment") } answers { tables[index.get()].comment }
        }
    }

    private fun batchedStatement(): Statement =
        mockk(relaxed = true) {
            every { executeQuery(any()) } answers
                {
                    val sql = firstArg<String>()
                    executedQueries.add(sql)
                    if (failingQuery(sql)) throw SQLException("boom: $sql")
                    val parts = sql.split("|")
                    val tables = database[parts[1]].orEmpty()
                    when (parts[0]) {
                        "SHOW_IN" -> resultSetOf(tables)
                        "SHOW_SW" -> resultSetOf(tables.filter { it.name.startsWith(parts[2]) })
                        else -> error("unexpected query $sql")
                    }
                }
        }

    /** Mirrors `show tables like ? in schema "<db>"."<ns>"` (LIKE is case-insensitive). */
    private fun perTableStatement(sql: String): PreparedStatement {
        val namespace = Regex("""in schema "[^"]*"\."(.*)"\s*$""").find(sql)!!.groupValues[1]
        val pattern = slot<String>()
        return mockk(relaxed = true) {
            every { setString(1, capture(pattern)) } answers {}
            every { executeQuery() } answers
                {
                    executedQueries.add("LIKE|$namespace|${pattern.captured}")
                    resultSetOf(
                        database[namespace].orEmpty().filter {
                            it.name.equals(pattern.captured, ignoreCase = true)
                        }
                    )
                }
        }
    }

    private fun stream(
        namespace: String,
        name: String,
        tempTable: TableName = tempTableNameGenerator.generate(TableName(namespace, name)),
    ): DestinationStream {
        val finalTable = TableName(namespace, name)
        return mockk(relaxed = true) {
            every { tableSchema } returns
                StreamTableSchema(
                    tableNames = TableNames(finalTableName = finalTable, tempTableName = tempTable),
                    columnSchema = ColumnSchema(emptyMap(), emptyMap(), emptyMap()),
                    importType = Append,
                )
            every { mappedDescriptor } returns DestinationStream.Descriptor(namespace, name)
        }
    }

    private fun DestinationStream.finalTable() = tableSchema.tableNames.finalTableName!!
    private fun DestinationStream.tempTable() = tableSchema.tableNames.tempTableName!!

    private fun addTable(table: TableName, rows: Long, comment: String? = null) {
        database
            .getOrPut(table.namespace) { mutableListOf() }
            .add(FakeTable(table.name, rows, comment))
    }

    private fun gatherer(streams: List<DestinationStream>) =
        SnowflakeDirectLoadDatabaseInitialStatusGatherer(DestinationCatalog(streams), client)

    private fun perTableGatherer(streams: List<DestinationStream>) =
        object : BaseDirectLoadInitialStatusGatherer(client, DestinationCatalog(streams)) {}

    @Test
    fun testBatchesOneShowPerNamespace() {
        val longNamespace = "TENANT_ALPHA_PRODUCTION_UKS"
        val streams =
            listOf(
                stream("NS_A", "USERS"),
                stream("NS_A", "ORDERS"),
                stream("NS_A", "ITEMS"),
                stream("NS_B", "USERS"),
                stream(longNamespace, "USERS"),
                stream(longNamespace, "ORDERS"),
            )
        streams.forEach { addTable(it.finalTable(), rows = 1) }

        val result = runBlocking { gatherer(streams).gatherInitialStatus() }

        assertEquals(streams.size, result.size)
        assertEquals(
            setOf(
                "SHOW_IN|NS_A",
                "SHOW_IN|NS_B",
                "SHOW_IN|$longNamespace",
                "SHOW_SW|$internalNamespace|NS_A",
                "SHOW_SW|$internalNamespace|NS_B",
                // first 8 + last 8 characters of the long namespace, as in the temp table names
                "SHOW_SW|$internalNamespace|TENANT_ATION_UKS",
            ),
            executedQueries.toSet(),
        )
        assertEquals(6, executedQueries.size, "Each SHOW must run exactly once: $executedQueries")
        assertTrue(executedQueries.none { it.startsWith("LIKE") })
    }

    @Test
    fun testReturnsIdenticalStatusToPerTableForMissingEmptyNonEmpty() {
        val missingBoth = stream("NS_A", "MISSING")
        val emptyReal = stream("NS_A", "EMPTY_REAL")
        val nonEmptyReal = stream("NS_A", "FULL_REAL")
        val emptyTemp = stream("NS_B", "EMPTY_TEMP")
        val nonEmptyTemp = stream("NS_B", "FULL_TEMP")
        val streams = listOf(missingBoth, emptyReal, nonEmptyReal, emptyTemp, nonEmptyTemp)

        addTable(emptyReal.finalTable(), rows = 0)
        addTable(nonEmptyReal.finalTable(), rows = 42)
        addTable(emptyTemp.finalTable(), rows = 3)
        addTable(emptyTemp.tempTable(), rows = 0)
        addTable(nonEmptyTemp.tempTable(), rows = 7)
        // An unrelated table must not affect anything.
        addTable(TableName("NS_A", "SOMETHING_ELSE"), rows = 5)

        val batched = runBlocking { gatherer(streams).gatherInitialStatus() }
        val perTable = runBlocking { perTableGatherer(streams).gatherInitialStatus() }

        assertEquals(perTable, batched)
        assertEquals(DirectLoadInitialStatus(null, null), batched[missingBoth])
        assertEquals(
            DirectLoadInitialStatus(DirectLoadTableStatus(isEmpty = true), null),
            batched[emptyReal]
        )
        assertEquals(
            DirectLoadInitialStatus(DirectLoadTableStatus(isEmpty = false), null),
            batched[nonEmptyReal]
        )
        assertEquals(
            DirectLoadInitialStatus(
                DirectLoadTableStatus(isEmpty = false),
                DirectLoadTableStatus(isEmpty = true)
            ),
            batched[emptyTemp]
        )
        assertEquals(
            DirectLoadInitialStatus(null, DirectLoadTableStatus(isEmpty = false)),
            batched[nonEmptyTemp]
        )
    }

    @Test
    fun testFailsOnRowCapWithoutPerTableQueries() {
        val requested = stream("NS_A", "USERS")
        addTable(requested.finalTable(), rows = 9)
        repeat(SHOW_TABLES_ROW_CAP - 1) { addTable(TableName("NS_A", "FILLER_$it"), rows = 1) }

        val exception =
            assertThrows<IllegalStateException> {
                runBlocking { gatherer(listOf(requested)).gatherInitialStatus() }
            }

        assertTrue(exception.message!!.contains("NS_A"))
        assertTrue(exception.message!!.contains("10000"))
        assertTrue(exception.message!!.contains("truncated"))
        assertTrue(executedQueries.none { it.startsWith("LIKE") })
        assertTrue(client.lastBatchComments.isEmpty())
    }

    @Test
    fun testListingBelowCapResolvesWithoutPerTableQueries() {
        val requested = stream("NS_A", "USERS")
        addTable(requested.finalTable(), rows = 9)
        repeat(SHOW_TABLES_ROW_CAP - 2) { addTable(TableName("NS_A", "FILLER_$it"), rows = 1) }

        val result = runBlocking { gatherer(listOf(requested)).gatherInitialStatus() }

        assertEquals(DirectLoadTableStatus(isEmpty = false), result[requested]!!.realTable)
        assertTrue(executedQueries.none { it.startsWith("LIKE") })
    }

    @Test
    fun testSqlErrorStopsDiscoveryWithoutPublishingPartialComments() {
        val requested = stream("NS_A", "USERS")
        addTable(requested.finalTable(), rows = 0, comment = "airbyte-schema:v1:0123456789abcdef")
        failingQuery = { it.startsWith("SHOW_SW|") }

        val exception =
            assertThrows<IllegalStateException> {
                runBlocking { gatherer(listOf(requested)).gatherInitialStatus() }
            }

        assertTrue(exception.message!!.contains(internalNamespace))
        assertTrue(exception.message!!.contains("SHOW_SW"))
        assertTrue(exception.cause is SQLException)
        assertTrue(exception.cause!!.message!!.contains("boom"))
        assertTrue(executedQueries.none { it.startsWith("LIKE") })
        assertNull(client.lastBatchComments[requested.finalTable()])
    }

    @Test
    fun testUnsafeTempPrefixFailsWithoutListingSharedSchema() {
        val requested = stream("NS_A", "USERS", TableName(internalNamespace, "UNRELATED_TEMP"))

        val exception =
            assertThrows<IllegalStateException> {
                runBlocking { gatherer(listOf(requested)).gatherInitialStatus() }
            }

        assertTrue(exception.message!!.contains("UNRELATED_TEMP"))
        assertTrue(exception.message!!.contains("NS_A"))
        assertTrue(executedQueries.isEmpty())
    }

    @Test
    fun testAmbiguousCaseInsensitiveNameFails() {
        val requested = stream("NS_A", "uSeRs")
        addTable(TableName("NS_A", "USERS"), rows = 1)
        addTable(TableName("NS_A", "users"), rows = 0)

        val exception =
            assertThrows<IllegalStateException> {
                runBlocking { gatherer(listOf(requested)).gatherInitialStatus() }
            }

        assertTrue(exception.message!!.contains("Ambiguous"))
        assertTrue(exception.message!!.contains("uSeRs"))
        assertTrue(executedQueries.none { it.startsWith("LIKE") })
    }

    @Test
    fun testUniqueCaseInsensitiveNameStillResolves() {
        val requested = stream("NS_A", "users")
        addTable(TableName("NS_A", "USERS"), rows = 1)

        val result = runBlocking { gatherer(listOf(requested)).gatherInitialStatus() }

        assertEquals(DirectLoadTableStatus(isEmpty = false), result[requested]!!.realTable)
    }

    @Test
    fun testMissingTableNamesFailWithStreamContextBeforeQueries() {
        val requested = stream("NS_A", "USERS")
        val schema = requested.tableSchema
        val missingNames =
            listOf(
                TableNames(
                    rawTableName = TableName("NS_A", "RAW_USERS"),
                    finalTableName = null,
                    tempTableName = requested.tempTable(),
                ),
                TableNames(finalTableName = requested.finalTable(), tempTableName = null),
            )
        missingNames.forEach { names ->
            every { requested.tableSchema } returns schema.copy(tableNames = names)
            val exception =
                assertThrows<IllegalArgumentException> {
                    runBlocking { client.batchGatherTableStatus(listOf(requested)) }
                }
            assertTrue(exception.message!!.contains("NS_A"))
            assertTrue(exception.message!!.contains("USERS"))
        }
        assertTrue(executedQueries.isEmpty())
    }

    @Test
    fun testEmptyCatalogIsRejectedBeforeQueries() {
        assertThrows<IllegalArgumentException> { gatherer(emptyList()) }
        assertTrue(executedQueries.isEmpty())
    }

    @Test
    fun testExactNameMatchingInSharedInternalSchema() {
        // Both namespaces share the first 8 and last 8 characters, so their temp tables share
        // the SHOW TABLES STARTS WITH prefix.
        val ours = stream("CUSTOMER1_ZZZ_PROD_UKS", "USERS")
        val otherTenant = stream("CUSTOMER2_ZZZ_PROD_UKS", "USERS")
        val sameCaseInsensitiveName = stream("CUSTOMER1_ZZZ_PROD_UKS", "ORDERS")
        assertTrue(otherTenant.tempTable().name.startsWith("CUSTOMERPROD_UKS"))
        assertTrue(ours.tempTable().name.startsWith("CUSTOMERPROD_UKS"))

        // Only the other tenant's temp table exists for USERS.
        addTable(otherTenant.tempTable(), rows = 5)
        // For ORDERS, an exact-case table and a case-variant exist; the exact one wins.
        addTable(sameCaseInsensitiveName.tempTable(), rows = 0)
        addTable(
            TableName(internalNamespace, sameCaseInsensitiveName.tempTable().name.uppercase()),
            rows = 8
        )

        val result = runBlocking {
            gatherer(listOf(ours, sameCaseInsensitiveName)).gatherInitialStatus()
        }

        assertNull(result[ours]!!.tempTable)
        assertEquals(
            DirectLoadTableStatus(isEmpty = true),
            result[sameCaseInsensitiveName]!!.tempTable
        )
        assertEquals(
            listOf("SHOW_SW|$internalNamespace|CUSTOMERPROD_UKS"),
            executedQueries.filter { it.startsWith("SHOW_SW") },
        )
    }

    @Test
    fun testCapturedCommentsExposedForFingerprinting() {
        val fingerprinted = stream("NS_A", "USERS")
        val userComment = stream("NS_A", "ORDERS")
        val noComment = stream("NS_A", "ITEMS")
        val fingerprint = "airbyte-schema:v1:0123456789abcdef"
        addTable(fingerprinted.finalTable(), rows = 1, comment = fingerprint)
        addTable(userComment.finalTable(), rows = 1, comment = "owned by the finance team")
        addTable(noComment.finalTable(), rows = 1, comment = null)

        runBlocking {
            gatherer(listOf(fingerprinted, userComment, noComment)).gatherInitialStatus()
        }

        assertEquals(fingerprint, client.lastBatchComments[fingerprinted.finalTable()])
        assertNull(client.lastBatchComments[userComment.finalTable()])
        assertNull(client.lastBatchComments[noComment.finalTable()])
    }

    @Test
    fun testNewNamespaceTreatedAsNoFingerprint() {
        // The writer creates namespaces before gathering, so a new namespace is simply empty.
        val streamInNewNamespace = stream("BRAND_NEW", "USERS")
        database["BRAND_NEW"] = mutableListOf()

        val result = runBlocking { gatherer(listOf(streamInNewNamespace)).gatherInitialStatus() }

        assertEquals(DirectLoadInitialStatus(null, null), result[streamInNewNamespace])
        assertNull(client.lastBatchComments[streamInNewNamespace.finalTable()])
        assertTrue(client.lastBatchComments.isEmpty())
        assertTrue(executedQueries.none { it.startsWith("LIKE") })
    }
}
