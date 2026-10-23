/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.snowflake.client

import io.airbyte.cdk.ConfigErrorException
import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.component.ColumnType
import io.airbyte.cdk.load.component.ColumnTypeChange
import io.airbyte.cdk.load.component.TableSchema
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_GENERATION_ID
import io.airbyte.cdk.load.message.Meta.Companion.COLUMN_NAME_AB_RAW_ID
import io.airbyte.cdk.load.schema.model.ColumnSchema
import io.airbyte.cdk.load.schema.model.StreamTableSchema
import io.airbyte.cdk.load.schema.model.TableName
import io.airbyte.cdk.load.schema.model.TableNames
import io.airbyte.cdk.load.table.ColumnNameMapping
import io.airbyte.cdk.load.table.DefaultTempTableNameGenerator
import io.airbyte.cdk.load.table.directload.DirectLoadInitialStatus
import io.airbyte.cdk.load.table.directload.DirectLoadTableDedupStreamLoader
import io.airbyte.cdk.load.table.directload.DirectLoadTableDedupTruncateStreamLoader
import io.airbyte.cdk.load.table.directload.DirectLoadTableStatus
import io.airbyte.cdk.load.write.StreamStateStore
import io.airbyte.integrations.destination.snowflake.schema.SnowflakeColumnManager
import io.airbyte.integrations.destination.snowflake.schema.toSnowflakeCompatibleName
import io.airbyte.integrations.destination.snowflake.spec.SnowflakeConfiguration
import io.airbyte.integrations.destination.snowflake.sql.QUOTE
import io.airbyte.integrations.destination.snowflake.sql.SnowflakeDirectLoadSqlGenerator
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import io.mockk.verifyOrder
import java.security.MessageDigest
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource
import kotlinx.coroutines.runBlocking
import net.snowflake.client.jdbc.SnowflakeSQLException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class SnowflakeAirbyteClientTest {

    private lateinit var client: SnowflakeAirbyteClient
    private lateinit var dataSource: DataSource
    private lateinit var sqlGenerator: SnowflakeDirectLoadSqlGenerator
    private lateinit var snowflakeConfiguration: SnowflakeConfiguration
    private lateinit var columnManager: SnowflakeColumnManager

    @BeforeEach
    fun setup() {
        dataSource = mockk()
        sqlGenerator = mockk(relaxed = true)
        snowflakeConfiguration =
            mockk(relaxed = true) { every { database } returns "test_database" }
        columnManager = mockk(relaxed = true)
        client =
            SnowflakeAirbyteClient(dataSource, sqlGenerator, snowflakeConfiguration, columnManager)
    }

    @Test
    fun testCountTable() {
        val tableName = TableName(namespace = "namespace", name = "name")
        val resultSet =
            mockk<ResultSet> {
                every { next() } returns true
                every { getLong("rows") } returns 1L
            }
        val preparedStatement =
            mockk<PreparedStatement> {
                every { setString(any(), any()) } just runs
                every { executeQuery() } returns resultSet
                every { close() } just runs
            }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { prepareStatement(any()) } returns preparedStatement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            val result = client.countTable(tableName)
            assertEquals(1L, result)
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun testCountMissingTable() {
        val tableName = TableName(namespace = "namespace", name = "name")
        val resultSet = mockk<ResultSet> { every { next() } returns false }
        val preparedStatement =
            mockk<PreparedStatement> {
                every { setString(any(), any()) } just runs
                every { executeQuery() } returns resultSet
                every { close() } just runs
            }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { prepareStatement(any()) } returns preparedStatement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            val result = client.countTable(tableName)
            assertEquals(null, result)
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun testCountEmptyTable() {
        val tableName = TableName(namespace = "namespace", name = "name")
        val resultSet =
            mockk<ResultSet> {
                every { next() } returns true
                every { getLong("rows") } returns 0L
            }
        val preparedStatement =
            mockk<PreparedStatement> {
                every { setString(any(), any()) } just runs
                every { executeQuery() } returns resultSet
                every { close() } just runs
            }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { prepareStatement(any()) } returns preparedStatement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            val result = client.countTable(tableName)
            assertEquals(0L, result)
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun testCreateNamespace() {
        val namespace = "namespace"

        // Mock for schema check - schema doesn't exist
        val schemaCheckResultSet =
            mockk<ResultSet> {
                every { next() } returns true
                every { getBoolean("SCHEMA_EXISTS") } returns false
                every { close() } just Runs
            }

        // Mock for other operations
        val createResultSet = mockk<ResultSet>(relaxed = true)

        val preparedStatement =
            mockk<PreparedStatement>(relaxed = true) {
                every { executeQuery() } returns schemaCheckResultSet
                every { close() } just Runs
            }

        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } returns createResultSet
                every { close() } just Runs
            }

        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { createStatement() } returns statement
                every { prepareStatement(any()) } returns preparedStatement
            }

        every { dataSource.connection } returns mockConnection
        runBlocking {
            client.createNamespace(namespace)
            verify(exactly = 1) { sqlGenerator.createNamespace(namespace) }
            verify(exactly = 1) { preparedStatement.close() }
            verify(exactly = 1) { statement.close() }
            verify(exactly = 2) { mockConnection.close() }
        }
    }

    @Test
    fun testCreateNamespaceWhenAlreadyExists() {
        val namespace = "namespace"

        // Mock for schema check - schema already exists
        val schemaCheckResultSet =
            mockk<ResultSet> {
                every { next() } returns true
                every { getBoolean("SCHEMA_EXISTS") } returns true
                every { close() } just Runs
            }

        val preparedStatement =
            mockk<PreparedStatement>(relaxed = true) {
                every { executeQuery() } returns schemaCheckResultSet
                every { close() } just Runs
            }

        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { prepareStatement(any()) } returns preparedStatement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            client.createNamespace(namespace)
            verify(exactly = 0) {
                sqlGenerator.createNamespace(namespace)
            } // Should NOT create schema
            verify(exactly = 1) { preparedStatement.close() }
            verify(exactly = 1) { mockConnection.close() } // Only 2 closes: check + format
        }
    }

    @Test
    fun testCreateTable() {
        val columnNameMapping = mockk<ColumnNameMapping>(relaxed = true)
        val stream = mockk<DestinationStream>(relaxed = true)
        val tableName = TableName(namespace = "namespace", name = "name")
        val resultSet = mockk<ResultSet>(relaxed = true)
        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } returns resultSet
                every { close() } just Runs
            }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { createStatement() } returns statement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            client.createTable(
                stream = stream,
                tableName = tableName,
                columnNameMapping = columnNameMapping,
                replace = true,
            )
            verify(exactly = 1) { sqlGenerator.createTable(tableName, any(), true) }
            verify(exactly = 1) { sqlGenerator.createSnowflakeStage(tableName) }
            verify(exactly = 2) { mockConnection.close() }
        }
    }

    @Test
    fun `zero-record incremental dedup skips temp DDL merge and drop`() {
        val columnNameMapping = mockk<ColumnNameMapping>(relaxed = true)
        val stream = mockk<DestinationStream>(relaxed = true)
        val tempTableName = TableName(namespace = "namespace", name = "temp")
        val realTableName = TableName(namespace = "namespace", name = "real")

        runBlocking {
            client.createTempTable(stream, tempTableName, columnNameMapping, replace = true)
            client.upsertTable(stream, columnNameMapping, tempTableName, realTableName)
            client.dropTable(tempTableName)
        }

        verify(exactly = 0) { sqlGenerator.createTable(any(), any(), any()) }
        verify(exactly = 0) { sqlGenerator.createSnowflakeStage(any()) }
        verify(exactly = 0) { sqlGenerator.upsertTable(any(), any(), any()) }
        verify(exactly = 0) { sqlGenerator.dropTable(any()) }
        verify(exactly = 0) { dataSource.connection }
    }

    @Test
    fun `empty overwrite materializes source and replaces target`() {
        val columnNameMapping = mockk<ColumnNameMapping>(relaxed = true)
        val stream = mockk<DestinationStream>(relaxed = true)
        val tempTableName = TableName(namespace = "namespace", name = "temp")
        val realTableName = TableName(namespace = "namespace", name = "real")
        val resultSet =
            mockk<ResultSet>(relaxed = true) {
                every { next() } returns true
                every { getLong("rows") } returns 1L
            }
        val preparedStatement =
            mockk<PreparedStatement>(relaxed = true) { every { executeQuery() } returns resultSet }
        val statement =
            mockk<Statement>(relaxed = true) { every { executeQuery(any()) } returns resultSet }
        val connection =
            mockk<Connection>(relaxed = true) {
                every { prepareStatement(any()) } returns preparedStatement
                every { createStatement() } returns statement
            }
        every { dataSource.connection } returns connection

        runBlocking {
            client.createTempTable(stream, tempTableName, columnNameMapping, replace = true)
            client.overwriteTable(tempTableName, realTableName)
        }

        verifyOrder {
            sqlGenerator.createTable(tempTableName, any(), true)
            sqlGenerator.createSnowflakeStage(tempTableName)
            sqlGenerator.cloneTableWith(tempTableName, realTableName)
            sqlGenerator.dropTable(tempTableName)
        }
    }

    @Test
    fun `concurrent puts wait for one complete table and stage initialization`() {
        val columnNameMapping = mockk<ColumnNameMapping>(relaxed = true)
        val stream = mockk<DestinationStream>(relaxed = true)
        val tableName = TableName(namespace = "namespace", name = "temp")
        val createStarted = CountDownLatch(1)
        val allowCreateToFinish = CountDownLatch(1)
        val putAttempted = CountDownLatch(1)
        val stageReady = AtomicBoolean(false)
        val resultSet = mockk<ResultSet>(relaxed = true)

        every { sqlGenerator.createTable(tableName, any(), true) } returns "create-table"
        every { sqlGenerator.createSnowflakeStage(tableName) } returns "create-stage"
        every { sqlGenerator.putInStage(tableName, "/tmp/one.csv") } returns "put-one"
        every { sqlGenerator.putInStage(tableName, "/tmp/two.csv") } returns "put-two"

        val statement =
            mockk<Statement>(relaxed = true) {
                every { executeQuery(any()) } answers
                    {
                        when (firstArg<String>()) {
                            "create-table" -> {
                                createStarted.countDown()
                                assertTrue(allowCreateToFinish.await(5, TimeUnit.SECONDS))
                            }
                            "create-stage" -> stageReady.set(true)
                            "put-one",
                            "put-two" -> {
                                putAttempted.countDown()
                                assertTrue(
                                    stageReady.get(),
                                    "PUT executed before stage creation completed"
                                )
                            }
                        }
                        resultSet
                    }
            }
        val connection =
            mockk<Connection>(relaxed = true) { every { createStatement() } returns statement }
        every { dataSource.connection } returns connection

        runBlocking { client.createTempTable(stream, tableName, columnNameMapping, replace = true) }

        val executor = Executors.newFixedThreadPool(2)
        try {
            val first = executor.submit { client.putInStage(tableName, "/tmp/one.csv") }
            assertTrue(createStarted.await(5, TimeUnit.SECONDS))
            val second = executor.submit { client.putInStage(tableName, "/tmp/two.csv") }

            assertFalse(
                putAttempted.await(250, TimeUnit.MILLISECONDS),
                "A concurrent PUT ran while stage creation was still in progress",
            )
            allowCreateToFinish.countDown()
            first.get(5, TimeUnit.SECONDS)
            second.get(5, TimeUnit.SECONDS)
        } finally {
            allowCreateToFinish.countDown()
            executor.shutdownNow()
        }

        verify(exactly = 1) { sqlGenerator.createTable(tableName, any(), true) }
        verify(exactly = 1) { sqlGenerator.createSnowflakeStage(tableName) }
        verify(exactly = 1) { sqlGenerator.putInStage(tableName, "/tmp/one.csv") }
        verify(exactly = 1) { sqlGenerator.putInStage(tableName, "/tmp/two.csv") }
    }

    @Test
    fun `non-empty dedup materializes deferred merge target before upsert`() {
        val columnNameMapping = mockk<ColumnNameMapping>(relaxed = true)
        val stream = mockk<DestinationStream>(relaxed = true)
        val sourceTableName = TableName(namespace = "namespace", name = "source")
        val targetTableName = TableName(namespace = "namespace", name = "target")
        val resultSet = mockk<ResultSet>(relaxed = true)
        val statement =
            mockk<Statement>(relaxed = true) { every { executeQuery(any()) } returns resultSet }
        val connection =
            mockk<Connection>(relaxed = true) { every { createStatement() } returns statement }
        every { dataSource.connection } returns connection

        runBlocking {
            client.createTempTable(stream, sourceTableName, columnNameMapping, replace = true)
            client.putInStage(sourceTableName, "/tmp/data.csv")
            client.createTempTable(stream, targetTableName, columnNameMapping, replace = true)
            client.upsertTable(stream, columnNameMapping, sourceTableName, targetTableName)
        }

        verifyOrder {
            sqlGenerator.createTable(sourceTableName, any(), true)
            sqlGenerator.createSnowflakeStage(sourceTableName)
            sqlGenerator.putInStage(sourceTableName, "/tmp/data.csv")
            sqlGenerator.createTable(targetTableName, any(), true)
            sqlGenerator.createSnowflakeStage(targetTableName)
            sqlGenerator.upsertTable(any(), sourceTableName, targetTableName)
        }
    }

    @Test
    fun testCopyTable() {
        val columnNameMapping = mockk<ColumnNameMapping>(relaxed = true)
        val sourceTableName = TableName(namespace = "namespace", name = "source")
        val destinationTableName = TableName(namespace = "namespace", name = "destination")
        val resultSet = mockk<ResultSet>(relaxed = true)
        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } returns resultSet
                every { close() } just Runs
            }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { createStatement() } returns statement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            client.copyTable(
                columnNameMapping = columnNameMapping,
                sourceTableName = sourceTableName,
                targetTableName = destinationTableName,
            )
            verify(exactly = 1) {
                sqlGenerator.copyTable(any<Set<String>>(), sourceTableName, destinationTableName)
            }
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun testUpsertTable() {
        val columnNameMapping = mockk<ColumnNameMapping>(relaxed = true)
        val sourceTableName = TableName(namespace = "namespace", name = "source")
        val destinationTableName = TableName(namespace = "namespace", name = "destination")
        val stream = mockk<DestinationStream>(relaxed = true)
        val resultSet = mockk<ResultSet>(relaxed = true)
        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } returns resultSet
                every { close() } just Runs
            }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { createStatement() } returns statement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            client.upsertTable(
                stream = stream,
                columnNameMapping = columnNameMapping,
                sourceTableName = sourceTableName,
                targetTableName = destinationTableName,
            )
            verify(exactly = 1) {
                sqlGenerator.upsertTable(any(), sourceTableName, destinationTableName)
            }
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun testDropTable() {
        val tableName = TableName(namespace = "namespace", name = "name")
        val resultSet = mockk<ResultSet>(relaxed = true)
        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } returns resultSet
                every { close() } just Runs
            }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { createStatement() } returns statement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            client.dropTable(tableName)
            verify(exactly = 1) { sqlGenerator.dropTable(tableName) }
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun testGetGenerationId() {
        val generationId = 2L
        val tableName = TableName(namespace = "namespace", name = "name")
        val generationIdColumnName = COLUMN_NAME_AB_GENERATION_ID.toSnowflakeCompatibleName()
        val resultSet =
            mockk<ResultSet> {
                every { next() } returns true
                every { getLong(generationIdColumnName) } returns generationId
            }
        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } returns resultSet
                every { close() } just Runs
            }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { createStatement() } returns statement
            }

        every { dataSource.connection } returns mockConnection
        every { columnManager.getGenerationIdColumnName() } returns generationIdColumnName
        every { sqlGenerator.getGenerationId(tableName) } returns
            "SELECT $generationIdColumnName FROM ${tableName.toPrettyString(QUOTE)}"

        runBlocking {
            val result = client.getGenerationId(tableName)
            assertEquals(generationId, result)
            verify(exactly = 1) { sqlGenerator.getGenerationId(tableName) }
            verify(exactly = 1) { statement.close() }
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun testGetGenerationIdNoResult() {
        val tableName = TableName(namespace = "namespace", name = "name")
        val resultSet = mockk<ResultSet> { every { next() } returns false }
        val statement = mockk<Statement> { every { executeQuery(any()) } returns resultSet }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { createStatement() } returns statement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            val result = client.getGenerationId(tableName)
            assertEquals(0L, result)
            verify(exactly = 1) { sqlGenerator.getGenerationId(tableName) }
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun testGetGenerationIdError() {
        val tableName = TableName(namespace = "namespace", name = "name")
        val resultSet = mockk<ResultSet> { every { next() } throws SQLException("error") }
        val statement = mockk<Statement> { every { executeQuery(any()) } returns resultSet }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { createStatement() } returns statement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            val result = client.getGenerationId(tableName)
            assertEquals(0L, result)
            verify(exactly = 1) { sqlGenerator.getGenerationId(tableName) }
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun testCreateStaging() {
        val tableName = TableName(namespace = "namespace", name = "name")
        val resultSet = mockk<ResultSet>(relaxed = true)
        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } returns resultSet
                every { close() } just Runs
            }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { createStatement() } returns statement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            client.createSnowflakeStage(tableName)
            verify(exactly = 1) { sqlGenerator.createSnowflakeStage(tableName) }
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun testPutInStaging() {
        val tableName = TableName(namespace = "namespace", name = "name")
        val tempFilePath = "/some/file/path.csv"
        val resultSet = mockk<ResultSet>(relaxed = true)
        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } returns resultSet
                every { close() } just Runs
            }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { createStatement() } returns statement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            client.putInStage(tableName, tempFilePath)
            verify(exactly = 1) { sqlGenerator.putInStage(tableName, tempFilePath) }
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun testCopyFromStaging() {
        val tableName = TableName(namespace = "namespace", name = "name")
        val resultSet = mockk<ResultSet>(relaxed = true)
        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } returns resultSet
                every { close() } just Runs
            }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { createStatement() } returns statement
            }

        every { dataSource.connection } returns mockConnection

        runBlocking {
            client.copyFromStage(tableName, "test.csv.gz", listOf())
            verify(exactly = 1) { sqlGenerator.copyFromStage(tableName, "test.csv.gz", listOf()) }
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun testDescribeTable() {
        val tableName = TableName(namespace = "namespace", name = "name")
        val column1 = "column1"
        val column1Type = """{"type":"VARIANT","nullable":false}"""
        val column2 = "column2"
        val column2Type =
            """{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":false,"fixed":false}"""
        val resultSet =
            mockk<ResultSet> {
                every { next() } returns true andThen true andThen false
                every { getString(DESCRIBE_TABLE_COLUMN_NAME_FIELD) } returns
                    column1 andThen
                    column2
                every { getString(DESCRIBE_TABLE_COLUMN_TYPE_FIELD) } returns
                    column1Type andThen
                    column2Type
            }
        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } returns resultSet
                every { close() } just Runs
            }
        val mockConnection =
            mockk<Connection> {
                every { close() } just Runs
                every { createStatement() } returns statement
            }
        val expectedColumns = linkedMapOf(column1 to "VARIANT", column2 to "TEXT")

        every { dataSource.connection } returns mockConnection

        runBlocking {
            val columns = client.describeTable(tableName)
            assertEquals(expectedColumns, columns)
            verify(exactly = 1) { sqlGenerator.showColumns(tableName) }
            verify(exactly = 1) { mockConnection.close() }
        }
    }

    @Test
    fun `getColumnsFromDb should return correct column definitions`() {
        val tableName = TableName("test_namespace", "test_table")
        val resultSet = mockk<ResultSet>()
        every { resultSet.next() } returns true andThen true andThen true andThen false
        every { resultSet.getString("name") } returns
            "COL1" andThen
            COLUMN_NAME_AB_RAW_ID.toSnowflakeCompatibleName() andThen
            "COL2"
        every { resultSet.getString("type") } returns "VARCHAR(255)" andThen "NUMBER"
        every { resultSet.getString("null?") } returns "Y" andThen "N" andThen "N"

        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } returns resultSet
                every { close() } just Runs
            }

        val connection = mockk<Connection>()
        every { connection.createStatement() } returns statement
        every { connection.close() } just Runs

        every { dataSource.connection } returns connection

        // Mock the columnManager to return the correct set of meta columns
        every { columnManager.getMetaColumnNames() } returns
            setOf(COLUMN_NAME_AB_RAW_ID.toSnowflakeCompatibleName())

        val result = client.getColumnsFromDb(tableName)

        val expectedColumns =
            mapOf(
                "COL1" to ColumnType("VARCHAR", true),
                "COL2" to ColumnType("NUMBER", false),
            )

        assertEquals(expectedColumns, result)
    }

    @Test
    fun testCreateNamespaceWithNetworkFailure() {
        val namespace = "test_namespace"
        val sql = "CREATE SCHEMA test_namespace"

        every { sqlGenerator.createNamespace(namespace) } returns sql

        // Mock for schema check - should fail and throw exception
        val schemaCheckResultSet =
            mockk<ResultSet> {
                every { next() } returns true
                every { getBoolean("SCHEMA_EXISTS") } returns false
                every { close() } just Runs
            }

        val preparedStatement =
            mockk<PreparedStatement>(relaxed = true) {
                every { executeQuery() } returns schemaCheckResultSet
                every { close() } just Runs
            }

        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } throws SQLException("Network error", "08S01")
                every { close() } just Runs
            }

        val connection =
            mockk<Connection> {
                every { createStatement() } returns statement
                every { prepareStatement(any()) } returns preparedStatement
                every { close() } just Runs
            }

        every { dataSource.connection } returns connection

        runBlocking {
            try {
                client.createNamespace(namespace)
                assert(false) { "Expected SQLException" }
            } catch (e: SQLException) {
                assertEquals("Network error", e.message)
                assertEquals("08S01", e.sqlState)
            }
            verify(exactly = 1) { preparedStatement.close() }
            verify(exactly = 1) { statement.close() }
            verify(exactly = 2) { connection.close() }
        }
    }

    @Test
    fun testCountTableWithClosedConnection() {
        val tableName = TableName("namespace", "table")
        val sql = "SELECT COUNT(*) FROM namespace.table"

        every { sqlGenerator.countTable(tableName) } returns sql

        val connection = mockk<Connection>()

        every { dataSource.connection } returns connection
        every { connection.isClosed } returns true
        every { connection.close() } just Runs

        runBlocking {
            try {
                client.countTable(tableName)
                assert(false) { "Expected error for closed connection" }
            } catch (_: Exception) {
                // Expected - connection was closed
            }
        }
    }

    @Test
    fun testExecuteWithTransientNetworkError() {
        val connection = mockk<Connection>()
        val statement = mockk<Statement>()
        val sql = "INSERT INTO table VALUES (1)"

        every { dataSource.connection } returns connection
        every { connection.createStatement() } returns statement
        every { statement.close() } just Runs

        // Simulate transient network error (typically retryable)
        every { statement.executeQuery(sql) } throws
            SnowflakeSQLException("Request reached its timeout", "HY000", 390114)
        every { statement.close() } just Runs
        every { connection.close() } just Runs

        try {
            client.execute(sql)
            assert(false) { "Expected SnowflakeSQLException" }
        } catch (e: SnowflakeSQLException) {
            assertEquals(390114, e.errorCode) // NETWORK_ERROR
            // In production, this would typically trigger a retry
        }
    }

    @Test
    fun testExecuteWithNoPrivilegesError() {
        val connection = mockk<Connection>()
        val statement = mockk<Statement>()
        val sql = "CREATE TABLE test_table (id INT)"

        every { dataSource.connection } returns connection
        every { connection.createStatement() } returns statement
        every { statement.close() } just Runs

        // Simulate permission error matching the user's case
        every { statement.executeQuery(sql) } throws
            SnowflakeSQLException(
                "SQL compilation error:\n" +
                    "Table 'APXT_REDLINING__CONTRACT_AGREEMENT__HISTORY' already exists, " +
                    "but current role has no privileges on it. " +
                    "If this is unexpected and you cannot resolve this problem, " +
                    "contact your system administrator. " +
                    "ACCOUNTADMIN role may be required to manage the privileges on the object."
            )
        every { connection.close() } just Runs

        val exception = assertThrows<ConfigErrorException> { client.execute(sql) }

        // Verify the error message was wrapped as ConfigErrorException with original message
        assertTrue(exception.message!!.contains("current role has no privileges on it"))
        // Verify the cause is the original SnowflakeSQLException
        assertTrue(exception.cause is SnowflakeSQLException)
    }

    @Test
    fun testExecuteWithNonPermissionError() {
        val connection = mockk<Connection>()
        val statement = mockk<Statement>()
        val sql = "SELECT * FROM nonexistent_table"

        every { dataSource.connection } returns connection
        every { connection.createStatement() } returns statement
        every { statement.close() } just Runs

        // Simulate non-permission error (e.g., table not found)
        every { statement.executeQuery(sql) } throws
            SnowflakeSQLException("Table 'NONEXISTENT_TABLE' does not exist")
        every { connection.close() } just Runs

        // Non-permission errors should be thrown as-is, not wrapped
        val exception = assertThrows<SnowflakeSQLException> { client.execute(sql) }

        assertEquals("Table 'NONEXISTENT_TABLE' does not exist", exception.message)
    }

    @Test
    fun testCreateNamespaceWithPermissionError() {
        val namespace = "test_namespace"
        val sql = "CREATE SCHEMA test_namespace"

        every { sqlGenerator.createNamespace(namespace) } returns sql

        // Mock for schema check - returns false (schema doesn't exist)
        val schemaCheckResultSet =
            mockk<ResultSet> {
                every { next() } returns true
                every { getBoolean("SCHEMA_EXISTS") } returns false
                every { close() } just Runs
            }

        val preparedStatement =
            mockk<PreparedStatement>(relaxed = true) {
                every { executeQuery() } returns schemaCheckResultSet
                every { close() } just Runs
            }

        val statement =
            mockk<Statement> {
                every { executeQuery(any()) } throws
                    SnowflakeSQLException(
                        "Schema 'TEST_NAMESPACE' already exists, but current role has no privileges on it"
                    )
                every { close() } just Runs
            }

        val connection =
            mockk<Connection> {
                every { createStatement() } returns statement
                every { prepareStatement(any()) } returns preparedStatement
                every { close() } just Runs
            }

        every { dataSource.connection } returns connection

        runBlocking {
            val exception = assertThrows<ConfigErrorException> { client.createNamespace(namespace) }

            assertTrue(exception.message!!.contains("current role has no privileges on it"))
            assertTrue(exception.cause is SnowflakeSQLException)
        }
    }

    // ---------------------------------------------------------------------------------------
    // Fingerprint-gated ensureSchemaMatches
    // ---------------------------------------------------------------------------------------

    private val fpFinalTable = TableName(namespace = "NS", name = "USERS")
    private val fpTempTable = TableName(namespace = "airbyte_internal", name = "NSUSERSdeadbeef")
    private val baseColumns =
        linkedMapOf(
            "ID" to ColumnType("NUMBER", true),
            "NAME" to ColumnType("VARCHAR", true),
        )
    private val mapping = ColumnNameMapping(emptyMap())

    /** Fake final/temp table state backing DESCRIBE, plus a log of every executed statement. */
    private class SchemaDb {
        val columns = linkedMapOf<String, ColumnType>()
        val executed: MutableList<String> = Collections.synchronizedList(mutableListOf())
        var commentFailure: SQLException? = null
        var alterFails = false
        var storedComment: String? = null
        val plannedColumns = mutableMapOf<String, ColumnType>()
        var showTablesRows: List<Triple<String, Long, String?>> = emptyList()

        fun count(prefix: String) = executed.count { it.startsWith(prefix) }
    }

    private fun installSchemaDb(): SchemaDb {
        val db = SchemaDb()
        every { sqlGenerator.describeTable(any(), any()) } answers
            {
                "DESCRIBE ${firstArg<String>()}.${secondArg<String>()}"
            }
        every { sqlGenerator.commentOnTable(any(), any()) } answers
            {
                "COMMENT ${firstArg<TableName>().name} ${secondArg<String>()}"
            }
        every { sqlGenerator.alterTable(any(), any(), any()) } answers
            {
                val added = secondArg<Map<String, ColumnType>>()
                val changed = thirdArg<Map<String, ColumnTypeChange>>()
                db.plannedColumns.putAll(added)
                db.plannedColumns.putAll(changed.mapValues { it.value.newType })
                (added.keys.map { "ALTER ADD $it" } + changed.keys.map { "ALTER CHANGE $it" })
                    .toSet()
            }
        every { sqlGenerator.createTable(any(), any(), any()) } answers
            {
                "CREATE ${firstArg<TableName>().name}"
            }
        every { sqlGenerator.createSnowflakeStage(any()) } answers
            {
                "STAGE ${firstArg<TableName>().name}"
            }
        every { sqlGenerator.putInStage(any(), any()) } answers
            {
                "PUT ${firstArg<TableName>().name}"
            }
        every { sqlGenerator.upsertTable(any(), any(), any()) } returns "UPSERT"
        every { sqlGenerator.dropTable(any()) } answers { "DROP ${firstArg<TableName>().name}" }
        every { sqlGenerator.showTablesInSchema(any()) } answers { "SHOW IN ${firstArg<String>()}" }
        every { sqlGenerator.showTablesStartsWith(any(), any()) } answers
            {
                "SHOW STARTS WITH ${firstArg<String>()}"
            }

        val statement =
            mockk<Statement>(relaxed = true) {
                every { executeQuery(any()) } answers
                    {
                        val sql = firstArg<String>()
                        db.executed.add(sql)
                        when {
                            sql.startsWith("DESCRIBE") -> describeResultSet(db.columns)
                            sql.startsWith("ALTER") -> {
                                if (db.alterFails) throw SQLException("DDL was rejected")
                                val column = sql.substringAfterLast(' ')
                                db.columns[column] = db.plannedColumns.getValue(column)
                                mockk(relaxed = true)
                            }
                            sql.startsWith("COMMENT") -> {
                                db.commentFailure?.let { throw it }
                                db.storedComment = sql.substringAfterLast(' ')
                                mockk(relaxed = true)
                            }
                            sql.startsWith("SHOW IN") -> showResultSet(db.showTablesRows)
                            sql.startsWith("SHOW STARTS WITH") -> showResultSet(emptyList())
                            else -> mockk(relaxed = true)
                        }
                    }
            }
        val connection =
            mockk<Connection>(relaxed = true) { every { createStatement() } returns statement }
        every { dataSource.connection } returns connection
        return db
    }

    private fun describeResultSet(columns: Map<String, ColumnType>): ResultSet {
        val rows = columns.entries.toList()
        val index = AtomicInteger(-1)
        return mockk(relaxed = true) {
            every { next() } answers { index.incrementAndGet() < rows.size }
            every { getString("name") } answers { rows[index.get()].key }
            every { getString("type") } answers { rows[index.get()].value.type }
            every { getString("null?") } answers
                {
                    if (rows[index.get()].value.nullable) "Y" else "N"
                }
        }
    }

    private fun showResultSet(rows: List<Triple<String, Long, String?>>): ResultSet {
        val index = AtomicInteger(-1)
        return mockk(relaxed = true) {
            every { next() } answers { index.incrementAndGet() < rows.size }
            every { getString("name") } answers { rows[index.get()].first }
            every { getLong("rows") } answers { rows[index.get()].second }
            every { getString("comment") } answers { rows[index.get()].third }
        }
    }

    private fun schemaStream(columns: Map<String, ColumnType>): DestinationStream =
        mockk(relaxed = true) {
            every { tableSchema } returns
                StreamTableSchema(
                    tableNames =
                        TableNames(finalTableName = fpFinalTable, tempTableName = fpTempTable),
                    columnSchema =
                        ColumnSchema(
                            inputSchema = emptyMap(),
                            inputToFinalColumnNames = emptyMap(),
                            finalSchema = columns,
                        ),
                    importType = Append,
                )
            every { mappedDescriptor } returns DestinationStream.Descriptor("NS", "USERS")
        }

    private fun fingerprintOf(columns: Map<String, ColumnType>) =
        client.computeFingerprint(TableSchema(columns))

    private fun withEmail(columns: Map<String, ColumnType>) =
        LinkedHashMap(columns).apply { put("EMAIL", ColumnType("VARCHAR", true)) }

    @Test
    fun testEnsureSchemaMatchesSkipsWhenFingerprintMatchesAndTempPending() {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        val stream = schemaStream(baseColumns)
        client.lastBatchComments[fpFinalTable] = fingerprintOf(baseColumns)

        runBlocking {
            client.createTempTable(stream, fpTempTable, mapping, replace = true)
            client.ensureSchemaMatches(stream, fpFinalTable, mapping)
        }

        assertEquals(emptyList<String>(), db.executed.toList())
        assertEquals(1L, client.schemaCheckSkipCount.get())
    }

    @Test
    fun testEnsureSchemaMatchesReconcilesWhenFingerprintMissing() {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        val stream = schemaStream(baseColumns)
        val expected = fingerprintOf(baseColumns)

        runBlocking {
            client.createTempTable(stream, fpTempTable, mapping, replace = true)
            client.ensureSchemaMatches(stream, fpFinalTable, mapping)
        }

        assertEquals(listOf("DESCRIBE NS.USERS", "COMMENT USERS $expected"), db.executed.toList())
        assertEquals(expected, client.lastBatchComments[fpFinalTable])
        assertEquals(0L, client.schemaCheckSkipCount.get())
    }

    @Test
    fun testEnsureSchemaMatchesReconcilesWhenFingerprintDiffers() {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        val stream = schemaStream(baseColumns)
        val expected = fingerprintOf(baseColumns)
        client.lastBatchComments[fpFinalTable] =
            fingerprintOf(mapOf("ID" to ColumnType("NUMBER", true)))

        runBlocking {
            client.createTempTable(stream, fpTempTable, mapping, replace = true)
            client.ensureSchemaMatches(stream, fpFinalTable, mapping)
        }

        assertEquals(listOf("DESCRIBE NS.USERS", "COMMENT USERS $expected"), db.executed.toList())
        assertEquals(expected, client.lastBatchComments[fpFinalTable])
        assertEquals(0L, client.schemaCheckSkipCount.get())
    }

    @Test
    fun testEnsureSchemaMatchesReconcilesWhenCommentNotInExpectedFormat() {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        val stream = schemaStream(baseColumns)
        val expected = fingerprintOf(baseColumns)
        // The comment carries the right hash but not the versioned prefix (e.g. a user comment).
        db.showTablesRows =
            listOf(Triple("USERS", 5L, expected.removePrefix(SCHEMA_FINGERPRINT_PREFIX)))

        runBlocking {
            client.batchGatherTableStatus(listOf(stream))
            assertNull(client.lastBatchComments[fpFinalTable])
            client.createTempTable(stream, fpTempTable, mapping, replace = true)
            client.ensureSchemaMatches(stream, fpFinalTable, mapping)
        }

        assertEquals(1, db.count("DESCRIBE"))
        assertEquals(1, db.count("COMMENT USERS $expected"))
        assertEquals(0L, client.schemaCheckSkipCount.get())
    }

    @Test
    fun testEnsureSchemaMatchesReconcilesWhenTempNotPending() {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        val stream = schemaStream(baseColumns)
        client.lastBatchComments[fpFinalTable] = fingerprintOf(baseColumns)

        runBlocking {
            client.createTempTable(stream, fpTempTable, mapping, replace = true)
            // A record was written, so the temp table is materialized.
            client.putInStage(fpTempTable, "/tmp/data.csv")
            client.ensureSchemaMatches(stream, fpFinalTable, mapping)
        }

        assertEquals(1, db.count("DESCRIBE NS.USERS"))
        // The fingerprint already matches, so the comment is not rewritten.
        assertEquals(0, db.count("COMMENT"))
        assertEquals(0L, client.schemaCheckSkipCount.get())
    }

    @Test
    fun testEnsureSchemaMatchesSkipDoesNotApplyToTempTable() {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        val stream = schemaStream(baseColumns)
        val expected = fingerprintOf(baseColumns)
        client.lastBatchComments[fpFinalTable] = expected
        client.lastBatchComments[fpTempTable] = expected

        runBlocking {
            client.createTempTable(stream, fpTempTable, mapping, replace = true)
            client.ensureSchemaMatches(stream, fpTempTable, mapping)
        }

        assertEquals(listOf("DESCRIBE airbyte_internal.NSUSERSdeadbeef"), db.executed.toList())
        assertEquals(0L, client.schemaCheckSkipCount.get())
    }

    @Test
    fun testCommentWrittenOnlyWhenFingerprintChanges() {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        client.lastBatchComments[fpFinalTable] = fingerprintOf(baseColumns)

        // Temp not pending (records written): reconcile runs, but the comment is already right.
        runBlocking { client.ensureSchemaMatches(schemaStream(baseColumns), fpFinalTable, mapping) }
        assertEquals(0, db.count("COMMENT"))

        val newColumns = withEmail(baseColumns)
        val changed = schemaStream(newColumns)
        runBlocking {
            client.ensureSchemaMatches(changed, fpFinalTable, mapping)
            client.ensureSchemaMatches(changed, fpFinalTable, mapping)
        }

        assertEquals(
            listOf("COMMENT USERS ${fingerprintOf(newColumns)}"),
            db.executed.filter { it.startsWith("COMMENT") },
        )
        assertEquals(3, db.count("DESCRIBE"))
    }

    @Test
    fun testFingerprintDeterministicAndOrderedByColumnName() {
        val forward =
            linkedMapOf(
                "A_COL" to ColumnType("VARCHAR", true),
                "B_COL" to ColumnType("NUMBER", false),
            )
        val reversed =
            linkedMapOf(
                "B_COL" to ColumnType("NUMBER", false),
                "A_COL" to ColumnType("VARCHAR", true),
            )
        val fingerprint = fingerprintOf(forward)

        val sha =
            MessageDigest.getInstance("SHA-256")
                .digest("A_COL|VARCHAR|true\nB_COL|NUMBER|false".toByteArray(Charsets.UTF_8))
                .joinToString("") { "%02x".format(it) }
        assertEquals("airbyte-schema:v1:${sha.take(16)}", fingerprint)
        assertTrue(Regex("^airbyte-schema:v1:[0-9a-f]{16}$").matches(fingerprint))
        assertEquals(fingerprint, fingerprintOf(reversed))
        assertEquals(fingerprint, fingerprintOf(LinkedHashMap(forward)))
        assertTrue(
            fingerprint !=
                fingerprintOf(
                    linkedMapOf(
                        "A_COL" to ColumnType("VARCHAR", false),
                        "B_COL" to ColumnType("NUMBER", false),
                    )
                ),
            "nullability must affect the fingerprint",
        )
        assertTrue(
            fingerprint !=
                fingerprintOf(
                    linkedMapOf(
                        "A_COL" to ColumnType("VARIANT", true),
                        "B_COL" to ColumnType("NUMBER", false),
                    )
                ),
            "type must affect the fingerprint",
        )
    }

    private fun runZeroRecordTruncateSync(storedFingerprint: String?): SchemaDb {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        val stream = schemaStream(baseColumns)
        storedFingerprint?.let { client.lastBatchComments[fpFinalTable] = it }
        val loader =
            DirectLoadTableDedupTruncateStreamLoader(
                stream,
                DirectLoadInitialStatus(
                    realTable = DirectLoadTableStatus(isEmpty = true),
                    tempTable = null,
                ),
                realTableName = fpFinalTable,
                tempTableName = fpTempTable,
                mapping,
                client,
                client,
                StreamStateStore(),
                DefaultTempTableNameGenerator("airbyte_internal"),
            )
        runBlocking {
            loader.start()
            loader.teardown(completedSuccessfully = true)
        }
        return db
    }

    @Test
    fun testTruncateModeSkipsOnlyWithMatchingFingerprint() {
        val matching = runZeroRecordTruncateSync(fingerprintOf(baseColumns))
        assertEquals(emptyList<String>(), matching.executed.toList())
        assertEquals(1L, client.schemaCheckSkipCount.get())

        setup()
        val stale =
            runZeroRecordTruncateSync(fingerprintOf(mapOf("ID" to ColumnType("NUMBER", true))))
        assertEquals(1, stale.count("DESCRIBE NS.USERS"))
        assertEquals(1, stale.count("COMMENT USERS ${fingerprintOf(baseColumns)}"))
        assertEquals(0L, client.schemaCheckSkipCount.get())

        setup()
        val missing = runZeroRecordTruncateSync(null)
        assertEquals(1, missing.count("DESCRIBE NS.USERS"))
        assertEquals(0L, client.schemaCheckSkipCount.get())
    }

    private fun dedupLoader(stream: DestinationStream) =
        DirectLoadTableDedupStreamLoader(
            stream,
            DirectLoadInitialStatus(
                realTable = DirectLoadTableStatus(isEmpty = false),
                tempTable = null,
            ),
            realTableName = fpFinalTable,
            tempTableName = fpTempTable,
            mapping,
            client,
            client,
            StreamStateStore(),
        )

    @Test
    fun testPropagateColumnsFirstNonEmptySyncAfterCatalogChange() {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        // The table was fingerprinted for the old catalog; the new catalog adds EMAIL.
        client.lastBatchComments[fpFinalTable] = fingerprintOf(baseColumns)
        val newColumns = withEmail(baseColumns)
        val loader = dedupLoader(schemaStream(newColumns))

        runBlocking {
            loader.start()
            client.putInStage(fpTempTable, "/tmp/data.csv")
            loader.teardown(completedSuccessfully = true)
        }

        val executed = db.executed.toList()
        assertTrue(
            executed.indexOf("ALTER ADD EMAIL") in 0 until executed.indexOf("UPSERT"),
            "$executed"
        )
        assertEquals(1, db.count("COMMENT USERS ${fingerprintOf(newColumns)}"))
        assertEquals(fingerprintOf(newColumns), client.lastBatchComments[fpFinalTable])
        assertEquals(0L, client.schemaCheckSkipCount.get())
    }

    @Test
    fun testCatalogChangeOnZeroRecordStreamReconciles() {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        // Stale fingerprint + zero records: the temp table stays pending, but must not skip.
        client.lastBatchComments[fpFinalTable] = fingerprintOf(baseColumns)
        val newColumns = withEmail(baseColumns)
        val loader = dedupLoader(schemaStream(newColumns))

        runBlocking {
            loader.start()
            loader.teardown(completedSuccessfully = true)
        }

        assertEquals(
            listOf(
                "DESCRIBE NS.USERS",
                "ALTER ADD EMAIL",
                "COMMENT USERS ${fingerprintOf(newColumns)}",
            ),
            db.executed.toList(),
        )
        assertEquals(0L, client.schemaCheckSkipCount.get())
    }

    @Test
    fun testSchemaFailureLeavesBothFingerprintsUnchangedAndRetrySucceeds() {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        val oldFingerprint = fingerprintOf(baseColumns)
        db.storedComment = oldFingerprint
        client.lastBatchComments[fpFinalTable] = oldFingerprint
        val changedColumns = withEmail(baseColumns)
        val changedStream = schemaStream(changedColumns)
        db.alterFails = true

        val exception =
            assertThrows<SQLException> {
                runBlocking {
                    val loader = dedupLoader(changedStream)
                    loader.start()
                    loader.teardown(completedSuccessfully = true)
                }
            }

        assertEquals("DDL was rejected", exception.message)
        assertEquals(baseColumns, db.columns)
        assertEquals(oldFingerprint, db.storedComment)
        assertEquals(oldFingerprint, client.lastBatchComments[fpFinalTable])
        assertEquals(0, db.count("COMMENT"))
        assertEquals(0, db.count("UPSERT"))

        db.alterFails = false
        runBlocking {
            val loader = dedupLoader(changedStream)
            loader.start()
            loader.teardown(completedSuccessfully = true)
        }

        val expected = fingerprintOf(changedColumns)
        assertEquals(changedColumns, runBlocking { client.discoverSchema(fpFinalTable) }.columns)
        assertEquals(expected, db.storedComment)
        assertEquals(expected, client.lastBatchComments[fpFinalTable])
        assertEquals(1, db.count("COMMENT"))

        // A later run recovers the persisted marker instead of relying on the old cache.
        client.lastBatchComments.clear()
        db.showTablesRows = listOf(Triple("USERS", 1L, db.storedComment))
        runBlocking {
            client.batchGatherTableStatus(listOf(changedStream))
            val loader = dedupLoader(changedStream)
            loader.start()
            loader.teardown(completedSuccessfully = true)
        }
        assertEquals(db.storedComment, client.lastBatchComments[fpFinalTable])
        assertEquals(1L, client.schemaCheckSkipCount.get())
        assertEquals(1, db.count("COMMENT"))
    }

    @Test
    fun testCommentPermissionFailureStopsSyncAndRetryUpdatesBothMarkers() {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        val oldFingerprint = fingerprintOf(baseColumns)
        db.storedComment = oldFingerprint
        client.lastBatchComments[fpFinalTable] = oldFingerprint
        db.commentFailure =
            SnowflakeSQLException(
                "Table 'USERS' already exists, but current role has no privileges on it"
            )
        val changedColumns = withEmail(baseColumns)
        val changedStream = schemaStream(changedColumns)

        val exception =
            assertThrows<ConfigErrorException> {
                runBlocking {
                    val loader = dedupLoader(changedStream)
                    loader.start()
                    loader.teardown(completedSuccessfully = true)
                }
            }

        assertTrue(exception.message!!.contains("USERS"))
        assertTrue(exception.message!!.contains("test_database", ignoreCase = true))
        assertTrue(exception.cause!!.cause is SnowflakeSQLException)
        assertEquals(changedColumns, runBlocking { client.discoverSchema(fpFinalTable) }.columns)
        assertEquals(oldFingerprint, db.storedComment)
        assertEquals(oldFingerprint, client.lastBatchComments[fpFinalTable])
        assertEquals(0, db.count("UPSERT"))

        db.commentFailure = null
        runBlocking {
            val loader = dedupLoader(changedStream)
            loader.start()
            loader.teardown(completedSuccessfully = true)
        }

        val expected = fingerprintOf(changedColumns)
        assertEquals(expected, db.storedComment)
        assertEquals(expected, client.lastBatchComments[fpFinalTable])
        assertEquals(1, db.count("ALTER ADD EMAIL"))
        assertEquals(2, db.count("COMMENT"))
    }

    @Test
    fun testCommentSqlFailureNamesTableAndPreservesCause() {
        val db = installSchemaDb()
        db.columns.putAll(baseColumns)
        val cause = SQLException("connection unavailable")
        db.commentFailure = cause

        val exception =
            assertThrows<IllegalStateException> {
                runBlocking {
                    client.ensureSchemaMatches(schemaStream(baseColumns), fpFinalTable, mapping)
                }
            }

        assertTrue(exception.message!!.contains("USERS"))
        assertTrue(exception.message!!.contains("test_database", ignoreCase = true))
        assertEquals(cause, exception.cause)
        assertNull(db.storedComment)
        assertNull(client.lastBatchComments[fpFinalTable])
    }
}
