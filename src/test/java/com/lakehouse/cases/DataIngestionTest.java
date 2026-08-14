package com.lakehouse.cases;

import com.lakehouse.base.BaseTest;
import org.junit.jupiter.api.Test;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Level A: Functional Data Correctness - Ingestion Integrity Tests
 * Validates table creation, record ingestion, and basic query assertions via
 * Trino on Apache Iceberg.
 */
public class DataIngestionTest extends BaseTest {

    private static final String TABLE_NAME = "iceberg.default.customer_clicks";

    @Test
    public void testSuccessfulDataIngestionAndVerification() throws SQLException {
        // Step 1: Clean up any previous test runs
        dropTableIfExists(TABLE_NAME);

        // Step 2: Create a new Apache Iceberg Table via Trino SQL
        String createTableSql = "CREATE TABLE " + TABLE_NAME + " (\n" +
                "    event_id VARCHAR,\n" +
                "    user_id VARCHAR,\n" +
                "    event_type VARCHAR,\n" +
                "    click_timestamp TIMESTAMP\n" +
                ") WITH (\n" +
                "    format = 'PARQUET'\n" +
                ")";
        trinoClient.executeUpdate(createTableSql);

        // Step 3: Insert Mock Telemetry Events (Simulating Flink/Spark batch writes)
        String insertSql = "INSERT INTO " + TABLE_NAME + " VALUES \n" +
                "('evt_001', 'alex_99', 'PlaybackStarted', TIMESTAMP '2026-08-12 10:00:00'),\n" +
                "('evt_002', 'alex_99', 'PlaybackPaused',  TIMESTAMP '2026-08-12 10:05:00'),\n" +
                "('evt_003', 'sam_22',  'PlaybackStarted', TIMESTAMP '2026-08-12 10:10:00')";
        trinoClient.executeUpdate(insertSql);

        // Step 4: Query the table and validate contents
        String selectSql = "SELECT event_id, user_id, event_type FROM " + TABLE_NAME + " ORDER BY event_id ASC";

        List<ClickRecord> records = new ArrayList<>();
        try (Statement stmt = trinoClient.getConnection().createStatement();
                ResultSet rs = stmt.executeQuery(selectSql)) {
            while (rs.next()) {
                records.add(new ClickRecord(
                        rs.getString("event_id"),
                        rs.getString("user_id"),
                        rs.getString("event_type")));
            }
        }

        // Step 5: Assertions using AssertJ
        assertThat(records)
                .as("Verify that exactly 3 events were ingested and retrieved")
                .hasSize(3);

        assertThat(records.get(0).eventId).isEqualTo("evt_001");
        assertThat(records.get(0).userId).isEqualTo("alex_99");
        assertThat(records.get(0).eventType).isEqualTo("PlaybackStarted");

        assertThat(records.get(2).userId)
                .as("Verify sam_22's click was written correctly")
                .isEqualTo("sam_22");

        // Clean up table after test completes successfully
        dropTableIfExists(TABLE_NAME);
    }

    private static class ClickRecord {
        String eventId;
        String userId;
        String eventType;

        ClickRecord(String eventId, String userId, String eventType) {
            this.eventId = eventId;
            this.userId = userId;
            this.eventType = eventType;
        }
    }
}