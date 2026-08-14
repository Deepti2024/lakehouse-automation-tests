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
 * Level B: Schema Evolution Safety Tests
 * Validates Apache Iceberg's superpower of in-place, metadata-only schema
 * evolution.
 * Verifies that adding columns does not break existing data files or query
 * execution,
 * and that historical records safely resolve the new column as NULL.
 */
public class SchemaEvolutionTest extends BaseTest {

    private static final String TABLE_NAME = "iceberg.default.customer_profiles";

    @Test
    public void testInPlaceSchemaEvolutionWithoutDataLoss() throws SQLException {
        // Step 1: Clean up any previous test runs
        dropTableIfExists(TABLE_NAME);

        // Step 2: Create a Table with v1 Schema (2 Columns)
        String createTableSql = "CREATE TABLE " + TABLE_NAME + " (\n" +
                "    user_id VARCHAR,\n" +
                "    email VARCHAR\n" +
                ") WITH (\n" +
                "    format = 'PARQUET'\n" +
                ")";
        trinoClient.executeUpdate(createTableSql);

        // Step 3: Insert v1 Data Record
        String insertV1Sql = "INSERT INTO " + TABLE_NAME + " VALUES ('usr_201', 'usr201@netflix.com')";
        trinoClient.executeUpdate(insertV1Sql);

        // Step 4: Evolve the Schema by Adding a Column (In-place metadata change)
        String alterTableSql = "ALTER TABLE " + TABLE_NAME + " ADD COLUMN country VARCHAR";
        trinoClient.executeUpdate(alterTableSql);

        // Step 5: Insert v2 Data Record (With the new column populated)
        String insertV2Sql = "INSERT INTO " + TABLE_NAME
                + " (user_id, email, country) VALUES ('usr_202', 'usr202@netflix.com', 'CA')";
        trinoClient.executeUpdate(insertV2Sql);

        // Step 6: Query both v1 and v2 records and validate Schema Evolution behavior
        String selectSql = "SELECT user_id, email, country FROM " + TABLE_NAME + " ORDER BY user_id ASC";

        List<ProfileRecord> profiles = new ArrayList<>();
        try (Statement stmt = trinoClient.getConnection().createStatement();
                ResultSet rs = stmt.executeQuery(selectSql)) {
            while (rs.next()) {
                profiles.add(new ProfileRecord(
                        rs.getString("user_id"),
                        rs.getString("email"),
                        rs.getString("country")));
            }
        }

        // Step 7: Assertions using AssertJ
        assertThat(profiles)
                .as("Verify both pre-evolution and post-evolution records are retrieved")
                .hasSize(2);

        // Assert v1 Record: The country column must be NULL because it didn't exist
        // when the row was written
        ProfileRecord v1Record = profiles.get(0);
        assertThat(v1Record.userId).isEqualTo("usr_201");
        assertThat(v1Record.email).isEqualTo("usr201@netflix.com");
        assertThat(v1Record.country)
                .as("Apache Iceberg must backfill missing columns for historical files as NULL")
                .isNull();

        // Assert v2 Record: The country column must contain 'CA'
        ProfileRecord v2Record = profiles.get(1);
        assertThat(v2Record.userId).isEqualTo("usr_202");
        assertThat(v2Record.email).isEqualTo("usr202@netflix.com");
        assertThat(v2Record.country)
                .as("New records must successfully write to the evolved column")
                .isEqualTo("CA");

        // Clean up table after test completes successfully
        dropTableIfExists(TABLE_NAME);
    }

    private static class ProfileRecord {
        String userId;
        String email;
        String country;

        ProfileRecord(String userId, String email, String country) {
            this.userId = userId;
            this.email = email;
            this.country = country;
        }
    }
}
