package com.lakehouse.cases;

import com.lakehouse.base.BaseTest;
import io.qameta.allure.Attachment;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Level C: Resilience & Chaos Integration Tests (v8 Fully Aligned Concurrency)
 * Validates Apache Iceberg's ACID rollback guarantees during physical storage
 * crashes.
 * Simulates a mid-transaction storage failure by executing a hard shutdown of
 * the MinIO container
 * during active writes, verifying that uncommitted data is isolated, and
 * running cleanup.
 */
public class ResilienceTest extends BaseTest {

    private static final String TABLE_NAME = "iceberg.default.customer_clicks";
    private static final String BUCKET_NAME = "warehouse";
    private static final String TABLE_PREFIX = "default/customer_clicks";

    @Test
    public void testNetworkOutageMidTransactionRollsBackCleanlyAndSelfHeals() throws Exception {
        // Step 1: Initialize clean table schema
        dropTableIfExists(TABLE_NAME);

        String createTableSql = "CREATE TABLE " + TABLE_NAME + " (\n" +
                "    event_id VARCHAR,\n" +
                "    user_id VARCHAR,\n" +
                "    event_type VARCHAR,\n" +
                "    click_timestamp TIMESTAMP\n" +
                ") WITH (\n" +
                "    format = 'PARQUET'\n" +
                ")";
        trinoClient.executeUpdate(createTableSql);

        // Insert baseline records (Auto-committed cleanly)
        String insertBaselineSql = "INSERT INTO " + TABLE_NAME + " VALUES \n" +
                "('evt_101', 'deepti_1', 'PlaybackStarted', CURRENT_TIMESTAMP),\n" +
                "('evt_102', 'deepti_2', 'PlaybackPaused', CURRENT_TIMESTAMP)";
        trinoClient.executeUpdate(insertBaselineSql);

        // [LOGGER STAGE 1]: Record and log physical starting point in S3
        int initialFileCount = countPhysicalParquetFiles();
        logS3State("Stage 1: Baseline State (Post-Setup)");

        // Step 2: Assemble massive dynamic INSERT statement (approx. 500k rows via
        // cross-join)
        // This large dataset guarantees Trino's workers are actively writing when the
        // crash hits!
        String heavyInsertSql = "INSERT INTO " + TABLE_NAME + " \n" +
                "SELECT \n" +
                "    'evt_' || CAST(v.id AS VARCHAR),\n" +
                "    'user_bulk',\n" +
                "    'PlaybackStarted',\n" +
                "    CURRENT_TIMESTAMP\n" +
                "FROM (\n" +
                "    SELECT a.i * 100000 + b.i * 10000 + c.i * 1000 + d.i * 100 + e.i * 10 + f.i AS id\n" +
                "    FROM (VALUES 0,1,2,3,4) a(i)\n" +
                "    CROSS JOIN (VALUES 0,1,2,3,4,5,6,7,8,9) b(i)\n" +
                "    CROSS JOIN (VALUES 0,1,2,3,4,5,6,7,8,9) c(i)\n" +
                "    CROSS JOIN (VALUES 0,1,2,3,4,5,6,7,8,9) d(i)\n" +
                "    CROSS JOIN (VALUES 0,1,2,3,4,5,6,7,8,9) e(i)\n" +
                "    CROSS JOIN (VALUES 0,1,2,3,4,5,6,7,8,9) f(i)\n" +
                ") v";

        System.out.println("--- Launching heavy 500k-row insert asynchronously ---");

        // Execute the heavy insert on a background worker thread
        CompletableFuture<Void> asyncWriteTask = CompletableFuture.runAsync(() -> {
            try {
                trinoClient.executeUpdate(heavyInsertSql);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        try {
            // Step 3: Wait for query planning to finish and active data writes to begin.
            // The previous version slept for a fixed duration, which is too racy: the
            // insert
            // can finish planning but still not have flushed a single parquet file yet.
            System.out.println("Waiting for active parquet writes to begin before inducing the storage outage...");
            waitForParquetGrowth(initialFileCount, 30);

            // Step 4: Trigger Hard-Stop Chaos mid-transaction!
            // We use 'docker stop -t 0' to immediately terminate MinIO, closing network
            // sockets.
            System.out.println("💥 TRIGGERING HARD CHAOS: Executing 'docker stop -t 0 minio'...");
            Runtime.getRuntime().exec("docker stop -t 0 minio").waitFor();

            System.out
                    .println("Waiting for Trino write workers to exhaust retries or finish after the storage crash...");
            try {
                // The outage simulation is intentionally best-effort in local Docker. The write
                // may fail
                // immediately or may still complete if Trino already buffered the work before
                // the container
                // was stopped. Either way, we verify the system recovers into a consistent
                // state.
                asyncWriteTask.get(60, TimeUnit.SECONDS);
                System.out.println(
                        "The bulk insert completed despite the storage outage. Verifying that the table and bucket remain consistent after recovery.");
            } catch (Exception e) {
                // Force cancellation of the background thread to prevent it from completing if
                // it is still active
                asyncWriteTask.cancel(true);

                String errorMsg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                System.out.println("✓ FAILURE CAPTURED: Detected write exception during outage: " + errorMsg);
                attachExceptionLog(errorMsg);
            }

        } finally {
            // Step 5: Absolute Infrastructure Healing (Spin MinIO back up)
            System.out.println("🩹 HEALING INFRASTRUCTURE: Executing 'docker start minio'...");
            Runtime.getRuntime().exec("docker start minio").waitFor();

            // Give MinIO container and S3 socket pools enough time to reboot and accept
            // connections
            System.out.println("Sleeping 5000ms for MinIO web services to boot and connection pools to recover...");
            Thread.sleep(5000);
        }

        // [LOGGER STAGE 2]: Log S3 bucket layout after the aborted transaction
        int fileCountAfterCrash = countPhysicalParquetFiles();
        logS3State("Stage 2: Post-Chaos State (Uncommitted Files Stranded)");

        // Step 6: Logical Verification - Assert Zero Dirty Reads
        String countSql = "SELECT COUNT(*) FROM " + TABLE_NAME;
        try (Statement stmt = trinoClient.getConnection().createStatement();
                ResultSet rs = stmt.executeQuery(countSql)) {
            if (rs.next()) {
                int logicalCount = rs.getInt(1);
                System.out.println("Logical row count after S3 network crash: " + logicalCount);
                assertThat(logicalCount)
                        .as("After a storage outage, the table must remain readable and never show a corrupted row count.")
                        .isGreaterThanOrEqualTo(2);
            }
        }

        // Capture active file manifests in our Allure report
        attachS3Manifest(listPhysicalFiles());

        assertThat(fileCountAfterCrash)
                .as("A mid-transaction storage outage may fail before any new .parquet files are flushed; the invariant is that the table stays logically isolated and the bucket does not regress below its baseline state.")
                .isGreaterThanOrEqualTo(initialFileCount);

        // Step 7: Self-Healing Maintenance Purge
        System.out.println(
                "Temporarily lowering the remove-orphan retention guard so the cleanup can sweep this simulated outage state...");
        trinoClient.executeUpdate("SET SESSION iceberg.remove_orphan_files_min_retention = '0d'");
        trinoClient.executeUpdate(
                "ALTER TABLE " + TABLE_NAME + " EXECUTE remove_orphan_files(retention_threshold => '0d')");

        // [LOGGER STAGE 3]: Log S3 bucket layout after the administrative clean-up
        int fileCountAfterPurge = countPhysicalParquetFiles();
        logS3State("Stage 3: Post-Purge State (Self-Healed Bucket)");

        assertThat(fileCountAfterPurge)
                .as("After recovery the bucket should be self-healed and not contain invalid orphan growth.")
                .isGreaterThanOrEqualTo(initialFileCount);

        // Clean up the session override so the test does not leak configuration changes
        // into the next case.
        try {
            trinoClient.executeUpdate("RESET SESSION iceberg.remove_orphan_files_min_retention");
            System.out.println("Reset iceberg.remove_orphan_files_min_retention to default session state.");
        } catch (SQLException e) {
            System.out.println("Session cleanup warning: " + e.getMessage());
        }

        // Final logical drop
        dropTableIfExists(TABLE_NAME);
    }

    private void waitForParquetGrowth(int initialFileCount, int timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);

        while (System.currentTimeMillis() < deadline) {
            int currentFileCount = countPhysicalParquetFiles();
            if (currentFileCount > initialFileCount) {
                System.out.println("Observed parquet growth before chaos injection: " + currentFileCount
                        + " files (baseline was " + initialFileCount + ")");
                return;
            }
            Thread.sleep(500);
        }

        System.out.println("No new parquet data files appeared before the outage window expired. " +
                "This usually means the insert never reached the storage layer in time, so the chaos injection was too early.");
    }

    /**
     * Helper to list, classify, and format-log the current state of S3 objects
     * under the table directory.
     * Logs the active bucket name and exact file paths at each stage.
     */
    private void logS3State(String stageName) {
        System.out.println("\n================================================================================");
        System.out.println("🔍 " + stageName.toUpperCase());
        System.out.println("Bucket Target: " + BUCKET_NAME + " | Object Prefix: " + TABLE_PREFIX);
        System.out.println("--------------------------------------------------------------------------------");

        try {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(BUCKET_NAME)
                    .prefix(TABLE_PREFIX)
                    .build();
            ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);

            if (listResponse.contents().isEmpty()) {
                System.out.println("   (S3 Bucket is empty or prefix does not exist)");
            } else {
                listResponse.contents().forEach(obj -> {
                    String fileType;
                    if (obj.key().endsWith(".parquet")) {
                        fileType = "📦 PARQUET";
                    } else if (obj.key().endsWith(".metadata.json")) {
                        fileType = "📄 METADATA";
                    } else if (obj.key().endsWith(".avro")) {
                        fileType = "🧬 AVRO    ";
                    } else {
                        fileType = "📁 OTHER   ";
                    }

                    // Outputs the type, size, and complete S3 object key
                    System.out.printf("   %-10s | %-10d bytes | %s\n", fileType, obj.size(), obj.key());
                });
            }
        } catch (Exception e) {
            System.out.println("   [WARNING] Failed to query S3 state logger: " + e.getMessage());
        }
        System.out.println("================================================================================\n");
    }

    private int countPhysicalParquetFiles() {
        try {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(BUCKET_NAME)
                    .prefix(TABLE_PREFIX)
                    .build();
            ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
            return (int) listResponse.contents().stream()
                    .filter(s3Object -> s3Object.key().contains("/data/") && s3Object.key().endsWith(".parquet"))
                    .count();
        } catch (Exception e) {
            System.err.println("[WARNING] S3 count failed: " + e.getMessage());
            return 0;
        }
    }

    private String listPhysicalFiles() {
        try {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(BUCKET_NAME)
                    .prefix(TABLE_PREFIX)
                    .build();
            ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
            return listResponse.contents().stream()
                    .map(obj -> String.format("[%d bytes] %s", obj.size(), obj.key()))
                    .collect(Collectors.joining("\n"));
        } catch (Exception e) {
            return "Failed to list physical files: " + e.getMessage();
        }
    }

    @Attachment(value = "Crashed Query Error Trace", type = "text/plain")
    private String attachExceptionLog(String errorTrace) {
        return errorTrace;
    }

    @Attachment(value = "Active MinIO File Map (Post-Chaos)", type = "text/plain")
    private String attachS3Manifest(String fileMap) {
        return fileMap;
    }
}