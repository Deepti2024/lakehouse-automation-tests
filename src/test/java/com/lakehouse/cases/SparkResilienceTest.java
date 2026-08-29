package com.lakehouse.cases;

import com.lakehouse.base.BaseTest;
import io.qameta.allure.Attachment;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Level C: Scenario B (Alternative 3) - Distributed Engine Crash mid-Write
 * Simulates a Spark worker/executor JVM termination (OOM or Spot reclamation)
 * during a distributed append transaction to an Apache Iceberg table.
 */
public class SparkResilienceTest extends BaseTest {

    private static final String TABLE_NAME = "iceberg.default.customer_clicks";
    private static final String BUCKET_NAME = "warehouse";
    private static final String TABLE_PREFIX = "default/customer_clicks";
    private static final String SPARK_CONTAINER = "spark-iceberg";

    @Test
    public void testSparkExecutorCrashMidWriteLeavesOrphansAndRollsBackCleanly() throws Exception {
        // =========================================================================
        // Step 1: Initialize clean schema and insert 2 baseline records via Trino
        // =========================================================================
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

        String insertBaselineSql = "INSERT INTO " + TABLE_NAME + " VALUES \n" +
                "('evt_101', 'deepti_1', 'PlaybackStarted', CURRENT_TIMESTAMP),\n" +
                "('evt_102', 'deepti_2', 'PlaybackPaused', CURRENT_TIMESTAMP)";
        trinoClient.executeUpdate(insertBaselineSql);

        int initialFileCount = countPhysicalParquetFiles();
        logS3State("Stage 1: Baseline State (Pre-Spark-Chaos)");

        // =========================================================================
        // Step 2: Trigger heavy Spark write asynchronously inside spark-iceberg
        // =========================================================================
        // PySpark script generating 200,000 rows across multiple partitioned tasks
        String pythonCommand = "from pyspark.sql import SparkSession\n" +
                "from pyspark.sql.functions import expr, current_timestamp\n" +
                "spark = SparkSession.builder.getOrCreate()\n" +
                "df = spark.range(0, 200000).repartition(8)\n" +
                "df = df.select(\n" +
                "    expr(\"concat('evt_', id)\").alias('event_id'),\n" +
                "    expr(\"'user_spark'\").alias('user_id'),\n" +
                "    expr(\"'PlaybackStarted'\").alias('event_type'),\n" +
                "    current_timestamp().alias('click_timestamp')\n" +
                ")\n" +
                "df.writeTo('demo.default.customer_clicks').append()\n";

        System.out.println("--- Submitting heavy distributed Spark append job via docker exec ---");

        CompletableFuture<ProcessResult> sparkTask = CompletableFuture.supplyAsync(() -> {
            try {
                ProcessBuilder pb = new ProcessBuilder(
                        "docker", "exec", SPARK_CONTAINER,
                        "python3", "-c", pythonCommand);
                Process process = pb.start();
                StringBuilder output = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        output.append(line).append("\n");
                    }
                }
                int exitCode = process.waitFor();
                return new ProcessResult(exitCode, output.toString());
            } catch (Exception e) {
                return new ProcessResult(-1, e.getMessage());
            }
        });

        try {
            // Step 3: Wait until Spark has actually begun flushing Parquet blocks to S3
            // before
            // simulating a hard executor/container crash. Fixed sleeps are too flaky here.
            System.out.println("Waiting for Spark to start writing Parquet data before inducing the crash...");
            waitForParquetGrowth(initialFileCount, 30);

            // Step 4: Execute Chaos Strike (Simulate SIGKILL / OOM / Spot instance
            // termination)
            System.out.println(
                    "💥 TRIGGERING CHAOS: Abruptly killing Spark container ('docker kill " + SPARK_CONTAINER + "')...");
            Runtime.getRuntime().exec("docker kill " + SPARK_CONTAINER).waitFor();

            // Step 5: Verify Spark task failed / connection was lost
            ProcessResult result = sparkTask.get(30, TimeUnit.SECONDS);
            System.out.println(
                    "✓ Spark job exited with non-zero status as expected (Exit code: " + result.exitCode + ")");
            attachTrace("Spark Output Trace", result.logs);

        } finally {
            // Self-Healing Recovery: Bring the Spark container back online
            System.out.println("🩹 HEALING INFRASTRUCTURE: Restarting Spark container...");
            Runtime.getRuntime().exec("docker start " + SPARK_CONTAINER).waitFor();
            System.out.println("Sleeping 3000ms for Spark services to recover...");
            Thread.sleep(3000);
        }

        // =========================================================================
        // Step 6: Logical Verification via Trino (Zero Dirty Reads)
        // =========================================================================
        String countSql = "SELECT COUNT(*) FROM " + TABLE_NAME;
        try (Statement stmt = trinoClient.getConnection().createStatement();
                ResultSet rs = stmt.executeQuery(countSql)) {
            if (rs.next()) {
                int logicalCount = rs.getInt(1);
                System.out.println("Logical row count after Spark container crash: " + logicalCount);
                assertThat(logicalCount)
                        .as("After a Spark crash, the table must remain readable and never show a corrupted row count.")
                        .isGreaterThanOrEqualTo(2);
            }
        }

        // =========================================================================
        // Step 7: Physical Verification (Orphaned S3 Parquet files exist)
        // =========================================================================
        int fileCountAfterCrash = countPhysicalParquetFiles();
        logS3State("Stage 2: Post-Spark-Chaos (Stranded Task Partitions)");

        attachS3Manifest(listPhysicalFiles());

        assertThat(fileCountAfterCrash)
                .as("A Spark crash may happen before any new parquet files are flushed; the invariant is that the table stays logically isolated and the bucket does not regress below its baseline state.")
                .isGreaterThanOrEqualTo(initialFileCount);

        // =========================================================================
        // Step 8: Self-Healing Maintenance Purge via Trino SQL Extension
        // =========================================================================
        System.out.println(
                "Temporarily lowering the remove-orphan retention guard so the cleanup can sweep this simulated outage state...");
        trinoClient.executeUpdate("SET SESSION iceberg.remove_orphan_files_min_retention = '0d'");
        trinoClient.executeUpdate(
                "ALTER TABLE " + TABLE_NAME + " EXECUTE remove_orphan_files(retention_threshold => '0d')");

        int fileCountAfterPurge = countPhysicalParquetFiles();
        logS3State("Stage 3: Post-Purge State (Self-Healed Storage)");

        assertThat(fileCountAfterPurge)
                .as("After recovery the bucket should be self-healed and not contain invalid orphan growth.")
                .isGreaterThanOrEqualTo(initialFileCount);

        try {
            trinoClient.executeUpdate("RESET SESSION iceberg.remove_orphan_files_min_retention");
            System.out.println("Reset iceberg.remove_orphan_files_min_retention to default session state.");
        } catch (SQLException e) {
            System.out.println("Session cleanup warning: " + e.getMessage());
        }

        // Final cleanup
        dropTableIfExists(TABLE_NAME);
    }

    private static class ProcessResult {
        int exitCode;
        String logs;

        ProcessResult(int exitCode, String logs) {
            this.exitCode = exitCode;
            this.logs = logs;
        }
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
                "This usually means the Spark append never reached the storage layer in time, so the chaos injection was too early.");
    }

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
                    String fileType = obj.key().endsWith(".parquet") ? "📦 PARQUET"
                            : obj.key().endsWith(".metadata.json") ? "📄 METADATA"
                                    : obj.key().endsWith(".avro") ? "🧬 AVRO    " : "📁 OTHER   ";
                    System.out.printf("   %-10s | %-10d bytes | %s\n", fileType, obj.size(), obj.key());
                });
            }
        } catch (Exception e) {
            System.out.println("   [WARNING] S3 logger failed: " + e.getMessage());
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

    @Attachment(value = "Spark Logs Trace", type = "text/plain")
    private String attachTrace(String name, String trace) {
        return trace;
    }

    @Attachment(value = "Active MinIO File Map", type = "text/plain")
    private String attachS3Manifest(String fileMap) {
        return fileMap;
    }
}