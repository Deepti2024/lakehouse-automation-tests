package com.lakehouse.base;

import com.lakehouse.client.TrinoClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.sql.SQLException;

/**
 * Base test class to manage test lifecycle, JDBC clients, S3 storage buckets,
 * and environments.
 * Prevents inter-test pollution by logically dropping catalog registries before
 * physical S3 purges.
 */
public class BaseTest {
    protected TrinoClient trinoClient;
    protected S3Client s3Client; // Marked protected so child classes can access for assertions
    private static final String BUCKET_NAME = "warehouse";

    @BeforeEach
    public void setUp() throws SQLException {
        System.out.println("\n========== BaseTest.setUp() - Starting test environment setup ==========");

        // 1. Establish the database client connection first
        trinoClient = new TrinoClient();
        trinoClient.connect();

        // 2. LOGICAL CLEANUP FIRST: Unregister tables from the catalog while S3
        // metadata files still exist!
        // This prevents the REST Catalog database from getting out-of-sync with
        // physical storage.
        dropTableIfExists("iceberg.default.customer_clicks");
        dropTableIfExists("iceberg.default.customer_profiles");

        // 3. PHYSICAL CLEANUP SECOND: Purge the raw S3 bucket files cleanly
        cleanWarehouseBucket();

        // 4. Ensure the Iceberg schema/namespace is declared inside our clean bucket
        System.out.println("Ensuring Iceberg namespace 'default' exists...");
        trinoClient.executeUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.default");

        System.out.println("========== BaseTest.setUp() - Completed successfully ==========\n");
    }

    @AfterEach
    public void tearDown() {
        System.out.println("\n========== BaseTest.tearDown() - Starting cleanup ==========");

        // Logically drop tables at teardown to leave a clean environment for subsequent
        // test suites
        dropTableIfExists("iceberg.default.customer_clicks");
        dropTableIfExists("iceberg.default.customer_profiles");

        if (trinoClient != null) {
            trinoClient.close();
        }
        if (s3Client != null) {
            s3Client.close();
        }
        System.out.println("========== BaseTest.tearDown() - Completed ==========\n");
    }

    /**
     * Connects to MinIO S3 API and purges all active/stale objects recursively,
     * keeping the bucket itself alive to prevent S3 connection pool invalidation.
     */
    private void cleanWarehouseBucket() {
        try {
            s3Client = S3Client.builder()
                    .endpointOverride(URI.create("http://localhost:9000"))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("admin", "password")))
                    .region(Region.US_EAST_1)
                    .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                            .pathStyleAccessEnabled(true)
                            .build())
                    .build();

            System.out.println("S3 client connected. Checking if bucket '" + BUCKET_NAME + "' exists...");

            try {
                HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                        .bucket(BUCKET_NAME)
                        .build();
                s3Client.headBucket(headBucketRequest);

                System.out.println(
                        "Bucket '" + BUCKET_NAME + "' exists. Purging all objects to clean the environment...");

                ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                        .bucket(BUCKET_NAME)
                        .build();
                ListObjectsV2Response listObjectsResponse;
                int deletedCount = 0;

                do {
                    listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
                    for (S3Object s3Object : listObjectsResponse.contents()) {
                        System.out.println("Deleting object key: " + s3Object.key());
                        s3Client.deleteObject(DeleteObjectRequest.builder()
                                .bucket(BUCKET_NAME)
                                .key(s3Object.key())
                                .build());
                        deletedCount++;
                    }
                    listObjectsRequest = listObjectsRequest.toBuilder()
                            .continuationToken(listObjectsResponse.nextContinuationToken())
                            .build();
                } while (listObjectsResponse.isTruncated());

                System.out.println(
                        "Successfully purged " + deletedCount + " objects. Bucket '" + BUCKET_NAME + "' is clean.");

            } catch (NoSuchBucketException e) {
                System.out.println("Bucket '" + BUCKET_NAME + "' does not exist. Creating it fresh...");
                s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
                System.out.println("Bucket '" + BUCKET_NAME + "' created successfully.");
            }

        } catch (Exception e) {
            System.err.println("CRITICAL: Failed to programmatically clean the S3 bucket: " + e.getMessage());
            throw new RuntimeException("Environment setup failed. S3 bucket cleanup error.", e);
        }
    }

    /**
     * Helper to safely drop a table before/after a test case to prevent cross-test
     * interference.
     */
    protected void dropTableIfExists(String tableName) {
        try {
            System.out.println("Attempting to drop table '" + tableName + "' if it exists...");
            trinoClient.executeUpdate("DROP TABLE IF EXISTS " + tableName);
        } catch (SQLException e) {
            System.err.println(
                    "Non-critical cleanup failure: Failed to drop table " + tableName + " - " + e.getMessage());
        }
    }
}