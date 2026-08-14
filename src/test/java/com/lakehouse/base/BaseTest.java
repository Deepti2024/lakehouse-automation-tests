package com.lakehouse.base;

import com.lakehouse.client.TrinoClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.sql.SQLException;

/**
 * Base test class to manage test lifecycle, JDBC clients, and environment
 * setups.
 */
public class BaseTest {

    protected TrinoClient trinoClient;

    private S3Client s3Client;
    private static final String BUCKET_NAME = "warehouse";

    @BeforeEach
    public void setUp() throws SQLException {
        System.out.println("\n========== BaseTest.setUp() - Starting test environment setup ==========");

        // 1. Programmatically delete and recreate the storage bucket to guarantee a
        // 100% clean slate
        // recreateWarehouseBucket();

        // 1. Programmatically purge the storage bucket recursively without deleting the
        // bucket itself
        cleanWarehouseBucket();

        // 2. Connect to the SQL execution broker (Trino)
        trinoClient = new TrinoClient();
        trinoClient.connect();

        // Ensure the Iceberg namespace exists before any tests try to write tables.
        // The REST catalog is already scoped to the iceberg catalog; default is the
        // namespace, not a nested iceberg.default schema path.
        System.out.println("Ensuring Iceberg namespace 'default' exists...");
        trinoClient.executeUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.default");
        // trinoClient.executeUpdate("CREATE SCHEMA IF NOT EXISTS default");

        System.out.println("========== BaseTest.setUp() - Completed successfully ==========\n");
    }

    @AfterEach
    public void tearDown() {
        System.out.println("\n========== BaseTest.tearDown() - Starting cleanup ==========");
        if (trinoClient != null) {
            trinoClient.close();
        }
        if (s3Client != null) {
            s3Client.close();
        }
        System.out.println("========== BaseTest.tearDown() - Completed ==========\n");
    }

    /**
     * Helper to safely drop a table before/after a test case to prevent cross-test
     * interference.
     */
    protected void dropTableIfExists(String tableName) {
        System.out.println("Attempting to drop table '" + tableName + "' if it exists...");
        try {
            trinoClient.executeUpdate("DROP TABLE IF EXISTS " + tableName);
        } catch (SQLException e) {
            System.err.println(
                    "Non-critical cleanup failure: Failed to drop table " + tableName + " - " + e.getMessage());
        }
    }

    /**
     * Connects to MinIO S3 API, purges all active/stale objects, deletes, and
     * recreates the warehouse bucket.
     * 
     * private void recreateWarehouseBucket() {
     * System.out.println("Recreating S3 bucket '" + BUCKET_NAME + "' for a clean
     * test environment...");
     * try {
     * s3Client = S3Client.builder()
     * .endpointOverride(URI.create("http://localhost:9000"))
     * .credentialsProvider(StaticCredentialsProvider.create(
     * AwsBasicCredentials.create("admin", "password")))
     * .region(Region.US_EAST_1)
     * .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
     * .pathStyleAccessEnabled(true)
     * .build())
     * .build();
     * 
     * System.out.println("S3 client connected. Checking if bucket '" + BUCKET_NAME
     * + "' exists...");
     * 
     * // Check if bucket exists
     * try {
     * HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
     * .bucket(BUCKET_NAME)
     * .build();
     * s3Client.headBucket(headBucketRequest);
     * 
     * System.out
     * .println("Bucket '" + BUCKET_NAME + "' exists. Purging all objects to prepare
     * for deletion...");
     * 
     * // S3 Buckets cannot be deleted unless they are completely empty. List and
     * // delete all objects:
     * ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
     * .bucket(BUCKET_NAME)
     * .build();
     * ListObjectsV2Response listObjectsResponse;
     * 
     * int deletedCount = 0;
     * do {
     * listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
     * for (S3Object s3Object : listObjectsResponse.contents()) {
     * System.out.println(" Deleting object: " + s3Object.key());
     * s3Client.deleteObject(DeleteObjectRequest.builder()
     * .bucket(BUCKET_NAME)
     * .key(s3Object.key())
     * .build());
     * deletedCount++;
     * }
     * // Handle pagination for large buckets
     * listObjectsRequest = listObjectsRequest.toBuilder()
     * .continuationToken(listObjectsResponse.nextContinuationToken())
     * .build();
     * } while (listObjectsResponse.isTruncated());
     * 
     * System.out.println("Deleted " + deletedCount + " objects from bucket.");
     * 
     * // Delete the bucket
     * s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(BUCKET_NAME).build());
     * System.out.println("Bucket '" + BUCKET_NAME + "' successfully deleted.");
     * 
     * } catch (NoSuchBucketException e) {
     * System.out.println("Bucket '" + BUCKET_NAME + "' does not exist yet.
     * Proceeding straight to creation.");
     * }
     * 
     * // Create the bucket clean
     * s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
     * System.out.println("✓ Bucket '" + BUCKET_NAME + "' successfully recreated and
     * ready for tests.");
     * 
     * } catch (Exception e) {
     * System.err.println("CRITICAL: Failed to programmatically recreate the S3
     * bucket: " + e.getMessage());
     * e.printStackTrace();
     * throw new RuntimeException("Environment setup failed. S3 bucket cleanup
     * error.", e);
     * }
     * }
     */
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

                // List and delete all objects recursively (keeping the bucket alive)
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

}