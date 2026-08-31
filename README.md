🌊 Apache Iceberg Lakehouse Test Automation Framework
An enterprise-grade, multi-engine test automation framework designed to validate functional correctness, schema evolution, and physical resilience (chaos engineering) on an open Apache Iceberg Lakehouse running on Docker and orchestrated via GitHub Actions CI/CD.

🏛️ Architecture & System Topology
The framework runs end-to-end integration tests against a containerized distributed Lakehouse stack:

                               +--------------------------------------------+
                               |        JUnit 5 Test Automation Suite       |
                               |    (AssertJ, AWS SDK v2, Trino JDBC)       |
                               +--------------------------------------------+
                                     |                           |
            1. SQL DDL / DML / DQL  |                           | 2. Direct S3 Verification
               & Maintenance Checks  |                           |    & Orphan File Auditing
                                     v                           v
                      +-----------------------------+     +-----------------------------+
                      |     Trino Query Engine      |     |          MinIO S3           |
                      |        (Port 8082)          |     |    (S3 API: Port 9000)      |
                      +-----------------------------+     |  Bucket: 'warehouse'        |
                                     |                    +-----------------------------+
               Catalog Schema / Snapshot Updates                 ^               ^
                                     v                           |               |
                      +-----------------------------+            |               |
                      |  Iceberg REST Catalog       |------------+               |
                      |        (Port 8181)          |   Table Metadata JSON      |
                      +-----------------------------+   & Manifest Lists         |
                                     ^                                           |
                                     | Write Jobs & Task Splits                  |
                      +-----------------------------+                            |
                      |    Apache Spark Engine      |----------------------------+
                      |       (spark-iceberg)       |   Physical Parquet Blocks
                      +-----------------------------+

🧪 Test Matrix & Quality Scenarios
  Layer	Test Class	Core Scenario	Validations & Assertions
 
  Level A: Ingestion	DataIngestionTest	Batch Data Ingestion via Trino SQL	Table schema creation, multi-row commit, data type fidelity, AssertJ field-level verification.
  
  Level B: Evolution	SchemaEvolutionTest	Metadata-Only Schema Evolution	Add columns to active table without rewrites; historical rows safely resolve missing columns as NULL.
  
  Level C: Storage Chaos	ResilienceTest	S3 Network Storage Crash Mid-Write	docker stop -t 0 minio during active 500k-row write. Verifies ACID transaction rollback (zero dirty reads) and administrative orphan file cleanup (ALTER TABLE ... EXECUTE remove_orphan_files).
  
  Level C: Compute Chaos	SparkResilienceTest	Distributed Spark Worker Crash	docker kill spark-iceberg mid-partition flush. Verifies uncommitted task isolation and physical disk self-healing.

🚀 Key Architectural Innovations & Solved Edge Cases
  The S3 Bucket Recreation Trap:
  Avoiding connection pool invalidation in Trino by adopting a Purge-Only S3 cleanup strategy instead of deleting the bucket underneath a live query engine.
  
  Catalog vs. Storage Out-of-Sync Prevention:
  Enforcing logical catalog drops before raw S3 object purges to prevent metadata pointer corruption.
  
  TCP Black Hole Mitigation:
  Replacing soft Docker pauses with docker stop -t 0 (hard termination) to immediately sever TCP sockets and prevent client retries from masking mid-write storage dropouts.
  
  Trino-Native Maintenance Execution:
  Leveraging Trino's ALTER TABLE ... EXECUTE remove_orphan_files syntax instead of Spark-style system procedures.

🛠️ Getting Started (Local Execution)
Prerequisites
  Java 17+ (JDK)
  Maven 3.8+
  Docker Desktop running
Steps : 
  1. Clone the Repository
  git clone https://github.com/Deepti2024/lakehouse-automation-tests.git
  cd lakehouse-automation-tests
  2. Launch Local Lakehouse Stack
  docker compose up -d
  3. Run the Automated Test Suite
  mvn clean test -Dsurefire.useFile=false
  4. Serve the Live Allure Report Dashboard
  mvn allure:serve

🔄 CI/CD & Automated Reporting (GitHub Actions)
  This project features a fully automated GitHub Actions pipeline (.github/workflows/lakehouse-ci.yml):
  Provisions an ephemeral Ubuntu 24.04 runner with JDK 17 and Docker.
  Boots the multi-container Lakehouse topology (MinIO, REST Catalog, Trino, Spark).
  Polls HTTP endpoints to ensure distributed infrastructure readiness.
  Runs the test suite in headless mode with Surefire logging.
  Generates the static Allure HTML report and automatically deploys it to GitHub Pages.

🔗 Live Report Dashboard: https://Deepti2024.github.io/lakehouse-automation-tests/
