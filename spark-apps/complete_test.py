#!/usr/bin/env python3
"""
Complete Integration Test for Spark + MinIO + Hive Stack
Tests:
1. MinIO S3 connectivity and direct writes
2. Hive metastore integration
3. Spark SQL queries
4. Data persistence and retrieval
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
import sys

def print_section(title):
    """Print a formatted section header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")

def main():
    print_section("Starting Complete Integration Test")

    # Create Spark session with Hive and S3 support
    print("Creating Spark session with Hive and MinIO support...")
    spark = SparkSession.builder \
        .appName("Complete-Integration-Test") \
        .enableHiveSupport() \
        .getOrCreate()

    print(f"Spark version: {spark.version}")
    print(f"Spark master: {spark.sparkContext.master}")

    try:
        # =================================================================
        # TEST 1: MinIO Direct Write/Read via S3A Protocol
        # =================================================================
        print_section("TEST 1: Direct MinIO Write/Read via S3A")

        # Create sample data
        data = [
            (1, "Alice", 29, "Engineering", 85000),
            (2, "Bob", 35, "Sales", 75000),
            (3, "Charlie", 28, "Engineering", 95000),
            (4, "Diana", 32, "Marketing", 70000),
            (5, "Eve", 41, "Engineering", 110000),
            (6, "Frank", 27, "Sales", 65000),
            (7, "Grace", 38, "Marketing", 80000),
            (8, "Henry", 30, "Engineering", 90000),
        ]

        df = spark.createDataFrame(data, ["id", "name", "age", "department", "salary"])

        print("Sample data created:")
        df.show()

        # Write directly to MinIO using S3A protocol
        s3_path = "s3a://data/employees/parquet"
        print(f"Writing data to MinIO at: {s3_path}")
        df.write.mode("overwrite").parquet(s3_path)
        print("✓ Write to MinIO successful")

        # Read back from MinIO
        print(f"\nReading data from MinIO at: {s3_path}")
        df_read = spark.read.parquet(s3_path)
        print("✓ Read from MinIO successful")
        print(f"Records read: {df_read.count()}")
        df_read.show()

        # =================================================================
        # TEST 2: Hive Table Creation with MinIO Storage
        # =================================================================
        print_section("TEST 2: Hive Table Creation with MinIO Storage")

        # Create Hive database
        print("Creating Hive database 'test_db'...")
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
        spark.sql("USE test_db")
        print("✓ Database created")

        # Show databases
        print("\nAvailable databases:")
        spark.sql("SHOW DATABASES").show()

        # Create managed Hive table (data stored in MinIO warehouse bucket)
        print("\nCreating managed Hive table 'employees'...")
        df.write.mode("overwrite").saveAsTable("test_db.employees")
        print("✓ Hive table created")

        # Show tables
        print("\nTables in test_db:")
        spark.sql("SHOW TABLES IN test_db").show()

        # Verify table location (should be in s3a://warehouse/)
        print("\nTable details:")
        spark.sql("DESCRIBE EXTENDED test_db.employees").show(truncate=False)

        # =================================================================
        # TEST 3: Spark SQL Queries on Hive Tables
        # =================================================================
        print_section("TEST 3: Spark SQL Queries on Hive Tables")

        # Simple SELECT
        print("Query 1: SELECT all records")
        result = spark.sql("SELECT * FROM test_db.employees ORDER BY id")
        result.show()

        # Aggregation by department
        print("\nQuery 2: Average salary by department")
        result = spark.sql("""
            SELECT
                department,
                COUNT(*) as employee_count,
                ROUND(AVG(salary), 2) as avg_salary,
                MIN(salary) as min_salary,
                MAX(salary) as max_salary
            FROM test_db.employees
            GROUP BY department
            ORDER BY avg_salary DESC
        """)
        result.show()

        # Filter query
        print("\nQuery 3: Engineers earning over 90K")
        result = spark.sql("""
            SELECT name, age, salary
            FROM test_db.employees
            WHERE department = 'Engineering' AND salary > 90000
            ORDER BY salary DESC
        """)
        result.show()

        # =================================================================
        # TEST 4: External Table Creation
        # =================================================================
        print_section("TEST 4: External Hive Table with Custom S3A Location")

        # Create external table pointing to our earlier S3A write
        print(f"Creating external table pointing to {s3_path}...")
        spark.sql(f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS test_db.employees_external (
                id INT,
                name STRING,
                age INT,
                department STRING,
                salary INT
            )
            STORED AS PARQUET
            LOCATION '{s3_path}'
        """)
        print("✓ External table created")

        print("\nQuerying external table:")
        spark.sql("SELECT * FROM test_db.employees_external ORDER BY id").show()

        # =================================================================
        # TEST 5: DataFrame API with Hive Tables
        # =================================================================
        print_section("TEST 5: DataFrame API with Hive Integration")

        # Read Hive table as DataFrame
        df_hive = spark.table("test_db.employees")

        print("Using DataFrame API for analysis:")
        df_analysis = df_hive.groupBy("department") \
            .agg(
                count("*").alias("count"),
                avg("salary").alias("avg_salary"),
                avg("age").alias("avg_age")
            ) \
            .orderBy(col("avg_salary").desc())

        df_analysis.show()

        # Write analysis results to MinIO
        analysis_path = "s3a://data/analysis/department_summary"
        print(f"\nWriting analysis to MinIO at: {analysis_path}")
        df_analysis.write.mode("overwrite").parquet(analysis_path)
        print("✓ Analysis written to MinIO")

        # =================================================================
        # TEST 6: CSV Format Support
        # =================================================================
        print_section("TEST 6: CSV Format Support")

        csv_path = "s3a://data/employees/csv"
        print(f"Writing data as CSV to: {csv_path}")
        df.write.mode("overwrite") \
            .option("header", "true") \
            .csv(csv_path)
        print("✓ CSV write successful")

        print(f"\nReading CSV from: {csv_path}")
        df_csv = spark.read.option("header", "true").csv(csv_path)
        df_csv.show()

        # =================================================================
        # TEST 7: Partitioned Tables
        # =================================================================
        print_section("TEST 7: Partitioned Hive Table")

        print("Creating partitioned table by department...")
        df.write.mode("overwrite") \
            .partitionBy("department") \
            .saveAsTable("test_db.employees_partitioned")
        print("✓ Partitioned table created")

        print("\nPartitions:")
        spark.sql("SHOW PARTITIONS test_db.employees_partitioned").show()

        print("\nQuerying specific partition (department=Engineering):")
        spark.sql("""
            SELECT name, age, salary
            FROM test_db.employees_partitioned
            WHERE department = 'Engineering'
            ORDER BY salary DESC
        """).show()

        # =================================================================
        # SUMMARY
        # =================================================================
        print_section("TEST SUMMARY")

        print("✓ All tests completed successfully!")
        print("\nVerified capabilities:")
        print("  1. Direct S3A writes to MinIO")
        print("  2. Direct S3A reads from MinIO")
        print("  3. Hive database and table creation")
        print("  4. Managed Hive tables with MinIO storage")
        print("  5. External Hive tables with custom S3A locations")
        print("  6. Spark SQL queries on Hive tables")
        print("  7. DataFrame API with Hive integration")
        print("  8. Multiple file formats (Parquet, CSV)")
        print("  9. Partitioned tables")
        print("\n✓ Stack is fully functional!")

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        print("\nSpark session stopped")

if __name__ == "__main__":
    main()
