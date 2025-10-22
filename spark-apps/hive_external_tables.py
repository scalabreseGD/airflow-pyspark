"""
Hive External Tables Example - Best of Both Worlds

This job demonstrates using Hive Metastore for table metadata while keeping
data in MinIO. External tables provide:
- Persistent table definitions (schema, location) in Hive Metastore
- Data stored in MinIO via S3A (not in Hive's managed location)
- Table catalog features (SHOW TABLES, DESCRIBE, etc.)
- Query by table name instead of full S3A paths

Benefits:
- Data survives even if you drop the table (external = non-managed)
- Multiple jobs can reference tables by name
- Schema is documented in the metastore
- Easy data discovery via catalog queries

Prerequisites:
- Run create_sample_data.py first to populate the source data

Usage:
    ./submit.sh hive_external_tables.py
"""

from pyspark.sql import SparkSession


def main():
    print("=" * 80)
    print("  Hive External Tables: Combining Hive Metastore + MinIO")
    print("=" * 80)

    # Create Spark session WITH Hive support
    spark = SparkSession.builder \
        .appName("Hive-External-Tables") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Step 1: Create/Use a database
        print("\n[1/8] Creating Hive database 'analytics'...")
        spark.sql("CREATE DATABASE IF NOT EXISTS analytics")
        spark.sql("USE analytics")
        print("   ✓ Using database: analytics")

        # Step 2: Show existing databases
        print("\n[2/8] Listing all databases...")
        databases = spark.sql("SHOW DATABASES")
        databases.show()

        # Step 3: Create external table pointing to existing data in MinIO
        print("\n[3/8] Creating EXTERNAL table 'employees' in Hive Metastore...")
        print("   Table will point to: s3a://data/employees/")
        print("   Data stays in MinIO, only metadata stored in Hive")

        spark.sql("""
            CREATE EXTERNAL TABLE IF NOT EXISTS employees (
                id INT,
                name STRING,
                age INT,
                department STRING,
                salary INT
            )
            STORED AS PARQUET
            LOCATION 's3a://data/employees/'
        """)
        print("   ✓ External table 'employees' created")

        # Step 4: Show tables in the database
        print("\n[4/8] Listing tables in 'analytics' database...")
        tables = spark.sql("SHOW TABLES")
        tables.show()

        # Step 5: Describe the table
        print("\n[5/8] Describing table schema...")
        spark.sql("DESCRIBE EXTENDED employees").show(truncate=False)

        # Step 6: Query the external table by name (no S3A path needed!)
        print("\n[6/8] Querying external table 'employees' by name...")
        print("   SQL: SELECT * FROM employees")

        result = spark.sql("SELECT * FROM employees ORDER BY salary DESC")
        print(f"\n   Total employees: {result.count()}")
        result.show()

        # Step 7: Run analytics using table name
        print("\n[7/8] Running analytics on external table...")
        print("   Creating another external table for high performers")

        # First, create the data
        high_performers = spark.sql("""
            SELECT
                id,
                name,
                age,
                department,
                salary,
                CASE
                    WHEN salary >= 100000 THEN 'Senior'
                    WHEN salary >= 90000 THEN 'Mid-Level'
                    ELSE 'Junior'
                END as performance_tier
            FROM employees
            WHERE department = 'Engineering' AND salary >= 85000
        """)

        # Write to MinIO
        output_path = "s3a://data/high_performers_external"
        high_performers.write.mode("overwrite").parquet(output_path)
        print(f"   ✓ Data written to: {output_path}")

        # Create external table pointing to it
        spark.sql(f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS high_performers (
                id INT,
                name STRING,
                age INT,
                department STRING,
                salary INT,
                performance_tier STRING
            )
            STORED AS PARQUET
            LOCATION '{output_path}'
        """)
        print("   ✓ External table 'high_performers' created")

        # Step 8: Query using table joins
        print("\n[8/8] Demonstrating SQL with table names...")
        print("   SQL: Complex query using table names instead of S3A paths")

        stats = spark.sql("""
            SELECT
                performance_tier,
                COUNT(*) as employee_count,
                ROUND(AVG(salary), 2) as avg_salary,
                MIN(salary) as min_salary,
                MAX(salary) as max_salary
            FROM high_performers
            GROUP BY performance_tier
            ORDER BY avg_salary DESC
        """)

        print("\n   Performance Tier Statistics:")
        stats.show()

        # Show all tables we created
        print("\n   All tables in 'analytics' database:")
        spark.sql("SHOW TABLES").show()

        print("\n" + "=" * 80)
        print("  Hive External Tables Demo Completed!")
        print("=" * 80)
        print("\n  Key Concepts Demonstrated:")
        print("    ✓ CREATE DATABASE - Organize tables")
        print("    ✓ CREATE EXTERNAL TABLE - Table metadata in Hive, data in MinIO")
        print("    ✓ LOCATION 's3a://...' - Point to existing data in MinIO")
        print("    ✓ SHOW TABLES - Catalog discovery")
        print("    ✓ DESCRIBE EXTENDED - View schema and metadata")
        print("    ✓ Query by table name - No need for full S3A paths")
        print("    ✓ SQL joins/aggregations - Use table names in queries")
        print("\n  Tables Created:")
        print("    - analytics.employees -> s3a://data/employees/")
        print("    - analytics.high_performers -> s3a://data/high_performers_external/")
        print("\n  View data in MinIO Console: http://localhost:9001 (admin/admin123)")
        print("\n  Now you can query from any Spark job:")
        print("    spark.sql('SELECT * FROM analytics.employees').show()")
        print("\n  Advantages of External Tables:")
        print("    • Data persists in MinIO even if you DROP TABLE")
        print("    • Schema is documented and discoverable")
        print("    • Query by simple table names, not full paths")
        print("    • Multiple jobs can reference the same tables")
        print("    • Table catalog makes data discovery easy")

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
