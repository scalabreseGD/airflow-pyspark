"""
Example Spark ETL Job: Filter Employees

This job demonstrates a typical ETL pattern using direct MinIO access via S3A:
1. Read data from MinIO (s3a://data/employees/)
2. Apply business logic (filtering)
3. Write results to MinIO (s3a://data/high_performers/)

Prerequisites:
- Run create_sample_data.py first to populate the source data

Usage:
    ./submit.sh filter_employees.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when


def main():
    print("=" * 80)
    print("  Spark ETL Job: Filter High-Performing Engineering Employees")
    print("=" * 80)

    # Create Spark session
    spark = SparkSession.builder \
        .appName("Filter-Employees-ETL") \
        .getOrCreate()

    try:
        # Define paths
        input_path = "s3a://data/employees"
        output_path = "s3a://data/high_performers"

        print(f"\n[1/5] Reading source data from MinIO...")
        print(f"   Source: {input_path}")

        employees_df = spark.read.parquet(input_path)

        total_count = employees_df.count()
        print(f"\n   Total employees: {total_count}")

        if total_count == 0:
            print("\n⚠️  WARNING: No data found. Run create_sample_data.py first!")
            return

        print("\n   Source data sample:")
        employees_df.show(10)

        # Business Logic: Filter high-performing Engineering employees
        print("\n[2/5] Applying filters...")
        print("   - Department: Engineering")
        print("   - Salary: >= $85,000")

        filtered_df = employees_df.filter(
            (col("department") == "Engineering") &
            (col("salary") >= 85000)
        ).orderBy(col("salary").desc())

        filtered_count = filtered_df.count()
        print(f"\n   Filtered employees: {filtered_count}")
        print("\n   Filtered data:")
        filtered_df.show()

        # Additional transformation: Add performance tier
        print("\n[3/5] Adding performance tier column...")

        enriched_df = filtered_df.withColumn(
            "performance_tier",
            when(col("salary") >= 100000, "Senior")
            .when(col("salary") >= 90000, "Mid-Level")
            .otherwise("Junior")
        )

        print("   Enriched data with performance tier:")
        enriched_df.show()

        # Write results to MinIO
        print(f"\n[4/5] Writing results to MinIO...")
        print(f"   Output: {output_path}")

        enriched_df.write \
            .mode("overwrite") \
            .parquet(output_path)

        print("   ✓ Data written successfully")

        # Verify the written data
        print("\n[5/5] Verifying and generating summary statistics...")

        result_df = spark.read.parquet(output_path)
        print(f"   ✓ Verified: {result_df.count()} records written")

        stats = result_df.groupBy("performance_tier") \
            .agg(
                count("*").alias("employee_count"),
                avg("salary").alias("avg_salary")
            ) \
            .orderBy("avg_salary", ascending=False)

        print("\n   Summary by performance tier:")
        stats.show()

        print("\n" + "=" * 80)
        print("  ETL Job Completed Successfully!")
        print("=" * 80)
        print(f"\n  Input:  {input_path}")
        print(f"  Output: {output_path}")
        print("\n  View data in MinIO Console: http://localhost:9001 (admin/admin123)")
        print("  Navigate to 'data' bucket to see the output files")
        print("\n  You can now:")
        print("    - View the data in MinIO Console")
        print("    - Read it in another Spark job:")
        print(f"      spark.read.parquet('{output_path}').show()")

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
