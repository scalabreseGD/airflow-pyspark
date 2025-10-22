"""
Spark SQL Example Job: Employee Analysis using SQL

This job demonstrates using Spark SQL instead of the DataFrame API:
1. Read data from MinIO (s3a://data/employees/)
2. Create temporary SQL views
3. Run SQL queries with filtering, aggregations, and computed columns
4. Write results to MinIO (s3a://data/sql_analysis/)

Prerequisites:
- Run create_sample_data.py first to populate the source data

Usage:
    ./submit.sh sql_analysis.py
"""

from pyspark.sql import SparkSession


def main():
    print("=" * 80)
    print("  Spark SQL Example: Employee Analysis")
    print("=" * 80)

    # Create Spark session
    spark = SparkSession.builder \
        .appName("SQL-Employee-Analysis") \
        .getOrCreate()

    try:
        # Define paths
        input_path = "s3a://data/employees"
        output_path = "s3a://data/sql_analysis"

        print(f"\n[1/6] Reading employee data from MinIO...")
        print(f"   Source: {input_path}")

        employees_df = spark.read.parquet(input_path)
        total_count = employees_df.count()
        print(f"\n   Total employees: {total_count}")

        if total_count == 0:
            print("\n⚠️  WARNING: No data found. Run create_sample_data.py first!")
            return

        # Create temporary SQL view
        print("\n[2/6] Creating temporary SQL view 'employees'...")
        employees_df.createOrReplaceTempView("employees")
        print("   ✓ View created")

        # SQL Query 1: Filter high-performing Engineering employees
        print("\n[3/6] Running SQL Query: High-Performing Engineering Employees")
        print("   SQL: SELECT with WHERE, CASE, ORDER BY")

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
            WHERE department = 'Engineering'
                AND salary >= 85000
            ORDER BY salary DESC
        """)

        print(f"\n   Results: {high_performers.count()} employees")
        high_performers.show()

        # SQL Query 2: Department summary statistics
        print("\n[4/6] Running SQL Query: Department Statistics")
        print("   SQL: SELECT with GROUP BY and aggregate functions")

        dept_stats = spark.sql("""
            SELECT
                department,
                COUNT(*) as employee_count,
                ROUND(AVG(salary), 2) as avg_salary,
                MIN(salary) as min_salary,
                MAX(salary) as max_salary,
                ROUND(AVG(age), 1) as avg_age
            FROM employees
            GROUP BY department
            ORDER BY avg_salary DESC
        """)

        print("\n   Department Statistics:")
        dept_stats.show()

        # SQL Query 3: Salary brackets across all departments
        print("\n[5/6] Running SQL Query: Salary Distribution")
        print("   SQL: SELECT with nested CASE statements")

        salary_distribution = spark.sql("""
            SELECT
                CASE
                    WHEN salary >= 100000 THEN '$100K+'
                    WHEN salary >= 80000 THEN '$80K-$100K'
                    WHEN salary >= 60000 THEN '$60K-$80K'
                    ELSE 'Under $60K'
                END as salary_bracket,
                COUNT(*) as employee_count,
                ROUND(AVG(salary), 2) as avg_salary
            FROM employees
            GROUP BY salary_bracket
            ORDER BY avg_salary DESC
        """)

        print("\n   Salary Distribution:")
        salary_distribution.show()

        # Write high performers to MinIO
        print(f"\n[6/6] Writing high-performing employees to MinIO...")
        print(f"   Output: {output_path}")

        high_performers.write \
            .mode("overwrite") \
            .parquet(output_path)

        print("   ✓ Data written successfully")

        # Verify the written data
        print("\n   Verifying written data...")
        result_df = spark.read.parquet(output_path)
        print(f"   ✓ Verified: {result_df.count()} records written")

        print("\n" + "=" * 80)
        print("  SQL Analysis Completed Successfully!")
        print("=" * 80)
        print(f"\n  Input:  {input_path}")
        print(f"  Output: {output_path}")
        print("\n  View data in MinIO Console: http://localhost:9001 (admin/admin123)")
        print("  Navigate to 'data' bucket to see the output files")
        print("\n  Key SQL Features Demonstrated:")
        print("    ✓ CREATE TEMPORARY VIEW")
        print("    ✓ SELECT with WHERE clause")
        print("    ✓ CASE statements for computed columns")
        print("    ✓ GROUP BY with aggregate functions (COUNT, AVG, MIN, MAX)")
        print("    ✓ ORDER BY for sorting")
        print("    ✓ ROUND function for numeric formatting")

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
