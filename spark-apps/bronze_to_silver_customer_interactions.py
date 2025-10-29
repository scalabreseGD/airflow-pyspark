"""
Bronze to Silver - Transform Customer Interactions (Pure Spark SQL)

This script reads raw customer interaction data from bronze.customer_interactions_raw,
applies data quality transformations using pure Spark SQL, and creates the
silver.customer_interactions table.

Transformations applied using SQL:
- Type casting (string -> proper types: BIGINT, TIMESTAMP, DATE, INT, DECIMAL, BOOLEAN)
- Extract numeric IDs from string prefixes
- Extract interaction_date from interaction_timestamp
- Calculate interaction_year and interaction_month
- Determine is_resolved based on resolution completion
- Determine is_escalated based on notes content
- Add metadata timestamps

Input: bronze.customer_interactions_raw
Output: silver.customer_interactions (Parquet, partitioned by interaction_year, interaction_month)

Usage:
    ./submit.sh bronze_to_silver_customer_interactions.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession

print("=" * 80)
print("  Bronze to Silver - Customer Interactions Transformation (Spark SQL)")
print("=" * 80)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--name", dest="app_name")
known_args, _ = parser.parse_known_args()
app_name = known_args.app_name

builder = SparkSession.builder.enableHiveSupport()
spark = builder.appName(app_name).getOrCreate() if app_name else builder.getOrCreate()

# Register lineage listener to push sources/destinations to Neo4j
from neo4j_lineage import enable
enable(spark)

try:
    print(f"\nStarting transformation at: {datetime.now()}")

    # Read from bronze to get initial count
    print("\n[1/4] Reading from bronze.customer_interactions_raw...")
    initial_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.customer_interactions_raw").collect()[0]['cnt']
    print(f"  ✓ Read {initial_count:,} records from bronze layer")

    # Define the SQL transformation
    print("\n[2/4] Executing Spark SQL transformation...")

    transformation_sql = """
    INSERT OVERWRITE TABLE silver.customer_interactions
    PARTITION (interaction_year, interaction_month)
    SELECT
        -- Extract numeric IDs from string prefixes
        CAST(regexp_extract(interaction_id, '[0-9]+', 0) AS BIGINT) AS interaction_id,
        CAST(regexp_extract(customer_id, '[0-9]+', 0) AS BIGINT) AS customer_id,

        -- Cast timestamp fields
        CAST(interaction_timestamp AS TIMESTAMP) AS interaction_timestamp,
        CAST(interaction_timestamp AS DATE) AS interaction_date,

        -- String fields
        interaction_type,
        channel,

        -- Extract agent_id
        CAST(regexp_extract(agent_id, '[0-9]+', 0) AS BIGINT) AS agent_id,

        -- Classification fields
        category,
        subcategory,

        -- Metrics - cast to proper types
        CAST(sentiment_score AS DECIMAL(3,2)) AS sentiment_score,
        CAST(resolution_time_minutes AS INT) AS resolution_time_minutes,
        CAST(satisfaction_rating AS INT) AS satisfaction_rating,

        -- Determine is_resolved
        -- Resolved if: satisfaction_rating is provided (not null) and resolution_time_minutes > 0
        CASE
            WHEN satisfaction_rating IS NOT NULL
                 AND CAST(satisfaction_rating AS INT) > 0
                 AND resolution_time_minutes IS NOT NULL
                 AND CAST(resolution_time_minutes AS INT) > 0
                THEN TRUE
            ELSE FALSE
        END AS is_resolved,

        -- Determine is_escalated
        -- Escalated if notes contain keywords like "escalated", "escalate", "manager", "supervisor"
        CASE
            WHEN LOWER(notes) LIKE '%escalat%'
                 OR LOWER(notes) LIKE '%manager%'
                 OR LOWER(notes) LIKE '%supervisor%'
                 OR LOWER(notes) LIKE '%urgent%'
                THEN TRUE
            ELSE FALSE
        END AS is_escalated,

        -- Metadata
        _source_system AS source_system,
        CURRENT_TIMESTAMP() AS created_timestamp,

        -- Partition columns (must be last)
        YEAR(CAST(interaction_timestamp AS TIMESTAMP)) AS interaction_year,
        MONTH(CAST(interaction_timestamp AS TIMESTAMP)) AS interaction_month

    FROM bronze.customer_interactions_raw
    """

    print("  ✓ Executing SQL transformation...")
    spark.sql(transformation_sql)
    print("  ✓ SQL transformation complete")

    # Data quality checks
    print("\n[3/4] Data quality checks...")
    final_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.customer_interactions").collect()[0]['cnt']

    quality_metrics = spark.sql("""
                                SELECT COUNT(*)                                                as total_records,
                                       SUM(CASE WHEN interaction_id IS NULL THEN 1 ELSE 0 END) as null_interaction_ids,
                                       SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)    as null_customer_ids,
                                       SUM(CASE WHEN agent_id IS NULL THEN 1 ELSE 0 END)       as null_agent_ids,
                                       SUM(CASE WHEN is_resolved THEN 1 ELSE 0 END)            as resolved_count,
                                       SUM(CASE WHEN is_escalated THEN 1 ELSE 0 END)           as escalated_count,
                                       AVG(sentiment_score)                                    as avg_sentiment,
                                       AVG(resolution_time_minutes)                            as avg_resolution_time,
                                       AVG(satisfaction_rating)                                as avg_satisfaction
                                FROM silver.customer_interactions
                                """).collect()[0]

    print(f"  ✓ Final record count: {final_count:,}")
    print(f"  ✓ Records with null interaction_id: {quality_metrics['null_interaction_ids']}")
    print(f"  ✓ Records with null customer_id: {quality_metrics['null_customer_ids']}")
    print(f"  ✓ Records with null agent_id: {quality_metrics['null_agent_ids']}")
    print(f"  ✓ Resolved interactions: {quality_metrics['resolved_count']}")
    print(f"  ✓ Escalated interactions: {quality_metrics['escalated_count']}")
    print(f"  ✓ Average sentiment: {quality_metrics['avg_sentiment']:.2f}")
    print(f"  ✓ Average resolution time: {quality_metrics['avg_resolution_time']:.1f} minutes")
    print(f"  ✓ Average satisfaction: {quality_metrics['avg_satisfaction']:.2f}")

    if quality_metrics['null_interaction_ids'] > 0 or quality_metrics['null_customer_ids'] > 0:
        print("  ⚠️  Warning: Found null values in key fields")

    # Calculate data quality percentage
    null_keys = max(quality_metrics['null_interaction_ids'], quality_metrics['null_customer_ids'])
    valid_records = final_count - null_keys
    quality_pct = (valid_records / final_count * 100) if final_count > 0 else 0

    # Show sample data
    print("\n" + "=" * 80)
    print("  Sample Data from silver.customer_interactions")
    print("=" * 80)
    spark.sql("""
              SELECT interaction_id,
                     customer_id,
                     interaction_date,
                     interaction_type,
                     channel,
                     category,
                     sentiment_score,
                     resolution_time_minutes,
                     satisfaction_rating,
                     is_resolved,
                     is_escalated
              FROM silver.customer_interactions LIMIT 5
              """).show(truncate=False)

    # Summary statistics
    print("\n" + "=" * 80)
    print("  Transformation Summary")
    print("=" * 80)
    print(f"Input records (bronze):  {initial_count:,}")
    print(f"Output records (silver): {final_count:,}")
    print(f"Data quality: {quality_pct:.2f}% valid records")

    # Business metrics summary
    print("\n[4/4] Business Metrics Summary:")
    spark.sql("""
              SELECT COUNT(*)                                      as total_interactions,
                     COUNT(DISTINCT customer_id)                   as unique_customers,
                     COUNT(DISTINCT agent_id)                      as unique_agents,
                     SUM(CASE WHEN is_resolved THEN 1 ELSE 0 END)  as resolved,
                     SUM(CASE WHEN is_escalated THEN 1 ELSE 0 END) as escalated,
                     ROUND(AVG(sentiment_score), 2)                as avg_sentiment,
                     ROUND(AVG(resolution_time_minutes), 1)        as avg_resolution_mins,
                     ROUND(AVG(satisfaction_rating), 2)            as avg_satisfaction
              FROM silver.customer_interactions
              """).show(truncate=False)

    # Interaction type distribution
    print("\nInteraction type distribution:")
    spark.sql("""
              SELECT interaction_type,
                     COUNT(*) as count,
            ROUND(AVG(sentiment_score), 2) as avg_sentiment,
            ROUND(AVG(resolution_time_minutes), 1) as avg_resolution_time
              FROM silver.customer_interactions
              GROUP BY interaction_type
              ORDER BY count DESC
              """).show()

    # Channel distribution
    print("\nChannel distribution:")
    spark.sql("""
              SELECT channel,
                     COUNT(*) as count,
            ROUND(AVG(satisfaction_rating), 2) as avg_satisfaction,
            SUM(CASE WHEN is_escalated THEN 1 ELSE 0 END) as escalations
              FROM silver.customer_interactions
              GROUP BY channel
              ORDER BY count DESC
              """).show()

    # Category analysis
    print("\nCategory breakdown:")
    spark.sql("""
              SELECT category,
                     subcategory,
                     COUNT(*) as count,
            ROUND(AVG(sentiment_score), 2) as avg_sentiment,
            SUM(CASE WHEN is_resolved THEN 1 ELSE 0 END) as resolved_count
              FROM silver.customer_interactions
              GROUP BY category, subcategory
              ORDER BY count DESC
                  LIMIT 10
              """).show(truncate=False)

    # Sentiment analysis
    print("\nSentiment distribution:")
    spark.sql("""
              SELECT CASE
                         WHEN sentiment_score >= 0.5 THEN 'Positive'
                         WHEN sentiment_score >= 0 THEN 'Neutral'
                         ELSE 'Negative'
                         END as sentiment_category,
                     COUNT(*) as count,
            ROUND(AVG(satisfaction_rating), 2) as avg_satisfaction,
            ROUND(AVG(resolution_time_minutes), 1) as avg_resolution_time
              FROM silver.customer_interactions
              GROUP BY
                  CASE
                  WHEN sentiment_score >= 0.5 THEN 'Positive'
                  WHEN sentiment_score >= 0 THEN 'Neutral'
                  ELSE 'Negative'
              END
              ORDER BY count DESC
              """).show()

    # Escalated interactions
    print("\nEscalated interactions needing attention:")
    spark.sql("""
              SELECT interaction_id,
                     customer_id,
                     interaction_type,
                     category,
                     sentiment_score,
                     is_resolved
              FROM silver.customer_interactions
              WHERE is_escalated = true
              ORDER BY interaction_date DESC LIMIT 10
              """).show()

    print("\n" + "=" * 80)
    print("  SUCCESS! Customer interactions transformed to silver layer using Spark SQL")
    print("=" * 80)
    print("\nQuery examples:")
    print("  spark.sql('SELECT * FROM silver.customer_interactions WHERE is_escalated = true LIMIT 10').show()")
    print("  spark.sql('SELECT * FROM silver.customer_interactions WHERE sentiment_score < 0 LIMIT 10').show()")
    print("  spark.sql('SELECT category, COUNT(*) FROM silver.customer_interactions GROUP BY category').show()")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
