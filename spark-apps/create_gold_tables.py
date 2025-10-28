"""
Create Gold Tables - Initialize Gold Layer Schema

This script creates the gold database and all gold layer tables for business-level
aggregates and analytics. The gold layer contains pre-aggregated metrics optimized
for reporting and BI tools.

Tables created:
- customer_360
- product_performance
- cohort_analysis
- channel_attribution
- category_brand_performance
- store_performance
- subscription_health
- basket_analysis
- campaign_roi_analysis

Usage:
    ./submit.sh create_gold_tables.py
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession

print("=" * 80)
print("  Create Gold Tables")
print("=" * 80)

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--name", dest="app_name")
known_args, _ = parser.parse_known_args()
app_name = known_args.app_name
if app_name is None:
    app_name = 'CreateGoldTables'

builder = SparkSession.builder.enableHiveSupport()
spark = builder.appName(app_name).getOrCreate() if app_name else builder.getOrCreate()

# Register lineage listener to push sources/destinations to Neo4j
try:
    from lineage_listener import register_lineage_listener

    register_lineage_listener(spark)
except Exception as e:
    print(f"Warning: Could not register lineage listener: {e}")

try:
    print(f"\nStarting table creation at: {datetime.now()}")

    # Create gold database
    print("\n[1/10] Creating gold database...")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    print("  ✓ Gold database ready")

    # Table definitions
    tables = {
        "customer_360": """
            CREATE TABLE IF NOT EXISTS gold.customer_360 (
                customer_key BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                analysis_date DATE NOT NULL,
                customer_age INT,
                customer_segment STRING,
                lifetime_stage STRING,
                first_purchase_date DATE,
                last_purchase_date DATE,
                total_orders BIGINT,
                total_items_purchased BIGINT,
                total_revenue DECIMAL(15,2),
                total_profit DECIMAL(15,2),
                avg_order_value DECIMAL(10,2),
                avg_items_per_order DECIMAL(8,2),
                orders_last_30d INT,
                revenue_last_30d DECIMAL(12,2),
                orders_last_90d INT,
                revenue_last_90d DECIMAL(12,2),
                orders_last_365d INT,
                revenue_last_365d DECIMAL(12,2),
                unique_products_purchased_365d INT,
                unique_categories_purchased_365d INT,
                primary_channel STRING,
                channel_diversity_score DECIMAL(3,2),
                online_order_percentage DECIMAL(5,2),
                mobile_order_percentage DECIMAL(5,2),
                instore_order_percentage DECIMAL(5,2),
                most_purchased_category STRING,
                most_purchased_brand STRING,
                avg_price_point DECIMAL(10,2),
                premium_product_percentage DECIMAL(5,2),
                loyalty_tier STRING,
                total_loyalty_points_earned BIGINT,
                total_loyalty_points_redeemed BIGINT,
                loyalty_points_balance BIGINT,
                has_active_subscription BOOLEAN,
                subscription_count INT,
                subscription_mrr DECIMAL(12,2),
                days_since_last_purchase INT,
                purchase_frequency_days DECIMAL(8,2),
                recency_score INT,
                frequency_score INT,
                monetary_score INT,
                rfm_segment STRING,
                total_support_interactions BIGINT,
                avg_satisfaction_rating DECIMAL(3,2),
                unresolved_issues_count INT,
                total_returns BIGINT,
                return_rate DECIMAL(5,2),
                total_return_value DECIMAL(12,2),
                orders_with_discount BIGINT,
                discount_usage_rate DECIMAL(5,2),
                avg_discount_percentage DECIMAL(5,2),
                clv_prediction DECIMAL(12,2),
                churn_probability DECIMAL(3,2),
                next_purchase_probability DECIMAL(3,2),
                predicted_next_purchase_date DATE,
                campaign_response_rate DECIMAL(5,2),
                email_open_rate DECIMAL(5,2),
                email_click_rate DECIMAL(5,2),
                created_timestamp TIMESTAMP,
                updated_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (analysis_date)
            LOCATION 's3a://data/gold/customer_360'
        """,
        "product_performance": """
            CREATE TABLE IF NOT EXISTS gold.product_performance (
                product_key BIGINT NOT NULL,
                product_id BIGINT NOT NULL,
                analysis_date DATE NOT NULL,
                time_period STRING NOT NULL,
                product_name STRING,
                category_level1 STRING,
                category_level2 STRING,
                brand STRING,
                total_units_sold BIGINT,
                total_transactions BIGINT,
                total_customers BIGINT,
                new_customers BIGINT,
                repeat_customers BIGINT,
                gross_revenue DECIMAL(15,2),
                net_revenue DECIMAL(15,2),
                total_discounts DECIMAL(12,2),
                avg_selling_price DECIMAL(10,2),
                avg_discount_percentage DECIMAL(5,2),
                total_cost DECIMAL(15,2),
                gross_profit DECIMAL(15,2),
                gross_margin_percentage DECIMAL(5,2),
                contribution_margin DECIMAL(15,2),
                units_returned BIGINT,
                return_rate DECIMAL(5,2),
                return_value DECIMAL(12,2),
                top_return_reason STRING,
                avg_inventory_on_hand DECIMAL(12,2),
                stockout_days INT,
                stockout_rate DECIMAL(5,2),
                inventory_turnover DECIMAL(8,2),
                days_inventory_outstanding DECIMAL(8,2),
                units_per_day DECIMAL(10,2),
                revenue_per_day DECIMAL(12,2),
                sales_velocity_trend STRING,
                category_revenue_rank INT,
                category_units_rank INT,
                overall_revenue_rank INT,
                category_market_share DECIMAL(5,2),
                penetration_rate DECIMAL(5,2),
                repeat_purchase_rate DECIMAL(5,2),
                avg_units_per_customer DECIMAL(8,2),
                price_elasticity_coefficient DECIMAL(8,4),
                optimal_price_point DECIMAL(10,2),
                basket_attachment_rate DECIMAL(5,2),
                avg_basket_size_with_product DECIMAL(8,2),
                top_cross_sell_products ARRAY<STRING>,
                revenue_growth_rate DECIMAL(8,2),
                volume_growth_rate DECIMAL(8,2),
                market_share_change DECIMAL(8,2),
                is_trending BOOLEAN,
                is_seasonal BOOLEAN,
                is_low_performer BOOLEAN,
                is_high_performer BOOLEAN,
                requires_reorder BOOLEAN,
                created_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (analysis_date, time_period)
            LOCATION 's3a://data/gold/product_performance'
        """,
        "cohort_analysis": """
            CREATE TABLE IF NOT EXISTS gold.cohort_analysis (
                cohort_month DATE NOT NULL,
                months_since_first_purchase INT NOT NULL,
                cohort_size BIGINT,
                customers_active INT,
                retention_rate DECIMAL(5,2),
                cumulative_retention_rate DECIMAL(5,2),
                churn_rate DECIMAL(5,2),
                cumulative_churn_rate DECIMAL(5,2),
                cohort_revenue DECIMAL(15,2),
                cumulative_cohort_revenue DECIMAL(15,2),
                avg_revenue_per_customer DECIMAL(10,2),
                cumulative_avg_revenue_per_customer DECIMAL(10,2),
                total_orders BIGINT,
                cumulative_orders BIGINT,
                avg_orders_per_customer DECIMAL(8,2),
                cumulative_avg_orders_per_customer DECIMAL(8,2),
                repeat_purchase_rate DECIMAL(5,2),
                avg_purchase_frequency DECIMAL(8,2),
                avg_days_between_purchases DECIMAL(8,2),
                online_percentage DECIMAL(5,2),
                mobile_percentage DECIMAL(5,2),
                instore_percentage DECIMAL(5,2),
                subscription_adoption_rate DECIMAL(5,2),
                avg_subscription_value DECIMAL(10,2),
                created_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (cohort_month)
            LOCATION 's3a://data/gold/cohort_analysis'
        """,
        "channel_attribution": """
            CREATE TABLE IF NOT EXISTS gold.channel_attribution (
                attribution_date DATE NOT NULL,
                customer_id BIGINT NOT NULL,
                transaction_id BIGINT NOT NULL,
                transaction_revenue DECIMAL(12,2),
                transaction_profit DECIMAL(12,2),
                first_touch_channel STRING,
                first_touch_campaign_id BIGINT,
                first_touch_attribution_revenue DECIMAL(12,2),
                last_touch_channel STRING,
                last_touch_campaign_id BIGINT,
                last_touch_attribution_revenue DECIMAL(12,2),
                touchpoint_count INT,
                linear_attribution_per_touchpoint DECIMAL(12,2),
                time_decay_attribution_map MAP<STRING, DECIMAL(12,2)>,
                position_based_attribution_map MAP<STRING, DECIMAL(12,2)>,
                data_driven_attribution_map MAP<STRING, DECIMAL(12,2)>,
                journey_length_days INT,
                journey_touchpoints ARRAY<STRUCT<channel: STRING, timestamp: TIMESTAMP, campaign_id: BIGINT, event_type: STRING>>,
                created_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (attribution_date)
            LOCATION 's3a://data/gold/channel_attribution'
        """,
        "category_brand_performance": """
            CREATE TABLE IF NOT EXISTS gold.category_brand_performance (
                analysis_date DATE NOT NULL,
                time_period STRING NOT NULL,
                category_level1 STRING,
                category_level2 STRING,
                category_level3 STRING,
                brand STRING,
                total_revenue DECIMAL(15,2),
                total_units_sold BIGINT,
                total_transactions BIGINT,
                unique_customers BIGINT,
                avg_transaction_value DECIMAL(10,2),
                avg_units_per_transaction DECIMAL(8,2),
                avg_price_point DECIMAL(10,2),
                total_profit DECIMAL(15,2),
                profit_margin_percentage DECIMAL(5,2),
                category_revenue_percentage DECIMAL(5,2),
                brand_revenue_percentage DECIMAL(5,2),
                revenue_growth_wow DECIMAL(8,2),
                revenue_growth_mom DECIMAL(8,2),
                revenue_growth_yoy DECIMAL(8,2),
                units_growth_yoy DECIMAL(8,2),
                customer_penetration_rate DECIMAL(5,2),
                new_customer_percentage DECIMAL(5,2),
                repeat_customer_rate DECIMAL(5,2),
                avg_discount_percentage DECIMAL(5,2),
                promotional_sales_percentage DECIMAL(5,2),
                price_index DECIMAL(8,2),
                revenue_rank_in_category INT,
                units_rank_in_category INT,
                growth_rank_in_category INT,
                created_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (analysis_date, time_period)
            LOCATION 's3a://data/gold/category_brand_performance'
        """,
        "store_performance": """
            CREATE TABLE IF NOT EXISTS gold.store_performance (
                analysis_date DATE NOT NULL,
                store_id BIGINT NOT NULL,
                store_name STRING,
                store_region STRING,
                store_tier STRING,
                daily_revenue DECIMAL(12,2),
                daily_transactions INT,
                daily_customers INT,
                avg_transaction_value DECIMAL(10,2),
                revenue_per_square_foot DECIMAL(10,2),
                foot_traffic INT,
                conversion_rate DECIMAL(5,2),
                staff_count INT,
                revenue_per_employee DECIMAL(10,2),
                inventory_value DECIMAL(15,2),
                stockout_rate DECIMAL(5,2),
                inventory_turnover DECIMAL(8,2),
                avg_satisfaction_rating DECIMAL(3,2),
                nps_score DECIMAL(5,2),
                revenue_vs_target DECIMAL(8,2),
                revenue_rank_in_region INT,
                revenue_percentile DECIMAL(5,2),
                revenue_trend_7d STRING,
                revenue_growth_wow DECIMAL(8,2),
                revenue_growth_yoy DECIMAL(8,2),
                created_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (analysis_date)
            LOCATION 's3a://data/gold/store_performance'
        """,
        "subscription_health": """
            CREATE TABLE IF NOT EXISTS gold.subscription_health (
                analysis_date DATE NOT NULL,
                plan_id BIGINT NOT NULL,
                plan_name STRING,
                subscription_type STRING,
                billing_frequency STRING,
                active_subscribers BIGINT,
                new_subscribers_mtd INT,
                cancelled_subscribers_mtd INT,
                paused_subscribers INT,
                net_subscriber_change INT,
                mrr DECIMAL(15,2),
                arr DECIMAL(15,2),
                avg_revenue_per_subscriber DECIMAL(10,2),
                lifetime_value_avg DECIMAL(12,2),
                churn_rate_monthly DECIMAL(5,2),
                churn_rate_annual DECIMAL(5,2),
                revenue_churn_rate DECIMAL(5,2),
                reactivation_rate DECIMAL(5,2),
                retention_rate_30d DECIMAL(5,2),
                retention_rate_90d DECIMAL(5,2),
                retention_rate_365d DECIMAL(5,2),
                cohort_0_3_months_retention DECIMAL(5,2),
                cohort_3_6_months_retention DECIMAL(5,2),
                cohort_6_12_months_retention DECIMAL(5,2),
                cohort_12plus_months_retention DECIMAL(5,2),
                avg_subscription_duration_days DECIMAL(10,2),
                avg_payments_before_churn DECIMAL(8,2),
                subscriber_growth_rate DECIMAL(8,2),
                mrr_growth_rate DECIMAL(8,2),
                quick_ratio DECIMAL(8,2),
                top_cancellation_reason STRING,
                avg_days_to_cancel DECIMAL(10,2),
                voluntary_churn_rate DECIMAL(5,2),
                involuntary_churn_rate DECIMAL(5,2),
                created_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (analysis_date)
            LOCATION 's3a://data/gold/subscription_health'
        """,
        "basket_analysis": """
            CREATE TABLE IF NOT EXISTS gold.basket_analysis (
                analysis_date DATE NOT NULL,
                product_id_a BIGINT NOT NULL,
                product_id_b BIGINT NOT NULL,
                product_name_a STRING,
                product_name_b STRING,
                category_a STRING,
                category_b STRING,
                transactions_with_a BIGINT,
                transactions_with_b BIGINT,
                transactions_with_both BIGINT,
                total_transactions BIGINT,
                support DECIMAL(8,6),
                confidence_a_to_b DECIMAL(8,6),
                confidence_b_to_a DECIMAL(8,6),
                lift DECIMAL(10,4),
                conviction DECIMAL(10,4),
                avg_basket_value_with_both DECIMAL(10,2),
                avg_basket_value_with_a_only DECIMAL(10,2),
                incremental_basket_value DECIMAL(10,2),
                recommendation_score DECIMAL(5,2),
                is_strong_association BOOLEAN,
                created_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (analysis_date)
            LOCATION 's3a://data/gold/basket_analysis'
        """,
        "campaign_roi_analysis": """
            CREATE TABLE IF NOT EXISTS gold.campaign_roi_analysis (
                analysis_date DATE NOT NULL,
                campaign_id BIGINT NOT NULL,
                campaign_name STRING,
                campaign_type STRING,
                channel STRING,
                start_date DATE,
                end_date DATE,
                campaign_duration_days INT,
                total_budget DECIMAL(12,2),
                actual_spend DECIMAL(12,2),
                budget_utilization_rate DECIMAL(5,2),
                total_impressions BIGINT,
                total_clicks BIGINT,
                total_conversions BIGINT,
                unique_customers_reached BIGINT,
                click_through_rate DECIMAL(5,2),
                conversion_rate DECIMAL(5,2),
                attributed_revenue DECIMAL(15,2),
                attributed_transactions BIGINT,
                new_customer_revenue DECIMAL(12,2),
                existing_customer_revenue DECIMAL(12,2),
                attributed_profit DECIMAL(12,2),
                cost_per_impression DECIMAL(10,4),
                cost_per_click DECIMAL(10,2),
                cost_per_acquisition DECIMAL(10,2),
                roi_percentage DECIMAL(8,2),
                roas DECIMAL(8,2),
                incremental_revenue DECIMAL(12,2),
                incremental_profit DECIMAL(12,2),
                avg_clv_of_acquired_customers DECIMAL(10,2),
                estimated_lifetime_roi DECIMAL(8,2),
                revenue_per_impression DECIMAL(10,4),
                profit_per_dollar_spent DECIMAL(10,2),
                roi_vs_channel_avg DECIMAL(8,2),
                performance_rank_in_channel INT,
                created_timestamp TIMESTAMP
            )
            USING PARQUET
            PARTITIONED BY (analysis_date)
            LOCATION 's3a://data/gold/campaign_roi'
        """
    }

    # Create each table
    table_list = list(tables.keys())
    created_tables = []
    failed_tables = []

    for idx, (table_name, ddl) in enumerate(tables.items(), 2):
        print(f"\n[{idx}/10] Creating table: gold.{table_name}")
        try:
            spark.sql(ddl)
            print(f"  ✓ Table created: gold.{table_name}")
            created_tables.append(table_name)
        except Exception as e:
            print(f"  ❌ Failed to create {table_name}: {str(e)}")
            failed_tables.append(table_name)

    # Show created tables
    print("\n" + "=" * 80)
    print("  Gold Tables Summary")
    print("=" * 80)

    tables_df = spark.sql("SHOW TABLES IN gold")
    table_count = tables_df.count()
    print(f"\nTotal tables in gold database: {table_count}")
    print("\nTables:")
    tables_df.show(truncate=False)

    # Final summary
    print("\n" + "=" * 80)
    if len(failed_tables) == 0:
        print("  SUCCESS! All gold tables created")
        print("=" * 80)
        print(f"\nCreated {len(created_tables)}/{len(table_list)} tables:")
        for table in created_tables:
            print(f"  - gold.{table}")
        print("\nNext steps:")
        print("  1. Ensure silver tables are populated")
        print("  2. Run gold_*.py scripts to load analytics data")
    else:
        print("  COMPLETED WITH ERRORS")
        print("=" * 80)
        print(f"\nCreated: {len(created_tables)}/{len(table_list)} tables")
        print(f"Failed: {len(failed_tables)}/{len(table_list)} tables")
        if failed_tables:
            print("\nFailed tables:")
            for table in failed_tables:
                print(f"  - gold.{table}")

except Exception as e:
    print(f"\n❌ FATAL ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
