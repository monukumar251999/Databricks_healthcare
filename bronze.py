# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import random
from datetime import date, datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.functions import col
from delta.tables import DeltaTable

# COMMAND ----------

random.seed(42)

plans = ["PLN001", "PLN002", "PLN003", "PLN004", "PLN005"]
products = ["PROD_A", "PROD_B", "PROD_C"]
coverage_types = ["MEDICAL", "DENTAL", "VISION", "PHARMACY"]
member_statuses = ["ACTIVE", "INACTIVE", "TERMINATED"]
source_systems = ["SRC_A", "SRC_B", "SRC_C"]

rows = []
for i in range(1, 101):
    member_id = f"M{i:07d}"
    subscriber_id = f"S{i:07d}"
    eff_date = date(2020, 1, 1) + timedelta(days=random.randint(0, 1200))
    term_date = eff_date + timedelta(days=random.randint(30, 1000))
    plan_id = random.choice(plans)
    product_id = random.choice(products)
    coverage_type = random.choice(coverage_types)
    member_status = random.choice(member_statuses)
    pcp_id = f"PCP{random.randint(1, 50):03d}"
    source_system = random.choice(source_systems)
    updated_ts = datetime.combine(eff_date + timedelta(days=random.randint(0, 500)), datetime.min.time())
    rows.append(Row(
        member_id=member_id,
        subscriber_id=subscriber_id,
        effective_date=eff_date,
        termination_date=term_date,
        plan_id=plan_id,
        product_id=product_id,
        coverage_type=coverage_type,
        member_status=member_status,
        pcp_id=pcp_id,
        source_system=source_system,
        updated_timestamp=updated_ts,
    ))


new_df = spark.createDataFrame(rows)

# Idempotent upsert: MERGE on member_id so re-running this script
# never creates duplicate rows — existing members are updated,
# new members are inserted.
delta_table = DeltaTable.forName(spark, "health.bronze_db.member_eligibility")

(
    delta_table.alias("t")
    .merge(new_df.alias("s"), col("t.member_id") == col("s.member_id"))
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

final_count = spark.table("health.bronze_db.member_eligibility").count()
print(f"Upserted {len(rows)} rows into member_eligibility (idempotent MERGE)")
print(f"Total rows in member_eligibility: {final_count}")

spark.table("member_eligibility").display()