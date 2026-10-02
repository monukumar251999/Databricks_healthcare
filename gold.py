# Databricks notebook source
# DBTITLE 1,Create dim_month temp view

# Build dim_month temp view from dim_date
from pyspark.sql.functions import col, date_trunc, last_day, month, year

dim_month = (
    spark.table("health.bronze_db.dim_date")
    .select(
        date_trunc("month", col("date")).alias("month_start"),
        last_day(col("date")).alias("month_end"),
        year(col("date")).alias("year"),
        month(col("date")).alias("month"),
    )
    .distinct()
)

dim_month.createOrReplaceTempView("dim_month")

# COMMAND ----------

# DBTITLE 1,Insert member_month from eligibility

# Build member_month from eligibility join dim_month
from pyspark.sql.functions import coalesce, col, current_timestamp, lit, month, to_date, year

eligibility = spark.table("health.silver_db.eligibility")

member_month = (
    eligibility.alias("e")
    .join(
        dim_month.alias("m"),
        (col("m.month_start") <= coalesce(col("e.termination_date"), to_date(lit("9999-12-31"))))
        & (col("m.month_end") >= col("e.effective_date")),
    )
    .select(
        col("e.member_id"),
        col("e.subscriber_id"),
        col("m.month_start"),
        col("m.month_end"),
        year(col("m.month_start")).alias("year"),
        month(col("m.month_start")).alias("month"),
        col("e.plan_id"),
        col("e.product_id"),
        col("e.coverage_type"),
        col("e.pcp_id"),
        lit(1).alias("member_month"),
        col("e.effective_date"),
        col("e.termination_date"),
        current_timestamp().alias("created_timestamp"),
        current_timestamp().alias("updated_timestamp"),
    )
)

member_month.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("health.gold_db.member_month")

# COMMAND ----------

# DBTITLE 1,Create dim_member table
# # Create dim_member from silver member table
# dim_member = (
#     spark.table("health.silver_db.member")
#     .select(
#         "member_id",
#         "subscriber_id",
#         "gender",
#         "date_of_birth",
#         "race",
#         "state",
#         "zip_code",
#     )
#     .distinct()
# )

# dim_member.write.mode("overwrite").saveAsTable("health.gold_db.dim_member")