# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = (
    spark.table("health.bronze_db.member_eligibility")
    .withColumn(
        "effective_date",
        F.to_date("effective_date")
    )
    .withColumn(
        "termination_date",
        F.to_date("termination_date")
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy(
        "member_id",
        "effective_date",
        "plan_id"
    )
    .orderBy(
        F.col("updated_timestamp").desc()
    )
)

df_silver = (
    df
    .withColumn(
        "rn",
        F.row_number().over(window_spec)
    )
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("health.silver_db.eligibility")
)

# COMMAND ----------

from pyspark.sql import functions as F

eligibility = spark.table(
    "healthcare.silver_member_eligibility"
)

months = (
    spark.table("healthcare.dim_date")
    .select(
        F.date_trunc(
            "month",
            F.col("date")
        ).alias("month_start"),
        F.last_day(
            F.col("date")
        ).alias("month_end")
    )
    .distinct()
)

member_month = (
    eligibility.alias("e")
    .join(
        months.alias("m"),
        (
            F.col("m.month_start")
            <= F.coalesce(
                F.col("e.termination_date"),
                F.to_date(
                    F.lit("9999-12-31")
                )
            )
        )
        &
        (
            F.col("m.month_end")
            >= F.col("e.effective_date")
        ),
        "inner"
    )
)

# COMMAND ----------

member_month = member_month.select(
    F.col("e.member_id"),
    F.col("e.subscriber_id"),
    F.col("m.month_start"),
    F.col("m.month_end"),
    F.col("e.plan_id"),
    F.col("e.product_id"),
    F.col("e.coverage_type"),
    F.col("e.pcp_id"),
    F.lit(1).alias("member_month")
)