# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Identify Members with Multiple Records per Month
# MAGIC %sql
# MAGIC SELECT
# MAGIC     member_id,
# MAGIC     month_start,
# MAGIC     COUNT(*) AS cnt
# MAGIC FROM health.gold_db.member_month
# MAGIC GROUP BY
# MAGIC     member_id,
# MAGIC     month_start
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH periods AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         member_id,
# MAGIC         effective_date,
# MAGIC         termination_date,
# MAGIC         LAG(termination_date) OVER (
# MAGIC             PARTITION BY member_id
# MAGIC             ORDER BY effective_date
# MAGIC         ) AS previous_termination_date
# MAGIC
# MAGIC     FROM health.silver_db.eligibility
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM periods
# MAGIC WHERE
# MAGIC     previous_termination_date IS NOT NULL
# MAGIC     AND effective_date <= previous_termination_date;