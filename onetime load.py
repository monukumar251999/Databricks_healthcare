# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC create catalog health;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema health.bronze_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema health.silver_db

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema health.gold_db

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE health.bronze_db.member_eligibility
# MAGIC (
# MAGIC     member_id STRING,
# MAGIC     subscriber_id STRING,
# MAGIC     effective_date DATE,
# MAGIC     termination_date DATE,
# MAGIC     plan_id STRING,
# MAGIC     product_id STRING,
# MAGIC     coverage_type STRING,
# MAGIC     member_status STRING,
# MAGIC     pcp_id STRING,
# MAGIC     source_system STRING,
# MAGIC     updated_timestamp TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE health.bronze_db.dim_date
# MAGIC AS
# MAGIC SELECT
# MAGIC     date,
# MAGIC     year(date) AS year,
# MAGIC     month(date) AS month,
# MAGIC     date_trunc('month', date) AS month_start,
# MAGIC     last_day(date) AS month_end
# MAGIC FROM (
# MAGIC     SELECT explode(
# MAGIC         sequence(
# MAGIC             DATE '2020-01-01',
# MAGIC             DATE '2030-12-31',
# MAGIC             interval 1 day
# MAGIC         )
# MAGIC     ) AS date
# MAGIC );

# COMMAND ----------

# %sql
# CREATE OR REPLACE TEMP VIEW dim_month AS

# SELECT DISTINCT
#     date_trunc('month', date) AS month_start,
#     last_day(date) AS month_end,
#     year(date) AS year,
#     month(date) AS month
# FROM health.bronze_db.dim_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     e.member_id,
# MAGIC     m.month_start,
# MAGIC     m.month_end,
# MAGIC     e.plan_id,
# MAGIC     e.product_id,
# MAGIC     e.coverage_type,
# MAGIC     e.pcp_id,
# MAGIC     1 AS member_month
# MAGIC FROM health.silver_db.eligibility e
# MAGIC JOIN health.bronze_db.dim_date m
# MAGIC     ON m.month_start <= COALESCE(
# MAGIC         e.termination_date,
# MAGIC         DATE '9999-12-31'
# MAGIC     )
# MAGIC    AND m.month_end >= e.effective_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE health.gold_db.member_month
# MAGIC (
# MAGIC     member_id STRING,
# MAGIC     subscriber_id STRING,
# MAGIC
# MAGIC     month_start DATE,
# MAGIC     month_end DATE,
# MAGIC
# MAGIC     year INT,
# MAGIC     month INT,
# MAGIC
# MAGIC     plan_id STRING,
# MAGIC     product_id STRING,
# MAGIC     coverage_type STRING,
# MAGIC
# MAGIC     pcp_id STRING,
# MAGIC
# MAGIC     member_month INT,
# MAGIC
# MAGIC     effective_date DATE,
# MAGIC     termination_date DATE,
# MAGIC
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;