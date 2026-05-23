# Databricks notebook source
# MAGIC %md
# MAGIC # Generating Data Quality Metrics with DQX
# MAGIC
# MAGIC In this demo we demonstrate how to generate and persist a set of data quality summary metrics when applying DQX rules to your data. DQX can track aggregate-level quality metrics based on your quality rule definitions. Aggregate-level quality metrics can be tracked for both batch and streaming data processing workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install DQX

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx

%restart_python

# COMMAND ----------

default_catalog = "main"
default_schema = "default"

dbutils.widgets.text("demo_catalog", default_catalog, "Catalog Name")
dbutils.widgets.text("demo_schema", default_schema, "Schema Name")

demo_catalog_name = dbutils.widgets.get("demo_catalog")
demo_schema_name = dbutils.widgets.get("demo_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining Checks in YAML
# MAGIC We'll now define some data quality rules to apply to our dataset. We'll use some of DQX's built-in rules:
# MAGIC
# MAGIC - `is_not_null_and_not_empty`
# MAGIC - `is_in_range`
# MAGIC - `is_in_list`
# MAGIC
# MAGIC You can find a list of all available built-in checks in the documentation [here](https://databrickslabs.github.io/dqx/docs/reference/quality_checks/).
# MAGIC
# MAGIC You can define checks in two ways:
# MAGIC * Declarative approach (YAML or JSON, or a table): Ideal for scenarios where checks are externalized from code.
# MAGIC * Code-first (DQX classes): Ideal if you need type-safety and better IDE support
# MAGIC
# MAGIC We are demonstrating creating and validating a set of `quality rules (checks)` defined in YAML.
# MAGIC
# MAGIC We can use `validate_checks` to verify that the checks are defined correctly.

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine

checks_from_yaml = yaml.safe_load("""
# 1. Ensure id, age, country are not null or empty (error-level)
- check:
    function: is_not_null_and_not_empty
    for_each_column:  # define check for multiple columns at once
      - id
      - age
      - country
  criticality: error

# 2. Warn if age is outside [18, 120] for Germany or France
- check:
    function: is_in_range
    filter: country in ['Germany', 'France']
    arguments:
      column: age  # define check for a single column
      min_limit: 18
      max_limit: 120
  criticality: warn
  name: age_not_in_range  # optional check name, auto-generated if not provided

# 3. Warn if country is not Germany or France
- check:
    dimension: validity
    function: is_in_list
    for_each_column:
      - country
    arguments:
      allowed:
        - Germany
        - France
  criticality: warn

# 4. Error if id is not unique across the dataset
- check:
    function: is_unique
    arguments:
      columns:
        - id
  criticality: warn
""")

status = DQEngine.validate_checks(checks_from_yaml)
print(f"Checks from YAML: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using the `DQMetricsObserver`
# MAGIC
# MAGIC DQX provides a `DQMetricsObserver` for tracking aggregate-level metrics about the quality checks output. While checking data at a row level with DQX, `DQMetricsObserver` will track:
# MAGIC
# MAGIC - The number of input rows
# MAGIC - The number of rows with warnings
# MAGIC - The number of rows with errors
# MAGIC - Per-check error and warning counts (as a JSON breakdown per check name)
# MAGIC - User-defined custom metrics
# MAGIC
# MAGIC Summary metrics can be tracked for both streaming and batch datasets. To track summary metrics, pass a `DQMetricsObserver` when creating the `DQEngine`.

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.sdk import WorkspaceClient

# Create the metrics observer
observer = DQMetricsObserver(name="my_observation")  # the name is used as run_name when saving metrics to a table

# `WorkspaceClient` will be authenticated as the current user inside Databricks
ws = WorkspaceClient()
dq_engine = DQEngine(ws, observer=observer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating Quality Metrics
# MAGIC
# MAGIC Applying checks will generate both row-level warnings and errors and a [Spark Observation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Observation.html#pyspark.sql.Observation) with summary-level data quality metrics. Metrics will be tracked for all rows in the input dataset even if you use DQX to split data into valid and quarantine datasets or further process the data.

# COMMAND ----------

from pyspark.sql import Row

# Create some input data
new_users = [
    Row(id=1, age=23, country='Germany'),
    Row(id=2, age=30, country='France'),
    Row(id=3, age=16, country='Germany'),    # Invalid -> age - less than 18
    Row(id=None, age=29, country='France'),  # Invalid -> id - NULL
    Row(id=4, age=29, country=''),           # Invalid -> country - Empty
    Row(id=5, age=23, country='Italy'),      # Invalid -> country - not in allowed
    Row(id=6, age=123, country='France'),    # Invalid -> age - greater than 120
    Row(id=2, age=23, country='Germany'),    # duplicated id
]
new_users_df = spark.createDataFrame(new_users)

# Apply the checks and display the row-level results
# Below example uses apply_checks_by_metadata method, but metrics are supported in all apply check methods
validated_df, observation = dq_engine.apply_checks_by_metadata(new_users_df, checks_from_yaml)
display(validated_df)

# Get the summary-level metrics from the returned observation
validated_df.count()
metrics = observation.get
print(metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Quality Metrics to a Table
# MAGIC
# MAGIC Use `save_summary_metrics` to save aggregate-level quality metrics, or `save_results_in_table` to persist both checked data and the quality metrics. You can store data quality metrics from multiple workflows in the same summary table for reporting and alerting about data quality issues.
# MAGIC
# MAGIC ***NOTE:** Saving metrics to a table requires using a classic compute cluster in [Dedicated Access Mode](https://docs.databricks.com/aws/en/compute/configure#access-modes) due to Spark Connect limitations. This limitation will be lifted in the future.*

# COMMAND ----------

from databricks.labs.dqx.config import OutputConfig

# Create some input data
new_users = [
    Row(id=1, age=23, country='Germany'),
    Row(id=2, age=30, country='France'),
    Row(id=3, age=16, country='Germany'),    # Invalid -> age - less than 18
    Row(id=None, age=29, country='France'),  # Invalid -> id - NULL
    Row(id=4, age=29, country=''),           # Invalid -> country - Empty
    Row(id=5, age=23, country='Italy'),      # Invalid -> country - not in allowed
    Row(id=6, age=123, country='France'),    # Invalid -> age - greater than 120
    Row(id=2, age=23, country='Germany'),    # duplicated id
]
new_users_df = spark.createDataFrame(new_users)

# Apply the checks
validated_df, observation = dq_engine.apply_checks_by_metadata(new_users_df, checks_from_yaml)

# Setup the output table configuration
output_table_name = f"{demo_catalog_name}.{demo_schema_name}.output_table"
quarantine_table_name = f"{demo_catalog_name}.{demo_schema_name}.quarantine_table"

output_config = OutputConfig(location=output_table_name, mode="overwrite")
quarantine_config = OutputConfig(location=quarantine_table_name, mode="overwrite")


metrics_table_name = f"{demo_catalog_name}.{demo_schema_name}.metrics_table"
metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")

# COMMAND ----------

# Option 1: Use save metrics method
validated_df.count()  # Trigger an action to populate metrics (e.g. count, save to a table)
dq_engine.save_summary_metrics(observed_metrics=observation.get, metrics_config=metrics_config, output_config=output_config, quarantine_config=quarantine_config)
# for streaming use a listener (see dqx docs): spark.streams.addListener(get_streaming_metrics_listener(...))

# COMMAND ----------

# Option 2: Use save results method (save metrics and output)
dq_engine.save_results_in_table(
  output_df=validated_df,
  output_config=output_config,
  quarantine_config=quarantine_config,
  observation=observation,
  metrics_config=metrics_config
)

# COMMAND ----------

display(spark.table(output_table_name))
display(spark.table(quarantine_table_name))
display(spark.table(metrics_table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-end Quality Checking with Metrics
# MAGIC
# MAGIC You can generate quality metrics when running end-to-end validation when using DQX methods such as `apply_checks_and_save_in_table`, `apply_checks_by_metadata_and_save_in_table`, `apply_checks_and_save_in_tables` and `apply_checks_and_save_in_tables_for_patterns`. Pass information about your quality metrics table using the `metrics_config`.
# MAGIC
# MAGIC ***NOTE:** Saving metrics to a table requires using a classic compute cluster in [Dedicated Access Mode](https://docs.databricks.com/aws/en/compute/configure#access-modes) due to Spark Connect limitations. This limitation will be lifted in the future.*

# COMMAND ----------

from databricks.labs.dqx.config import InputConfig

# Create some input data
new_users = [
    Row(id=1, age=23, country='Germany'),
    Row(id=2, age=30, country='France'),
    Row(id=3, age=16, country='Germany'),    # Invalid -> age - less than 18
    Row(id=None, age=29, country='France'),  # Invalid -> id - NULL
    Row(id=4, age=29, country=''),           # Invalid -> country - Empty
    Row(id=5, age=23, country='Italy'),      # Invalid -> country - not in allowed
    Row(id=6, age=123, country='France'),    # Invalid -> age - greater than 120
    Row(id=2, age=23, country='Germany'),    # duplicated id
]
new_users_df = spark.createDataFrame(new_users)

# Write the input data to a table
input_table_name = f"{demo_catalog_name}.{demo_schema_name}.input_table"
new_users_df.write.mode("overwrite").saveAsTable(input_table_name)

# Setup the input and output table configuration
input_config = InputConfig(location=input_table_name)

output_table_name = f"{demo_catalog_name}.{demo_schema_name}.output_table"
output_config = OutputConfig(location=output_table_name, mode="overwrite")

quarantine_table_name = f"{demo_catalog_name}.{demo_schema_name}.quarantine_table"
quarantine_config = OutputConfig(location=quarantine_table_name, mode="overwrite")

metrics_table_name = f"{demo_catalog_name}.{demo_schema_name}.metrics_table"
metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")

# Read the input data, apply checks, generate metrics, and save results
# By default, apply_checks_and_save_in_table method apply checks to the entire input table.
# Incremental processing is supported using streaming with the AvailableNow trigger for batch-style execution, along with checkpointing to ensure consistency across runs.
dq_engine.apply_checks_by_metadata_and_save_in_table(
  checks=checks_from_yaml,  # or provide checks_location and run_config_name to auto-load from checks storage
  input_config=input_config,
  output_config=output_config,
  quarantine_config=quarantine_config,
  metrics_config=metrics_config
)

# COMMAND ----------

display(spark.table(output_table_name))
display(spark.table(quarantine_table_name))
display(spark.table(metrics_table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring Detailed Results from Summary-Level Metrics
# MAGIC
# MAGIC When you need to explore detailed row-level quality results based on summary metrics for **troubleshooting**, the example below illustrates how to do so:

# COMMAND ----------

import pyspark.sql.functions as F

# fetch any metrics row as an example
metrics_row = spark.table(metrics_table_name).collect()[0]
run_id = metrics_row["run_id"]
quarantine_table_name = metrics_row["quarantine_location"]

# retrieve detailed results
output_df = spark.table(quarantine_table_name)

# extract errors
results_df = output_df.select(
  F.explode(F.col("_errors")).alias("result"),
).select(F.expr("result.*"))

# extract warnings
results_df = output_df.select(
  F.explode(F.col("_warnings")).alias("result"),
).select(F.expr("result.*"))

display(results_df)

# You can fetch detailed quality results using the run_id from summary metrics
filtered_results_df = results_df.filter(F.col("run_id") == run_id)  # filter, or join
display(filtered_results_df)

# COMMAND ----------

# SQL equivalent using spark.sql with dynamic table names
latest_row = spark.sql(f"""
    SELECT run_id, run_time, output_location, quarantine_location, rule_set_fingerprint
    FROM {metrics_table_name}
    ORDER BY run_time DESC
    LIMIT 1
""").first()
display(latest_row)

run_id_sql = latest_row["run_id"]

display(spark.sql(f"""
    SELECT t.*, e.*
    FROM {quarantine_table_name} t
    LATERAL VIEW explode(t._errors) AS e
    WHERE e.run_id = '{run_id_sql}'
"""))

display(spark.sql(f"""
    SELECT t.*, w.*
    FROM {quarantine_table_name} t
    LATERAL VIEW explode(t._warnings) AS w
    WHERE w.run_id = '{run_id_sql}'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cookbook Query Validation
# MAGIC
# MAGIC The following cells validate every recipe in the [Lineage Query Cookbook](/docs/reference/query_cookbook).
# MAGIC
# MAGIC This section saves checks to a table so that `checks_location` is populated in metrics,
# MAGIC enabling Recipes 3 (to checks table), 4, 5, and 6. It also runs the pipeline twice so
# MAGIC Recipe 7 (run tracking over time) has multiple runs to show.

# COMMAND ----------

from databricks.labs.dqx.config import TableChecksStorageConfig

checks_table_name = f"{demo_catalog_name}.{demo_schema_name}.checks_table"

# Save checks to a table so checks_location is populated in metrics
dq_engine.save_checks(
    checks_from_yaml,
    config=TableChecksStorageConfig(location=checks_table_name, mode="overwrite"),
)

# Run the pipeline twice with a fresh engine each time so each run gets a unique run_id.
# DQMetricsObserver.id is a cached_property — reusing the same engine/observer across
# runs produces the same run_id for every run, which breaks Recipe 2 and run-scoped queries.
for i in range(2):
    run_engine = DQEngine(ws, observer=DQMetricsObserver())
    run_engine.apply_checks_by_metadata_and_save_in_table(
        checks_location=checks_table_name,
        input_config=InputConfig(location=input_table_name),
        output_config=OutputConfig(location=output_table_name, mode="overwrite"),
        quarantine_config=OutputConfig(location=quarantine_table_name, mode="overwrite"),
        metrics_config=OutputConfig(location=metrics_table_name, mode="append"),
    )

# COMMAND ----------

### Recipe 1: A quick peek at each table

display(spark.table(metrics_table_name).limit(5))
display(spark.table(output_table_name).limit(5))
display(spark.table(quarantine_table_name).limit(5))
display(spark.table(checks_table_name).limit(5))

# COMMAND ----------

### Recipe 2: Summary to Row-Level Errors

import pyspark.sql.functions as F

latest = (
    spark.table(metrics_table_name)
    .orderBy(F.col("run_time").desc())
    .first()
)
run_id = latest["run_id"]
quarantine_table = latest["quarantine_location"]
# spark.table() works when quarantine_location is a table name (e.g. catalog.schema.table).
# If it is a storage path (/Volumes/..., s3://..., abfss://...), use spark.read.format(...).load(quarantine_table) instead.

errors_df = (
    spark.table(quarantine_table)
    .select(F.explode(F.col("_errors")).alias("result"))
    .select(F.expr("result.*"))
    .filter(F.col("run_id") == run_id)
)
warnings_df = (
    spark.table(quarantine_table)
    .select(F.explode(F.col("_warnings")).alias("result"))
    .select(F.expr("result.*"))
    .filter(F.col("run_id") == run_id)
)
display(errors_df)
display(warnings_df)

# COMMAND ----------

### Recipe 3: Row Details
#### To Run Summary

import pyspark.sql.functions as F

quarantine_df = spark.table(quarantine_table_name)
error_run_ids = (
    quarantine_df
    .select(F.explode(F.col("_errors")).alias("result"))
    .select(F.expr("result.*"))
    .select("run_id")
)
warning_run_ids = (
    quarantine_df
    .select(F.explode(F.col("_warnings")).alias("result"))
    .select(F.expr("result.*"))
    .select("run_id")
)
run_ids_df = error_run_ids.union(warning_run_ids).distinct()
result_df = spark.table(metrics_table_name).join(run_ids_df, on="run_id")
display(result_df)

# COMMAND ----------

#### To Checks Table

import pyspark.sql.functions as F

errors_df = (
    spark.table(quarantine_table_name)
    .select(F.explode(F.col("_errors")).alias("result"))
    .select(F.expr("result.*"))
)
checks_df = spark.table(checks_table_name)
result_df = (
    errors_df.alias("e")
    .join(checks_df.alias("c"), on=["rule_fingerprint", "rule_set_fingerprint"])
    .select(
        F.col("e.name").alias("error_name"),
        F.col("e.message"),
        F.col("e.columns"),
        F.col("c.check.function"),
        F.col("c.check.arguments"),
        F.col("c.filter"),
        F.col("c.criticality"),
    )
)
display(result_df)

# COMMAND ----------

### Recipe 4: Summary to Checks Table

import pyspark.sql.functions as F

run_info = (
    spark.table(metrics_table_name)
    .filter(F.col("run_id") == run_id)
    .select("rule_set_fingerprint")
    .first()
)
checks_df = (
    spark.table(checks_table_name)
    .filter(F.col("rule_set_fingerprint") == run_info["rule_set_fingerprint"])
)
display(checks_df)

# COMMAND ----------

### Recipe 5: Which Checks Failed for a Run
#### From check_metrics (no joins needed)

from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType
import pyspark.sql.functions as F

check_metrics_schema = ArrayType(StructType([
    StructField("check_name", StringType()),
    StructField("error_count", LongType()),
    StructField("warning_count", LongType()),
]))

failed_checks_df = (
    spark.table(metrics_table_name)
    .filter((F.col("run_id") == run_id) & (F.col("metric_name") == "check_metrics"))
    .select(F.explode(F.from_json(F.col("metric_value"), check_metrics_schema)).alias("cm"))
    .select("cm.*")
    .filter((F.col("error_count") > 0) | (F.col("warning_count") > 0))
)
display(failed_checks_df)

# COMMAND ----------

#### With check definitions (join to checks table)

import pyspark.sql.functions as F

errors_df = (
    spark.table(quarantine_table_name)
    .select(F.explode(F.col("_errors")).alias("result"))
    .select(F.expr("result.*"))
    .filter(F.col("run_id") == run_id)
)
checks_df = spark.table(checks_table_name)
failed_checks_df = (
    errors_df
    .select(
        F.col("name").alias("error_name"),
        "rule_fingerprint",
        "rule_set_fingerprint",
    )
    .distinct()
    .join(checks_df, on=["rule_fingerprint", "rule_set_fingerprint"], how="left")
)
display(failed_checks_df)

# COMMAND ----------

### Recipe 6: Which Rules Were Applied in a Run

import pyspark.sql.functions as F

rsf = (
    spark.table(metrics_table_name)
    .filter(F.col("run_id") == run_id)
    .select("rule_set_fingerprint")
    .first()["rule_set_fingerprint"]
)
rules_df = (
    spark.table(checks_table_name)
    .filter(F.col("rule_set_fingerprint") == rsf)
    .select("name", "criticality", "check.function", "check.arguments", "filter")
    .orderBy("name")
)
display(rules_df)

# COMMAND ----------

### Recipe 7: Run Tracking Over Time

import pyspark.sql.functions as F

trend_df = (
    spark.table(metrics_table_name)
    .filter(F.col("metric_name") != "check_metrics")
    .groupBy("run_id", "run_time", "input_location")
    .pivot("metric_name")
    .agg(F.first(F.col("metric_value").cast("double")))
    .withColumn(
        "error_rate_pct",
        F.round(F.col("error_row_count") / F.col("input_row_count") * 100, 2),
    )
    .orderBy(F.col("run_time").desc())
)
display(trend_df)

# COMMAND ----------

### Recipe 8: Scoping to One Table

import pyspark.sql.functions as F

summary_df = spark.table(metrics_table_name)

# List all input tables tracked in the metrics table
summary_df.select("input_location").distinct().orderBy("input_location").display()

# Get the latest run scoped to the specific input table used in this demo
latest_scoped = (
    summary_df
    .filter(F.col("input_location") == input_table_name)
    .orderBy(F.col("run_time").desc())
    .first()
)
display(latest_scoped)

# COMMAND ----------

### Recipe 9: Checks by Run Config

import pyspark.sql.functions as F

checks_df = spark.table(checks_table_name)

# List all run_config_name values in the checks table
checks_df.select("run_config_name").distinct().orderBy("run_config_name").display()

# Load checks for the default run config (used when run_config_name is not set explicitly)
checks_df.filter(F.col("run_config_name") == "default").display()

# COMMAND ----------

### Recipe 10: First Failure per Check

from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType
import pyspark.sql.functions as F

check_metrics_schema = ArrayType(StructType([
    StructField("check_name", StringType()),
    StructField("error_count", LongType()),
    StructField("warning_count", LongType()),
]))

first_failures_df = (
    spark.table(metrics_table_name)
    .filter(F.col("metric_name") == "check_metrics")
    .select(
        "run_id",
        "run_time",
        F.explode(F.from_json(F.col("metric_value"), check_metrics_schema)).alias("cm"),
    )
    .select("run_id", "run_time", F.col("cm.check_name"), F.col("cm.error_count"))
    .filter(F.col("error_count") > 0)
    .groupBy("check_name")
    .agg(
        F.min("run_time").alias("first_failure_time"),
        F.min_by("run_id", "run_time").alias("first_failure_run_id"),
    )
    .orderBy(F.col("first_failure_time").desc())
)
display(first_failures_df)

# COMMAND ----------


