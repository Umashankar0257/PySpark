# PySpark Complete Notes & Interview Preparation

> **Goal:** One consolidated revision guide for PySpark theory, DataFrame programming, SQL-style queries, performance tuning, and scenario-based interview questions.
>
> **Repository:** [Umashankar0257/PySpark](https://github.com/Umashankar0257/PySpark)  
> **Primary language:** Python + PySpark DataFrame API  
> **Use this README as:** theory notes + coding cookbook + final interview revision sheet.

---

## Table of Contents

1. [How to use this guide today](#1-how-to-use-this-guide-today)
2. [Apache Spark and PySpark](#2-apache-spark-and-pyspark)
3. [Spark architecture](#3-spark-architecture)
4. [SparkSession and DataFrame basics](#4-sparksession-and-dataframe-basics)
5. [RDD vs DataFrame](#5-rdd-vs-dataframe)
6. [Partitions and parallelism](#6-partitions-and-parallelism)
7. [Transformations, actions, and lazy evaluation](#7-transformations-actions-and-lazy-evaluation)
8. [Narrow vs wide transformations and shuffle](#8-narrow-vs-wide-transformations-and-shuffle)
9. [DataFrame creation, schema, and data types](#9-dataframe-creation-schema-and-data-types)
10. [Reading and writing data](#10-reading-and-writing-data)
11. [Core DataFrame operations](#11-core-dataframe-operations)
12. [Null handling and data quality](#12-null-handling-and-data-quality)
13. [String, date, and type-conversion functions](#13-string-date-and-type-conversion-functions)
14. [Aggregations and groupBy](#14-aggregations-and-groupby)
15. [Joins and join strategies](#15-joins-and-join-strategies)
16. [Window functions](#16-window-functions)
17. [Spark SQL](#17-spark-sql)
18. [UDFs and built-in functions](#18-udfs-and-built-in-functions)
19. [Caching and persistence](#19-caching-and-persistence)
20. [repartition vs coalesce](#20-repartition-vs-coalesce)
21. [Spark execution plan and Catalyst](#21-spark-execution-plan-and-catalyst)
22. [Performance tuning checklist](#22-performance-tuning-checklist)
23. [Parquet and Delta Lake](#23-parquet-and-delta-lake)
24. [Incremental processing and deduplication patterns](#24-incremental-processing-and-deduplication-patterns)
25. [Structured Streaming fundamentals](#25-structured-streaming-fundamentals)
26. [Error handling and debugging](#26-error-handling-and-debugging)
27. [Practical programming cookbook](#27-practical-programming-cookbook)
28. [Scenario-based interview questions and answers](#28-scenario-based-interview-questions-and-answers)
29. [Theory interview questions and answers](#29-theory-interview-questions-and-answers)
30. [Coding interview questions](#30-coding-interview-questions)
31. [Common mistakes and corrections](#31-common-mistakes-and-corrections)
32. [Final one-page revision sheet](#32-final-one-page-revision-sheet)
33. [Official references](#33-official-references)

---

## 1. How to use this guide today

Do not try to memorize every line. Learn the idea, say the interview answer aloud, and type the code examples at least once.

### Final-day sequence

- **Pass 1 — Theory (60–90 minutes):** sections 2–8, 19–23.
- **Pass 2 — Queries and programming (90–120 minutes):** sections 9–18 and 27.
- **Pass 3 — Interview scenarios (60 minutes):** sections 28–30.
- **Pass 4 — Final recall (20 minutes):** section 32. Close the notes and explain each item from memory.

For each topic, be ready to answer:
1. What is it?
2. Why do we use it?
3. How does it work?
4. When would you use it in a real pipeline?
5. What can go wrong, and how would you debug it?

**Important:** Spark chooses physical execution strategies based on the query, statistics, configuration, and Spark version. Examples in this guide illustrate common patterns; always verify the actual plan with `explain()` and the Spark UI.

---

## 2. Apache Spark and PySpark

### What is Apache Spark?

Apache Spark is an open-source distributed processing engine for large-scale data processing. It divides work into tasks that can run in parallel across partitions and machines.

### What is PySpark?

PySpark is the Python API for Apache Spark. It lets a developer use Python to work with Spark DataFrames, Spark SQL, RDDs, and Structured Streaming.

### Why use Spark?

- Processes large datasets in parallel.
- Supports batch and streaming workloads.
- Provides DataFrame and SQL APIs.
- Optimizes declarative queries through Spark SQL's optimizer.
- Can recover work using lineage and recomputation.
- Connects to formats and systems such as CSV, JSON, Parquet, Hive-compatible tables, cloud storage, and supported data sources.

### Is Spark written in Python?

No. Spark's core is primarily implemented in Scala and runs on the JVM. PySpark exposes Spark functionality through Python. Python code communicates with Spark; the DataFrame API is planned and executed by Spark's engine.

### Why can Spark be fast?

Spark can process partitions in parallel, optimize query plans, reduce unnecessary I/O, and reuse intermediate results when caching/persistence is appropriate. It is **not automatically faster for every workload**: small tasks, Python-heavy UDFs, shuffles, skew, and poor partitioning can make a job slow.

### Common workload types

- **Batch:** process a bounded dataset, such as yesterday's payroll files.
- **Streaming:** process continuously arriving events.
- **SQL/analytics:** filter, join, aggregate, and query structured data.
- **Machine learning:** Spark's ML libraries support distributed machine-learning workflows.

**Interview answer:**  
“Apache Spark is a distributed data-processing engine. PySpark is its Python API. Spark divides data into partitions, processes tasks in parallel, and optimizes DataFrame and SQL operations to process large datasets efficiently.”

---

## 3. Spark architecture

### Main components

| Component | Responsibility |
|---|---|
| **Driver** | Runs the application logic, creates the Spark session/context, builds the execution plan, schedules work, and tracks job progress. |
| **Cluster manager** | Allocates resources for applications. Depending on deployment, this can be Standalone, YARN, Kubernetes, or a managed platform. |
| **Worker node** | A machine/container that provides resources for processing. |
| **Executor** | Process that runs tasks for an application and may keep cached data in memory/disk. |
| **Job** | Work triggered by an action. |
| **Stage** | A group of tasks separated by shuffle boundaries or other execution requirements. |
| **Task** | Work performed on one partition for a stage. |
| **Partition** | A logical chunk of distributed data. |

### Execution story

```text
Python / DataFrame code
        |
        v
     Driver
        |
        +--> Build and optimize query plan
        |
        +--> Schedule jobs / stages / tasks
                    |
          +---------+---------+
          v         v         v
       Executor  Executor  Executor
        tasks     tasks     tasks
          +---------+---------+
                    |
                    v
                Output
```

### What is the Driver?

The Driver is the coordinator. It creates plans and schedules tasks. It should not collect a huge distributed dataset into driver memory.

### What is an Executor?

An Executor runs tasks and can cache partitions. Executors work in parallel when resources and partitioning allow it.

### Deployment modes

- **Local mode:** processing runs on one machine; useful for learning and small tests.
- **Client mode:** the Driver runs on the submitting/client machine; executors run in the cluster.
- **Cluster mode:** the Driver runs inside the cluster; the client submits the application.

The exact deployment options depend on the cluster manager and platform.

**Memory trick:** Local = one machine; Client = Driver with client; Cluster = Driver inside cluster.

### Job, stage, and task

An action such as `count()` or `write` can trigger a job. Spark divides the job into stages. A stage usually contains tasks that can run without crossing a shuffle boundary. A task commonly processes one partition for that stage.

---

## 4. SparkSession and DataFrame basics

### Create a SparkSession

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PySparkInterviewPractice")
    .getOrCreate()
)
```

In Databricks or managed notebooks, `spark` is commonly created for you.

### Inspect a DataFrame

```python
df.show(10, truncate=False)
df.printSchema()
df.columns
df.dtypes
df.count()
df.describe().show()
df.summary().show()
df.limit(5).show()
```

`count()` is an action and can scan/process data. Avoid running it repeatedly on a large source only for curiosity.

### Select and rename columns

```python
from pyspark.sql import functions as F

df.select("employee_id", "name", "salary").show()

df.select(
    F.col("employee_id"),
    F.col("salary").alias("annual_salary")
).show()

df = df.withColumnRenamed("name", "employee_name")
```

### Add or replace a column

```python
df = df.withColumn("annual_salary", F.col("monthly_salary") * 12)
df = df.withColumn("country", F.lit("India"))
```

### Drop columns

```python
df = df.drop("temporary_column")
```

A DataFrame is immutable: these methods return a new DataFrame plan; they do not mutate the original object in place.

---

## 5. RDD vs DataFrame

### RDD

RDD means **Resilient Distributed Dataset**. It is an immutable distributed collection of objects that can be processed in parallel. Spark can recover lost partitions using lineage where possible.

### DataFrame

A DataFrame is distributed data organized into named columns with a schema. Spark can optimize DataFrame operations because the engine understands the columns, expressions, and types.

### Comparison

| Feature | RDD | DataFrame |
|---|---|---|
| Structure | Collection of objects | Named columns and schema |
| Optimization | Less high-level relational optimization | Spark SQL optimizer can optimize relational operations |
| Type of operations | Lower-level functional operations | Declarative column expressions, SQL, joins, aggregates |
| Typical use | Specialized low-level processing | Preferred for most structured ETL work |
| Recommendation | Use when a low-level API is genuinely needed | Prefer for ordinary data engineering transformations |

### Examples

```python
# DataFrame
df.filter(F.col("salary") > 50000).select("employee_id", "salary")

# RDD (illustrative)
rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
squared = rdd.map(lambda x: x * x)
print(squared.collect())
```

`collect()` brings all results to the driver. Use it only when the result is known to be small.

**Interview answer:**  
“An RDD is a lower-level distributed collection of objects. A DataFrame is structured data with named columns and a schema, allowing Spark SQL to optimize operations. For most structured ETL pipelines, I prefer DataFrames or Spark SQL.”

---

## 6. Partitions and parallelism

A partition is a logical portion of a DataFrame/RDD that Spark can process as a task unit. Partitions enable parallelism, but more partitions do not always mean better performance.

### Inspect partition count

```python
print(df.rdd.getNumPartitions())
```

This is useful for learning/debugging; avoid converting to RDD unnecessarily in production code just to inspect unrelated properties.

### Why partition count matters

- Too few partitions: not enough parallel work; some tasks may be very large.
- Too many tiny partitions: scheduling overhead and many small output files.
- Uneven partition sizes: some tasks finish much later than others.
- Good partitioning depends on data size, cluster resources, and workload.

### Partitioning by a key

```python
df_by_department = df.repartition(8, "department_id")
```

This redistributes data and performs a shuffle. It may help a later operation using that key, but it is not automatically beneficial.

---

## 7. Transformations, actions, and lazy evaluation

### Transformation

A transformation defines a new DataFrame or RDD plan. Examples include `select`, `filter`, `withColumn`, `join`, `groupBy`, and `orderBy`.

### Action

An action requests a result or causes work to be materialized. Common examples include `show`, `count`, `collect`, `take`, and writing output.

### Lazy evaluation

Spark generally records transformations and executes the necessary plan when an action requires results. This allows Spark to optimize a sequence of operations instead of executing each transformation immediately.

```python
filtered = df.filter(F.col("salary") > 50000)  # transformation
selected = filtered.select("employee_id", "salary")  # transformation
selected.show()  # action: triggers required execution
```

### Why lazy evaluation helps

Spark can optimize the combined query plan, push filters toward data sources when possible, prune unused columns, and choose physical operators.

### Common trap

Calling `df.count()` and then `df.show()` can cause separate work if the result is not cached and Spark cannot reuse prior computation. Cache only when repeated use justifies the memory/storage cost.

**Interview answer:**  
“Lazy evaluation means transformations build a plan rather than immediately processing the entire dataset. An action triggers the required execution. This lets Spark optimize the plan before running it.”

---

## 8. Narrow vs wide transformations and shuffle

### Narrow transformation

Each input partition can contribute to one output partition without requiring a full redistribution across the cluster. Typical examples include `filter` and `select`. `union` commonly combines the input partition sets rather than redistributing rows by key.

### Wide transformation

An operation may require data from multiple input partitions to be redistributed into output partitions. This commonly creates a **shuffle**. Examples often include `groupBy`, `distinct`, joins, and global sorting; exact behavior depends on the operation and plan.

### What is shuffle?

Shuffle is data redistribution across partitions, often across executor processes or machines. It can involve network transfer, serialization, disk spill, and extra stages, so it is often expensive.

### Memory trick

- Narrow: process locally where possible.
- Wide: data may need to move so related records can meet.

Do not state that every join always shuffles: a broadcast join can avoid shuffling the large side, and an existing compatible partitioning or other physical strategy can change the plan.

---

## 9. DataFrame creation, schema, and data types

### Create a DataFrame from Python data

```python
data = [
    (1, "Uma", "IT", 70000),
    (2, "Ravi", "HR", 60000),
    (3, "Priya", "IT", 85000),
]

columns = ["employee_id", "name", "department", "salary"]
df = spark.createDataFrame(data, columns)
df.show()
```

### Define an explicit schema

Explicit schemas are safer for production ingestion than relying on inference.

```python
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DecimalType
)

schema = StructType([
    StructField("employee_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("department", StringType(), nullable=True),
    StructField("salary", DecimalType(12, 2), nullable=True),
])

df = spark.createDataFrame(
    [(1, "Uma", "IT", 70000.00), (2, "Ravi", "HR", 60000.00)],
    schema=schema
)
```

### Common Spark SQL data types

- `StringType`
- `IntegerType`, `LongType`, `ShortType`
- `FloatType`, `DoubleType`, `DecimalType`
- `BooleanType`
- `DateType`, `TimestampType`
- `ArrayType`, `MapType`, `StructType`

For money or exact decimal arithmetic, consider `DecimalType` instead of floating-point types.

### Schema evolution

Schema evolution means the source schema changes over time, such as a new column being added. Define whether the pipeline should reject, accept, map, or quarantine unexpected changes. Do not silently assume all schema changes are safe.

---

## 10. Reading and writing data

### CSV

```python
csv_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)  # convenient for exploration, not always best for production
    .csv("/path/employees.csv")
)
```

Production-style CSV read with an explicit schema:

```python
csv_df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .option("mode", "PERMISSIVE")
    .csv("/path/employees.csv")
)
```

### JSON

```python
json_df = spark.read.option("multiLine", True).json("/path/input.json")
```

The `multiLine` setting depends on whether each file contains one JSON object per line or a multi-line JSON document.

### Parquet

```python
parquet_df = spark.read.parquet("/path/bronze/employees/")
```

### Delta (when Delta Lake is available)

```python
delta_df = spark.read.format("delta").load("/path/delta/employees")
```

### Write data

```python
(
    df.write
    .mode("overwrite")
    .format("parquet")
    .save("/path/output/employees")
)
```

Write as CSV:

```python
df.write.mode("overwrite").option("header", True).csv("/path/output/csv")
```

### Write modes

| Mode | Meaning |
|---|---|
| `error` / `errorifexists` | Fail if the target already exists (default behavior for many file writes). |
| `overwrite` | Replace existing target data according to the data source's semantics. |
| `append` | Add data to the existing target. |
| `ignore` | Do nothing if the target already exists. |

**Important:** overwrite and append behavior can differ by data source and table/path configuration. Understand the target before writing.

### Corrupt records and read modes

For supported file sources such as CSV/JSON, parsing modes commonly include:

- `PERMISSIVE`: keep processing and capture malformed-record information where supported/configured.
- `FAILFAST`: fail when malformed input is encountered.
- `DROPMALFORMED`: drop malformed records for supported formats.

A separate bad-record path can be configured for supported readers/options, but support and exact behavior depend on the format and Spark version. Test it with sample malformed data before relying on it.

### Small output file warning

Writing a DataFrame with many partitions may produce many part files. If you need fewer files, consider `coalesce()` or `repartition()` before writing, based on data size and required parallelism. Do not force one partition for a large dataset just to create one file.

---

## 11. Core DataFrame operations

Use the following example for the remaining code:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

employees = [
    (1, "Uma", "IT", 70000, "2025-01-10"),
    (2, "Ravi", "HR", 60000, "2025-02-15"),
    (3, "Priya", "IT", 85000, "2025-03-01"),
    (4, "John", "Finance", 75000, "2025-02-20"),
    (5, "Maya", "HR", 65000, "2025-04-05"),
]
df = spark.createDataFrame(
    employees,
    ["employee_id", "name", "department", "salary", "join_date"]
).withColumn("join_date", F.to_date("join_date"))
```

### Filter rows

```python
df.filter(F.col("salary") > 65000).show()
df.where((F.col("department") == "IT") & (F.col("salary") >= 70000)).show()
df.filter(F.col("department").isin("IT", "HR")).show()
df.filter(~F.col("department").isin("HR")).show()
```

Use `&`, `|`, and `~` with parenthesized PySpark column conditions, not Python's `and`, `or`, and `not`.

### Sort

```python
df.orderBy(F.col("salary").desc()).show()
df.orderBy(F.col("department").asc(), F.col("salary").desc()).show()
```

### Distinct rows

```python
df.select("department").distinct().show()
df.dropDuplicates(["employee_id"]).show()
```

`distinct()` removes duplicate full rows. `dropDuplicates(["key"])` removes duplicates based on selected columns but does not guarantee which non-key row is retained. If the latest record must survive, use a window and a deterministic ordering (see section 24).

### Conditional columns

```python
df = df.withColumn(
    "salary_band",
    F.when(F.col("salary") >= 80000, "High")
     .when(F.col("salary") >= 65000, "Medium")
     .otherwise("Low")
)
```

### `when` with multiple conditions

```python
df = df.withColumn(
    "bonus_eligible",
    F.when(
        (F.col("salary") >= 70000) & (F.col("department") == "IT"),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)
```

### Union two DataFrames

```python
combined = df1.unionByName(df2)
```

`union()` aligns columns by position. `unionByName()` aligns columns by name and is usually safer when schemas may differ in column order. Use `allowMissingColumns=True` only when filling absent columns with nulls is the intended behavior.

### Add a constant and current timestamp

```python
df = df.withColumn("source_system", F.lit("HR"))
df = df.withColumn("processed_at", F.current_timestamp())
```

`current_timestamp()` is evaluated by Spark for the query; it is not a substitute for a source-system event timestamp.

---

## 12. Null handling and data quality

### Find nulls

```python
df.filter(F.col("name").isNull()).show()
df.filter(F.col("name").isNotNull()).show()
```

### Count nulls by column

```python
df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c + "_null_count")
    for c in df.columns
]).show()
```

### Fill nulls

```python
df.fillna({"department": "Unknown", "salary": 0}).show()
```

Only fill with values that make business sense. Replacing missing salary with zero may be wrong for payroll or finance reporting.

### Drop null rows

```python
df.na.drop(subset=["employee_id"])
df.na.drop(how="all")
```

### Replace values

```python
df = df.replace({"N/A": None, "UNKNOWN": None}, subset=["department"])
```

For string cleanup, trim before checking blank values:

```python
df = df.withColumn("name", F.trim(F.col("name")))
df = df.withColumn(
    "name",
    F.when(F.col("name") == "", None).otherwise(F.col("name"))
)
```

### Data quality checks

```python
# Required key should not be null
bad_keys = df.filter(F.col("employee_id").isNull())

# Business rule: salary should be non-negative
bad_salary = df.filter(F.col("salary") < 0)

# Duplicate key check
duplicate_keys = (
    df.groupBy("employee_id")
      .count()
      .filter(F.col("count") > 1)
)

# Basic row count
row_count = df.count()
```

A production pipeline should define what happens when a check fails: stop the job, quarantine bad records, log the issue, or continue with an explicit accepted threshold.

### Data quality dimensions

- **Completeness:** required values are present.
- **Validity:** values follow allowed rules/ranges.
- **Uniqueness:** keys are not duplicated when uniqueness is required.
- **Consistency:** related fields agree.
- **Accuracy:** values match the trusted source/business rule.
- **Timeliness:** data arrives within the expected window.

---

## 13. String, date, and type-conversion functions

### String functions

```python
df.select(
    F.upper("name").alias("upper_name"),
    F.lower("department").alias("department_lower"),
    F.trim("name").alias("trimmed_name"),
    F.length("name").alias("name_length")
).show()

df.select(F.concat_ws(" - ", "name", "department").alias("label")).show()
df.filter(F.col("name").contains("a")).show()
df.filter(F.col("name").startswith("M")).show()
df.select(F.regexp_replace("name", "[^A-Za-z]", "").alias("letters_only")).show()
```

### Split a string

```python
sample = spark.createDataFrame([("Uma,Reddy",)], ["full_name"])
sample.select(F.split("full_name", ",").alias("parts")).show(truncate=False)
```

### Cast data types

```python
df = df.withColumn("salary", F.col("salary").cast("decimal(12,2)"))
df = df.withColumn("employee_id", F.col("employee_id").cast("int"))
```

Validate casts where invalid input is possible; conversion can result in null or an error depending on the expression, ANSI settings, and Spark version.

### Date functions

```python
df = df.withColumn("join_date", F.to_date("join_date"))
df.select(F.year("join_date").alias("join_year")).show()
df.select(F.month("join_date").alias("join_month")).show()
df.select(F.date_format("join_date", "yyyy-MM").alias("join_month")).show()
df.select(F.datediff(F.current_date(), F.col("join_date")).alias("days_since_join")).show()
df.select(F.add_months("join_date", 3).alias("after_3_months")).show()
```

### Parse a date string with a format

```python
dates = spark.createDataFrame([("31-12-2025",)], ["date_text"])
dates = dates.withColumn("parsed_date", F.to_date("date_text", "dd-MM-yyyy"))
```

### Timestamp functions

```python
df = df.withColumn("ingestion_timestamp", F.current_timestamp())
df = df.withColumn("event_date", F.to_date("event_timestamp"))
```

Be explicit about timezone expectations when processing timestamps. Distinguish event time from processing/ingestion time.

---

## 14. Aggregations and groupBy

### Common aggregate functions

```python
df.select(
    F.count("*").alias("row_count"),
    F.count("salary").alias("non_null_salary_count"),
    F.sum("salary").alias("total_salary"),
    F.avg("salary").alias("average_salary"),
    F.min("salary").alias("minimum_salary"),
    F.max("salary").alias("maximum_salary")
).show()
```

`count("*")` counts rows. `count("column")` counts non-null values in that column.

### Group by one column

```python
df.groupBy("department").agg(
    F.count("*").alias("employee_count"),
    F.avg("salary").alias("average_salary"),
    F.max("salary").alias("maximum_salary")
).show()
```

### Group by multiple columns

```python
df.groupBy("department", F.year("join_date").alias("join_year")).count().show()
```

If grouping by a derived expression, it may be clearer to add the expression as a column first.

### Filter aggregated groups with `having`-style logic

```python
dept_summary = (
    df.groupBy("department")
      .agg(F.sum("salary").alias("total_salary"))
      .filter(F.col("total_salary") > 100000)
)
dept_summary.show()
```

### Conditional aggregation

```python
df.groupBy("department").agg(
    F.sum(F.when(F.col("salary") >= 70000, 1).otherwise(0)).alias("high_salary_count"),
    F.sum(F.when(F.col("salary") < 70000, 1).otherwise(0)).alias("lower_salary_count")
).show()
```

### Collect values carefully

```python
df.groupBy("department").agg(F.collect_set("name").alias("unique_names")).show(truncate=False)
```

`collect_list` and `collect_set` can create large arrays for high-cardinality groups. Avoid collecting unbounded data into one row.

---

## 15. Joins and join strategies

### Join types

| Join | Result |
|---|---|
| Inner | Only matching rows from both sides. |
| Left outer | All left rows; matching right values, otherwise nulls. |
| Right outer | All right rows; matching left values, otherwise nulls. |
| Full outer | All rows from both sides; unmatched side is null. |
| Left semi | Left rows that have a match on the right; right columns are not returned. |
| Left anti | Left rows with no match on the right. |
| Cross | Every left row paired with every right row; can produce a huge result. |

### Basic join

```python
departments = spark.createDataFrame(
    [( "IT", "Technology"), ("HR", "Human Resources"), ("Finance", "Finance")],
    ["department", "department_name"]
)

joined = df.join(departments, on="department", how="left")
joined.show()
```

### Join using different key names

```python
# employees.employee_department_id == departments.department_id
joined = employees_df.join(
    departments_df,
    employees_df.employee_department_id == departments_df.department_id,
    "left"
)
```

When both tables contain similarly named columns, select and alias the columns you need to avoid ambiguous-column errors.

### Left anti join: find missing dimension records

```python
unmatched = source_df.join(reference_df, on="department_id", how="left_anti")
```

Useful for finding source keys that do not exist in a reference/dimension table.

### Left semi join: keep only matched left records

```python
matched = source_df.join(reference_df, on="department_id", how="left_semi")
```

### Broadcast join

Use when one side is sufficiently small to fit safely in executor memory.

```python
from pyspark.sql.functions import broadcast

result = large_df.join(broadcast(small_lookup_df), "lookup_key", "left")
result.explain()
```

The physical plan may show `BroadcastHashJoin`. A broadcast avoids shuffling the large side for that join strategy, but broadcasting a table that is too large can create memory pressure or failures. Spark can also choose broadcast automatically based on its threshold and statistics.

### Shuffle join / sort-merge join / shuffle hash join

For large datasets, Spark may redistribute data by join key. A common physical strategy is `SortMergeJoin`, especially for large equi-joins. `ShuffledHashJoin` may be chosen under appropriate conditions. Do not assume a specific join strategy without checking `explain()`.

### Join performance checklist

1. Filter rows before joining where logically valid.
2. Select only needed columns before joining.
3. Check key data types and key quality.
4. Inspect the physical plan.
5. Broadcast only a genuinely small side.
6. Check for skewed join keys.
7. Avoid accidental many-to-many joins caused by duplicate keys.
8. Verify row counts and uniqueness after joining.

---

## 16. Window functions

Window functions calculate a value across related rows without collapsing those rows into one row per group.

```python
from pyspark.sql.window import Window
from pyspark.sql import functions as F
```

### Rank employees by salary in each department

```python
w = Window.partitionBy("department").orderBy(F.col("salary").desc())

ranked = df.withColumn("salary_rank", F.rank().over(w))
ranked.show()
```

### `row_number` vs `rank` vs `dense_rank`

- `row_number`: unique sequential number, even when values tie.
- `rank`: tied rows receive the same rank; later ranks can have gaps.
- `dense_rank`: tied rows receive the same rank; no gaps after ties.

### Highest-paid employee in each department

```python
w = Window.partitionBy("department").orderBy(
    F.col("salary").desc(),
    F.col("employee_id").asc()  # deterministic tie-breaker
)

top_employee_per_department = (
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn")
)
```

### Top 3 employees per department

```python
top3 = (
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") <= 3)
      .drop("rn")
)
```

Use `rank()` instead if the business rule requires including all employees tied within the top ranks.

### Running total

```python
w_running = (
    Window.partitionBy("department")
    .orderBy("join_date", "employee_id")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

df.withColumn("running_salary", F.sum("salary").over(w_running)).show()
```

### Previous/next row

```python
w_date = Window.partitionBy("department").orderBy("join_date", "employee_id")

df.withColumn("previous_salary", F.lag("salary", 1).over(w_date)) \
  .withColumn("next_salary", F.lead("salary", 1).over(w_date)) \
  .show()
```

### Compare current value with department average

```python
w_dept = Window.partitionBy("department")
df.withColumn("dept_avg_salary", F.avg("salary").over(w_dept)) \
  .withColumn("above_avg", F.col("salary") > F.col("dept_avg_salary")) \
  .show()
```

**Interview answer:**  
“`groupBy` reduces each group to aggregate rows. A window function calculates a result across a partition of related rows while preserving individual rows.”

---

## 17. Spark SQL

### Register a temporary view

```python
df.createOrReplaceTempView("employees")
```

### Query with SQL

```python
spark.sql("""
    SELECT department,
           COUNT(*) AS employee_count,
           AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
    HAVING AVG(salary) > 60000
    ORDER BY avg_salary DESC
""").show()
```

### SQL window example

```python
spark.sql("""
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY department
               ORDER BY salary DESC, employee_id ASC
           ) AS rn
    FROM employees
""").show()
```

### DataFrame API vs Spark SQL

Both are Spark SQL interfaces and are usually optimized by the same engine. Choose the one that makes the transformation clearer to your team. Avoid building SQL by concatenating untrusted strings; parameterize/validate dynamic values appropriately.

---

## 18. UDFs and built-in functions

### What is a UDF?

A User-Defined Function lets you apply custom logic that is not conveniently expressed with built-in Spark functions. Traditional Python UDFs can add serialization and Python execution overhead and can limit some optimizer improvements.

### Prefer built-in functions

```python
# Prefer native Spark expression
df.withColumn("name_upper", F.upper("name"))
```

### Python UDF example

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def salary_label(value):
    if value is None:
        return "Unknown"
    return "High" if value >= 80000 else "Standard"

salary_label_udf = udf(salary_label, StringType())

df.withColumn("salary_label", salary_label_udf("salary")).show()
```

### UDF interview guidance

- First check whether built-in Spark functions can express the logic.
- Define the return type for a standard Python UDF.
- Handle nulls and invalid inputs.
- Test behavior with edge cases.
- Use Pandas UDFs/vectorized approaches only when appropriate and supported by the environment.
- Benchmark; do not assume a UDF is faster because the Python code looks shorter.

---

## 19. Caching and persistence

### Cache

`cache()` marks a DataFrame for reuse. The data is generally materialized when an action requires it, not simply when `cache()` is called.

```python
df_cached = df.cache()
df_cached.count()  # materializes the cached computation
# reuse df_cached for multiple downstream actions
df_cached.unpersist()
```

### Persist

`persist()` lets you select a storage level, depending on the Spark API/version.

```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()
# after downstream reuse is finished:
df.unpersist()
```

### When should you cache?

Cache when:
- A costly DataFrame is reused by multiple downstream actions.
- Recomputing it is more expensive than storing it.
- There is sufficient memory/storage.

Do not cache every DataFrame. Cache consumes resources and can make other tasks slower. Persisting a one-use DataFrame may add overhead without benefit.

### Interview answer

“Caching stores computed partitions for reuse. I cache a costly DataFrame only when it is reused multiple times, materialize it with an action, and unpersist it when it is no longer needed.”

---

## 20. repartition vs coalesce

### `repartition`

Redistributes data and causes a shuffle. It can increase or decrease the partition count and can repartition by columns.

```python
df_repartitioned = df.repartition(8)
df_by_key = df.repartition(8, "department")
```

### `coalesce`

Primarily reduces the number of partitions while avoiding a full shuffle in the common case.

```python
df_fewer = df.coalesce(2)
```

### Comparison

| Feature | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle | Yes, redistributes data | Avoids a full shuffle in the common use case |
| Increase partitions | Yes | Not its intended use |
| Decrease partitions | Yes | Yes |
| Typical reason | Redistribute data or increase parallelism | Reduce partitions / output files |
| Main risk | Shuffle cost | Uneven partitions or reduced parallelism |

**Interview answer:**  
“`repartition` redistributes data with a shuffle and can increase or decrease partitions. `coalesce` is mainly used to reduce partitions with less data movement. I choose based on data size, distribution, and downstream work.”

---

## 21. Spark execution plan and Catalyst

### What is Catalyst?

Catalyst is Spark SQL's query optimizer. It applies rules to logical plans and helps produce an optimized plan before physical execution.

### Simplified planning flow

```text
DataFrame / SQL expression
          |
          v
  Logical plan
          |
          v
  Analysis / resolution
          |
          v
  Catalyst optimization
          |
          v
  Physical planning
          |
          v
  Jobs / stages / tasks
          |
          v
      Executors
```

### Useful optimizations

- **Predicate pushdown:** push filters closer to the data source when supported.
- **Column pruning:** read/use only needed columns where possible.
- **Constant folding:** compute constant expressions during planning.
- **Join strategy selection:** choose a physical join based on query, statistics, configuration, and runtime behavior.
- **Adaptive Query Execution (AQE):** supported Spark versions can adapt parts of the plan using runtime statistics.

These optimizations are conditional; verify what happened for the actual job.

### `explain()`

```python
df.filter(F.col("salary") > 70000).explain()
df.filter(F.col("salary") > 70000).explain("formatted")
```

Use the plan to look for:
- `BroadcastHashJoin`
- `SortMergeJoin`
- `Exchange` (often indicates a shuffle/repartition boundary)
- `Filter`
- `Project`
- Scan operators and pushed filters, where shown

The exact plan labels vary by Spark version and data source.

### Driver vs executor work

The Driver coordinates planning and scheduling. Executors run tasks. If the driver collects huge results, it may run out of memory even if the distributed processing itself could handle the data.

---

## 22. Performance tuning checklist

When a Spark job is slow, do not randomly increase the cluster size first. Diagnose the bottleneck.

### Step-by-step troubleshooting

1. **Understand the workload:** source size, number of files, transformations, output size, SLA.
2. **Inspect the Spark UI:** stages, task duration, shuffle read/write, spill, failed tasks, executor utilization.
3. **Inspect `explain("formatted")`:** look for shuffles, join strategy, scans, and repeated operations.
4. **Filter early:** apply valid filters before expensive joins/aggregations.
5. **Select only needed columns:** reduce data read and shuffled.
6. **Use built-in expressions:** avoid unnecessary Python UDFs.
7. **Check partition sizing:** avoid too few huge partitions and too many tiny partitions.
8. **Check skew:** one key may own far more rows than others.
9. **Choose join strategies carefully:** broadcast only a genuinely small lookup table.
10. **Cache only reused results:** unpersist when finished.
11. **Choose storage formats sensibly:** Parquet/Delta are often appropriate for analytics.
12. **Review output file sizes:** avoid a large number of tiny files.
13. **Validate results:** optimization must not change business correctness.
14. **Measure before and after:** compare runtime and resource metrics.

### Data skew

Data skew occurs when a small number of keys contain a disproportionate amount of data. Tasks handling those keys become stragglers.

Possible approaches, depending on the workload:
- Filter irrelevant data early.
- Use AQE/skew handling where supported and enabled.
- Broadcast a small side if suitable.
- Repartition by a more appropriate key when it helps.
- Use key salting for suitable aggregations/joins, then remove the salt and re-aggregate correctly.
- Investigate hot keys and duplicates before changing the architecture.

### What is AQE?

Adaptive Query Execution can use runtime statistics to modify parts of a physical plan. Depending on Spark version and configuration, it may coalesce shuffle partitions, improve join selection, or handle certain skewed joins.

### Why is shuffle expensive?

It can move data over the network, serialize/deserialize records, write/read intermediate data, and spill to disk. Reduce unnecessary shuffles, but do not eliminate a shuffle if it is required for correctness.

### Why not always use broadcast join?

The broadcast side must fit safely in executor memory. Broadcasting a large table can cause memory pressure and failures. Use statistics, reasonable size estimates, and `explain()`.

---

## 23. Parquet and Delta Lake

### Parquet

Parquet is a columnar file format. It can reduce I/O when queries read only selected columns and supports metadata/statistics that may help skip irrelevant data.

```python
df.write.mode("overwrite").parquet("/path/employees_parquet")
parquet_df = spark.read.parquet("/path/employees_parquet")
```

### Delta Lake

Delta Lake is a table/storage layer commonly built on Parquet data files with a transaction log in `_delta_log`. It adds capabilities such as ACID transactions and table version history/time travel in supported environments.

```python
df.write.format("delta").mode("overwrite").save("/path/employees_delta")
delta_df = spark.read.format("delta").load("/path/employees_delta")
```

### Parquet vs Delta

| Feature | Parquet | Delta Lake |
|---|---|---|
| Main purpose | Columnar file storage | Reliable table management over data files |
| Data files | Parquet | Commonly Parquet |
| Transaction log | No Delta transaction log | `_delta_log` |
| ACID table transactions | Not provided by plain Parquet alone | Supported by Delta Lake |
| Version/time travel | Not a native table feature of plain Parquet | Supported by Delta, subject to retention/configuration |
| Schema enforcement/evolution | Reader/writer-dependent | Delta table features available, subject to configuration |

### Delta `MERGE` concept

`MERGE` can insert new records and update matched records. It is commonly used for upserts in Delta-enabled platforms.

```sql
MERGE INTO target AS t
USING source AS s
ON t.employee_id = s.employee_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

This is illustrative SQL: exact syntax and wildcard support can depend on the platform/version and schema. Ensure the source has at most one matching row per target key or define deduplication first; multiple source matches can make a merge ambiguous or fail.

### Time travel

Delta supports reading earlier table versions or timestamps when the transaction history and data files are still available. Retention and vacuum policies can affect how far back you can read.

---

## 24. Incremental processing and deduplication patterns

### Full load vs incremental load

- **Full load:** read/process the entire source dataset for each run.
- **Incremental load:** process only new or changed records according to a defined change-detection strategy.

### Common incremental strategies

- Source-provided `updated_at`/modified timestamp.
- Increasing ID/watermark when the source guarantees suitable ordering.
- Change Data Capture (CDC) events.
- File arrival/batch identifiers.
- Snapshot comparison or hash comparison when timestamps are unreliable.

### Critical watermark warning

A timestamp watermark can miss changes if records are updated without changing the timestamp used by the pipeline. Agree on source semantics, use CDC/change tracking if available, or implement a reliable comparison strategy. Consider late-arriving data and reruns.

### Example watermark filter

```python
# watermark_value must come from reliable persisted pipeline state.
incremental_df = source_df.filter(
    F.col("updated_at") > F.lit(watermark_value).cast("timestamp")
)
```

A production design must decide how to handle records equal to the watermark, late arrivals, deletes, failed runs, and watermark advancement. Advance the watermark only after the intended output is committed successfully.

### Keep the latest record per business key

```python
updates = spark.createDataFrame(
    [
        (101, "Uma", "2025-01-01 10:00:00"),
        (101, "Uma Reddy", "2025-02-01 10:00:00"),
        (102, "Ravi", "2025-01-15 09:00:00"),
    ],
    ["employee_id", "name", "updated_at"]
).withColumn("updated_at", F.to_timestamp("updated_at"))

w_latest = Window.partitionBy("employee_id").orderBy(
    F.col("updated_at").desc(),
    F.col("name").desc()  # illustrative tie-breaker; choose a real deterministic key
)

latest_per_employee = (
    updates.withColumn("rn", F.row_number().over(w_latest))
           .filter(F.col("rn") == 1)
           .drop("rn")
)
```

Use a meaningful tie-breaker. Ordering only by a timestamp may not be deterministic when two records share the same timestamp.

### Idempotency

A pipeline is idempotent when rerunning the same logical input does not create unintended duplicates or change the result unexpectedly. Strategies include stable business keys, deterministic deduplication, merge/upsert semantics, batch identifiers, and controlled overwrite of a well-defined partition.

### Bronze / Silver / Gold pattern

- **Bronze:** raw or minimally processed source data, with ingestion metadata.
- **Silver:** cleaned, typed, validated, deduplicated, conformed data.
- **Gold:** business aggregates, reporting tables, and curated consumption datasets.

Layer names and exact responsibilities vary by organization. The important thing is that each layer has a clear contract.

---

## 25. Structured Streaming fundamentals

Structured Streaming applies Spark's structured processing model to continuously arriving data.

### Core concepts

- **Source:** where streaming data comes from.
- **Trigger:** when Spark processes available data.
- **Checkpoint:** stores progress/state information needed for recovery.
- **Output mode:** which rows are written to the sink (`append`, `update`, or `complete`, depending on query/sink support).
- **Watermark:** helps bound state for event-time operations and manage late data.
- **Sink:** destination such as a supported file/table/streaming system.

### Basic file-stream example

```python
stream_df = (
    spark.readStream
    .schema(schema)
    .json("/path/stream_input")
)

query = (
    stream_df.writeStream
    .format("parquet")
    .option("path", "/path/stream_output")
    .option("checkpointLocation", "/path/checkpoints/employee_stream")
    .outputMode("append")
    .start()
)

# In a managed notebook, monitor query status and stop it when appropriate.
```

Check source/sink support and output-mode restrictions for the actual operation. A checkpoint location should be stable and dedicated to the query; do not casually delete it during normal restarts.

### Event time vs processing time

- **Event time:** timestamp when the event actually happened at the source.
- **Processing time:** when Spark processes the event.

Event-time windows and watermarks are useful when events can arrive late.

### Watermark example

```python
events = (
    stream_df
    .withWatermark("event_timestamp", "10 minutes")
    .groupBy(
        F.window("event_timestamp", "5 minutes"),
        F.col("event_type")
    )
    .count()
)
```

A watermark does not mean every late event is guaranteed to be accepted or that data is automatically deleted at exactly that instant. Behavior depends on the stateful operation and engine semantics.

---

## 26. Error handling and debugging

### Start with the actual error

1. Read the first meaningful exception and root cause.
2. Identify whether the failure occurred during analysis, reading, transformation, shuffle, writing, or resource allocation.
3. Inspect schema and sample records.
4. Reproduce with a small safe sample if possible.
5. Inspect the physical plan and Spark UI.
6. Validate the correction against edge cases and row counts.

### Common issues

| Symptom | Possible cause | First checks |
|---|---|---|
| `AnalysisException` / unresolved column | Typo, wrong alias, missing column, schema change | `printSchema()`, `columns`, aliases |
| Ambiguous column | Same name exists on both join sides | Alias DataFrames and explicitly select columns |
| Wrong row count after join | Duplicate keys or many-to-many join | Check key uniqueness before and after join |
| Out of memory on driver | Huge `collect()`, large local conversion, large result | Remove collection; aggregate/filter distributed-side |
| Slow stage | Skew, shuffle, spill, too few/too many partitions | Spark UI, task durations, shuffle metrics |
| Too many small files | Many output partitions or frequent tiny writes | Review partitioning and file compaction strategy |
| Missing incremental updates | Bad watermark semantics, late updates, timestamp not maintained | Confirm source change tracking and watermark state |
| Bad values become null | Cast/parsing errors or malformed source data | Inspect raw value, schema, parse options, bad-record handling |
| Repeated records on rerun | Non-idempotent append, missing key/dedup logic | Batch IDs, deterministic dedup, merge/upsert |

### Logging concept

In production, log the run ID, source/target, input/output counts, watermark, start/end times, rejected-record count, and failure details. Do not log secrets or unnecessary sensitive data.

---

## 27. Practical programming cookbook

The following are common interview coding patterns. Run them against the sample `df` in section 11 or adapt the column names to the question.

### 27.1 Employees earning more than 70,000

```python
df.filter(F.col("salary") > 70000).show()
```

### 27.2 Average salary by department

```python
df.groupBy("department").agg(F.avg("salary").alias("avg_salary")).show()
```

### 27.3 Count employees by department

```python
df.groupBy("department").count().show()
```

### 27.4 Second-highest distinct salary

```python
w = Window.orderBy(F.col("salary").desc())
second_highest = (
    df.select("salary").distinct()
      .withColumn("rn", F.dense_rank().over(w))
      .filter(F.col("rn") == 2)
      .select("salary")
)
second_highest.show()
```

### 27.5 Top salary in every department

```python
w = Window.partitionBy("department").orderBy(
    F.col("salary").desc(), F.col("employee_id").asc()
)
(
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn")
      .show()
)
```

### 27.6 Find duplicate employee IDs

```python
(
    df.groupBy("employee_id")
      .count()
      .filter(F.col("count") > 1)
      .show()
)
```

### 27.7 Keep the latest record per ID

```python
w = Window.partitionBy("employee_id").orderBy(
    F.col("updated_at").desc(), F.col("source_sequence").desc()
)
latest = (
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn")
)
```

Replace `source_sequence` with a real tie-breaker that exists in your data.

### 27.8 Find records with no dimension match

```python
missing = fact_df.join(dim_df, "dimension_key", "left_anti")
```

### 27.9 Running total by account/date

```python
w = (
    Window.partitionBy("account_id")
    .orderBy("transaction_date", "transaction_id")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
transactions.withColumn("running_amount", F.sum("amount").over(w)).show()
```

### 27.10 Difference from previous transaction

```python
w = Window.partitionBy("account_id").orderBy("transaction_date", "transaction_id")
transactions.withColumn(
    "previous_amount", F.lag("amount").over(w)
).withColumn(
    "amount_change", F.col("amount") - F.col("previous_amount")
).show()
```

### 27.11 Pivot by category

```python
pivoted = (
    sales_df.groupBy("region")
    .pivot("product_category")
    .agg(F.sum("sales_amount"))
)
```

A pivot can create many columns if the category domain is large. Specify expected pivot values when appropriate.

### 27.12 Unpivot concept

In versions with the required API, `unpivot` converts multiple value columns into key/value rows. Otherwise, use a carefully constructed `stack` SQL expression or `unionByName` of selected columns. Confirm API availability for the Spark version in use.

### 27.13 Explode an array

```python
array_df = spark.createDataFrame([(1, ["SQL", "Python"]), (2, ["Spark"])], ["id", "skills"])
array_df.select("id", F.explode("skills").alias("skill")).show()
```

### 27.14 Parse JSON stored in a string

```python
from pyspark.sql.types import StructType, StructField, StringType

json_schema = StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
])

parsed = raw_df.withColumn("payload", F.from_json("json_text", json_schema))
parsed.select("payload.city", "payload.country").show()
```

### 27.15 Validate and separate bad rows

```python
valid_df = df.filter(
    F.col("employee_id").isNotNull() &
    F.col("salary").isNotNull() &
    (F.col("salary") >= 0)
)

invalid_df = df.filter(
    F.col("employee_id").isNull() |
    F.col("salary").isNull() |
    (F.col("salary") < 0)
)
```

### 27.16 Compare source and target counts

```python
source_count = source_df.count()
target_count = target_df.count()
print({"source_count": source_count, "target_count": target_count})
```

Counts alone do not prove correctness. Compare keys, aggregates, checksums/hashes where suitable, and rejected records.

### 27.17 Explain a join

```python
joined = fact_df.join(dim_df, "dimension_key", "left")
joined.explain("formatted")
```

### 27.18 Use `selectExpr`

```python
df.selectExpr(
    "employee_id",
    "salary * 12 AS annual_salary",
    "upper(name) AS name_upper"
).show()
```

### 27.19 Drop duplicates by all columns vs key

```python
all_row_duplicates_removed = df.distinct()
key_duplicates_removed = df.dropDuplicates(["employee_id"])
```

The second form does not guarantee that the latest row is selected.

### 27.20 Save a table/query result

```python
result.write.mode("overwrite").format("parquet").save("/path/output/result")
```

On platforms supporting managed tables, use the platform's table-write conventions and catalog permissions.

---

## 28. Scenario-based interview questions and answers

Answer using **problem → investigation → solution → validation**. Do not jump straight to a code change without explaining how you prove the result is correct.

### Scenario 1: A PySpark job that usually takes 10 minutes now takes 45 minutes. What do you do?

**Answer:**
1. Compare input volume, file count, and recent source/schema changes.
2. Check Spark UI for slow stages, skewed tasks, shuffle read/write, spill, and failed/retried tasks.
3. Inspect `explain("formatted")` for unexpected exchanges, joins, or scans.
4. Check whether a join key became skewed or the broadcast side grew.
5. Apply a targeted fix—filter early, prune columns, adjust partitioning, handle skew, or revise join strategy.
6. Compare runtime and output correctness against the previous run.

### Scenario 2: A join is very slow.

**Answer:** Check row counts, key data types, duplicate keys, data skew, and the physical plan. Filter and select before the join. If one side is genuinely small, test a broadcast join. For large joins, inspect shuffle and sort costs. Validate row counts and unmatched keys after the change.

### Scenario 3: One task takes 20 minutes while all other tasks finish in 1 minute.

**Answer:** This often indicates data skew or an unusually large partition. Inspect task input sizes, shuffle metrics, and hot keys. Consider AQE skew handling, repartitioning, or salting where appropriate. Check that the chosen solution preserves join/aggregation correctness.

### Scenario 4: You need to join a 200 GB fact table to a small department lookup.

**Answer:** If the lookup really is small enough for executor memory, broadcasting it may avoid shuffling the fact table for the join. Use `broadcast(lookup_df)` as a test and verify the physical plan. Do not use broadcast just because a table is called a lookup; measure its actual size.

### Scenario 5: A dataset contains duplicate business keys. You must retain the latest update.

**Answer:** Define what “latest” means, including a tie-breaker. Use `row_number()` over `Window.partitionBy(key).orderBy(updated_at.desc(), tie_breaker.desc())`, then retain row number 1. Validate that one row remains per key and inspect ties/null timestamps.

### Scenario 6: You need the top three salaries per department.

**Answer:** Use a window partitioned by department and ordered by salary descending. Use `row_number` for exactly three rows per department; use `rank`/`dense_rank` if the business rule requires ties to be included. Define deterministic tie-breaking if exact row counts matter.

### Scenario 7: The source adds a new column. What should your pipeline do?

**Answer:** Check whether the schema contract permits additive changes. Decide whether to accept the new field, map it, ignore it, or quarantine/fail. Validate downstream consumers and write behavior. Schema evolution should be an explicit policy, not an accidental side effect.

### Scenario 8: Some CSV rows have invalid data.

**Answer:** Define the expected schema and parsing behavior. Use an appropriate supported read mode and capture malformed rows where configured. Separate valid and invalid records, log counts and examples safely, correct or quarantine bad records, and set an agreed threshold for failing the pipeline.

### Scenario 9: Your output has thousands of tiny files.

**Answer:** Inspect the number of output partitions and how the job writes data. Reduce partitions before writing when suitable, using `coalesce` if reducing without a full shuffle is acceptable or `repartition` if redistribution is required. Do not force one partition for a large dataset. Consider platform-specific compaction for table formats.

### Scenario 10: You call `collect()` and the driver crashes.

**Answer:** `collect()` brings all rows to driver memory. Remove it for large data. Perform filtering, aggregation, or writing in Spark; use `take(n)` or `limit(n)` for small debugging samples. Never collect an unbounded production dataset just to inspect it.

### Scenario 11: A pipeline is re-run after a failure and creates duplicate rows.

**Answer:** Review write mode and whether the operation is idempotent. Use stable keys, deterministic deduplication, controlled partition overwrite, or Delta merge/upsert where available. Persist batch/run identifiers. Advance a watermark only after the target write succeeds and is validated.

### Scenario 12: Incremental data is missing updates.

**Answer:** Check the watermark column and source guarantees. If the source updates rows without changing the chosen timestamp, timestamp-based extraction can miss changes. Consider CDC, change tracking, reliable modified timestamps, overlap windows plus deduplication, or snapshot comparison. Test late-arriving updates and reruns.

### Scenario 13: How do you handle null values?

**Answer:** Determine whether null means unknown, not applicable, missing, or invalid. Use `isNull`/`isNotNull`, `fillna`, `when`, or a quarantine path according to the business rule. Do not replace nulls with arbitrary defaults without validating their meaning.

### Scenario 14: How do you optimize a groupBy aggregation?

**Answer:** Reduce rows and columns before aggregation when logically valid, inspect shuffle size and skew, check partitioning and AQE settings, and aggregate at the correct granularity. For very skewed keys, investigate salting or other skew-aware strategies. Compare results and runtime.

### Scenario 15: What is the difference between cache and persist?

**Answer:** `cache()` uses the API's default storage level for that version; `persist()` allows an explicit storage level. Both help reuse computed data. Materialize only when useful, and unpersist when finished.

### Scenario 16: What happens when you call `df.filter(...)`?

**Answer:** It returns a new DataFrame representing a filtered plan; because DataFrames are immutable and Spark is lazy, the full computation normally waits until an action needs the result. Spark may optimize the filter into the larger plan.

### Scenario 17: Why is `repartition()` expensive?

**Answer:** It redistributes records and usually causes a shuffle, which can involve network transfer, serialization, and disk I/O. Use it when the redistribution or increased parallelism benefits downstream work, not automatically after every transformation.

### Scenario 18: How do you investigate a wrong output count after a join?

**Answer:** Verify join type and keys, check duplicate keys on both sides, inspect null join keys, and compare matched/unmatched records. A many-to-many relationship can multiply rows. Validate with key-level counts, not only total counts.

### Scenario 19: How do you process late-arriving streaming events?

**Answer:** Use event-time processing and a watermark appropriate to the business lateness expectation. Understand the stateful operation's late-data behavior, checkpoint the query, and test events arriving within and beyond the watermark. A watermark is not a guarantee that all late data will be retained.

### Scenario 20: How would you design a Bronze–Silver–Gold pipeline?

**Answer:** Ingest source data into Bronze with traceable metadata; clean, type, validate, and deduplicate it in Silver; build business aggregates and consumption-ready tables in Gold. Add monitoring, data-quality checks, restart/idempotency handling, and reconciliation at each important boundary.

### Scenario 21: A broadcast join causes executor memory errors.

**Answer:** Verify the broadcast side's actual size and plan. Remove the forced broadcast or reduce/filter the lookup first. Check executor memory and Spark's chosen strategy. Use an appropriate distributed join if broadcasting is unsafe.

### Scenario 22: A filter seems not to improve performance.

**Answer:** Check whether the filter is selective, whether it can be pushed to the source, and whether the source format/statistics support pruning. Verify the plan and Spark UI. If most rows pass the filter or the expensive shuffle dominates, the improvement may be small.

### Scenario 23: A Python UDF slows down the pipeline.

**Answer:** Check whether built-in Spark functions can express the transformation. Built-in expressions generally allow more optimizer visibility and avoid standard Python UDF overhead. If a UDF is necessary, define its type, handle nulls, benchmark it, and consider vectorized approaches only when appropriate.

### Scenario 24: How do you prove that a data pipeline succeeded?

**Answer:** Check job status, source and target counts, key uniqueness, rejected records, required-column nulls, business aggregates, and watermark/batch state. Compare against expected tolerances and log run metadata. A successful Spark job alone does not guarantee business correctness.

### Scenario 25: How would you explain your debugging process to an interviewer?

**Answer:** “I first reproduce and understand the issue, inspect schema/data and the Spark plan, identify whether it is a logic, data-quality, or performance problem, make the smallest safe change, and validate the result with counts, key-level checks, and representative records before considering it resolved.”

---

## 29. Theory interview questions and answers

### Q1. What is PySpark?
PySpark is the Python API for Apache Spark, used to process data through distributed DataFrames, SQL, RDDs, and Structured Streaming.

### Q2. What is lazy evaluation?
Spark records transformations as a plan and runs the required computation when an action requests results.

### Q3. What is a partition?
A logical chunk of distributed data that can be processed by a task.

### Q4. What is a shuffle?
Redistribution of data across partitions, often across machines, so related records can be processed together. It can be expensive.

### Q5. What is the difference between transformation and action?
Transformations build a new plan; actions request results or materialize output and trigger execution.

### Q6. What is the difference between narrow and wide transformations?
Narrow transformations can process each input partition locally without a shuffle; wide transformations may require redistribution across partitions.

### Q7. What is the difference between `repartition` and `coalesce`?
`repartition` redistributes data with a shuffle and can increase/decrease partitions; `coalesce` is primarily used to reduce partitions with less movement.

### Q8. What is broadcast join?
Spark sends a small table to executors so each can join it with its local part of the large table, potentially avoiding a shuffle of the large side.

### Q9. What is a sort-merge join?
A physical join strategy commonly used for large equi-joins. Spark typically partitions and sorts data by join key before merging matching keys.

### Q10. What is data skew?
Uneven distribution of data across partitions/keys, causing a few tasks to run much longer than others.

### Q11. What is Catalyst?
Spark SQL's optimizer, which analyzes and optimizes logical query plans and helps select physical execution plans.

### Q12. What is AQE?
Adaptive Query Execution uses runtime statistics to adjust parts of the execution plan when supported and enabled.

### Q13. What is the difference between `rank`, `dense_rank`, and `row_number`?
`row_number` always gives unique sequential numbers; `rank` gives ties equal ranks and leaves gaps; `dense_rank` gives ties equal ranks without gaps.

### Q14. What is the difference between `groupBy` and a window?
`groupBy` generally collapses each group into aggregate rows. A window calculates across related rows while keeping the individual rows.

### Q15. What is the difference between `distinct` and `dropDuplicates`?
`distinct` removes duplicate full rows. `dropDuplicates` can use selected columns as the duplicate key, but does not guarantee which non-key row survives.

### Q16. What is the difference between `union` and `unionByName`?
`union` aligns columns by position; `unionByName` aligns by column name.

### Q17. What is the difference between cache and persist?
Both support reuse of computed data. `cache()` uses a default storage level; `persist()` lets you select a storage level, depending on API/version.

### Q18. Why prefer DataFrames to RDDs for structured data?
DataFrames have schema and relational expressions that Spark SQL can optimize. They are the default choice for most structured ETL workloads.

### Q19. What is Parquet?
A columnar storage format that can reduce I/O for analytical queries and supports metadata/statistics.

### Q20. What is Delta Lake?
A table/storage layer commonly built on Parquet that adds a transaction log and capabilities such as ACID transactions and time travel.

### Q21. What is a watermark in streaming?
A mechanism based on event time that helps Spark reason about late data and bound state for supported stateful operations.

### Q22. What is checkpointing?
Persisting streaming progress/state information so a query can recover from failure according to the source/sink and query guarantees.

### Q23. What is idempotency?
Repeating the same logical operation does not create unintended duplicate or inconsistent results.

### Q24. Why should you avoid `collect()` on large data?
It transfers all selected rows to driver memory and can cause driver memory failure.

### Q25. How do you debug a slow Spark job?
Use Spark UI, task/stage metrics, shuffle/spill metrics, `explain("formatted")`, input sizes, key skew, and targeted measurements.

### Q26. What is schema enforcement?
Checking that incoming data conforms to an expected schema/contract rather than silently accepting incompatible fields/types.

### Q27. What is a UDF?
A user-defined function for custom logic. Prefer native Spark functions when possible because they are generally more optimizable and avoid standard Python UDF overhead.

### Q28. What is the difference between event time and processing time?
Event time is when an event occurred; processing time is when Spark processed it.

### Q29. What is a left anti join?
It returns rows from the left DataFrame that have no matching key in the right DataFrame.

### Q30. What is the Driver's responsibility?
The Driver coordinates the application, builds plans, schedules jobs/tasks, and tracks execution; Executors run tasks.

---

## 30. Coding interview questions

Practice writing these without looking at the solution first.

1. Filter employees with salary greater than a threshold.
2. Calculate average salary per department.
3. Find duplicate business keys.
4. Keep the latest record for each key using a window.
5. Find the second-highest distinct salary.
6. Find the top N rows per group.
7. Calculate a running total per customer/account.
8. Compare each row with the previous row using `lag`.
9. Find source keys missing from a dimension using left anti join.
10. Replace nulls based on a business rule.
11. Parse a date string and extract year/month.
12. Explode an array column into multiple rows.
13. Parse JSON stored in a string column.
14. Union two DataFrames with different column ordering.
15. Join a fact DataFrame to a small lookup and verify the join plan.
16. Count malformed/invalid rows and separate them from valid rows.
17. Write output in Parquet/Delta and explain the write mode.
18. Explain how to avoid duplicate output when rerunning a failed incremental job.
19. Diagnose a skewed aggregation.
20. Explain why `collect()` is unsafe for large datasets.

### Interview coding checklist

- State assumptions and expected output.
- Handle nulls and duplicate keys.
- Choose deterministic ordering for window functions.
- Use native Spark expressions when possible.
- Do not call `collect()` on unbounded data.
- Explain potential shuffle and performance implications.
- Validate the result.

---

## 31. Common mistakes and corrections

| Mistake | Better approach |
|---|---|
| Using Python `and`/`or` between Spark columns | Use `&` / `|` with parentheses. |
| Assuming `dropDuplicates(["id"])` keeps the latest row | Use a window with deterministic ordering. |
| Broadcasting a table without checking its size | Estimate size, inspect the plan, and verify executor memory. |
| Calling `collect()` on large datasets | Keep processing distributed; collect only bounded results. |
| Caching every intermediate DataFrame | Cache only expensive data that will be reused. |
| Increasing partitions without reason | Measure task size, parallelism, and shuffle overhead first. |
| Assuming every join shuffles | Inspect physical plan; broadcast and existing partitioning can change behavior. |
| Assuming a successful job means correct data | Check counts, keys, nulls, aggregates, rejected records, and business rules. |
| Using an unreliable watermark | Confirm source change semantics, late updates, and rerun strategy. |
| Treating null and zero as the same | Follow the business definition of missing values. |
| Using `union` when column order differs | Prefer `unionByName` for name-based alignment. |
| Assuming output is globally sorted after sorting partitions locally | Use the appropriate global `orderBy` when global ordering is required. |
| Treating a watermark as a guarantee for all late streaming events | Understand event-time and stateful-operation semantics. |
| Saying Spark is always faster because it is in-memory | Performance depends on workload, shuffles, storage, planning, and resources. |

---

## 32. Final one-page revision sheet

Memorize these lines and be able to explain each one:

- **Spark:** distributed engine for parallel data processing.
- **PySpark:** Python API for Spark.
- **Driver:** coordinates the application and schedules work.
- **Executor:** runs tasks and may cache partitions.
- **Partition:** logical chunk of distributed data.
- **Transformation:** creates a new plan/DataFrame.
- **Action:** requests a result and triggers required execution.
- **Lazy evaluation:** Spark plans first and executes when needed.
- **Narrow transformation:** can process partitions locally without a shuffle.
- **Wide transformation:** may redistribute data through a shuffle.
- **Shuffle:** network/disk-intensive redistribution of data.
- **DataFrame:** structured distributed data with named columns and schema.
- **RDD:** lower-level immutable distributed collection.
- **Catalyst:** Spark SQL query optimizer.
- **AQE:** can adapt the physical plan using runtime statistics.
- **Broadcast join:** copy a sufficiently small table to executors to avoid shuffling the large side for that join.
- **Sort-merge join:** common strategy for large equi-joins.
- **Skew:** uneven data distribution that creates straggler tasks.
- **`repartition`:** shuffle and redistribute data.
- **`coalesce`:** mainly reduce partitions with less movement.
- **`cache` / `persist`:** reuse computed data when beneficial.
- **Window:** calculate across related rows without collapsing them.
- **`row_number`:** unique sequence; **`rank`:** gaps after ties; **`dense_rank`:** no gaps.
- **Parquet:** columnar file format.
- **Delta Lake:** table layer with transaction log and table reliability features.
- **Watermark:** event-time mechanism for late-data/state management in streaming.
- **Checkpoint:** recovery/progress information for streaming.
- **Idempotency:** safe repeat without unintended duplicate results.
- **Best performance habit:** inspect Spark UI and `explain()` before guessing.
- **Best correctness habit:** validate row counts, keys, nulls, rejected rows, and business aggregates.

### Five answers you should deliver confidently

**1. What is lazy evaluation?**  
“Spark records transformations in a plan and executes the required computation when an action requests a result. This gives Spark an opportunity to optimize the combined plan.”

**2. What is shuffle?**  
“Shuffle redistributes data across partitions, often across executors, so records needed by an operation can meet. It can be expensive due to network transfer, serialization, and disk I/O.”

**3. How do you optimize a slow job?**  
“I inspect the Spark UI and physical plan, identify the slow stage, check shuffle, skew, spill, partition sizing and join strategy, apply a targeted change, and compare runtime and data correctness.”

**4. How do you keep the latest row per key?**  
“I use `row_number()` over a window partitioned by the business key and ordered by update timestamp descending plus a deterministic tie-breaker, then keep row number 1.”

**5. How do you build a reliable incremental pipeline?**  
“I use a reliable change-detection method, persist watermark/batch state, deduplicate by business key, write idempotently, validate output, and advance the watermark only after a successful commit.”

---

## 33. Official references

Use official documentation when a version-specific detail matters:

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Documentation](https://docs.delta.io/)

---

## Final reminder

This README is a consolidated revision resource, not a promise that every API behaves identically in every Spark version or platform. For interviews, explain the concept clearly, give a small example, describe a real-world use case, and mention how you would validate the result. For production code, check the actual Spark/Databricks version and test edge cases.
