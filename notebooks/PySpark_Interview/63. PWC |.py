# Databricks notebook source
# MAGIC %md
# MAGIC # Round 1 - Technical 1 (SQL | Python | Data Engineering Basics)
# MAGIC
# MAGIC 1. **Write a SQL query to find duplicate records and remove them efficiently.**
# MAGIC 2. **What is the difference between `ROW_NUMBER()`, `RANK()`, and `DENSE_RANK()`? Provide real examples.**
# MAGIC 3. **Given a large dataset, how would you optimize a slow-running SQL query?**
# MAGIC 4. **Python: How do you handle missing or null values in a dataset?**
# MAGIC 5. **Write a Python/PySpark script to read a large CSV file and process it efficiently.**
# MAGIC 6. **What are different types of joins in SQL and when would you use each?**
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Write a SQL query to find duplicate records and remove them efficiently.**

# COMMAND ----------

# Create a dataframe with duplicate data
data = [
    (1, "Alice", 100),
    (2, "Bob", 200),
    (1, "Alice", 100),
    (3, "Charlie", 300),
    (2, "Bob", 200)
]
columns = ["id", "name", "score"]
df = spark.createDataFrame(data, columns)
display(df)

# Register the dataframe as a temp view
df.createOrReplaceTempView("duplicates_table")

# SQL query to find duplicate records
duplicates_sql = """
SELECT id, name, score, COUNT(*) as cnt
FROM duplicates_table
GROUP BY id, name, score
HAVING cnt > 1
"""
duplicates_df = spark.sql(duplicates_sql)
display(duplicates_df)

# SQL query to remove duplicates efficiently
dedup_sql = """
SELECT DISTINCT id, name, score
FROM duplicates_table
"""
dedup_df = spark.sql(dedup_sql)
display(dedup_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. ### Difference between `ROW_NUMBER()`, `RANK()`, and `DENSE_RANK()`
# MAGIC
# MAGIC - **ROW_NUMBER()**: Assigns a unique sequential number to each row, even if there are duplicate values.
# MAGIC - **RANK()**: Assigns the same rank to duplicate values, but leaves gaps in the ranking sequence.
# MAGIC - **DENSE_RANK()**: Assigns the same rank to duplicates, but does not leave gaps; ranks are consecutive.
# MAGIC
# MAGIC **Example:**  
# MAGIC Suppose we have scores: `[300, 200, 200, 100, 100]`
# MAGIC
# MAGIC | Score | ROW_NUMBER | RANK | DENSE_RANK |
# MAGIC |-------|------------|------|------------|
# MAGIC | 300   |     1      |  1   |     1      |
# MAGIC | 200   |     2      |  2   |     2      |
# MAGIC | 200   |     3      |  2   |     2      |
# MAGIC | 100   |     4      |  4   |     3      |
# MAGIC | 100   |     5      |  4   |     3      |

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **Given a large dataset, how would you optimize a slow-running SQL query?**
# MAGIC
# MAGIC - **Use proper indexing** on columns used in WHERE, JOIN, and ORDER BY clauses.
# MAGIC - **Select only necessary columns** instead of using `SELECT *`.
# MAGIC - **Filter early** by applying WHERE clauses as soon as possible.
# MAGIC - **Avoid unnecessary subqueries** and use JOINs efficiently.
# MAGIC - **Partition large tables** to enable partition pruning.
# MAGIC - **Use appropriate file formats** (e.g., Parquet, ORC) for better I/O performance.
# MAGIC - **Analyze and update statistics** to help the query optimizer.
# MAGIC - **Leverage caching** for intermediate results if reused.
# MAGIC - **Rewrite complex queries** for better readability and execution plans.
# MAGIC - **Monitor query execution plans** to identify bottlenecks.

# COMMAND ----------

# MAGIC %md
# MAGIC 4. ### How do you handle missing or null values in a dataset?
# MAGIC
# MAGIC - **Remove rows/columns** with missing values (`dropna` in Pandas, `dropna()` in PySpark).
# MAGIC - **Inpute values** using mean, median, mode, or custom logic (`fillna` in Pandas/PySpark).
# MAGIC - **Replace with default values** (e.g., 0, "Unknown")using NVL().
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. What are different types of joins in SQL and when would you use each?
# MAGIC
# MAGIC - **INNER JOIN**: Returns records with matching values in both tables.  
# MAGIC   *Use when you need only the rows that have matches in both tables.*
# MAGIC
# MAGIC - **LEFT JOIN (LEFT OUTER JOIN)**: Returns all records from the left table and matched records from the right table.  
# MAGIC   *Use when you want all rows from the left table, even if there are no matches in the right table.*
# MAGIC
# MAGIC - **RIGHT JOIN (RIGHT OUTER JOIN)**: Returns all records from the right table and matched records from the left table.  
# MAGIC   *Use when you want all rows from the right table, even if there are no matches in the left table.*
# MAGIC
# MAGIC - **FULL JOIN (FULL OUTER JOIN)**: Returns all records when there is a match in either left or right table.  
# MAGIC   *Use when you want all rows from both tables, with NULLs where there is no match.*
# MAGIC
# MAGIC - **CROSS JOIN**: Returns the Cartesian product of both tables (all possible combinations).  
# MAGIC   *Use rarely, typically for generating combinations.*
# MAGIC
# MAGIC - **SELF JOIN**: A table is joined with itself.  
# MAGIC   *Use for hierarchical or recursive data relationships.*

# COMMAND ----------

# MAGIC %md
# MAGIC # Round 2 - Technical 2 (ADF | Azure | Databricks)
# MAGIC
# MAGIC 1. **How do you design an end-to-end ETL pipeline in Azure Data Factory?**
# MAGIC 2. **Explain incremental data loading strategies in ADF.**
# MAGIC 3. **What is partitioning in Databricks and how does it improve performance?**
# MAGIC 4. **Difference between Data Lake vs Data Warehouse vs Lakehouse architecture.**
# MAGIC 5. **How does Delta Lake ensure ACID transactions?**
# MAGIC 6. **How do you monitor and troubleshoot failures in ADF pipelines?**

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **How do you design an end-to-end ETL pipeline in Azure Data Factory?**
# MAGIC
# MAGIC - **Requirement Analysis:** Understand data sources, transformation logic, and destination requirements.
# MAGIC - **Source Connection:** Create Linked Services to connect to data sources (e.g., SQL, Blob, REST).
# MAGIC - **Data Ingestion:** Use Datasets and Copy Data activities to extract data from sources.
# MAGIC - **Data Transformation:** Use Data Flow or Databricks/Synapse activities for data cleansing, transformation, and enrichment.
# MAGIC - **Data Loading:** Load transformed data into target systems (e.g., Data Lake, SQL Data Warehouse).
# MAGIC - **Orchestration:** Use Pipelines to sequence activities, manage dependencies, and control workflow.
# MAGIC - **Parameterization:** Use parameters, variables, and triggers for dynamic and reusable pipelines.
# MAGIC - **Monitoring & Logging:** Monitor pipeline runs, set up alerts, and review logs for troubleshooting.
# MAGIC - **Error Handling:** Implement retry policies, error handling, and notifications for failures.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. **Explain incremental data loading strategies in ADF.**
# MAGIC
# MAGIC - **Watermark Columns:** Track the maximum value of a column (e.g., timestamp, ID) to load only new or changed records since the last run.
# MAGIC - **Change Data Capture (CDC):** Use source system features or ADF's built-in CDC to identify and load only changed data.
# MAGIC - **Timestamps/Modified Date:** Filter records based on a `last_modified` or `updated_at` column.
# MAGIC - **Soft Deletes:** Handle deleted records by tracking a status or flag column.
# MAGIC - **Delta/Upsert Patterns:** Use merge operations to insert new and update existing records in the target.
# MAGIC - **Metadata Tables:** Maintain tables to store last load time or watermark values for each pipeline/table.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **What is partitioning in Databricks and how does it improve performance?**
# MAGIC
# MAGIC - **Partitioning** is the process of dividing a large dataset into smaller, manageable parts (partitions) based on the values of one or more columns.
# MAGIC - In Databricks, partitioning can be applied at both the DataFrame and file system (e.g., Delta Lake, Parquet) levels.
# MAGIC - **Benefits:**
# MAGIC   - **Improved Query Performance:** Queries can skip irrelevant partitions (partition pruning), reducing the amount of data scanned.
# MAGIC   - **Efficient Data Management:** Makes it easier to manage, update, and delete subsets of data.
# MAGIC   - **Parallel Processing:** Enables Spark to process partitions in parallel, leveraging cluster resources efficiently.
# MAGIC - **Best Practices:**
# MAGIC   - Choose partition columns with high cardinality and even data distribution.
# MAGIC   - Avoid over-partitioning (too many small files) or under-partitioning (few large files).
# MAGIC
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC 4. **Difference between Data Lake vs Data Warehouse vs Lakehouse architecture**
# MAGIC
# MAGIC - **Data Lake**
# MAGIC   - Stores raw, unstructured, semi-structured, and structured data.
# MAGIC   - Scalable and cost-effective for big data storage.
# MAGIC   - Schema-on-read; flexible for data science and machine learning.
# MAGIC   - Examples: Azure Data Lake, Amazon S3.
# MAGIC
# MAGIC - **Data Warehouse**
# MAGIC   - Stores structured, processed data optimized for analytics and reporting.
# MAGIC   - Schema-on-write; enforces data quality and consistency.
# MAGIC   - High performance for complex queries and BI workloads.
# MAGIC   - Examples: Azure Synapse, Amazon Redshift, Snowflake.
# MAGIC
# MAGIC - **Lakehouse**
# MAGIC   - Combines features of Data Lakes and Data Warehouses.
# MAGIC   - Supports both structured and unstructured data with ACID transactions.
# MAGIC   - Enables advanced analytics, BI, and machine learning on a unified platform.
# MAGIC   - Examples: Databricks Lakehouse, Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC 5. **How does Delta Lake ensure ACID transactions?**
# MAGIC
# MAGIC - **Atomicity:** Operations are all-or-nothing using transaction logs; either all changes are committed or none.
# MAGIC - **Consistency:** Data is always in a valid state; schema enforcement and constraints prevent corrupt data.
# MAGIC - **Isolation:** Concurrent reads and writes are managed using optimistic concurrency and versioned data, ensuring no conflicts.
# MAGIC - **Durability:** Committed data and transaction logs are stored reliably, so data is not lost even after failures.
# MAGIC
# MAGIC Delta Lake uses a transaction log (`_delta_log`) to record all changes, enabling these ACID guarantees on data lakes.

# COMMAND ----------

# MAGIC %md
# MAGIC 6. **How do you monitor and troubleshoot failures in ADF pipelines?**
# MAGIC
# MAGIC - **Monitor Pipeline Runs:** Use the ADF Monitoring UI to track pipeline, activity, and trigger runs.
# MAGIC - **Alerts & Notifications:** Set up alerts for failed pipeline runs using Azure Monitor or Logic Apps.
# MAGIC - **Review Logs & Output:** Examine activity output, error messages, and diagnostic logs for details.
# MAGIC - **Retry Policies:** Configure retries and timeouts for transient failures.
# MAGIC - **Custom Logging:** Use Web or Azure Function activities to log custom events or errors.
# MAGIC - **Integration Runtime Monitoring:** Check the status and logs of Integration Runtimes for connectivity or resource issues.
# MAGIC - **Root Cause Analysis:** Analyze error codes, stack traces, and input data to identify and resolve issues.
# MAGIC - **Pipeline Debugging:** Use the Debug mode to test pipelines with sample data before production runs.

# COMMAND ----------

# MAGIC %md
# MAGIC # Round 3 - Managerial / Behavioral
# MAGIC
# MAGIC 1. **Tell me about a challenging data engineering project you worked on.**
# MAGIC 2. **How do you handle tight deadlines and multiple priorities?**
