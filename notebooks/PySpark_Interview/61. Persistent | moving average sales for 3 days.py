# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC # Find Minimum, Maximum and Average Salary for every department in a company

# COMMAND ----------

# Sample data for department and employee salary
dept_data = [
    (1, "HR"),
    (2, "Engineering"),
    (3, "Sales")
]
dept_schema = ["dept_id", "dept_name"]

emp_data = [
    (101, 1, 50000),
    (102, 1, 60000),
    (103, 2, 80000),
    (104, 2, 120000),
    (105, 2, 95000),
    (106, 3, 40000),
    (107, 3, 45000)
]
emp_schema = ["emp_id", "dept_id", "salary"]

dept_df = spark.createDataFrame(dept_data, dept_schema)
emp_df = spark.createDataFrame(emp_data, emp_schema)
display(dept_df)
display(emp_df)


# COMMAND ----------

result_df = emp_df.groupBy("dept_id")\
                  .agg(max("salary").alias("maximum_salary"),
                       min("salary").alias("minimum_salary"),
                       avg("salary").alias("average_salary"))\
                  .join(dept_df, "dept_id")
                  
result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Find moving average sales for 3 Days (Friday, Saturday and Sunday) in a quarter

# COMMAND ----------

# Sample sales data for a quarter (18 weeks), only for Friday, Saturday, Sunday, Monday
from datetime import datetime, timedelta

days = ['Friday', 'Saturday', 'Sunday', 'Monday']
start_date = datetime(2026, 1, 2)  
sales_data = []
for week in range(18):
    for i, day in enumerate(days):
      # print('i :',i)
      # print('day :',day)
      # print('timedelta :',timedelta(weeks=week, days=i))
      date = start_date + timedelta(weeks=week, days=i)
      # print('date :',date)
      # print('*'*10)
      sales = 1000 + week * 100 + i * 50  

      sales_data.append((date.strftime('%Y-%m-%d'), day, sales))
print(sales_data)

# COMMAND ----------

sales_schema = ["date", "day", "sales"]
sales_df = spark.createDataFrame(sales_data, sales_schema)
sales_df = sales_df.withColumn("date", to_date("date", "yyyy-MM-dd"))

display(sales_df)

# COMMAND ----------

moving_avg_df = sales_df.withColumn('querter', quarter('date'))\
                       .withColumn('dayofweek', dayofweek('date'))\
                        .filter('dayofweek IN (6,7,1)')
final_moving_avg_df = moving_avg_df.withColumn('moving_avg_sales', avg('sales').over(Window.partitionBy('querter').orderBy('date').rowsBetween(-2, 0)))
display(final_moving_avg_df)


# COMMAND ----------

# MAGIC %md
# MAGIC # From a flight database, find out total delay for each flight in a week

# COMMAND ----------

# Sample flight delay data for a week
flight_data = [
    ("AA101", "2025-04-20", 15),
    ("AA101", "2025-04-21", 10),
    ("AA101", "2025-04-22", 5),
    ("AA102", "2025-04-20", 0),
    ("AA102", "2025-04-21", 20),
    ("AA102", "2025-04-22", 30),
    ("AA103", "2025-04-20", 5),
    ("AA103", "2025-04-21", 10),
    ("AA103", "2025-04-22", 0),
    ("AA101", "2025-04-15", 20),
    ("AA102", "2025-04-16", 10),
    ("AA103", "2025-04-17", 15)
]
flight_schema = ["flight_id", "date", "delay_minutes"]

flight_df = spark.createDataFrame(flight_data, flight_schema)
flight_df = flight_df.withColumn("date", to_date("date", "yyyy-MM-dd"))

display(flight_df)


# COMMAND ----------

final_flight_df = flight_df.withColumn('week_of_year', weekofyear('date'))
total_delay_df = final_flight_df.groupBy("flight_id", "week_of_year").agg(sum("delay_minutes").alias("total_delay_minutes"))    
display(total_delay_df)

