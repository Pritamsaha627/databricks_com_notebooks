# Databricks notebook source
df = spark.read.csv('dbfs:/FileStore/source_file/example.csv',header=True).drop('rank')
df.show()
df.createOrReplaceTempView('tp')

# COMMAND ----------

Find out the h_name whose score is grater than the total average score

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH avg_score(avg_sc) as(
# MAGIC     select avg(score) as avg_sc from tp )
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tp,average_score
# MAGIC where score > avg_sc

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,rank(score) over (partition by challenge_id order by score desc) as rank from tp

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS student(course VARCHAR(10), mark int, name varchar(10));
# MAGIC
# MAGIC INSERT INTO student VALUES 
# MAGIC   ('Maths', 60, 'Thulile'),
# MAGIC   ('Maths', 60, 'Pritha'),
# MAGIC   ('Maths', 70, 'Voitto'),
# MAGIC   ('Maths', 55, 'Chun'),
# MAGIC   ('Biology', 60, 'Bilal'),
# MAGIC    ('Biology', 70, 'Roger');
# MAGIC
# MAGIC SELECT 
# MAGIC   RANK() OVER (PARTITION BY course ORDER BY mark DESC) AS rank, 
# MAGIC   DENSE_RANK() OVER (PARTITION BY course ORDER BY mark DESC) AS dense_rank, 
# MAGIC   ROW_NUMBER() OVER (PARTITION BY course ORDER BY mark DESC) AS row_num, 
# MAGIC   course, mark, name 
# MAGIC FROM student ORDER BY course, mark DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC with tempview(total_score) as 
# MAGIC (SELECT sum(score) FROM student)
# MAGIC select * from student 
# MAGIC where tempview.total_score > student.score
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH temporaryTable (averageValue,sum) as
# MAGIC     (SELECT avg(Attr1),sum(att2)
# MAGIC     FROM Table)
# MAGIC     SELECT Attr1
# MAGIC     FROM Table, temporaryTable
# MAGIC     WHERE Table.Attr1 > temporaryTable.averageValue;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cashflow.Transaction_Date,  Cashflow.FundFamily, cashflow.IRR_Cashflow ---, cashflow.Strategy, cashflow.PME_Cashflow, cashflow.DA_Cashflow, ITD_Cashflow_Json.Reporting_Start_Date
# MAGIC
# MAGIC   FROM Cashflow
# MAGIC
# MAGIC   lateral view explode(ITD_Cashflow_Json.Cashflow) as cashflow
# MAGIC

# COMMAND ----------

'''
Total sales from each stores
Find the average sales with respect all the stores
Find the stores where total sales is greater than average sales of all stores.
'''

# COMMAND ----------

# MAGIC %sql
# MAGIC use default

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sale (
# MAGIC 	store_id INT, 
# MAGIC 	store_name VARCHAR (50),
# MAGIC 	product VARCHAR (50),
# MAGIC 	quantity DECIMAL (10,2),
# MAGIC   price INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO sale (store_id, store_name, product, quantity, price) VALUES (101, 'Apple_store', 'iphone', 1, 1000);
# MAGIC INSERT INTO sale (store_id, store_name, product, quantity, price) VALUES (101, 'Apple_store', 'mac', 3, 2000);
# MAGIC INSERT INTO sale (store_id, store_name, product, quantity, price) VALUES (101, 'Apple_store', 'airpod', 2, 280);
# MAGIC INSERT INTO sale (store_id, store_name, product, quantity, price) VALUES (102, 'Nokia_store', 'phone', 2, 1000);
# MAGIC INSERT INTO sale (store_id, store_name, product, quantity, price) VALUES (103, 'Samsung_store', 'phone', 1, 1000);
# MAGIC INSERT INTO sale (store_id, store_name, product, quantity, price) VALUES (103, 'Samsung_store', 'laptop', 1, 2000);
# MAGIC INSERT INTO sale (store_id, store_name, product, quantity, price) VALUES (103, 'Samsung_store', 'airpod', 4, 1100);
# MAGIC INSERT INTO sale (store_id, store_name, product, quantity, price) VALUES (103, 'Samsung_store', 'watch', 2, 1000);
# MAGIC INSERT INTO sale (store_id, store_name, product, quantity, price) VALUES (103, 'Samsung_store', 'band', 3, 280);
# MAGIC INSERT INTO sale (store_id, store_name, product, quantity, price) VALUES (104, 'Mi_store', 'phone', 2, 1000);
# MAGIC INSERT INTO sale (store_id, store_name, product, quantity, price) VALUES (104, 'Mi_store', 'band', 1, 2500);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sale

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted sale
