# Databricks notebook source
# MAGIC %md
# MAGIC # Masking

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GROUPS

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GROUPS WITH USER `pritamsaha627@gmail.com`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM deltatable_tutorial.demo.cust

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION deltatable_tutorial.demo.c_acctbal_musking(c_acctbal STRING)
# MAGIC -- RETURNS INT
# MAGIC RETURN CASE 
# MAGIC     WHEN is_account_group_member('HR') THEN c_acctbal 
# MAGIC     ELSE "****"
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE deltatable_tutorial.demo.cust ALTER COLUMN c_acctbal SET MASK deltatable_tutorial.demo.c_acctbal_musking

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from deltatable_tutorial.demo.cust

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE deltatable_tutorial.demo.cust ALTER COLUMN c_acctbal DROP MASK;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from deltatable_tutorial.demo.cust

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS deltatable_tutorial.demo.c_acctbal_musking
