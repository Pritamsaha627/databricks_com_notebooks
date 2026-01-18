# Databricks notebook source
date_to_run = [{'date': '2009-03-29'}, {'date': '2009-04-05'}, {'date': '2009-04-26'}, {'date': '2009-05-24'}, {'date': '2009-06-07'}, {'date': '2009-06-21'}, {'date': '2009-07-12'}, {'date': '2009-08-23'}, {'date': '2009-08-23'}, {'date': '2009-09-13'}, {'date': '2009-10-04'}, {'date': '2009-10-18'}, {'date': '2008-03-23'}, {'date': '2008-04-06'}, {'date': '2009-08-30'}]

# COMMAND ----------

import concurrent.futures


def run_notebook(date_to_run):
    dbutils.notebook.run("/Users/pritamsaha627@gmail.com/filter by date",300,date_to_run)

    

# COMMAND ----------

workers = 2


with concurrent.futures.ThreadPoolExecutor(max_workers = workers) as executor :
    executor.map(run_notebook,date_to_run)
