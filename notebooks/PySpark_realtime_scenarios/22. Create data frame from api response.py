# Databricks notebook source
url = "https://api.genderize.io?name=luca"

# COMMAND ----------

import requests

l = []

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    l.append(data)
    print(l)
else:
    print("Failed to fetch the data. Status Code :",response.status_code)

df = spark.createDataFrame(l)
df.show()


