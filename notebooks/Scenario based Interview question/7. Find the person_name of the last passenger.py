# Databricks notebook source
Imagine you're managing a queue of people waiting to board a bus, each with a specific weight and turn that determines the order of which the people will board the bus. The bus has a weight limit of 1000 kilograms.

Task :
Your task is to write PySpark code that find the person_name of the last passenger who can board the bus without exceeding the weight limit.

# COMMAND ----------

data = [
(5, "Alice", 250, 1),
(4, "Bob", 175, 5),
(3, "Alex", 350, 2),
(6, "John Cena", 400, 3),    
(1, "Winston", 500, 6),
(2, "Marie", 200, 4)
]

schema = ["person_id", "person_name", "weight", "turn"]

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df_sort = df.sort('turn').collect()
print(df_sort)

# COMMAND ----------

count = 0
final_data = []

for row in df_sort:
    count += row['weight']
    if count >1000:
        break
    else:
        last_person = row['person_name']

final_data.append(last_person)
print(final_data)

# COMMAND ----------

df_final = spark.createDataFrame([final_data],['person_name'])
df_final.show()
