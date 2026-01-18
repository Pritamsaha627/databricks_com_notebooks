# Databricks notebook source
# MAGIC %run ./transformations

# COMMAND ----------

from unittest import *
from pyspark.sql import SparkSession

# COMMAND ----------

class TransformationsTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder\
                    .appName("test-cases")\
                    .getOrCreate()


    def test_load_file(self):
        sample_df = load_file(self.spark,"/FileStore/source_file/races.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count,1058,"Count should be 1058")


    def test_count_by_year(self):
        sample_df = load_file(self.spark,"/FileStore/source_file/races.csv")
        count_list = count_by_year(sample_df).collect()

        count_dict = dict()
        for row in count_list:
            count_dict[row["year"]] = row["count"]

        self.assertEqual(count_dict[2018],21,"Count on 2018 should be 21")
        self.assertEqual(count_dict[2019],21,"Count on 2019 should be 21")
        self.assertEqual(count_dict[2016],21,"Count on 2016 should be 21")
        self.assertEqual(count_dict[2021],23,"Count on 2021 should be 23")

# COMMAND ----------

def testsuit():
    suit = TestSuite()
    suit.addTest(TransformationsTestCase('test_load_file'))
    suit.addTest(TransformationsTestCase('test_count_by_year'))
    return suit

# COMMAND ----------

runner = TextTestRunner()
runner.run(testsuit())
