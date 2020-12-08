#!/usr/local/bin/python
"""Transform XML file to CSV"""

import sys
import os
import shutil
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit


output=sys.argv[2]
input_file = sys.argv[1]
script_args = sys.argv[3]

temporary_directory=output + "_tmp/"

if os.path.exists(temporary_directory):
    shutil.rmtree(temporary_directory, ignore_errors=True)

os.makedirs(temporary_directory)

print("Starting data transformation..." + input_file)
spark = SparkSession\
        .builder\
        .appName(input_file)\
        .getOrCreate()

schema = StructType([
    StructField('TAGS', StringType(), True),
    StructField('TEXT', StringType(), True)
    ])

df = spark.read.format("com.databricks.spark.xml") \
        .options(rowTag="PatientMatching") \
        .load(input_file, schema=schema)


run_id = script_args
df = df.withColumn("partition_id", lit(run_id))

df.write.format("com.databricks.spark.csv").option("header", "false").option("escape", '"').mode("overwrite").save(temporary_directory)

if os.path.exists(temporary_directory + "_SUCCESS"):
    print("Removing file " + temporary_directory + "_SUCCESS")
    os.remove(temporary_directory + "_SUCCESS")
    files = [f for f in os.listdir(temporary_directory)]
    files = list(filter(lambda f: f.endswith('.csv'), files))
    if len(files) > 1:
        print("More than one file has been generated. Total csv files {}.", len(files))
    else:
        print("Renaming file from {} to {}".format(files[0], output))
        os.rename(temporary_directory + files[0], output)
