from pyspark.sql import SparkSession
import findspark
import pandas as pd
import numpy as np
import os

#spark = SparkSession.builder.appName("test").getOrCreate()
# sparkSession.createDataFrame([(1, "value1"), (2, "value2")], ["id", "value"]).show()
#spark.sparkContext.addPyFile("neo4j-connector-apache-spark_2.12-5.0.0_for_spark_3.jar")
# spark = SparkSession.builder.appName('DF_to_dict').getOrCreate()
#spark.sparkContext.addPyFile("/Users/tonywu/Documents/dev/spring/testPyspark1/neo4j-connector-apache-spark_2.12-4.0.2_for_spark_3.jar")
URI = "neo4j://localhost:11003"
#AUTH = ("neo4j", "Ne04j!")

spark =(SparkSession.builder.master('local[*]')
        .appName('Leverage Neo4j')
#        .config('spark.ui.port', '4050')
        .config('spark.sql.repl.eagerEval.enabled', True)
        .config('spark.jars', '/Users/tonywu/Documents/dev/spring/testPyspark1/neo4j-connector-apache-spark_2.12-4.0.2_for_spark_3.jar')
        .config("neo4j.url", URI).config("neo4j.authentication.type", "basic")
        .config("neo4j.authentication.basic.username", 'neo4j')
        .config("neo4j.authentication.basic.password", 'Neo4j!').getOrCreate())
# Create data in dataframe
data = [(('Ram'), '1991-04-01', 'M', 3000),
        (('Mike'), '2000-05-19', 'M', 4000),
        (('Rohini'), '1978-09-05', 'M', 4000),
        (('Maria'), '1967-12-01', 'F', 4000),
        (('Jenis'), '1980-02-17', 'F', 1200)]

# Column names in dataframe
columns = ["Name", "BirthDate", "Gender", "Salary"]
# Create the spark dataframe
df = spark.createDataFrame(data=data,
                           schema=columns)

# spark.read.format("org.neo4j.spark.DataSource") \
#     .option("authentication.basic.username", "neo4j") \
#     .option("authentication.basic.password", "Ne04j!") \
#     .option("url", "bolt://localhost:11003") \
#     .option("labels", ":Person") \
#     .load()

df.write.format("org.neo4j.spark.DataSource") \
    .mode("Append")\
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "Ne04j!") \
    .option("url", "bolt://localhost:11003") \
    .option("labels", ":Person") \
    .save()


