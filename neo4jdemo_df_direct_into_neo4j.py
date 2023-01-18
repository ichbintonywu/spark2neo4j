from pyspark.sql import SparkSession

spark =(SparkSession.builder.master('local[*]')
        .appName('Leverage Neo4j')
        .config('spark.sql.repl.eagerEval.enabled', True)
        .config('spark.jars', 'PathTo/neo4j-connector-apache-spark_2.12-4.0.2_for_spark_3.jar')
        .getOrCreate())

# Create data in dataframe
data = [(('Ram'), '1991-04-01', 'M'),
        (('Mike'), '2000-05-19', 'M'),
        (('Rohini'), '1978-09-05', 'M'),
        (('Maria'), '1967-12-01', 'F',),
        (('Jenis'), '1980-02-17', 'F')]

# Column names in dataframe
columns = ["Name", "BirthDate", "Gender"]
# Create the spark dataframe
df = spark.createDataFrame(data=data,
                           schema=columns)

# spark.read.format("org.neo4j.spark.DataSource") \
#     .option("authentication.basic.username", "neo4j") \
#     .option("authentication.basic.password", "password") \
#     .option("url", "bolt://localhost:7687") \
#     .option("labels", ":Person") \
#     .load()

#append mode
# df.write.format("org.neo4j.spark.DataSource") \
#     .mode("Append")\
#     .option("authentication.basic.username", "neo4j") \
#     .option("authentication.basic.password", "password") \
#     .option("url", "bolt://localhost:7687") \
#     .option("labels", ":Person") \
#     .save()

#write mode with node key
df.write.format("org.neo4j.spark.DataSource") \
    .mode("OverWrite") \
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "password") \
    .option("url", "bolt://localhost:7687") \
    .option("labels", ":Person") \
    .option("node.keys", "Name") \
    .save()

data2 = [(('Ram'),  3000),
        (('Mike'), 4000),
        (('Rohini'), 5000),
        (('Maria'), 6000),
        (('Jenis'), 1200)]

# Column names in dataframe
columns2 = ["Name", "Salary"]
# Create the spark dataframe
df2 = spark.createDataFrame(data=data2,
                           schema=columns2)
df2.write.format("org.neo4j.spark.DataSource") \
    .mode("OverWrite") \
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "password") \
    .option("url", "bolt://localhost:7687") \
    .option("labels", ":Salary") \
    .option("node.keys", "Name") \
    .save()

#write relationship
df.write.format("org.neo4j.spark.DataSource") \
    .mode("Append") \
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "password") \
    .option("url", "bolt://localhost:7687") \
    .option("relationship", "EARNED_SALARY") \
    .option("relationship.save.strategy", "keys") \
    .option("relationship.source.labels", ":Person") \
    .option("relationship.source.save.mode", "overwrite") \
    .option("relationship.source.node.keys", "Name")\
    .option("relationship.target.labels", ":Salary") \
    .option("relationship.target.node.keys", "Name") \
    .option("relationship.target.save.mode", "overwrite") \
    .save()

