from pyspark.sql import SparkSession

URI = "neo4j://localhost:11003"

spark =(SparkSession.builder.master('local[*]')
        .appName('Leverage Neo4j')
#        .config('spark.ui.port', '4050')
        .config('spark.sql.repl.eagerEval.enabled', True)
        .config('spark.jars', '/Users/tonywu/Documents/dev/spring/testPyspark1/neo4j-connector-apache-spark_2.12-4.0.2_for_spark_3.jar')
        .config("neo4j.url", URI).config("neo4j.authentication.type", "basic")
        .config("neo4j.authentication.basic.username", 'neo4j')
        .config("neo4j.authentication.basic.password", 'Neo4j!').getOrCreate())

# Create data in dataframe
# data = [(('Ram'), '1991-04-01', 'M', 3000),
#         (('Mike'), '2000-05-19', 'M', 4000),
#         (('Rohini'), '1978-09-05', 'M', 4000),
#         (('Maria'), '1967-12-01', 'F', 4000),
#         (('Jenis'), '1980-02-17', 'F', 1200)]
#columns = ["Name", "BirthDate", "Gender", "Salary"]
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
#     .option("authentication.basic.password", "Ne04j!") \
#     .option("url", "bolt://localhost:11003") \
#     .option("labels", ":Person") \
#     .load()

#append mode
# df.write.format("org.neo4j.spark.DataSource") \
#     .mode("Append")\
#     .option("authentication.basic.username", "neo4j") \
#     .option("authentication.basic.password", "Ne04j!") \
#     .option("url", "bolt://localhost:11003") \
#     .option("labels", ":Person") \
#     .save()

#write mode with node key
df.write.format("org.neo4j.spark.DataSource") \
    .mode("OverWrite") \
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "Ne04j!") \
    .option("url", "bolt://localhost:11003") \
    .option("labels", ":Person") \
    .option("node.keys", "Name") \
    .save()

data2 = [(('Ram'),  3000),
        (('Mike'),  4000),
        (('Rohini'),  4000),
        (('Maria'), 4000),
        (('Jenis'), 1200)]

# Column names in dataframe
columns2 = ["Name", "Salary"]
# Create the spark dataframe
df2 = spark.createDataFrame(data=data2,
                           schema=columns2)
df2.write.format("org.neo4j.spark.DataSource") \
    .mode("OverWrite") \
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "Ne04j!") \
    .option("url", "bolt://localhost:11003") \
    .option("labels", ":Salary") \
    .option("node.keys", "Name") \
    .save()

#write relationship
df.write.format("org.neo4j.spark.DataSource") \
    .mode("Append") \
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "Ne04j!") \
    .option("url", "bolt://localhost:11003") \
    .option("relationship", "EARNED_SALARY") \
    .option("relationship.save.strategy", "keys") \
    .option("relationship.source.labels", ":Person") \
    .option("relationship.source.save.mode", "overwrite") \
    .option("relationship.source.node.keys", "Name")\
    .option("relationship.target.labels", ":Salary") \
    .option("relationship.target.node.keys", "Name") \
    .option("relationship.target.save.mode", "overwrite") \
    .save()

