from pyspark.sql import SparkSession
from neo4j import GraphDatabase
from pyspark.sql import Row

URI = "neo4j://localhost:11003"
AUTH = ("neo4j", "Ne04j!")

def merge_nodes_batchmode(tx, dict_list):
    query = (
        """CALL apoc.periodic.iterate(
        'WITH $batch AS batch UNWIND batch AS bb RETURN bb',
        '
        MERGE (p:Person {name:bb.Name,birth:bb.BirthDate,gender:bb.Gender,salary:bb.Salary})
        ',
        {batchSize: 1000, parallel:true, concurrency:16, retries:5, params: {batch: $batch}}
        )
        """
    )
    result = tx.run(query, batch=dict_list)

def merge_nodes_one_batch(tx, dict_list):
    query = (
        """
        WITH $batch AS batch UNWIND batch AS bb 
        MERGE (p:Person {name:bb.Name,birth:bb.BirthDate,gender:bb.Gender,salary:bb.Salary})
        """
    )
    result = tx.run(query, batch=dict_list)

def create_dict(df):
    data_collect = df.collect()
    my_dict=[]
    i=0
    # looping thorough each row of the dataframe
    for row in data_collect:
        row2 = Row(Name=row["Name"], BirthDate=row["BirthDate"],Gender=row["Gender"],Salary=row["Salary"])
        row3=row2.asDict()
        my_dict.append(row3)
        i +=1
    print(my_dict)
    return my_dict

# Create a spark session
spark = SparkSession.builder.appName('DF_to_dict').getOrCreate()

# Create data in dataframe
data = [(('Ram'), '1991-04-01', 'M', 3000),
        (('Mike'), '2000-05-19', 'M', 4000),
        (('Rohini'), '1978-09-05', 'M', 4000),
        (('Maria'), '1967-12-01', 'F', 4000),
        (('Jenis'), '1980-02-17', 'F', 1200)]

columns = ["Name", "BirthDate", "Gender", "Salary"]
df = spark.createDataFrame(data=data,
                           schema=columns)
batch_dict=create_dict(df)

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    with driver.session() as session:
        session = driver.session(database='neo4j')
        session.write_transaction(merge_nodes_batchmode, batch_dict)


