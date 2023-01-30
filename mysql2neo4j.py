## install mysql connector using pip
## pip3 install mysql-connector-python

from neo4j import GraphDatabase
import mysql.connector
import neo4j

URI = "neo4j://localhost:11003"
AUTH = ("neo4j", "Ne04j!")
BATCH_SIZE = 10000

def create_customer_node(tx, company, firstname, lastname, title):
    query = ("CREATE (c:Customer {company: $company,firstname: $firstname,lastname: $lastname,title: $title})")
    tx.run(query, company=company, firstname=firstname, lastname=lastname, title=title)

def transfer_to_neo4j(driver, user_data):
    with driver.session(
            database="mysql",
            # optional, defaults to WRITE_ACCESS
            default_access_mode=neo4j.WRITE_ACCESS
    ) as session:
        count = 1
        tx = session.begin_transaction()
        try:
            for row in user_data:
                create_customer_node(tx, row[1], row[3], row[2], row[5])
                if (count % BATCH_SIZE == 0) or (count == len(user_data)):
                    tx.commit()
                    tx = session.begin_transaction()
                count = count + 1
        finally:
            tx.commit()
            tx.close()

# main block to execute fetch data from RDBMS like MySQL
mysqlDBconn = mysql.connector.connect(
    host="localhost", user="root", passwd="Ne04j!", database="northwind")
# preparing a cursor object
cursor = mysqlDBconn.cursor()
sql = "SELECT * FROM customers"
cursor.execute(sql)
user_data = cursor.fetchall()

for row in user_data:
    print(row[1], row[3], row[2], row[5])
# disconnecting from server

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    with driver.session() as session:
        transfer_to_neo4j(driver, user_data)
        print("Data transfer completed")

# close MySQL connection
mysqlDBconn.close()
