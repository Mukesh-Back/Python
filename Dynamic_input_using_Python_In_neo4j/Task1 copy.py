from neo4j import GraphDatabase
import pandas as pd
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "12345678")
driver = GraphDatabase.driver(URI, auth=AUTH)

df=pd.read_csv("c:\\Users\\visualapp\\Downloads\\customers-1000.csv")
df = df.drop("Index", axis=1)
df.columns = df.columns.str.replace(' ', '_', regex=True)
keys=df.keys()
keys={k+"1":k for k in keys}

def insert_data(tx, customer):
    pre = ""
    val = ''
    for key, value in customer.items():
        query = f'CREATE (n:{key} {{{key}: "{value}"}})'
        tx.run(query)
        if pre != "":
            query1 = f'MATCH (a:{pre} {{{pre}: "{val}"}}) MATCH (b:{key} {{{key}: "{value}"}}) CREATE (a)-[:{key}]->(b)'
            tx.run(query1)
        pre = key
        val = value



with driver.session() as session:
    for index, row in df.iterrows():
        #print(row)
        customer = row.to_dict()
        session.execute_write(insert_data, customer)


