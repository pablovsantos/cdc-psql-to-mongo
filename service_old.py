import cx_Oracle
import pymongo
import time

# Aguardar alguns segundos antes de tentar se conectar
time.sleep(30)

# Configuração do Oracle
oracle_dsn = cx_Oracle.makedsn("oracle-db", 1521, service_name="FREEPDB1")
oracle_connection = cx_Oracle.connect(user='system', password='Oradoc_db1', dsn=oracle_dsn)

# Configuração do MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongo-db:27017/")
mongo_db = mongo_client["test_db"]
mongo_collection = mongo_db["test_collection"]

def check_and_insert_data():
    cursor = oracle_connection.cursor()
    cursor.execute("SELECT campo1, campo2 FROM system.teste_cdc")  # Especificando o esquema SYSTEM
    rows = cursor.fetchall()
    
    for row in rows:
        data = {"campo1": row[0], "campo2": row[1]}
        mongo_collection.update_one({"campo1": row[0]}, {"$set": data}, upsert=True)

    cursor.close()

while True:
    check_and_insert_data()
    time.sleep(10)
