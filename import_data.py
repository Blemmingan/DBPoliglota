import pandas as pd
from pymongo import MongoClient
from cassandra.cluster import Cluster

# === MongoDB ===
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["poliglota_db"]

# === Cassandra ===
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()
session.execute("CREATE KEYSPACE IF NOT EXISTS poliglota_db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
session.set_keyspace("poliglota_db")

def import_to_mongo(filename, collection_name):
    df = pd.read_csv(filename)
    records = df.to_dict(orient="records")
    mongo_db[collection_name].insert_many(records)
    print(f"✅ Imported {len(records)} records into Mongo collection '{collection_name}'")

def import_to_cassandra(filename, table_name):
    df = pd.read_csv(filename)
    cols = df.columns
    col_defs = ", ".join([f"{col} text" for col in cols])
    pk = cols[0]  # first column as primary key (adjust if needed)

    session.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs}, PRIMARY KEY ({pk}))")

    for _, row in df.iterrows():
        values = tuple(str(x) for x in row)
        placeholders = ", ".join(["%s"] * len(cols))
        session.execute(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})", values)

    print(f"✅ Imported {len(df)} records into Cassandra table '{table_name}'")

# === MAIN ===
datasets = ["agentes", "clientes", "polizas", "siniestros", "vehiculos"]

for name in datasets:
    path = f"data/{name}.csv"
    import_to_mongo(path, name)
    import_to_cassandra(path, name)

print("🎉 All data imported successfully!")
