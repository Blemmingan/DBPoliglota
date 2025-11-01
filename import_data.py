import pandas as pd
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# === MongoDB ===
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_client.drop_database("poliglota_db")  # Wipe clean
mongo_db = mongo_client["poliglota_db"]

# === Cassandra ===
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()
# Drop keyspace if exists
session.execute("DROP KEYSPACE IF EXISTS poliglota_db")
session.execute("""
    CREATE KEYSPACE poliglota_db 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace("poliglota_db")

# === Load CSVs ===
agentes_df = pd.read_csv("data/agentes.csv")
clientes_df = pd.read_csv("data/clientes.csv")
polizas_df = pd.read_csv("data/polizas.csv")
siniestros_df = pd.read_csv("data/siniestros.csv")
vehiculos_df = pd.read_csv("data/vehiculos.csv")

# === MONGO MODE ===
clientes_docs = []
for _, cliente in clientes_df.iterrows():
    cliente_id = cliente["id_cliente"]

    
    cliente_polizas = polizas_df[polizas_df["id_cliente"] == cliente_id].to_dict(orient="records")
    
    for poliza in cliente_polizas:
        nro_poliza = poliza["nro_poliza"]
        poliza_siniestros = siniestros_df[siniestros_df["nro_poliza"] == nro_poliza].to_dict(orient="records")
        poliza["siniestros"] = poliza_siniestros

    
    cliente_vehiculos = vehiculos_df[vehiculos_df["id_cliente"] == cliente_id].to_dict(orient="records")

    cliente_doc = cliente.to_dict()
    cliente_doc["polizas"] = cliente_polizas
    cliente_doc["vehiculos"] = cliente_vehiculos
    clientes_docs.append(cliente_doc)

mongo_db["clientes"].insert_many(clientes_docs)
mongo_db["agentes"].insert_many(agentes_df.to_dict(orient="records"))
print(f"✅ Imported {len(clientes_docs)} clientes with embedded polizas+siniestros+vehiculos and {len(agentes_df)} agentes into MongoDB")

# === CASSANDRA MODEL ===
# Agents table
session.execute("""
    CREATE TABLE agentes (
        id_agente text PRIMARY KEY,
        nombre text,
        apellido text,
        matricula text,
        telefono text,
        email text,
        zona text,
        activo text
    )
""")

# Clients table with embedded
session.execute("""
    CREATE TABLE clientes (
        id_cliente text PRIMARY KEY,
        nombre text,
        apellido text,
        dni text,
        email text,
        telefono text,
        direccion text,
        ciudad text,
        provincia text,
        activo text,
        polizas list<frozen<map<text,text>>>,
        vehiculos list<frozen<map<text,text>>>
    )
""")

# Helper 
def embed_polizas_cassandra(cliente_id):
    cliente_polizas = polizas_df[polizas_df["id_cliente"] == cliente_id].to_dict(orient="records")
    embedded_polizas = []
    for poliza in cliente_polizas:
        nro_poliza = poliza["nro_poliza"]
        poliza_siniestros = siniestros_df[siniestros_df["nro_poliza"] == nro_poliza].to_dict(orient="records")
        poliza_copy = poliza.copy()
        for i, s in enumerate(poliza_siniestros):
            for k, v in s.items():
                poliza_copy[f"siniestro_{i}_{k}"] = str(v)
        embedded_polizas.append({k: str(v) for k, v in poliza_copy.items()})
    return embedded_polizas

# Helper 
def embed_vehiculos_cassandra(cliente_id):
    cliente_vehiculos = vehiculos_df[vehiculos_df["id_cliente"] == cliente_id].to_dict(orient="records")
    embedded_vehiculos = []
    for veh in cliente_vehiculos:
        embedded_vehiculos.append({k: str(v) for k, v in veh.items()})
    return embedded_vehiculos

# Insert agents
def insert_df(df, table):
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    prepared = SimpleStatement(query)
    for _, row in df.iterrows():
        session.execute(prepared, tuple(str(x) for x in row))
    print(f"✅ Imported {len(df)} records into Cassandra table '{table}'")

insert_df(agentes_df, "agentes")

# Insert clientes 
cols = "id_cliente, nombre, apellido, dni, email, telefono, direccion, ciudad, provincia, activo, polizas, vehiculos"
query = f"INSERT INTO clientes ({cols}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
prepared = SimpleStatement(query)

for _, cliente in clientes_df.iterrows():
    polizas_embedded = embed_polizas_cassandra(cliente["id_cliente"])
    vehiculos_embedded = embed_vehiculos_cassandra(cliente["id_cliente"])
    values = (
        str(cliente["id_cliente"]),
        str(cliente["nombre"]),
        str(cliente["apellido"]),
        str(cliente["dni"]),
        str(cliente["email"]),
        str(cliente["telefono"]),
        str(cliente["direccion"]),
        str(cliente["ciudad"]),
        str(cliente["provincia"]),
        str(cliente["activo"]),
        polizas_embedded,
        vehiculos_embedded
    )
    session.execute(prepared, values)

print(f"✅ Imported {len(clientes_df)} clientes with embedded polices+vehicles+accidents into Cassandra")
print("🎉 All data imported successfully into MongoDB and Cassandra!")
