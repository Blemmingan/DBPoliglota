#!/usr/bin/env python3
import os
import pandas as pd
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
from pymongo import MongoClient

RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

# --- Cassandra setup ---
cluster = Cluster(["127.0.0.1"])
session = cluster.connect("poliglota_db")

# --- MongoDB setup ---
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["poliglota_db"]

# --- Helper functions ---
def safe_polizas(r):
    """Return list of dict polizas, skip non-dict items."""
    polizas_list = getattr(r, "polizas", []) or []
    return [p for p in polizas_list if isinstance(p, dict)]



# ---------------------------
# Query 8: Siniestros tipo “Accidente” del último año
# ---------------------------
one_year_ago = datetime.today() - timedelta(days=365)
acc_siniestros = []

for r in session.execute("SELECT id_cliente, polizas FROM clientes"):
    for pol in safe_polizas(r):
        for pol_data in pol.values():
            if not isinstance(pol_data, dict):
                continue
            for k, v in pol_data.items():
                if k.startswith("siniestro") and isinstance(v, dict):
                    if v.get("tipo") == "Accidente":
                        fecha_str = v.get("fecha")
                        if fecha_str:
                            try:
                                fecha_dt = datetime.fromisoformat(fecha_str)
                                if fecha_dt >= one_year_ago:
                                    acc_siniestros.append({"id_cliente": r.id_cliente, **v})
                            except ValueError:
                                continue

pd.DataFrame(acc_siniestros).to_csv(
    os.path.join(RESULTS_DIR, "query08_siniestros_accidente_ultimo_anio.csv"),
    index=False
)

# ---------------------------
# Query 9: Vista de pólizas activas ordenadas por fecha de inicio
# ---------------------------
active_polizas = []
for r in session.execute("SELECT id_cliente, polizas FROM clientes"):
    for pol in safe_polizas(r):
        for pol_data in pol.values():
            if not isinstance(pol_data, dict):
                continue
            if str(pol_data.get("vigente", "false")).lower() == "true":
                active_polizas.append({"id_cliente": r.id_cliente, **pol_data})

df = pd.DataFrame(active_polizas)

# Sort only if 'fecha_inicio' exists
if not df.empty and "fecha_inicio" in df.columns:
    df = df.sort_values("fecha_inicio")

df.to_csv(os.path.join(RESULTS_DIR, "query09_polizas_activas_ordenadas.csv"), index=False)

# ---------------------------
# Query 10: Pólizas suspendidas con estado del cliente
# ---------------------------
suspended_polizas = []
for r in session.execute("SELECT id_cliente, activo, polizas FROM clientes"):
    for pol in safe_polizas(r):
        for pol_data in pol.values():
            if not isinstance(pol_data, dict):
                continue
            if pol_data.get("estado") == "suspendida":
                suspended_polizas.append({
                    "id_cliente": r.id_cliente,
                    "estado_cliente": r.activo,
                    **pol_data
                })

pd.DataFrame(suspended_polizas).to_csv(
    os.path.join(RESULTS_DIR, "query10_polizas_suspendidas.csv"),
    index=False
)

# ---------------------------
# Query 11: Clientes con más de un vehículo asegurado
# ---------------------------
multi_vehiculos = []
for r in session.execute("SELECT id_cliente, vehiculos, nombre, apellido FROM clientes"):
    vehs = getattr(r, "vehiculos", []) or []
    if len(vehs) > 1:
        multi_vehiculos.append({"nombre": f"{r.nombre} {r.apellido}"})

pd.DataFrame(multi_vehiculos).to_csv(
    os.path.join(RESULTS_DIR, "query11_clientes_multi_vehiculos.csv"),
    index=False
)

# ---------------------------
# Query 12: Agentes y cantidad de siniestros asociados
# ---------------------------
agente_siniestros = []
for agente in session.execute("SELECT id_agente, nombre, apellido FROM agentes"):
    total = 0
    for cliente in session.execute("SELECT polizas FROM clientes"):
        for pol in safe_polizas(cliente):
            for pol_data in pol.values():
                if not isinstance(pol_data, dict):
                    continue
                for k, v in pol_data.items():
                    if k.startswith("siniestro") and isinstance(v, dict):
                        total += 1
    agente_siniestros.append({
        "nombre": f"{agente.nombre} {agente.apellido}",
        "cantidad_siniestros": total
    })

pd.DataFrame(agente_siniestros).to_csv(
    os.path.join(RESULTS_DIR, "query12_agentes_siniestros.csv"),
    index=False
)

# ---------------------------
# Queries 13–15: Inserts with Mongo sync
# ---------------------------
def insert_cliente(cliente):
    """Insert new client into Cassandra and Mongo."""
    # Cassandra
    session.execute("""
        INSERT INTO clientes (id_cliente, nombre, apellido, dni, email, telefono, direccion, ciudad, provincia, activo, polizas, vehiculos)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        cliente["id_cliente"], cliente["nombre"], cliente["apellido"], cliente["dni"],
        cliente["email"], cliente["telefono"], cliente["direccion"], cliente["ciudad"],
        cliente["provincia"], cliente["activo"], [], []
    ))
    # Mongo
    cliente_doc = cliente.copy()
    cliente_doc["polizas"] = []
    cliente_doc["vehiculos"] = []
    mongo_db["clientes"].insert_one(cliente_doc)

def insert_siniestro(cliente_id, poliza_idx, siniestro_data):
    """Insert new siniestro into poliza in Cassandra and Mongo."""
    # Cassandra
    cliente = session.execute("SELECT polizas FROM clientes WHERE id_cliente=%s", (cliente_id,)).one()
    polizas = getattr(cliente, "polizas", [])
    while len(polizas) <= poliza_idx:
        polizas.append({})
    polizas[poliza_idx][f"siniestro_{len(polizas[poliza_idx])}"] = siniestro_data
    session.execute("UPDATE clientes SET polizas=%s WHERE id_cliente=%s", (polizas, cliente_id))

    # Mongo
    cliente_doc = mongo_db["clientes"].find_one({"id_cliente": cliente_id})
    cliente_polizas = cliente_doc.get("polizas", [])
    while len(cliente_polizas) <= poliza_idx:
        cliente_polizas.append({"siniestros": []})
    cliente_polizas[poliza_idx].setdefault("siniestros", []).append(siniestro_data)
    mongo_db["clientes"].update_one({"id_cliente": cliente_id}, {"$set": {"polizas": cliente_polizas}})

def insert_poliza(cliente_id, poliza_data):
    """Insert new poliza into Cassandra and Mongo."""
    # Cassandra
    cliente = session.execute("SELECT polizas FROM clientes WHERE id_cliente=%s", (cliente_id,)).one()
    polizas = getattr(cliente, "polizas", [])
    polizas.append({f"poliza_{len(polizas)}": poliza_data})
    session.execute("UPDATE clientes SET polizas=%s WHERE id_cliente=%s", (polizas, cliente_id))

    # Mongo
    mongo_db["clientes"].update_one({"id_cliente": cliente_id}, {"$push": {"polizas": poliza_data}})

print("✅ Cassandra queries executed, inserts synced with MongoDB, and CSVs generated in 'results/'")
