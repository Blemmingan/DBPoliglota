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
    for pol in getattr(r, "polizas", []) or []:
        pol = dict(pol)
    for key, value in pol.items():
        if "_tipo" in key and key.startswith("siniestro_"):
            idx = key.split("_")[1]
            tipo = pol.get(f"siniestro_{idx}_tipo")
            if tipo and tipo.strip().lower() == "accidente":
                fecha_str = pol.get(f"siniestro_{idx}_fecha", "").strip()
                try:
                    fecha_dt = datetime.strptime(fecha_str, "%d/%m/%Y")
                except ValueError:
                    continue
                if fecha_dt >= one_year_ago:
                    acc_siniestros.append({
                        "id_cliente": r.id_cliente,
                        "nro_poliza": pol.get("nro_poliza"),
                        "id_siniestro": pol.get(f"siniestro_{idx}_id_siniestro"),
                        "tipo": tipo,
                        "fecha": fecha_str,
                        "monto_estimado": pol.get(f"siniestro_{idx}_monto_estimado"),
                        "descripcion": pol.get(f"siniestro_{idx}_descripcion"),
                        "estado": pol.get(f"siniestro_{idx}_estado")
                    })


df = pd.DataFrame(acc_siniestros)
df.to_csv(os.path.join(RESULTS_DIR, "query08_siniestros_accidente_ultimo_anio.csv"), index=False)

# ---------------------------
# Query 9: Vista de pólizas activas ordenadas por fecha de inicio
# ---------------------------

rows = []

for r in session.execute("SELECT id_cliente, nombre, apellido, polizas FROM clientes"):
    for pol in getattr(r, "polizas", []) or []:
        pol = dict(pol)  

        if str(pol.get("estado", "")).strip().lower() != "activa":
            continue

     
        row = {
            "id_cliente": r.id_cliente,
            "nombre": r.nombre,
            "apellido": r.apellido,
        }
        # Include fields we care about
        for k in ["nro_poliza", "fecha_inicio", "fecha_fin", "tipo", "cobertura_total", "prima_mensual", "estado"]:
            if k in pol:
                row[k] = pol[k]

        rows.append(row)

df = pd.DataFrame(rows)

if "fecha_inicio" in df.columns:
    df["fecha_inicio_dt"] = pd.to_datetime(df["fecha_inicio"], format="%d/%m/%Y", errors="coerce")
    df = df.sort_values("fecha_inicio_dt").drop(columns=["fecha_inicio_dt"])

df.to_csv(os.path.join(RESULTS_DIR, "query09_polizas_activas_ordenadas.csv"), index=False)


# ---------------------------
# Query 10: Pólizas suspendidas con estado del cliente
# ---------------------------


def safe_int(value, default=0):
    """Convert to int safely, treating None or NaN as default."""
    try:
        if value is None:
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default

suspended_polizas = []

for r in session.execute("SELECT id_cliente, activo, polizas FROM clientes"):
    for pol in getattr(r, "polizas", []) or []:
        pol = dict(pol)
        if str(pol.get("estado", "")).strip().lower() == "suspendida":
            row = {
                "id_cliente": r.id_cliente,
                "estado_cliente": r.activo,
                "nro_poliza": pol.get("nro_poliza"),
                "fecha_inicio": pol.get("fecha_inicio"),
                "fecha_fin": pol.get("fecha_fin"),
                "tipo": pol.get("tipo"),
                "cobertura_total": safe_int(pol.get("cobertura_total")),
                "prima_mensual": safe_int(pol.get("prima_mensual")),
                "estado": pol.get("estado"),
                "id_agente": safe_int(pol.get("id_agente"))
            }
            suspended_polizas.append(row)

df = pd.DataFrame(suspended_polizas)

df["fecha_inicio_dt"] = pd.to_datetime(df["fecha_inicio"], format="%d/%m/%Y", errors="coerce")
df = df.sort_values("fecha_inicio_dt").drop(columns=["fecha_inicio_dt"])


df.to_csv(os.path.join(RESULTS_DIR, "query10_polizas_suspendidas.csv"), index=False)


# ---------------------------
# Query 11: Clientes con más de un vehículo asegurado
# ---------------------------
multi_vehiculos = []
for r in session.execute("SELECT id_cliente, vehiculos, nombre, apellido FROM clientes"):
    vehs = getattr(r, "vehiculos", []) or []
    vehs = [dict(v) for v in vehs if isinstance(v, dict)] 
    if len(vehs) > 1:
        multi_vehiculos.append({"nombre": f"{r.nombre} {r.apellido}", "num_vehiculos": len(vehs)})

pd.DataFrame(multi_vehiculos).to_csv(
    os.path.join(RESULTS_DIR, "query11_clientes_multi_vehiculos.csv"),
    index=False
)

#####    This query will always return empty results with the current data model.


# ---------------------------
# Query 12: Cantidad de siniestros por agente
# ---------------------------

import math

def safe_int(value):
    """Normalize a value to int or return None (handles '101.0', 'nan', None, etc.)."""
    try:
        if value is None:
            return None
        s = str(value).strip()
        if s == "":
            return None
        f = float(s)
        if math.isnan(f):
            return None
        return int(f)
    except (ValueError, TypeError):
        return None

agentes = list(session.execute("SELECT id_agente, nombre, apellido FROM agentes"))
agentes_map = {}
for a in agentes:
    aid = safe_int(a.id_agente)
    name = f"{a.nombre} {a.apellido}"
    if aid is None:
        continue
    agentes_map[aid] = name



siniestros_por_agente = {}

for cliente in session.execute("SELECT polizas FROM clientes"):
    for pol in getattr(cliente, "polizas", []) or []:
        pol = dict(pol)
        agente_id = safe_int(pol.get("id_agente"))

        siniestro_keys = [k for k in pol.keys() if k.startswith("siniestro_") and k.endswith("_id_siniestro") and pol.get(k)]
        count = len(siniestro_keys)
        if agente_id is None:
            if "unassigned" not in siniestros_por_agente:
                siniestros_por_agente["unassigned"] = 0
            siniestros_por_agente["unassigned"] += count
            continue

        # Ensure key exists and increment
        siniestros_por_agente.setdefault(agente_id, 0)
        siniestros_por_agente[agente_id] += count

rows = []
seen_agent_ids = set()

for aid, name in agentes_map.items():
    cnt = siniestros_por_agente.get(aid, 0)
    rows.append({"nombre": name, "cantidad_siniestros": cnt})
    seen_agent_ids.add(aid)

# Add any agent IDs that were present in policies but not in agentes table
for aid, cnt in siniestros_por_agente.items():
    if aid == "unassigned":
        rows.append({"nombre": "Agente (no asignado)", "cantidad_siniestros": cnt})
        continue
    if aid not in seen_agent_ids:
        rows.append({"nombre": f"Agente {aid} (no registrado)", "cantidad_siniestros": cnt})

df = pd.DataFrame(rows)

df = df.sort_values("cantidad_siniestros", ascending=False).reset_index(drop=True)

df.to_csv(os.path.join(RESULTS_DIR, "query12_agentes_siniestros.csv"), index=False)

# ---------------------------
# Queries 13–15: Inserts with Mongo sync
# ---------------------------
import csv
import os

RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

def export_to_csv(filename, data, fieldnames):
    """Helper to export data to CSV."""
    filepath = os.path.join(RESULTS_DIR, filename)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"📁 Exported: {filepath}")

# ---------------------------
# INSERTS + Immediate SELECT + CSV Export
# ---------------------------

def insert_cliente(cliente):
    """Insert new client into Cassandra and Mongo, then verify and export."""
    # Cassandra insert
    session.execute("""
        INSERT INTO clientes (id_cliente, nombre, apellido, dni, email, telefono, direccion, ciudad, provincia, activo, polizas, vehiculos)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        cliente["id_cliente"], cliente["nombre"], cliente["apellido"], cliente["dni"],
        cliente["email"], cliente["telefono"], cliente["direccion"], cliente["ciudad"],
        cliente["provincia"], cliente["activo"], [], []
    ))

    # Mongo insert
    cliente_doc = cliente.copy()
    cliente_doc["polizas"] = []
    cliente_doc["vehiculos"] = []
    mongo_db["clientes"].insert_one(cliente_doc)

    # Verification
    cassandra_result = session.execute(
        "SELECT * FROM clientes WHERE id_cliente=%s",
        (cliente["id_cliente"],)
    ).one()
    mongo_result = mongo_db["clientes"].find_one({"id_cliente": cliente["id_cliente"]}, {"_id": 0})

    # Define exact fields for CSV
    csv_fields = ["id_cliente", "nombre", "apellido", "dni", "email", "telefono", "direccion", "ciudad", "provincia", "activo"]

    # Filter dicts to only include CSV fields
    cassandra_filtered = {k: v for k, v in dict(cassandra_result._asdict()).items() if k in csv_fields}
    mongo_filtered = {k: v for k, v in mongo_result.items() if k in csv_fields}

    export_to_csv(
        f"insert_cliente_{cliente['id_cliente']}.csv",
        [cassandra_filtered, mongo_filtered],
        fieldnames=csv_fields
    )

    print(f"✅ Cliente {cliente['id_cliente']} insertado y exportado.")


def insert_poliza(cliente_id, poliza_data):
    """Insert new poliza into Cassandra and Mongo, verify and export only selected fields."""
    # Validate client (activo is text)
    cliente = session.execute(
        "SELECT * FROM clientes WHERE id_cliente=%s AND activo=%s ALLOW FILTERING",
        (cliente_id, "True")
    ).one()
    if not cliente:
        print(f"❌ Cliente {cliente_id} no existe o está inactivo.")
        return

    # Validate agent (activo is text)
    agente_id = poliza_data.get("id_agente") or poliza_data.get("agente_id")
    agente = session.execute(
        "SELECT * FROM agentes WHERE id_agente=%s AND activo=%s ALLOW FILTERING",
        (agente_id, "True")
    ).one()
    if not agente:
        print(f"❌ Agente {agente_id} no existe o está inactivo.")
        return

    # Cassandra update
    polizas = getattr(cliente, "polizas") or []
    safe_poliza = {str(k): str(v) for k, v in poliza_data.items()}
    polizas.append(safe_poliza)  # store as map<text,text>
    session.execute(
        "UPDATE clientes SET polizas=%s WHERE id_cliente=%s",
        (polizas, cliente_id)
    )

    # Mongo update
    mongo_db["clientes"].update_one(
        {"id_cliente": cliente_id},
        {"$push": {"polizas": poliza_data}}
    )

    # Verification
    mongo_result = mongo_db["clientes"].find_one(
        {"id_cliente": cliente_id},
        {"_id": 0}
    )

    # Prepare CSV row with only the desired fields
    poliza_idx = len(mongo_result["polizas"]) - 1
    poliza = mongo_result["polizas"][poliza_idx]
    csv_row = {
        "nro_poliza": poliza.get("numero"),
        "id_cliente": cliente_id,
        "tipo": poliza.get("tipo"),
        "fecha_inicio": poliza.get("fecha_emision"),
        "fecha_fin": poliza.get("fecha_fin"),
        "prima_mensual": poliza.get("prima_mensual"),
        "cobertura_total": poliza.get("cobertura"),
        "id_agente": poliza.get("id_agente"),
        "estado": poliza.get("estado")
    }

    # Export CSV with exactly these fields
    export_to_csv(
        f"insert_poliza_cliente_{cliente_id}.csv",
        [csv_row],
        fieldnames=["nro_poliza","id_cliente","tipo","fecha_inicio","fecha_fin",
                    "prima_mensual","cobertura_total","id_agente","estado"]
    )

    print(f"✅ Póliza agregada y exportada para cliente {cliente_id} (agente {agente_id}).")



def insert_siniestro(cliente_id, poliza_idx, siniestro_data):
    """Insert siniestro into Cassandra and Mongo, verify and export only selected fields."""
    # --- Cassandra: fetch polizas ---
    cliente = session.execute(
        "SELECT polizas FROM clientes WHERE id_cliente=%s",
        (cliente_id,)
    ).one()
    polizas_raw = getattr(cliente, "polizas", [])
    
    # Convert to list of dicts (flattened maps)
    polizas = []
    for p in polizas_raw or []:
        if isinstance(p, dict):
            polizas.append(dict(p))
        else:
            polizas.append({})

    # Ensure poliza index exists
    while len(polizas) <= poliza_idx:
        polizas.append({})

    # Determine next siniestro number
    existing_siniestros = [k for k in polizas[poliza_idx].keys() if k.startswith("siniestro_")]
    siniestro_id = len(existing_siniestros) // 3  # assuming each siniestro uses 3 keys

    # Flatten siniestro into map<text,text>
    for key, value in siniestro_data.items():
        polizas[poliza_idx][f"siniestro_{siniestro_id}_{key}"] = str(value)
    polizas[poliza_idx][f"siniestro_{siniestro_id}_id"] = str(siniestro_id)

    # Update Cassandra
    session.execute(
        "UPDATE clientes SET polizas=%s WHERE id_cliente=%s",
        (polizas, cliente_id)
    )

    # --- Mongo update (keep full structure) ---
    cliente_doc = mongo_db["clientes"].find_one({"id_cliente": cliente_id})
    cliente_polizas = cliente_doc.get("polizas", [])
    while len(cliente_polizas) <= poliza_idx:
        cliente_polizas.append({"siniestros": []})
    siniestro_data["id_siniestro"] = siniestro_id
    cliente_polizas[poliza_idx].setdefault("siniestros", []).append(siniestro_data)
    mongo_db["clientes"].update_one(
        {"id_cliente": cliente_id},
        {"$set": {"polizas": cliente_polizas}}
    )

    # --- CSV export ---
    poliza = cliente_polizas[poliza_idx]
    siniestro = poliza["siniestros"][-1]
    csv_row = {
        "id_siniestro": siniestro_id,
        "nro_poliza": poliza.get("numero"),
        "fecha": siniestro.get("fecha"),
        "tipo": poliza.get("tipo"),
        "monto_estimado": siniestro.get("monto"),
        "descripcion": siniestro.get("descripcion"),
        "estado": siniestro.get("estado", "")
    }

    export_to_csv(
        f"insert_siniestro_cliente_{cliente_id}.csv",
        [csv_row],
        fieldnames=["id_siniestro", "nro_poliza", "fecha", "tipo",
                    "monto_estimado", "descripcion", "estado"]
    )

    print(f"✅ Siniestro agregado y exportado para cliente {cliente_id}, poliza {poliza_idx}.")




##################################
# Example Inserts
##################################
    
insert_cliente({
    "id_cliente": "2001",
    "nombre": "Laura",
    "apellido": "Gómez",
    "dni": "30222333",
    "email": "laura@correo.com",
    "telefono": "1199887766",
    "direccion": "Av. Corrientes 1234",
    "ciudad": "Buenos Aires",
    "provincia": "Buenos Aires",
    "activo": "True"
})

insert_poliza("2001", {
    "numero": "POL-9999",
    "fecha_emision": "2025-10-31",
    "id_agente": "101",
    "tipo": "Hogar",
    "cobertura": "Completa"
})

insert_siniestro("2001", 0, {
    "fecha": "2025-11-01",
    "descripcion": "Daño por agua",
    "monto": 25000
})


print("✅ Cassandra queries executed, inserts synced with MongoDB, and CSVs generated in 'results/'")
