#!/usr/bin/env python3
import os
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta

RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["poliglota_db"]
clientes = db["clientes"]
agentes = db["agentes"]

today = datetime.today()

# --- Query 1: Clientes activos con pólizas vigentes ---
pipeline = [
    {"$match": {"activo": True}},  # boolean True
    {"$project": {
        "nombre": 1,
        "apellido": 1,
        "polizas": {
            "$filter": {
                "input": "$polizas",
                "as": "p",
                "cond": {"$eq": ["$$p.estado", "Activa"]}  # estado = "Activa"
            }
        }
    }}
]

q1 = list(clientes.aggregate(pipeline))
pd.DataFrame(q1).to_csv(f"{RESULTS_DIR}/query01_clientes_activos_polizas_vigentes.csv", index=False)

# --- Query 2: Siniestros abiertos ---
pipeline = [
    {"$unwind": "$polizas"},
    {"$unwind": "$polizas.siniestros"},
    {"$match": {"polizas.siniestros.estado": "Abierto"}},
    {"$project": {
        "cliente": {"$concat": ["$nombre", " ", "$apellido"]},
        "tipo": "$polizas.siniestros.tipo",
        "monto_estimado": "$polizas.siniestros.monto_estimado"
    }}
]
q2 = list(clientes.aggregate(pipeline))
pd.DataFrame(q2).to_csv(f"{RESULTS_DIR}/query02_siniestros_abiertos.csv", index=False)

# --- Query 3: Vehículos asegurados con su cliente y póliza ---
pipeline = [
    {"$unwind": "$vehiculos"},
    {"$unwind": "$polizas"},
    {"$project": {
        "cliente": {"$concat": ["$nombre", " ", "$apellido"]},
        "vehiculo": "$vehiculos",
        "poliza": "$polizas"
    }}
]
q3 = list(clientes.aggregate(pipeline))
pd.DataFrame(q3).to_csv(f"{RESULTS_DIR}/query03_vehiculos_asegurados.csv", index=False)

# --- Query 4: Clientes sin pólizas activas ---
pipeline = [
    {"$project": {
        "nombre": 1,
        "apellido": 1,
        "polizas_activas": {"$filter": {"input": "$polizas", "as": "p", "cond": {"$eq": ["$$p.vigente", True]}}}
    }},
    {"$match": {"polizas_activas": {"$size": 0}}}
]
q4 = list(clientes.aggregate(pipeline))
pd.DataFrame(q4).to_csv(f"{RESULTS_DIR}/query04_clientes_sin_polizas_activas.csv", index=False)

# ---------------------------
# Query 5: Agentes activos con cantidad de pólizas (Mongo)
# ---------------------------

pipeline = [
    # Only active agents
    {"$match": {"activo": True}},
    
    # Lookup clientes to count only active polizas assigned to this agent
    {"$lookup": {
        "from": "clientes",
        "let": {"agente_id": "$id_agente"},
        "pipeline": [
            {"$project": {
                "polizas": {
                    "$filter": {
                        "input": "$polizas",
                        "as": "p",
                        "cond": {
                            "$and": [
                                {"$eq": ["$$p.id_agente", "$$agente_id"]},
                                {"$eq": ["$$p.estado", "Activa"]}
                            ]
                        }
                    }
                }
            }},
            {"$project": {"num_polizas": {"$size": "$polizas"}}}
        ],
        "as": "clientes_polizas"
    }},
    
    # Sum polizas per agent
    {"$addFields": {
        "cantidad_polizas": {"$sum": "$clientes_polizas.num_polizas"}
    }},
    
    # Project final fields
    {"$project": {
        "_id": 0,
        "nombre": {"$concat": ["$nombre", " ", "$apellido"]},
        "cantidad_polizas": 1
    }}
]

q5 = list(agentes.aggregate(pipeline))
pd.DataFrame(q5).to_csv(
    os.path.join(RESULTS_DIR, "query05_agentes_activos_polizas.csv"),
    index=False
)

# --- Query 6: Pólizas vencidas con nombre del cliente ---
pipeline = [
    {"$project": {
        "nombre": 1,
        "apellido": 1,
        "polizas_vencidas": {"$filter": {"input": "$polizas", "as": "p", "cond": {"$lt": ["$$p.fecha_fin", today]}}}
    }},
    {"$match": {"polizas_vencidas": {"$ne": []}}}
]
q6 = list(clientes.aggregate(pipeline))
pd.DataFrame(q6).to_csv(f"{RESULTS_DIR}/query06_polizas_vencidas.csv", index=False)

# --- Query 7: Top 10 clientes por cobertura total ---
pipeline = [
    {"$project": {
        "nombre": 1,
        "apellido": 1,
        "total_cobertura": {"$sum": "$polizas.cobertura"}
    }},
    {"$sort": {"total_cobertura": -1}},
    {"$limit": 10}
]
q7 = list(clientes.aggregate(pipeline))
pd.DataFrame(q7).to_csv(f"{RESULTS_DIR}/query07_top10_clientes_cobertura.csv", index=False)

print("✅ Mongo queries completed and CSVs generated in 'results/'")
