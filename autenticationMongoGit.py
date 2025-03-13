import os
import requests
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import time

# 🔹 Configuración de MongoDB
MONGODB_HOST = "localhost"
MONGODB_PORT = 27017
DB_NAME = "github"
COLLECTION_COMMITS = "commits"

# 🔹 Conexión a MongoDB
try:
    print("Conectando a MongoDB...")
    connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
    collCommits = connection[DB_NAME][COLLECTION_COMMITS]
    print("Conexión a MongoDB exitosa.")
except Exception as e:
    print(f"Error de conexión a MongoDB: {e}")
    exit(1)

# Crear índice para evitar duplicados
try:
    collCommits.create_index("sha", unique=True)
except Exception as e:
    print(f"Error al crear el índice: {e}")
    exit(1)

# 🔹 Token de GitHub (debe estar en una variable de entorno)
TOKEN = os.getenv("GITHUB_TOKEN")
if not TOKEN:
    raise ValueError("❌ El token de GitHub no está definido en las variables de entorno")

headers = {
    "Authorization": f"token {TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# 🔹 Configuración del repositorio
user = "microsoft"
project = "vscode"
base_url = f"https://api.github.com/repos/{user}/{project}/commits"

# 🔹 Parámetros para filtrar desde el 1 de enero de 2018
params = {
    "since": "2018-01-01T00:00:00Z",  # Filtrar desde esta fecha
    "per_page": 100  # Máximo permitido por GitHub
}

# 🔹 Función para verificar el Rate Limit de GitHub
def check_rate_limit():
    try:
        response = requests.get("https://api.github.com/rate_limit", headers=headers)
        response.raise_for_status()  # Lanza un error si el código de estado no es 200
        data = response.json()
        remaining = data["rate"]["remaining"]
        reset_time = datetime.utcfromtimestamp(data["rate"]["reset"]).strftime('%Y-%m-%d %H:%M:%S')
        print(f"🛑 Rate Limit restante: {remaining} - Reseteo: {reset_time}")

        if remaining == 0:
            wait_time = (datetime.utcfromtimestamp(data["rate"]["reset"]) - datetime.utcnow()).total_seconds()
            print(f"⏳ Esperando {wait_time} segundos hasta que se reinicie el Rate Limit...")
            time.sleep(wait_time + 1)  # Esperar hasta que GitHub libere el límite
    except requests.exceptions.RequestException as e:
        print(f"Error al verificar el rate limit: {e}")
        exit(1)

# 🔹 Obtener y almacenar commits con paginación
page = 1
total_commits = 0

while True:
    check_rate_limit()  # Verificar si hay peticiones disponibles
    params["page"] = page
    try:
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()  # Lanza un error si el código de estado no es 200
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Error en la solicitud a la API: {e}")
        break

    commits_dict = response.json()
    
    if not commits_dict:
        print("✅ No hay más commits disponibles.")
        break  # No hay más commits

    for commit in commits_dict:
        commit_sha = commit["sha"]
        commit_url = f"https://api.github.com/repos/{user}/{project}/commits/{commit_sha}"
        
        # 🔹 Obtener detalles adicionales del commit (archivos modificados y estadísticas)
        try:
            commit_details_response = requests.get(commit_url, headers=headers)
            commit_details_response.raise_for_status()  # Lanza un error si el código de estado no es 200
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Error obteniendo detalles del commit {commit_sha}: {e}")
            continue
        
        commit_details = commit_details_response.json()

        # 🔹 Extraer información extendida
        modified_files = commit_details.get("files", [])  # Archivos modificados
        stats = commit_details.get("stats", {})  # Estadísticas de cambios

        commit_data = {
            "sha": commit_sha,
            "author": commit.get("commit", {}).get("author", {}),
            "message": commit.get("commit", {}).get("message", ""),
            "date": commit.get("commit", {}).get("author", {}).get("date", ""),
            "projectId": project,
            "modified_files": modified_files,
            "stats": stats
        }

        try:
            collCommits.insert_one(commit_data)
            total_commits += 1
            print(f"Commit {commit_sha} insertado.")
        except pymongo.errors.DuplicateKeyError:
            print(f"🔄 Commit duplicado: {commit_sha} - No insertado.")
        except Exception as e:
            print(f"Error al insertar commit {commit_sha}: {e}")

    print(f"✅ Página {page} procesada.")
    page += 1

print(f"📊 Total de commits insertados: {total_commits}") 
