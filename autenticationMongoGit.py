import os
import requests
import pymongo
from pymongo import MongoClient
from datetime import datetime

# 🔹 Configurar MongoDB
MONGODB_HOST = "localhost"
MONGODB_PORT = 27017
DB_NAME = "github"
COLLECTION_COMMITS = "commits"

# 🔹 Conexión a MongoDB
connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
collCommits = connection[DB_NAME][COLLECTION_COMMITS]

# 🔹 Token de GitHub (debe estar en una variable de entorno)
TOKEN = os.getenv("GITHUB_TOKEN")
if not TOKEN:
    raise ValueError("El token de GitHub no está definido en las variables de entorno")

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
    "since": "2018-01-01T00:00:00Z",  # Desde esta fecha
    "per_page": 100  # Máximo permitido por GitHub
}

# 🔹 Control del Rate Limit de GitHub
def check_rate_limit():
    response = requests.get("https://api.github.com/rate_limit", headers=headers)
    data = response.json()
    remaining = data["rate"]["remaining"]
    reset_time = datetime.utcfromtimestamp(data["rate"]["reset"]).strftime('%Y-%m-%d %H:%M:%S')
    print(f"🛑 Rate Limit restante: {remaining} - Reseteo: {reset_time}")
    if remaining == 0:
        raise Exception("⚠️ Has alcanzado el límite de peticiones, espera antes de continuar.")

# 🔹 Obtener y almacenar commits con paginación
page = 1
total_commits = 0

while True:
    check_rate_limit()  # Verificar si hay peticiones disponibles
    params["page"] = page
    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code != 200:
        print(f"⚠️ Error en la solicitud: {response.status_code} - {response.text}")
        break

    commits_dict = response.json()
    
    if not commits_dict:
        break  # No hay más commits

    for commit in commits_dict:
        commit_sha = commit["sha"]
        commit_url = f"https://api.github.com/repos/{user}/{project}/commits/{commit_sha}"
        
        # 🔹 Obtener detalles adicionales del commit (archivos modificados y estadísticas)
        commit_details = requests.get(commit_url, headers=headers).json()
        
        commit["projectId"] = project
        commit["modified_files"] = commit_details.get("files", [])  # Ficheros modificados
        commit["stats"] = commit_details.get("stats", {})  # Estadísticas de cambios
        
        try:
            collCommits.insert_one(commit)
            total_commits += 1
        except pymongo.errors.DuplicateKeyError:
            print(f"🔄 Commit duplicado: {commit_sha} - No insertado.")

    print(f"✅ Página {page} procesada.")
    page += 1

print(f"📊 Total de commits insertados: {total_commits}")
