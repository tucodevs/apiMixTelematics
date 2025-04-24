import os
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from core.auth import autenticar  # Usa seu próprio auth.py

# Carregar variáveis do .env
load_dotenv()

BASE_URL = os.getenv("MIX_API_URL")
ORGANISATION_ID = os.getenv("MIX_ORGANISATION_ID")
QUANTITY = "1000"

def gerar_token_ultima_hora():
    ultima_hora = datetime.now(timezone.utc) - timedelta(hours=1)
    return ultima_hora.strftime('%Y%m%d%H%M%S') + "000"

def buscar_trips_ultima_hora():
    token = autenticar()
    since_token = gerar_token_ultima_hora()

    url = f"{BASE_URL}/api/trips/groups/createdsince/organisation/{ORGANISATION_ID}/sincetoken/{since_token}/quantity/{QUANTITY}?includeSubTrips=true"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    print(f"📡 Buscando trips com since_token: {since_token}")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Erro {response.status_code}: {response.text}")
        return

    trips = response.json()
    print(f"✅ Total de registros recebidos: {len(trips)}")
    print("🔍 Exemplos:")
    for i, trip in enumerate(trips[:3], 1):
        print(f"\nTrip {i}:")
        print(trip)

if __name__ == "__main__":
    buscar_trips_ultima_hora()
