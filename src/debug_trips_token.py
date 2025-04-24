import os
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

# Autenticação
def autenticar():
    data = {
        "grant_type": "password",
        "client_id": os.getenv("MIX_CLIENT_ID"),
        "client_secret": os.getenv("MIX_CLIENT_SECRET"),
        "username": os.getenv("MIX_USERNAME"),
        "password": os.getenv("MIX_PASSWORD"),
        "scope": "offline_access MiX.Integrate"
    }
    response = requests.post("https://identity.us.mixtelematics.com/core/connect/token", data=data)
    response.raise_for_status()
    return response.json()["access_token"]

# Gera um sinceToken com base em N horas atrás
def gerar_since_token(horas_atras=24):
    data = datetime.now(timezone.utc) - timedelta(hours=horas_atras)
    return data.strftime('%Y%m%d%H%M%S') + "000"

# Teste de requisição de trips com exibição de cabeçalho e começo do JSON
def testar_trips():
    token = autenticar()
    since_token = gerar_since_token()
    organisation_id = os.getenv("MIX_ORGANISATION_ID")
    base_url = os.getenv("MIX_API_URL")

    url = f"{base_url}/api/trips/groups/createdsince/organisation/{organisation_id}/sincetoken/{since_token}/quantity/10?includeSubTrips=true"
    headers = {"Authorization": f"Bearer {token}"}

    print(f"\n🔗 URL consultada:\n{url}")

    response = requests.get(url, headers=headers)
    print("\n✅ Cabeçalho da resposta:")
    for key, value in response.headers.items():
        print(f"{key}: {value}")

    print("\n📦 Início do JSON de resposta:")
    try:
        json_data = response.json()
        if isinstance(json_data, list):
            print(json_data[:1])
        elif isinstance(json_data, dict):
            preview = {k: json_data[k] for k in list(json_data)[:3]}
            print(preview)
        else:
            print(json_data)
    except Exception as e:
        print(f"Erro ao processar JSON: {e}")

if __name__ == "__main__":
    testar_trips()
