import os
import json
import requests
from core.auth import autenticar
from core.db import conectar_banco
from dotenv import load_dotenv

load_dotenv()

def buscar_drivers():
    token = autenticar()
    organisation_id = os.getenv("MIX_ORGANISATION_ID")

    if not organisation_id:
        print("⚠️ ORGANISATION_ID não definido no .env")
        return

    url = f"{os.getenv('MIX_API_URL')}/api/drivers/organisation/{organisation_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Erro ao buscar drivers: {response.status_code} - {response.text}")
        return

    drivers = response.json()

    with open("drivers_organisation.txt", "w", encoding="utf-8") as f:
        json.dump(drivers, f, indent=2, ensure_ascii=False)

    print(f"✅ JSON de drivers salvo com sucesso.")

def buscar_driverlicence_group():
    token = autenticar()
    group_id = os.getenv("MIX_ORGANISATION_ID")

    if not group_id:
        print("⚠️ GROUP_ID não definido no .env")
        return

    url = f"{os.getenv('MIX_API_URL')}/api/driverlicence/group/{group_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Erro ao buscar driver licences: {response.status_code} - {response.text}")
        return

    driver_licences = response.json()

    with open("driverlicence_group.txt", "w", encoding="utf-8") as f:
        json.dump(driver_licences, f, indent=2, ensure_ascii=False)

    print(f"✅ JSON de driver licences salvo com sucesso.")

def main():
    buscar_drivers()
    buscar_driverlicence_group()

if __name__ == "__main__":
    main()
