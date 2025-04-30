import os
import json
import requests
from core.auth import autenticar
from core.db import conectar_banco
from dotenv import load_dotenv

load_dotenv()

def importar_drivers():
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
    if not isinstance(drivers, list):
        print("Resposta da API não está em formato de lista.")
        return

    conn = conectar_banco()
    cursor = conn.cursor()

    for driver in drivers:
        cursor.execute("""
            INSERT INTO drivers (
                DriverId, SiteId, Name, ImageUri, FmDriverId,
                EmployeeNumber, IsSystemDriver, MobileNumber, Email,
                ExtendedDriverId, ExtendedDriverIdType, Country, AdditionalDetailFields
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                SiteId=VALUES(SiteId),
                Name=VALUES(Name),
                ImageUri=VALUES(ImageUri),
                FmDriverId=VALUES(FmDriverId),
                EmployeeNumber=VALUES(EmployeeNumber),
                IsSystemDriver=VALUES(IsSystemDriver),
                MobileNumber=VALUES(MobileNumber),
                Email=VALUES(Email),
                ExtendedDriverId=VALUES(ExtendedDriverId),
                ExtendedDriverIdType=VALUES(ExtendedDriverIdType),
                Country=VALUES(Country),
                AdditionalDetailFields=VALUES(AdditionalDetailFields)
        """, (
            driver.get("DriverId"),
            driver.get("SiteId"),
            driver.get("Name"),
            driver.get("ImageUri"),
            driver.get("FmDriverId"),
            driver.get("EmployeeNumber"),
            driver.get("IsSystemDriver"),
            driver.get("MobileNumber"),
            driver.get("Email"),
            driver.get("ExtendedDriverId"),
            driver.get("ExtendedDriverIdType"),
            driver.get("Country"),
            json.dumps(driver.get("AdditionalDetailFields")) if driver.get("AdditionalDetailFields") else None
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ {len(drivers)} drivers importados/atualizados com sucesso.")
