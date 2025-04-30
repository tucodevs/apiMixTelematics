import os
import json
import requests
from core.auth import autenticar
from core.db import conectar_banco
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

def importar_trips():
    token = autenticar()
    organisation_id = os.getenv("MIX_ORGANISATION_ID")

    if not organisation_id:
        print("⚠️ ORGANISATION_ID não definido no .env")
        return

    since_time = (datetime.utcnow() - timedelta(days=1)).isoformat() + "Z"
    quantity = 500

    url = f"{os.getenv('MIX_API_URL')}/api/trips/groups/createdsince/organisation/{organisation_id}/sincetoken/{since_time}/quantity/{quantity}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Erro ao buscar trips: {response.status_code} - {response.text}")
        return

    trips_data = response.json()

    if not trips_data.get("Items"):
        print("Nenhuma trip encontrada.")
        return

    conn = conectar_banco()
    cursor = conn.cursor()

    for trip in trips_data["Items"]:
        cursor.execute("""
            INSERT INTO trips (
                TripId, AssetId, DriverId, StartTime, EndTime, Distance,
                AverageSpeed, MaxSpeed, FuelUsed, StartLocation, EndLocation,
                IdleTime, HarshEvents, SpeedingEvents, StopCount
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                AssetId=VALUES(AssetId),
                DriverId=VALUES(DriverId),
                StartTime=VALUES(StartTime),
                EndTime=VALUES(EndTime),
                Distance=VALUES(Distance),
                AverageSpeed=VALUES(AverageSpeed),
                MaxSpeed=VALUES(MaxSpeed),
                FuelUsed=VALUES(FuelUsed),
                StartLocation=VALUES(StartLocation),
                EndLocation=VALUES(EndLocation),
                IdleTime=VALUES(IdleTime),
                HarshEvents=VALUES(HarshEvents),
                SpeedingEvents=VALUES(SpeedingEvents),
                StopCount=VALUES(StopCount)
        """, (
            trip.get("TripId"),
            trip.get("AssetId"),
            trip.get("DriverId"),
            trip.get("StartTime"),
            trip.get("EndTime"),
            trip.get("Distance"),
            trip.get("AverageSpeed"),
            trip.get("MaxSpeed"),
            trip.get("FuelUsed"),
            json.dumps(trip.get("StartLocation")) if trip.get("StartLocation") else None,
            json.dumps(trip.get("EndLocation")) if trip.get("EndLocation") else None,
            trip.get("IdleTime"),
            trip.get("HarshEvents"),
            trip.get("SpeedingEvents"),
            trip.get("StopCount")
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ {len(trips_data['Items'])} trips importadas/atualizadas com sucesso.")
