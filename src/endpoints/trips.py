import os
import json
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from core.auth import autenticar
from core.db import conectar_banco

load_dotenv()

BASE_URL = os.getenv("MIX_API_URL")
ORGANISATION_ID = os.getenv("MIX_ORGANISATION_ID")
QUANTITY = 1000
SINCE_TOKEN_FILE = "since_tokens/since_token_trips.txt"
FUSO_MANAUS = timezone(timedelta(hours=-4))

def since_token_path():
    os.makedirs(os.path.dirname(SINCE_TOKEN_FILE), exist_ok=True)
    return SINCE_TOKEN_FILE

def carregar_since_token():
    path = since_token_path()
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read().strip()
    return gerar_since_token()

def salvar_since_token(token):
    with open(since_token_path(), "w") as f:
        f.write(token)

def gerar_since_token(horas=24):
    agora_utc = datetime.utcnow()
    dt_manaus = agora_utc - timedelta(hours=4)
    dt_inicio_manaus = dt_manaus - timedelta(hours=horas)
    dt_inicio_utc = dt_inicio_manaus + timedelta(hours=4)

    print(f"[TRIPS] ⏰ Agora em Manaus: {dt_manaus.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[TRIPS] ⏰ SinceToken (UTC): {dt_inicio_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    return dt_inicio_utc.strftime("%Y%m%d%H%M%S") + "000"

def traduzir_token(token):
    try:
        return datetime.strptime(token[:14], "%Y%m%d%H%M%S").strftime("%d/%m/%Y %H:%M:%S")
    except:
        return "inválido"

def converter_utc_para_manaus(data_str):
    if data_str is None:
        return None
    try:
        dt_utc = datetime.strptime(data_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return dt_utc.astimezone(FUSO_MANAUS).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"[TRIPS] ❌ Erro ao converter data: {data_str} -> {e}")
        return None

def importar_trips():
    print("🧭 Importando viagens (trips)...")
    token_api = autenticar()
    since_token = carregar_since_token()
    print(f"[TRIPS] SinceToken: {since_token} ({traduzir_token(since_token)})")

    url = f"{BASE_URL}/api/trips/groups/createdsince/organisation/{ORGANISATION_ID}/sincetoken/{since_token}/quantity/{QUANTITY}"
    headers = {
        "Authorization": f"Bearer {token_api}",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code not in (200, 206):
        print(f"[TRIPS] ❌ Erro ao buscar trips: {response.status_code}")
        return

    try:
        trips_data = response.json()
    except Exception as e:
        print(f"[TRIPS] ❌ Erro ao interpretar JSON: {e}")
        return

    items = trips_data if isinstance(trips_data, list) else trips_data.get("Items", [])

    if not items:
        print("[TRIPS] ⚠️ Nenhuma trip retornada.")
        return

    print(f"[TRIPS] ➕ {len(items)} trips recebidas")
    percentual = (min(len(items), QUANTITY) / QUANTITY) * 100
    print(f"[TRIPS] Progresso: {percentual:.1f}% do lote")

    conn = conectar_banco()
    cursor = conn.cursor()

    inseridas, ignoradas = 0, 0

    for trip in items:
        try:
            cursor.execute("""
                INSERT INTO trips (
                    TripId, AssetId, DistanceKilometers, DriverId, DrivingTime,
                    Duration, EndEngineSeconds, EndOdometerKilometers, EngineSeconds,
                    FirstDepart, FuelUsedLitres, LastHalt,
                    MaxAccelerationKilometersPerHourPerSecond, MaxDecelerationKilometersPerHourPerSecond,
                    MaxRpm, MaxSpeedKilometersPerHour, Notes, PulseValue,
                    StandingTime, StartEngineSeconds, StartOdometerKilometers,
                    TripEnd, TripStart
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    AssetId=VALUES(AssetId),
                    DriverId=VALUES(DriverId),
                    TripEnd=VALUES(TripEnd)
            """, (
                trip.get("TripId"),
                trip.get("AssetId"),
                trip.get("DistanceKilometers"),
                trip.get("DriverId"),
                trip.get("DrivingTime"),
                trip.get("Duration"),
                trip.get("EndEngineSeconds"),
                trip.get("EndOdometerKilometers"),
                trip.get("EngineSeconds"),
                converter_utc_para_manaus(trip.get("FirstDepart")),
                trip.get("FuelUsedLitres"),
                converter_utc_para_manaus(trip.get("LastHalt")),
                trip.get("MaxAccelerationKilometersPerHourPerSecond"),
                trip.get("MaxDecelerationKilometersPerHourPerSecond"),
                trip.get("MaxRpm"),
                trip.get("MaxSpeedKilometersPerHour"),
                trip.get("Notes"),
                trip.get("PulseValue"),
                trip.get("StandingTime"),
                trip.get("StartEngineSeconds"),
                trip.get("StartOdometerKilometers"),
                converter_utc_para_manaus(trip.get("TripEnd")),
                converter_utc_para_manaus(trip.get("TripStart"))
            ))
            inseridas += 1
        except Exception as e:
            ignoradas += 1
            print(f"[TRIPS] ⚠️ Erro ao inserir TripId {trip.get('TripId')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"[TRIPS] ✅ Incluídas: {inseridas} | Ignoradas: {ignoradas}")

    novo_token = response.headers.get("GetSinceToken")
    has_more = response.headers.get("HasMoreItems", "False") == "True"
    print(f"[TRIPS] HasMoreItems: {has_more}")

    if novo_token:
        salvar_since_token(novo_token)

    if not has_more:
        print("[TRIPS] 🚫 Fim dos dados. Próxima execução usará token das últimas 24h.")
        salvar_since_token(gerar_since_token())
