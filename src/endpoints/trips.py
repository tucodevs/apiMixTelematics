import os
import time
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from core.auth import autenticar
from core.db import conectar_banco

load_dotenv()

BASE_URL = os.getenv("MIX_API_URL")
ORGANISATION_ID = os.getenv("MIX_ORGANISATION_ID")
QUANTITY = 1000
SINCE_TOKEN_FILE = "since_token.txt"

def carregar_since_token():
    if os.path.exists(SINCE_TOKEN_FILE):
        with open(SINCE_TOKEN_FILE, "r") as f:
            return f.read().strip()
    return gerar_since_token()

def salvar_since_token(token):
    with open(SINCE_TOKEN_FILE, "w") as f:
        f.write(token)

def gerar_since_token(horas_atras=24):
    data = datetime.now(timezone.utc) - timedelta(hours=horas_atras)
    return data.strftime('%Y%m%d%H%M%S') + "000"

def traduzir_token(token):
    try:
        data_str = token[:14]
        return datetime.strptime(data_str, "%Y%m%d%H%M%S").strftime("%d/%m/%Y %H:%M:%S")
    except:
        return "inválido"

def buscar_trips(token, since_token):
    url = f"{BASE_URL}/api/trips/groups/createdsince/organisation/{ORGANISATION_ID}/sincetoken/{since_token}/quantity/{QUANTITY}?includeSubTrips=true"
    headers = {"Authorization": f"Bearer {token}"}
    return requests.get(url, headers=headers, timeout=30)

def salvar_trips_no_banco(trips):
    conn = conectar_banco()
    cursor = conn.cursor()

    total = len(trips)
    inseridos = 0

    for trip in trips:
        try:
            cursor.execute("""
                INSERT INTO trips (
                    TripId, AssetId, DriverId, TripStart, TripEnd, Notes,
                    EngineSeconds, FirstDepart, LastHalt, DrivingTime, StandingTime, Duration,
                    DistanceKilometers, StartOdometerKilometers, EndOdometerKilometers,
                    StartEngineSeconds, EndEngineSeconds, PulseValue, FuelUsedLitres,
                    MaxSpeedKilometersPerHour, MaxAccelerationKilometersPerHourPerSecond,
                    MaxDecelerationKilometersPerHourPerSecond, MaxRpm
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    TripStart=VALUES(TripStart),
                    TripEnd=VALUES(TripEnd),
                    Notes=VALUES(Notes),
                    EngineSeconds=VALUES(EngineSeconds),
                    FirstDepart=VALUES(FirstDepart),
                    LastHalt=VALUES(LastHalt),
                    DrivingTime=VALUES(DrivingTime),
                    StandingTime=VALUES(StandingTime),
                    Duration=VALUES(Duration),
                    DistanceKilometers=VALUES(DistanceKilometers),
                    StartOdometerKilometers=VALUES(StartOdometerKilometers),
                    EndOdometerKilometers=VALUES(EndOdometerKilometers),
                    StartEngineSeconds=VALUES(StartEngineSeconds),
                    EndEngineSeconds=VALUES(EndEngineSeconds),
                    PulseValue=VALUES(PulseValue),
                    FuelUsedLitres=VALUES(FuelUsedLitres),
                    MaxSpeedKilometersPerHour=VALUES(MaxSpeedKilometersPerHour),
                    MaxAccelerationKilometersPerHourPerSecond=VALUES(MaxAccelerationKilometersPerHourPerSecond),
                    MaxDecelerationKilometersPerHourPerSecond=VALUES(MaxDecelerationKilometersPerHourPerSecond),
                    MaxRpm=VALUES(MaxRpm)
            """, (
                trip.get("TripId"),
                trip.get("AssetId"),
                trip.get("DriverId"),
                parse_date(trip.get("TripStart")),
                parse_date(trip.get("TripEnd")),
                trip.get("Notes"),
                trip.get("EngineSeconds"),
                parse_date(trip.get("FirstDepart")),
                parse_date(trip.get("LastHalt")),
                trip.get("DrivingTime"),
                trip.get("StandingTime"),
                trip.get("Duration"),
                trip.get("DistanceKilometers"),
                trip.get("StartOdometerKilometers"),
                trip.get("EndOdometerKilometers"),
                trip.get("StartEngineSeconds"),
                trip.get("EndEngineSeconds"),
                trip.get("PulseValue"),
                trip.get("FuelUsedLitres"),
                trip.get("MaxSpeedKilometersPerHour"),
                trip.get("MaxAccelerationKilometersPerHourPerSecond"),
                trip.get("MaxDecelerationKilometersPerHourPerSecond"),
                trip.get("MaxRpm")
            ))
            inseridos += cursor.rowcount
        except Exception as e:
            print(f"[TRIPS] ⚠️ Erro ao inserir TripId {trip.get('TripId')}: {e}")

    ignorados = total - inseridos
    conn.commit()
    cursor.close()
    conn.close()

    print(f"[TRIPS] ✅ Incluídos: {inseridos} | Ignorados: {ignorados}")


def parse_date(data_str):
    if not data_str:
        return None
    try:
        return datetime.strptime(data_str.replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
    except:
        return None

def importar_trips():
    print("[TRIPS] Início da execução única")

    try:
        token = autenticar()
        since_token = carregar_since_token()
        print(f"[TRIPS] Utilizando since_token: {since_token} ({traduzir_token(since_token)})")

        response = buscar_trips(token, since_token)

        if response.status_code not in (200, 206):
            print(f"[TRIPS] ❌ Erro {response.status_code} na requisição.")
            return


        try:
            trips = response.json()
            if not isinstance(trips, list):
                trips = trips.get("Trips", [])
            if not isinstance(trips, list):
                print("[TRIPS] ⚠️ Estrutura de resposta inesperada.")
                return
        except:
            print("[TRIPS] ❌ Erro ao interpretar resposta da API.")
            return

        print(f"[TRIPS] ➕ {len(trips)} trips recebidas")
        salvar_trips_no_banco(trips)

        novo_token = response.headers.get("GetSinceToken")
        has_more = response.headers.get("HasMoreItems", "False") == "True"

        if novo_token:
            salvar_since_token(novo_token)

        progresso = min(len(trips), QUANTITY)
        percentual = (progresso / QUANTITY) * 100

        print(f"[TRIPS] Progresso: {percentual:.1f}% do lote ({progresso}/{QUANTITY})")
        print(f"[TRIPS] HasMoreItems: {has_more}")
        if not has_more:
            print("[TRIPS] 🚫 Fim dos dados. No próximo ciclo será usado since_token das últimas 24h.")
            salvar_since_token(gerar_since_token())

    except Exception:
        print("[TRIPS] ❌ Erro inesperado. Execução abortada com segurança.")

if __name__ == "__main__":
    importar_trips()
