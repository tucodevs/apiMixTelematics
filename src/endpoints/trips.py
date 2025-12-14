import os
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from core.auth import autenticar
from core.db import conectar_banco
from core.since_token import (
    datetime_para_token,
    gerar_token_relativo_info,
    traduzir_token as traduzir_token_fmt,
    token_para_datetime,
    validar_idade_token,
    formatar_timedelta,
)

load_dotenv()

BASE_URL = os.getenv("MIX_API_URL")
ORGANISATION_ID = os.getenv("MIX_ORGANISATION_ID")
QUANTITY = 1000
SINCE_TOKEN_FILE = "since_tokens/since_token_trips.txt"
FUSO_MANAUS = timezone(timedelta(hours=-4))

def _format_token_debug(token):
    if not token:
        return "vazio"
    if len(token) <= 10:
        return token
    return f"{token[:6]}...{token[-4:]} (len={len(token)})"

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
    token, dt_manaus, dt_utc = gerar_token_relativo_info(horas)
    print(f"[TRIPS] 🕒 Origem Manaus (-4): {dt_manaus.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"[TRIPS] 🌐 Referência UTC (+0): {dt_utc.strftime('%d/%m/%Y %H:%M:%S')} -> token {token}")
    return token

def traduzir_token(token):
    return traduzir_token_fmt(token)

def converter_utc_para_manaus(data_str):
    if data_str is None:
        return None
    try:
        dt_utc = datetime.strptime(data_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return dt_utc.astimezone(FUSO_MANAUS).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"[TRIPS] ❌ Erro ao converter data: {data_str} -> {e}")
        return None

def garantir_token_na_janela(token_atual):
    valido, dt, idade, limite = validar_idade_token(token_atual)
    if valido:
        return token_atual

    if token_atual:
        msg_idade = formatar_timedelta(idade) if idade else "idade desconhecida"
        print(f"[TRIPS] ⚠️ SinceToken {token_atual} está fora do limite ({msg_idade} > {formatar_timedelta(limite)}).")
    else:
        print("[TRIPS] ⚠️ SinceToken inexistente ou inválido.")

    novo_token = gerar_since_token(24)
    salvar_since_token(novo_token)
    print(f"[TRIPS] 🔁 Novo since_token gerado automaticamente: {novo_token} ({traduzir_token(novo_token)})")
    return novo_token

def importar_trips():
    print("\n######## TRIPS ########")
    print("🧭 Importando viagens (trips)...")
    # print(f"[TRIPS][DEBUG] BASE_URL={BASE_URL} | ORGANISATION_ID={ORGANISATION_ID} | QUANTITY={QUANTITY}")
    # print(f"[TRIPS][DEBUG] SinceToken file: {since_token_path()} (abs: {os.path.abspath(since_token_path())})")
    token_api = autenticar()
    # print(f"[TRIPS][DEBUG] Token recebido: {_format_token_debug(token_api)}")
    since_token = carregar_since_token()
    since_token = garantir_token_na_janela(since_token)
    print(f"[TRIPS] SinceToken em uso: {since_token}")
    dt_utc = token_para_datetime(since_token)
    if dt_utc:
        dt_manaus = dt_utc.astimezone(FUSO_MANAUS)
        print(f"[TRIPS] • UTC/Londres (+0): {dt_utc.strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"[TRIPS] • Manaus (-4): {dt_manaus.strftime('%d/%m/%Y %H:%M:%S')}")
    else:
        print("[TRIPS] • Não foi possível interpretar o since_token.")
    print("--------------------------------------------------")

    url = f"{BASE_URL}/api/trips/groups/createdsince/organisation/{ORGANISATION_ID}/sincetoken/{since_token}/quantity/{QUANTITY}"
    headers = {
        "Authorization": f"Bearer {token_api}",
        "Accept": "application/json"
    }

    # print(f"[TRIPS][DEBUG] URL requisitada: {url}")
    # print(f"[TRIPS][DEBUG] Headers enviados: {{'Authorization': 'Bearer {_format_token_debug(token_api)}', 'Accept': 'application/json'}}")
    try:
        response = requests.get(url, headers=headers, timeout=60)
    except Exception as exc:
        # print(f"[TRIPS][DEBUG] Falha na requisição: {exc}")
        raise
    # print(f"[TRIPS][DEBUG] Status {response.status_code} | Headers: {dict(response.headers)}")

    if response.status_code not in (200, 206):
        print(f"[TRIPS] ❌ Erro ao buscar trips: {response.status_code}")
        # print(f"[TRIPS][DEBUG] Corpo de erro: {response.text}")
        return

    try:
        trips_data = response.json()
    except Exception as e:
        print(f"[TRIPS] ❌ Erro ao interpretar JSON: {e}")
        # print(f"[TRIPS][DEBUG] Corpo bruto: {response.text}")
        return

    items = trips_data if isinstance(trips_data, list) else trips_data.get("Items", [])
    # print(f"[TRIPS][DEBUG] Tipo de resposta: {type(trips_data)} | Chaves: {list(trips_data.keys()) if isinstance(trips_data, dict) else 'n/a'}")

    if not items:
        print("[TRIPS] ⚠️ Nenhuma trip retornada.")
        # print(f"[TRIPS][DEBUG] Conteúdo integral: {trips_data}")
        return

    print(f"[TRIPS] ➕ {len(items)} trips recebidas")
    percentual = (min(len(items), QUANTITY) / QUANTITY) * 100
    print(f"[TRIPS] Progresso: {percentual:.1f}% do lote")

    # print("[TRIPS][DEBUG] Abrindo conexão com o banco...")
    conn = conectar_banco()
    cursor = conn.cursor()
    # print("[TRIPS][DEBUG] Conexão estabelecida, iniciando inserções...")

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
    proximo_legivel = None
    has_more = response.headers.get("HasMoreItems", "False") == "True"
    print(f"[TRIPS] HasMoreItems: {has_more}")

    if novo_token:
        salvar_since_token(novo_token)
        proximo_legivel = traduzir_token(novo_token)

    if has_more:
        complemento = f" (próximo lote a partir de {proximo_legivel})" if proximo_legivel else ""
        print(f"[TRIPS] 🔁 Ainda existem dados pendentes.{complemento}")
        print("[TRIPS] ▶️ Rode novamente para continuar a importação.")
    else:
        print("[TRIPS] 🚫 Fim dos dados. Próxima execução usará token das últimas 24h.")
        salvar_since_token(gerar_since_token())
