import os
import requests
import time
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from auth import autenticar
from db import conectar_banco

load_dotenv()

BASE_URL = os.getenv("MIX_API_URL")
ORGANISATION_ID = os.getenv("MIX_ORGANISATION_ID")
QUANTITY = "1000"
FUSO_MANAUS = timezone(timedelta(hours=-4))  # UTC-4 para Manaus

EVENTOS_TR = {
    -614457561876096876: ("tr_aceleracao_brusca", "Aceleração Brusca"),
    -4465594527070247088: ("tr_batendo_transmissao", "Batendo Transmissão"),
    3296322604872944138: ("tr_curva_brusca", "Curva Brusca"),
    -1988381093544824498: ("tr_embreagem_acionada_indevida", "Embreagem Acionada Indevidamente"),
    2164520525956490666: ("tr_excesso_rpm_parado", "Excesso RPM Parado"),
    74735825877637374: ("tr_excesso_velocidade_20km", "Excesso de Velocidade 20km"),
    -6248653914463313400: ("tr_excesso_velocidade_30km", "Excesso de Velocidade 30km"),
    6474504604434952727: ("tr_excesso_velocidade_40km_1", "Excesso de Velocidade 40km 1"),
    -1992910974424714295: ("tr_excesso_velocidade_40km_2", "Excesso de Velocidade 40km 2"),
    5511057473630489154: ("tr_excesso_velocidade_50km", "Excesso de Velocidade 50km"),
    6580201539568389304: ("tr_excesso_velocidade_55km_1", "Excesso de Velocidade 55km 1"),
    -9050647299058098294: ("tr_excesso_velocidade_55km_2", "Excesso de Velocidade 55km 2"),
    908787025131282024: ("tr_excesso_velocidade_60km", "Excesso de Velocidade 60km"),
    -6437542951044419628: ("tr_fora_faixa_verde", "Fora da Faixa Verde"),
    337658916843834225: ("tr_freada_brusca", "Freada Brusca"),
    -1150311268842644462: ("tr_freada_brusca_grave", "Freada Brusca Grave"),
    6314588935029952465: ("tr_inercia_aproveitada", "Inércia Aproveitada"),
    2561992611692992861: ("tr_marcha_lenta", "Marcha Lenta"),
    -154632669554799975: ("tr_marcha_lenta_5min", "Marcha Lenta 5min"),
    8889515098300962737: ("tr_excesso_rotacao", "Excesso de Rotação")
}

def gerar_since_token(horas_atras=24):
    data_manaus = datetime.now(FUSO_MANAUS) - timedelta(hours=horas_atras)
    data_utc = data_manaus.astimezone(timezone.utc)
    return data_utc.strftime('%Y%m%d%H%M%S') + "000"

def normalizar_data(data_str):
    if not data_str:
        return None
    try:
        dt_utc = datetime.strptime(data_str.replace("Z", ""), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        return dt_utc.astimezone(FUSO_MANAUS).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None

def buscar_eventos(token, since_token, tentativas=3, espera=5):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/api/events/groups/createdsince/organisation/{ORGANISATION_ID}/sincetoken/{since_token}/quantity/{QUANTITY}"

    for tentativa in range(1, tentativas + 1):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as err:
            if response.status_code == 429 and tentativa < tentativas:
                print(f"🔁 Tentativa {tentativa}: Limite de requisições atingido. Aguardando {espera}s...")
                time.sleep(espera)
            else:
                raise

def inserir_evento(cursor, evento, nome_tabela):
    sql = f'''
        INSERT INTO {nome_tabela} (
            AssetId, DriverId, EventId, EventTypeId, EventCategory,
            StartDateTime, StartLatitude, StartLongitude, StartSpeedKph, StartOdometer,
            EndDateTime, EndLatitude, EndLongitude, EndSpeedKph, EndOdometer,
            Value, FuelUsedLitres, ValueType, ValueUnits,
            TotalTimeSeconds, TotalOccurances, SpeedLimit
        ) VALUES (
            %(AssetId)s, %(DriverId)s, %(EventId)s, %(EventTypeId)s, %(EventCategory)s,
            %(StartDateTime)s, %(StartLatitude)s, %(StartLongitude)s, %(StartSpeedKph)s, %(StartOdometer)s,
            %(EndDateTime)s, %(EndLatitude)s, %(EndLongitude)s, %(EndSpeedKph)s, %(EndOdometer)s,
            %(Value)s, %(FuelUsedLitres)s, %(ValueType)s, %(ValueUnits)s,
            %(TotalTimeSeconds)s, %(TotalOccurances)s, %(SpeedLimit)s
        )
        ON DUPLICATE KEY UPDATE EventId = EventId;
    '''

    dados = {
        "AssetId": evento.get("AssetId"),
        "DriverId": evento.get("DriverId"),
        "EventId": evento.get("EventId"),
        "EventTypeId": evento.get("EventTypeId"),
        "EventCategory": evento.get("EventCategory"),
        "StartDateTime": normalizar_data(evento.get("StartDateTime")),
        "StartLatitude": evento.get("StartPosition", {}).get("Latitude"),
        "StartLongitude": evento.get("StartPosition", {}).get("Longitude"),
        "StartSpeedKph": evento.get("StartPosition", {}).get("SpeedKilometresPerHour"),
        "StartOdometer": evento.get("StartPosition", {}).get("OdometerKilometres"),
        "EndDateTime": normalizar_data(evento.get("EndDateTime")),
        "EndLatitude": evento.get("EndPosition", {}).get("Latitude"),
        "EndLongitude": evento.get("EndPosition", {}).get("Longitude"),
        "EndSpeedKph": evento.get("EndPosition", {}).get("SpeedKilometresPerHour"),
        "EndOdometer": evento.get("EndPosition", {}).get("OdometerKilometres"),
        "Value": evento.get("Value"),
        "FuelUsedLitres": evento.get("FuelUsedLitres"),
        "ValueType": evento.get("ValueType"),
        "ValueUnits": evento.get("ValueUnits"),
        "TotalTimeSeconds": evento.get("TotalTimeSeconds"),
        "TotalOccurances": evento.get("TotalOccurances"),
        "SpeedLimit": evento.get("SpeedLimit")
    }

    cursor.execute(sql, dados)
    return cursor.rowcount > 0

def importar_eventos_lote():
    print("📡 Buscando todos os eventos de uma vez...")
    since_token = gerar_since_token()
    token = autenticar()
    eventos = buscar_eventos(token, since_token)

    if not eventos:
        print("Nenhum evento encontrado.")
        return

    eventos_por_tipo = {}
    for evento in eventos:
        tipo_id = evento.get("EventTypeId")
        if tipo_id in EVENTOS_TR:
            eventos_por_tipo.setdefault(tipo_id, []).append(evento)

    conn = conectar_banco()
    cursor = conn.cursor()

    for tipo_id, eventos_filtrados in eventos_por_tipo.items():
        nome_tabela, nome_legivel = EVENTOS_TR[tipo_id]
        print(f"📥 Importando {len(eventos_filtrados)} eventos de {nome_legivel}...")
        inseridos = 0
        for evento in eventos_filtrados:
            if inserir_evento(cursor, evento, nome_tabela):
                inseridos += 1
        print(f"✅ {inseridos} novos inseridos na tabela {nome_tabela} ({len(eventos_filtrados)} recebidos)")

    conn.commit()
    cursor.close()
    conn.close()
    print("🏁 Importação finalizada.")
