import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from core.auth import autenticar
from core.db import conectar_banco

load_dotenv()

BASE_URL = os.getenv("MIX_API_URL")
ORGANISATION_ID = os.getenv("MIX_ORGANISATION_ID")
QUANTITY = 1000
SINCE_TOKEN_DIR = "since_tokens"
FUSO_MANAUS = timezone(timedelta(hours=-4))

EVENTOS_TR = {
    -614457561876096876: ("tr_aceleracao_brusca", "Aceleração Brusca"),
    3296322604872944138: ("tr_curva_brusca", "Curva Brusca"),
    -1988381093544824498: ("tr_embreagem_acionada_indevida", "Embreagem Indevida"),
    2164520525956490666: ("tr_excesso_rpm_parado", "Excesso RPM Parado"),
    74735825877637374: ("tr_excesso_velocidade_20km", "Excesso Velocidade 20km"),
    -6248653914463313400: ("tr_excesso_velocidade_30km", "Excesso Velocidade 30km"),
    6474504604434952727: ("tr_excesso_velocidade_40km_1", "Excesso Velocidade 40km 1"),
    -1992910974424714295: ("tr_excesso_velocidade_40km_2", "Excesso Velocidade 40km 2"),
    5511057473630489154: ("tr_excesso_velocidade_50km", "Excesso Velocidade 50km"),
    6580201539568389304: ("tr_excesso_velocidade_55km_1", "Excesso Velocidade 55km 1"),
    -9050647299058098294: ("tr_excesso_velocidade_55km_2", "Excesso Velocidade 55km 2"),
    908787025131282024: ("tr_excesso_velocidade_60km", "Excesso Velocidade 60km"),
    -6437542951044419628: ("tr_fora_faixa_verde", "Fora da Faixa Verde"),
    337658916843834225: ("tr_freada_brusca", "Freada Brusca"),
    -1150311268842644462: ("tr_freada_brusca_grave", "Freada Brusca Grave"),
    6314588935029952465: ("tr_inercia_aproveitada", "Inércia Aproveitada"),
    2561992611692992861: ("tr_marcha_lenta", "Marcha Lenta"),
    -154632669554799975: ("tr_marcha_lenta_5min", "Marcha Lenta 5min"),
    8889515098300962737: ("tr_excesso_rotacao", "Excesso de Rotação"),
    -4465594527070247088: ("tr_batendo_transmissao", "Batendo Transmissão")
}

def since_token_path():
    os.makedirs(SINCE_TOKEN_DIR, exist_ok=True)
    return os.path.join(SINCE_TOKEN_DIR, "since_token_eventos.txt")

def carregar_since_token():
    path = since_token_path()
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read().strip()
    return gerar_since_token()

def salvar_since_token(token):
    path = since_token_path()
    with open(path, "w") as f:
        f.write(token)

def gerar_since_token(horas_atras=24):
    dt_manaus = datetime.now(FUSO_MANAUS) - timedelta(hours=horas_atras)
    dt_utc = dt_manaus.astimezone(timezone.utc)
    return dt_utc.strftime('%Y%m%d%H%M%S') + "000"

def traduzir_token(token):
    try:
        return datetime.strptime(token[:14], "%Y%m%d%H%M%S").strftime("%d/%m/%Y %H:%M:%S")
    except:
        return "inválido"

def converter_utc_para_manaus(data_str):
    if not data_str:
        return None
    try:
        dt_utc = datetime.strptime(data_str.replace("Z", ""), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        return dt_utc.astimezone(FUSO_MANAUS).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None

def buscar_eventos(token, since_token):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE_URL}/api/events/groups/createdsince/organisation/{ORGANISATION_ID}/sincetoken/{since_token}/quantity/{QUANTITY}"
    return requests.get(url, headers=headers, timeout=30)

def importar_eventos_lote():
    token = autenticar()
    since_token = carregar_since_token()
    print(f"[EVENTOS] Utilizando since_token: {since_token} ({traduzir_token(since_token)})")

    response = buscar_eventos(token, since_token)

    if response.status_code not in (200, 206):
        print(f"[EVENTOS] ❌ Erro {response.status_code} ao buscar eventos.")
        return

    try:
        eventos = response.json()
        if not isinstance(eventos, list):
            eventos = eventos.get("Events", [])
        if not isinstance(eventos, list):
            print("[EVENTOS] ⚠️ Resposta inesperada.")
            return
    except:
        print("[EVENTOS] ❌ Erro ao interpretar resposta.")
        return

    print(f"[EVENTOS] ➕ {len(eventos)} eventos recebidos")
    progresso = min(len(eventos), QUANTITY)
    percentual = (progresso / QUANTITY) * 100
    print(f"[EVENTOS] Progresso: {percentual:.1f}% do lote ({progresso}/{QUANTITY})")

    conn = conectar_banco()
    cursor = conn.cursor()
    contadores = {}

    for evento in eventos:
        tipo = evento.get("EventTypeId")
        if tipo not in EVENTOS_TR:
            continue
        tabela, _ = EVENTOS_TR[tipo]
        contadores[tipo] = contadores.get(tipo, 0) + 1
        try:
            cursor.execute(f'''
                INSERT IGNORE INTO {tabela} (
                    AssetId, DriverId, EventId, EventTypeId, EventCategory,
                    StartDateTime, StartLatitude, StartLongitude, StartSpeedKph,
                    StartOdometer, EndDateTime, EndLatitude, EndLongitude,
                    EndSpeedKph, EndOdometer, Value, FuelUsedLitres,
                    ValueType, ValueUnits, TotalTimeSeconds, TotalOccurances, SpeedLimit
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                evento.get("AssetId"),
                evento.get("DriverId"),
                evento.get("EventId"),
                evento.get("EventTypeId"),
                evento.get("EventCategory"),
                converter_utc_para_manaus(evento.get("StartDateTime")),
                evento.get("StartLatitude"),
                evento.get("StartLongitude"),
                evento.get("StartSpeedKph"),
                evento.get("StartOdometer"),
                converter_utc_para_manaus(evento.get("EndDateTime")),
                evento.get("EndLatitude"),
                evento.get("EndLongitude"),
                evento.get("EndSpeedKph"),
                evento.get("EndOdometer"),
                evento.get("Value"),
                evento.get("FuelUsedLitres"),
                evento.get("ValueType"),
                evento.get("ValueUnits"),
                evento.get("TotalTimeSeconds"),
                evento.get("TotalOccurances"),
                evento.get("SpeedLimit")
            ))
        except Exception as e:
            print(f"[EVENTOS] ⚠️ Erro ao inserir EventId {evento.get('EventId')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    for tipo_id, qtd in contadores.items():
        print(f"[EVENTOS] ▶️ {EVENTOS_TR[tipo_id][1]}: {qtd} eventos")

    total_eventos = len(eventos)
    inseridos = sum(contadores.values())
    ignorados = total_eventos - inseridos

    print(f"[EVENTOS] ✅ Incluídos: {inseridos} | Ignorados: {ignorados}")

    novo_token = response.headers.get("GetSinceToken")
    has_more = response.headers.get("HasMoreItems", "False") == "True"
    print(f"[EVENTOS] HasMoreItems: {has_more}")

    if novo_token:
        salvar_since_token(novo_token)

    if not has_more:
        print("[EVENTOS] 🚫 Fim dos dados. Próxima execução usará token das últimas 24h.")
        salvar_since_token(gerar_since_token())
