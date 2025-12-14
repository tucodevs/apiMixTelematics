import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

from core.since_token import (
    datetime_para_token,
    traduzir_token,
    token_para_datetime,
    formatar_timedelta,
    validar_idade_token,
    timezone_from_name,
    FUSO_MANAUS,
)

BASE_DIR = Path(__file__).resolve().parent
SINCE_TOKEN_MAP = {
    "eventos": BASE_DIR / "since_tokens" / "since_token_eventos.txt",
    "trips": BASE_DIR / "since_tokens" / "since_token_trips.txt",
    "subtrips": BASE_DIR / "since_tokens" / "since_token_subtrips.txt",
}

FORMATOS_DATA = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
)


def resolver_timezone(args) -> timezone:
    if args.timezone:
        tz = timezone_from_name(args.timezone)
        if tz:
            return tz
        raise SystemExit(f"Timezone '{args.timezone}' não encontrado no sistema.")

    if args.offset is not None:
        return timezone(timedelta(hours=args.offset))

    if args.utc:
        return timezone.utc

    return FUSO_MANAUS


def parse_datetime(value: str, tz: timezone) -> datetime:
    for fmt in FORMATOS_DATA:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=tz)
        except ValueError:
            continue
    raise ValueError(f"Formato inválido para data/hora: {value}")


def ler_token(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    conteudo = path.read_text(encoding="utf-8").strip()
    return conteudo or None


def analisar_token(tipo: str) -> dict:
    path = SINCE_TOKEN_MAP[tipo]
    token = ler_token(path)
    valido, dt, idade, limite = validar_idade_token(token)
    if idade:
        idade_txt = formatar_timedelta(idade)
    elif token:
        idade_txt = "idade desconhecida"
    else:
        idade_txt = "n/a"
    return {
        "tipo": tipo,
        "path": path,
        "token": token,
        "valido": valido and token is not None,
        "dt": dt,
        "idade": idade,
        "limite": limite,
        "traduzido": traduzir_token(token) if token else None,
        "idade_txt": idade_txt,
    }


def formatar_status(info: dict) -> str:
    if not info["token"]:
        return f"Nenhum since_token salvo em {info['path']}"
    status = "OK" if info["valido"] else "FORA DA JANELA"
    detalhes = [
        f"Token: {info['token']}",
        f"Data local: {info['traduzido']}",
        f"Idade: {info['idade_txt']} | Limite: {formatar_timedelta(info['limite'])}",
        f"Situação: {status}",
    ]
    if info["dt"]:
        detalhes.append(f"Referência UTC: {info['dt'].strftime('%Y-%m-%d %H:%M:%S')}")
    return "\n".join(detalhes)


def mostrar_atual(tipo: str):
    info = analisar_token(tipo)
    print(f"[{tipo.upper()}] {formatar_status(info)}")


def salvar_token(tipo: str, token: str):
    destino = SINCE_TOKEN_MAP[tipo]
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(token, encoding="utf-8")
    print(f"[{tipo.upper()}] ✅ Token salvo em {destino}")


def confirmar(mensagem: str) -> bool:
    resposta = input(f"{mensagem} [s/N]: ").strip().lower()
    return resposta in {"s", "sim", "y", "yes"}


def aguardar():
    input("\nPressione Enter para continuar...")


def selecionar_tipo_interativo(tipo_atual: str) -> str:
    opcoes = list(SINCE_TOKEN_MAP.keys())
    print("\nSelecione o tipo de since_token:")
    for idx, nome in enumerate(opcoes, start=1):
        marcador = "*" if nome == tipo_atual else " "
        print(f"  {idx}) [{marcador}] {nome}")
    escolha = input("Opção (Enter mantém atual): ").strip()
    if not escolha:
        return tipo_atual
    try:
        escolha_idx = int(escolha)
        if 1 <= escolha_idx <= len(opcoes):
            return opcoes[escolha_idx - 1]
    except ValueError:
        pass
    print("⚠️ Opção inválida, mantendo valor anterior.")
    return tipo_atual


def solicitar_timezone_interativo() -> timezone:
    print("\nFuso horário para interpretar suas datas:")
    print("  Enter  -> America/Manaus (UTC-4)")
    print("  u      -> UTC")
    print("  o      -> Offset manual (ex: -3.5)")
    print("  z      -> Nome IANA (ex: America/Sao_Paulo)")
    escolha = input("Escolha: ").strip().lower()
    if escolha in ("", "m", "manaus"):
        return FUSO_MANAUS
    if escolha == "u":
        return timezone.utc
    if escolha == "o":
        valor = input("Offset em horas (ex: -4): ").strip()
        try:
            horas = float(valor)
            return timezone(timedelta(hours=horas))
        except ValueError:
            print("⚠️ Offset inválido, usando America/Manaus.")
            return FUSO_MANAUS
    if escolha == "z":
        nome = input("Nome IANA (ex: America/Sao_Paulo): ").strip()
        tz = timezone_from_name(nome)
        if tz:
            return tz
        print("⚠️ Nome de timezone não encontrado. Usando America/Manaus.")
        return FUSO_MANAUS
    print("⚠️ Opção desconhecida, usando America/Manaus.")
    return FUSO_MANAUS


def ui_gerar_por_data(tipo: str):
    tz = solicitar_timezone_interativo()
    inicio = input("Informe a data/hora inicial (ex: 2025-12-10 08:00): ").strip()
    if not inicio:
        print("⚠️ Valor vazio. Operação cancelada.")
        return
    try:
        dt_inicio = parse_datetime(inicio, tz)
    except ValueError as exc:
        print(f"⚠️ {exc}")
        return

    fim = input("Data/hora final (opcional, apenas referência): ").strip()
    if fim:
        try:
            dt_fim = parse_datetime(fim, tz)
        except ValueError as exc:
            print(f"⚠️ Valor de fim inválido: {exc}")
            dt_fim = None
    else:
        dt_fim = None

    token_inicio = datetime_para_token(dt_inicio)
    traduzido = traduzir_token(token_inicio, tz)
    print(f"\n[{tipo.upper()}] SinceToken gerado: {token_inicio} ({traduzido})")
    if dt_fim:
        token_fim = datetime_para_token(dt_fim)
        delta = dt_fim - dt_inicio
        print(f"[{tipo.upper()}] Fim informado: {dt_fim.strftime('%Y-%m-%d %H:%M:%S %Z')} -> token {token_fim}")
        print(f"[{tipo.upper()}] Duração entre início e fim: {formatar_timedelta(abs(delta))}")

    valido, _, idade, limite = validar_idade_token(token_inicio)
    idade_txt = formatar_timedelta(idade) if idade else "idade desconhecida"
    print(f"[{tipo.upper()}] Idade do token: {idade_txt} | Limite: {formatar_timedelta(limite)}")

    if not valido:
        print("⚠️ O token está fora da janela aceitável pela MiX (máx. 7 dias).")
        if not confirmar("Deseja mesmo salvar assim?"):
            print("Operação cancelada.")
            return

    if confirmar("Aplicar este since_token?"):
        salvar_token(tipo, token_inicio)
    else:
        print("Operação cancelada.")


def ui_gerar_por_horas(tipo: str):
    valor = input("Gerar token relativo a quantas horas atrás? (ex: 24): ").strip()
    if not valor:
        horas = 24
    else:
        try:
            horas = int(valor)
        except ValueError:
            print("⚠️ Valor inválido. Operação cancelada.")
            return
    tz = FUSO_MANAUS
    dt_inicio = datetime.now(tz) - timedelta(hours=horas)
    token = datetime_para_token(dt_inicio)
    print(f"[{tipo.upper()}] Token relativo ({horas}h atrás): {token} ({traduzir_token(token, tz)})")
    if confirmar("Aplicar este since_token?"):
        salvar_token(tipo, token)
    else:
        print("Operação cancelada.")


def ui_definir_manual(tipo: str):
    token = input("Informe o since_token completo (17 dígitos): ").strip()
    if not token:
        print("⚠️ Valor vazio. Operação cancelada.")
        return
    if len(token) < 14:
        print("⚠️ Token muito curto. Operação cancelada.")
        return
    dt = token_para_datetime(token)
    if not dt:
        print("⚠️ Token inválido (não segue o padrão YYYYMMDDHHMMSS###).")
        return
    traduzido = traduzir_token(token)
    print(f"[{tipo.upper()}] Token informado representa {traduzido} (fuso padrão).")
    if confirmar("Aplicar este since_token?"):
        salvar_token(tipo, token)
    else:
        print("Operação cancelada.")


def limpar_tela():
    comando = "cls" if os.name == "nt" else "clear"
    os.system(comando)


def executar_ui():
    tipo_atual = "eventos"
    while True:
        limpar_tela()
        info = analisar_token(tipo_atual)
        print("=" * 60)
        print(f" GERENCIADOR DE SINCE_TOKEN ".center(60, "="))
        print("=" * 60)
        print(f"Tipo selecionado: {tipo_atual.upper()} ({SINCE_TOKEN_MAP[tipo_atual]})")
        print(formatar_status(info))
        print("-" * 60)
        print("1) Alterar tipo")
        print("2) Mostrar detalhes novamente")
        print("3) Gerar token informando data/hora")
        print("4) Gerar token relativo (X horas atrás)")
        print("5) Digitar token manualmente")
        print("0) Sair")
        print("-" * 60)
        opcao = input("Escolha uma opção: ").strip()
        if opcao == "1":
            tipo_atual = selecionar_tipo_interativo(tipo_atual)
        elif opcao == "2":
            print("\n" + formatar_status(info))
            aguardar()
        elif opcao == "3":
            ui_gerar_por_data(tipo_atual)
            aguardar()
        elif opcao == "4":
            ui_gerar_por_horas(tipo_atual)
            aguardar()
        elif opcao == "5":
            ui_definir_manual(tipo_atual)
            aguardar()
        elif opcao == "0":
            print("Saindo...")
            break
        else:
            print("⚠️ Opção inválida.")
            aguardar()


def construir_parser():
    parser = argparse.ArgumentParser(
        description="Gera e gerencia arquivos de since_token para os importadores."
    )
    parser.add_argument("--tipo", choices=SINCE_TOKEN_MAP.keys(), required=True, help="Qual since_token deseja manipular.")
    parser.add_argument("--inicio", help="Data/hora inicial (ex: 2025-12-10 08:00 ou 10/12/2025 08:00).")
    parser.add_argument("--fim", help="Data/hora final (opcional, apenas para referência).")
    parser.add_argument("--timezone", help="Nome IANA do timezone (ex: America/Manaus).")
    parser.add_argument("--offset", type=float, help="Offset UTC em horas (ex: -4). Ignorado se --timezone for informado.")
    parser.add_argument("--utc", action="store_true", help="Indica que as datas fornecidas já estão em UTC.")
    parser.add_argument("--aplicar", action="store_true", help="Sobrescreve o arquivo de since_token com o novo valor.")
    parser.add_argument("--forcar", action="store_true", help="Permite salvar tokens fora do limite de 7 dias (não recomendado).")
    parser.add_argument("--mostrar-atual", action="store_true", help="Apenas mostra o valor salvo atualmente e sai.")
    return parser


def executar_cli(args):
    if args.mostrar_atual:
        mostrar_atual(args.tipo)
        return

    if not args.inicio:
        raise SystemExit("--inicio é obrigatório (a não ser que --mostrar-atual seja usado).")

    tz = resolver_timezone(args)
    dt_inicio = parse_datetime(args.inicio, tz)
    token_inicio = datetime_para_token(dt_inicio)
    traduzido_inicio = traduzir_token(token_inicio, tz)

    print(f"[{args.tipo.upper()}] Timezone usado: {tz}")
    print(f"[{args.tipo.upper()}] Início informado: {dt_inicio.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"[{args.tipo.upper()}] SinceToken gerado: {token_inicio} ({traduzido_inicio})")

    if args.fim:
        dt_fim = parse_datetime(args.fim, tz)
        token_fim = datetime_para_token(dt_fim)
        janela = dt_fim - dt_inicio
        if janela.total_seconds() <= 0:
            print(f"[{args.tipo.upper()}] ⚠️ Atenção: fim é menor/igual ao início.")
        print(f"[{args.tipo.upper()}] Fim informado: {dt_fim.strftime('%Y-%m-%d %H:%M:%S %Z')} -> token {token_fim}")
        print(f"[{args.tipo.upper()}] Duração entre início e fim: {formatar_timedelta(abs(janela))}")

    valido, _, idade, limite = validar_idade_token(token_inicio)
    idade_txt = formatar_timedelta(idade) if idade else "idade desconhecida"
    print(f"[{args.tipo.upper()}] Idade do token: {idade_txt} | Limite da API: {formatar_timedelta(limite)}")

    if not valido and not args.forcar:
        raise SystemExit(
            f"O token está fora da janela aceita pela MiX (> {formatar_timedelta(limite)}). "
            "Use uma data mais recente ou rode com --forcar se realmente precisar."
        )

    if args.aplicar:
        salvar_token(args.tipo, token_inicio)
    else:
        destino = SINCE_TOKEN_MAP[args.tipo]
        print(f"[{args.tipo.upper()}] (modo somente leitura) Use --aplicar para gravar em {destino}")


def main():
    if len(sys.argv) == 1:
        executar_ui()
        return

    parser = construir_parser()
    args = parser.parse_args()
    executar_cli(args)


if __name__ == "__main__":
    main()
