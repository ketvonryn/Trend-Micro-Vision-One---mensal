import requests
import zipfile
import pandas as pd
import json
import time
from io import BytesIO


def coleta_exportacao_trend(url_region, token, tipo_export = "inventory", tempo_espera=30, tentativas_max=20):
    """
    Executa exportações da API Trend Micro (Inventory, Vulnerabilidades, Contas Comprometidas)
    e retorna um DataFrame ou uma string de erro.
    """
    tipo_export = "inventory"
    print(f"\n[INÍCIO] Iniciando coleta de dados para: {tipo_export.upper()}")

    endpoints = {
        "inventory": "v3.0/endpointSecurity/endpoints/export",
    }
    endpoint_path = endpoints[tipo_export]

    # POST inicial
    print("EI - Enviando solicitação de exportação...")

    try:
        post_url = url_region+endpoint_path
        query_params = {}
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json;charset=utf-8"
        }
        body = {}

        r = requests.post(post_url, params=query_params, headers=headers, json=body, timeout=120)

    except Exception as e:
        return f"[ERRO] Falha no POST inicial: {e}"


    if r.status_code != 202:
        return f"[ERRO] Exportação não iniciada: HTTP {r.status_code} - {r.text[:300]}"

    operation_url = r.headers.get("Operation-Location") or r.headers.get("Location")
    if not operation_url:
        return "[ERRO] Operation-Location ausente na resposta da API."

    print("EI - Exportação iniciada. Aguardando processamento...")

    # Polling de status
    download_url = None
    for i in range(tentativas_max):
        try:
            resp = requests.get(operation_url, headers=headers, timeout=120)
            payload = resp.json()
        except Exception as e:
            return f"[ERRO] Falha ao consultar status: {e}"

        status_raw = str(payload.get("status", "")).lower()
        print(f"   > Tentativa {i+1}/{tentativas_max} - status: {status_raw}")

        if "succeed" in status_raw or "complete" in status_raw:
            download_url = (
                payload.get("resourceLocation")
                or (payload.get("result") or {}).get("resourceLocation")
                or payload.get("location")
            )
            break
        elif "fail" in status_raw or "cancel" in status_raw:
            return f"[ERRO] Exportação falhou (status: {status_raw})."

        time.sleep(tempo_espera)
    else:
        return f"[ERRO] Tempo limite excedido aguardando exportação de {tipo_export}"

    if not download_url:
        return "[ERRO] URL de download não encontrada após conclusão."

    print("EI - Exportação concluída! Baixando arquivo ZIP...")

    # Download do ZIP
    try:
        blob = (requests.get(download_url, timeout=120)).content
        print("EI - Download concluído.")
    except Exception as e:
        return f"[ERRO] Falha no download do arquivo ZIP: {e}"

    # Extração e processamento
    print("EI - Processando arquivo ZIP...")

    try:
        with zipfile.ZipFile(BytesIO(blob)) as z:
            for name in z.namelist():
                print(f"   > Encontrado arquivo: {name}")

                # Detecta tipo do arquivo
                if name.lower().endswith(".csv"):
                    print("   > Lendo arquivo CSV...")
                    with z.open(name) as f:
                        df = pd.read_csv(f)
                    print(f"   > Total de registros: {len(df)}")
                    return df

                elif name.lower().endswith(".json"):
                    print("   > Lendo arquivo JSON...")
                    with z.open(name) as f:
                        dados_json = json.load(f)

                    items = dados_json.get("items", dados_json)

                    if not items:
                        return "[ERRO] Nenhum item encontrado no arquivo JSON."

                    df = pd.json_normalize(items)

                    print(f"   > Total de registros: {len(df)}")
                    return df

        return "[ERRO] Nenhum arquivo CSV ou JSON encontrado dentro do ZIP."

    except Exception as e:
        return f"[ERRO] Falha ao processar o arquivo ZIP: {e}"
