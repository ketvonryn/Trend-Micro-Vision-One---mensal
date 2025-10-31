import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

# Cálculo do período (mês anterior)
primeiro_dia_mes_atual = datetime.now(timezone.utc).replace(
                 day=1, hour=0, minute=0, second=0, microsecond=0
                )

# Formatação no padrão ISO 8601 UTC (com 'Z')
startDateTime = (primeiro_dia_mes_atual - relativedelta(months=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
endDateTime = (primeiro_dia_mes_atual - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

# Função principal
def coletaWB(url_region, token):
    
    url_path = '/beta/xdr/workbench/alerts'
    url = url_region + url_path

    query_params = {
            "startDateTime": startDateTime,
            "endDateTime": endDateTime
        }

    headers = {'Authorization': 'Bearer ' + token}

    colunas_excluir = [
            "schemaVersion", "workbenchLink", "alertProvider", "modelId", "modelType",
            "ownerIds", "impactScope", "matchedRules", "indicators", "campaign",
            "industry", "regionAndCountry", "createdBy", "totalIndicatorCount",
            "matchedIndicatorCount", "reportLink", "matchedIndicatorPatterns"
        ]

    resultados = []
    contador = 0
    print("iniciando COLETA Workbenchs")
    
    #requisições WEB para coleta dos dados
    while url:
        try:
            resposta = requests.get(url, params=query_params, headers=headers)
        except Exception as e:
            print(f"COLETA WB [ERRO] Falha na requisição: {e}")
            break

        print(f"COLETA WB Status HTTP: {resposta.status_code}")

        if resposta.status_code != 200:
            print(f"[ERRO] Requisição falhou: {resposta.text}")
            break

        dados = resposta.json()
        itens = dados.get("items", [])
        resultados.extend(itens)
        contador += 1
        print(f"COLETA WB - requisição {contador}")

        url = dados.get("nextLink")
        query_params = None  # precisa apenas na primeira chamada

    if not resultados:
        return ("[ERRO] Nenhum dado retornado pela API que consulta Workbench.")
        
    df = pd.DataFrame(resultados).drop(columns=colunas_excluir, errors="ignore")
    print(f"Total de registros coletados: {len(df)}")
    return df