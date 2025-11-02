import os
import glob
import zipfile
import pandas as pd
from io import TextIOWrapper

def coletaZip_compliance(pasta, aba):
    """
    Busca e processa dados de arquivos ZIP contendo CSVs de compliance (SWP ou SEP).
    Retorna um DataFrame com os dados coletados ou uma string de erro.
    """

    # Mapeia o termo de busca de acordo com a aba informada
    if aba == "Compliance SWP":
        termo_busca = "Server & Workload Protection"
    elif aba == "Compliance SEP":
        termo_busca = "Standard Endpoint Protection"
    else:
        return f"[ERRO] Aba '{aba}' inválida. Use 'Compliance SWP' ou 'Compliance SEP'."

    # Localiza o arquivo ZIP alvo
    padrao_zip = "*Security*Configuration*.zip"
    try:
        caminho_zip = glob.glob(os.path.join(pasta, padrao_zip))[0]
    except IndexError:
        return f"[ERRO] Nenhum arquivo ZIP encontrado com o padrão: {padrao_zip}"

    # Função auxiliar para processar o CSV dentro do ZIP
    def processa_csv(zip_ref, termo_busca):
        try:
            csv_alvo = next(
                f for f in zip_ref.namelist()
                if f.startswith("csv/") and termo_busca in f and f.endswith(".csv")
            )
        except StopIteration:
            return pd.DataFrame(), f"[ERRO] CSV com termo '{termo_busca}' não encontrado no ZIP."

        try:
            with zip_ref.open(csv_alvo) as arquivo_csv:
                df = pd.read_csv(TextIOWrapper(arquivo_csv, encoding="utf-8"), usecols=[0, 1, 2])
                df["Enable %"] = (df["Feature enabled"] / df["Total endpoints"]).round(4)
                df = df.drop(columns=["Total endpoints", "Feature enabled"])
                df = df[["Feature name", "Enable %"]]
                print(f"dados de {aba} coletados com sucesso!")
                return df, None
        except Exception as e:
            return pd.DataFrame(), f"[ERRO] Falha ao processar CSV '{csv_alvo}': {e}"

    # Abre e processa o ZIP
    try:
        with zipfile.ZipFile(caminho_zip, "r") as zip_ref:
            df, erro = processa_csv(zip_ref, termo_busca)
            if erro:
                return erro
            if df.empty:
                return f"[AVISO] Nenhum dado encontrado para '{aba}'."
            return df
    except Exception as e:
        return f"[ERRO] Falha ao abrir ou processar o ZIP '{caminho_zip}': {e}"
