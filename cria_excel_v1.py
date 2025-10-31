from pathlib import Path
import re
import pandas as pd

def criar_planilha(cliente, data_ref):
    """
    Cria um arquivo Excel no formato {cliente}_base_dados_{data_ref}.xlsx
    com as abas:
      - Compliance SWP
      - Compliance SEP
      - Indices
      - Alertas WB
      - endpoint inventory
      - vulnerabilidades
    """
    # 2) Monta o nome do arquivo e o caminho final
    data_ref = data_ref.replace("/", "_")
    arquivo_excel = f"{cliente}_base_dados_{data_ref}.xlsx"
    caminho_arquivo = Path.cwd() / arquivo_excel 

    print(f"[INFO] Iniciando criação da planilha para cliente='{cliente}' e data_ref='{data_ref}'.")
    print(f"[INFO] Nome do arquivo definido: {arquivo_excel}")

    # 3) Define as abas solicitadas
    abas = [
        "Compliance SWP",
        "Compliance SEP",
        "Indices",
        "Alertas WB",
        "endpoint inventory",
        "vulnerabilidades",
    ]

    # 4) Cria o Excel com abas vazias usando pandas + ExcelWriter
    try:
        print("[INFO] Criando arquivo Excel e adicionando abas...")
        with pd.ExcelWriter(caminho_arquivo, engine="xlsxwriter") as writer:
            for aba in abas:
                print(f"  - Criando aba: {aba}")
                # Escreve um DataFrame vazio apenas para criar a aba
                pd.DataFrame().to_excel(writer, sheet_name=aba, index=False)
        print("[SUCESSO] Arquivo Excel criado com sucesso.")
    except Exception as e:
        print(f"[ERRO] Falha ao criar o arquivo Excel: {e}")
        # Propaga o erro para o chamador decidir como tratar
        raise

    # 5) Retorna o nome do arquivo (conforme solicitado)
    return arquivo_excel