from dotenv import load_dotenv
import os
import pandas as pd
import sys
from datetime import date, datetime, timedelta

from coleta_EI import coleta_exportacao_trend
from coletaZip_indices import coletaZip_indices
from coletaZip_compliance import coletaZip_compliance
from coletaWB import coletaWB
from atualizaAba_excel import atualiza_aba
from def_vulns import coleta_vulns
from cria_excel_v1 import criar_planilha
from Interface_grafica import LogViewer

load_dotenv()
cliente = os.getenv("cliente")
pasta = (os.getenv("pasta") or "")
url_region = os.getenv("url_region")
token = os.getenv("token")

# registrador de logs ------------------------------------------------------

class Tee:
    def __init__(self, *streams):
        self.streams = [s for s in streams if s is not None]

    def write(self, data):
        if data.strip():  # ignora quebras de linha extras
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data = f"[{timestamp}] {data}"
        for s in self.streams:
            s.write(data)
            s.flush()

    def flush(self):
        for s in self.streams:
            try:
                s.flush()
            except Exception:
                pass


os.makedirs("logs", exist_ok=True)
log = open(f"logs/log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log",
           "w", encoding="utf-8")  # cria arquivo log com data atual
sys.stdout = Tee(sys.stdout, log)

# ===================== cria interface grafica

# 1) cria a janela e inicia (não bloqueia)
viewer = LogViewer()
viewer.start()

# 2) reencaminha a saída também para a GUI (envolve o Tee atual)
# agora: terminal, arquivo e GUI
sys.stdout = Tee(sys.stdout, log, viewer.stream)


# ========================================== INICIO ==========================================--------------------------

print(f"INICIO - execução para cliente {cliente}")

# data que aparece na primeira coluna, para indicar a referencia dos dados - sempre dia 1 do mes anterior
data_ref = (date.today().replace(day=1) - timedelta(days=1)
            ).replace(day=1).strftime("%d/%m/%Y")
print(f"[main] data de referencia do book: {data_ref}")

# cria arquivo no excel para ser a base de dados do Vision One com o nome {cliente}_base_dados_{data_ref}.xlsx
arquivo_excel = criar_planilha(cliente, data_ref)
print(f"[main] nome do arquivo criado {arquivo_excel}")

# Coleta dados do executive dashboard do .zip security configuration e popula o excel (aba Compliance SWP e Compliance SEP)
result = coletaZip_compliance(pasta, "Compliance SWP")
print(result) if not isinstance(result, pd.DataFrame) else atualiza_aba(
    arquivo_excel=arquivo_excel,
    aba="Compliance SWP",
    dados=result,
    colunas_adicionais=[("ano_mes_ref", data_ref)]
)
result = coletaZip_compliance(pasta, "Compliance SEP")
print(result) if not isinstance(result, pd.DataFrame) else atualiza_aba(
    arquivo_excel=arquivo_excel,
    aba="Compliance SEP",
    dados=result,
    colunas_adicionais=[("ano_mes_ref", data_ref)]
)

# Coleta indices do executive dashboard nos .zip, popula o excel (aba Indices) e deleta os arquivos
# --> precisa rodar depois que as abas de compliance foram preenchidas
print(coletaZip_indices(pasta, arquivo_excel, cliente))

# coletar dados de Workbenchs
result = coletaWB(url_region, token)
print(result) if not isinstance(result, pd.DataFrame) else atualiza_aba(
    arquivo_excel=arquivo_excel,
    aba="Alertas WB",
    dados=result,
    colunas_adicionais=[("ano_mes_ref", data_ref)]
)

# Coleta dados do Endpoint Inventory
result = coleta_exportacao_trend(url_region, token)
print(result) if not isinstance(result, pd.DataFrame) else atualiza_aba(
    arquivo_excel=arquivo_excel,
    aba="endpoint inventory",
    dados=result,
    colunas_adicionais=[("ano_mes_ref", data_ref)]
)

# coletar dados de vulnerabilidades
# retorno precisa estar em dataframe, se for string é a mensagem de erro
result = coleta_vulns(url_region, token)
print(result) if not isinstance(result, pd.DataFrame) else atualiza_aba(
    arquivo_excel=arquivo_excel,
    aba="vulnerabilidades",
    dados=result,
    colunas_adicionais=[("ano_mes_ref", data_ref)]
)


print("[main] fim da execução! em caso de dúvidas consulte o arquivo de logs dessa execução")
