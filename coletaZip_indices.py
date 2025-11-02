import os
import glob
import zipfile
import pandas as pd
from datetime import date, timedelta

#data que aparece na primeira coluna, para indicar a referencia dos dados - sempre dia 1 do mes anterior
data_ref = (date.today().replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%d/%m/%Y")

def coletaZip_indices(pasta, arquivo_excel, cliente, data_ref=data_ref):

    # Cria dataframe base com os nomes das colunas
    dados = pd.DataFrame([{
            "ano_mes_ref": data_ref,
            "risk": 0,
            "exposure": 0,
            "attack": 0,
            "security": 0
        }])
    
    #Busca o .zip, extrai o .csv alvo, calcula a média e deleta o .zip após processar
    def extrai_indicador(pasta, padrao_zip, padrao_csv, coluna, amostra=30):
        
        try:
            caminho_zip = glob.glob(os.path.join(pasta, padrao_zip))[0]
        except IndexError:
            print(f"[ERRO] Arquivo ZIP não encontrado: {padrao_zip}")
            return 0

        try:
            with zipfile.ZipFile(caminho_zip, 'r') as z:
                # Localiza o CSV alvo dentro do ZIP
                csv_alvo = [
                    f for f in z.namelist()
                    if f.startswith("csv/") and padrao_csv in f and f.endswith(".csv")
                ][0]

                with z.open(csv_alvo) as f:
                    indice = pd.read_csv(f)[coluna].head(amostra).mean().round(2)
                    print(f"calculado {padrao_csv}: {indice}")

        except Exception as e:
            print(f"[ERRO] Falha ao processar {caminho_zip}: {e}")
            indice = 0

        # Após processar, tenta excluir o ZIP
        try:
            os.remove(caminho_zip)
            print(f"Arquivo ZIP removido: {os.path.basename(caminho_zip)}")
        except Exception as e:
            print(f"[AVISO] Não foi possível deletar {caminho_zip}: {e}")

        return indice

    # Coleta indicadores
    dados["risk"] = extrai_indicador(pasta,"*Risk*.zip", "Cyber Risk Index", "Your company")
    dados["exposure"] = extrai_indicador(pasta,"*Exposure*.zip", "Exposure Index", "Your company")
    dados["attack"] = extrai_indicador(pasta,"*Attack*.zip", "AttackIndex", "Your company")
    dados["security"] = extrai_indicador(pasta,"*Security*Configuration*.zip", "Security Configuration Index", "Your organization")

    # Salva no arquivo Excel
    nome_aba = "Indices"
    try:
        with pd.ExcelWriter(arquivo_excel, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
            dados.to_excel(writer, sheet_name=nome_aba, index=False, header=True)

        return "aba Indices populada com sucesso!"

    except Exception as e:
        print(f"[ERRO] Falha ao salvar no Excel: {e}")

        return None
