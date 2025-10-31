# Book Indicadores Trend Micro Vision One

Coleta relatórios (ZIP) e dados via API do Vision One, consolida tudo em um Excel com abas padronizadas, exibe a execução em tempo real numa interface grafica (além de registrar logs).

---

## Sumário
- [Como funciona o fluxo Principal]
- [Pré-requisitos](#pré-requisitos)
- [Configuração (.env)](#configuração-env)

---

## Como funciona o fluxo Principal
1. Inicializa **logging** e abre a **interface gráfica** (janela com área de logs).
2. Calcula a **`data_ref`** (sempre **1º dia do mês anterior**).
3. Cria o Excel **`{cliente}_base_dados_{data_ref}.xlsx`** (abas vazias).
4. Procura **.zip** dos relatórios na pasta de execução, processa indicadores, preenche excel e **apaga** os .zip após uso.
5. Faz **coletas via API** (workbenchs, endpoint inventory e vulnerabilidades) e alimenta as abas do Excel.
6. Mostra **status/logs** no terminal e na GUI durante todo o processo.
7. Finaliza com o Excel.

---

## Pré-requisitos
RODAR .exe:
- **Internet** para chamadas à API do Vision One.
- Pasta de trabalho contendo:
  - o executavel
  - `.env` (variáveis locais com as chaves de API)
  - **Relatórios ZIP** do Vision One (baixados pelo schedule report e salvos na pasta de forma autoamtica pelo Power automate ou manualmente)
- **Permissão de escrita** na pasta (para o exe)
- liberação do .exe no antivirus
  
RODAR VIA PYTHON (codigo puro), precisa das seguintes bibliotecas:
- Python
- pandas
- openpyxl
- xlsxwriter
- python-dotenv
- requests
- python-dateutil

---

## Configuração (.env)
Crie um arquivo **`.env`** na mesma pasta onde você vai rodar o app (ou ao lado do `.exe`, se empacotado):

```ini
# Região/endpoint, padrão do Brasil é USA
url_region=https://api.xdr.trendmicro.com/

# Identificação
cliente=nome

# Token de API (perfil auditor)
token=SEU_TOKEN_AQUI

# pasta (opcional)
pasta=



