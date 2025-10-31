import requests
import zipfile
import pandas as pd
import json
from io import BytesIO
import time
from urllib.parse import urljoin, urlparse, parse_qs

def coleta_vulns(
    url_region: str,
    token: str,
    poll_interval: int = 20,
    max_wait_seconds: int = 10 * 60,
    max_restarts: int = 2,
    stuck_minutes: int = 5
):
    """
    Coleta vulnerabilidades (ASRM) e retorna um DataFrame com uma linha por CVE.
    Em caso de erro, retorna **uma string** descrevendo o erro (em vez de lançar exceção).
    """
    try:
        # Garante barra final na base
        if not url_region.endswith('/'):
            url_region = url_region + '/'

        url_path = 'beta/asrm/vulnerableDevices/export'
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json;charset=utf-8',
            'Accept': 'application/json'
        }

        def start_export() -> str:
            """Inicia o export e retorna a Operation-Location resolvida."""
            r = requests.post(urljoin(url_region, url_path), headers=headers, json={})
            if r.status_code != 202:
                raise RuntimeError(f"Erro ao iniciar exportação: HTTP {r.status_code}, body={r.text}")
            op = r.headers.get('Operation-Location')
            if not op:
                raise RuntimeError("Operation-Location não encontrado na resposta do POST.")
            # Completa se vier relativo
            if op.startswith(('beta/', 'v3/', 'api/')):
                op = urljoin(url_region, op)
            print(f"[Vulns] CREM Operation-Location: {op}")
            return op

        def poll_until_done(operation_url: str) -> str:
            """
            Faz polling até finalizar ou estourar timeout. Tenta reiniciar se preso.
            Retorna a URL de download (resource/result uri).
            """
            start_ts = time.time()
            attempts = 0
            backoff = poll_interval
            restarts = 0

            last_status = None
            last_progress = None
            last_change_ts = start_ts

            terminal_success = {"succeeded", "completed", "done", "success"}
            terminal_fail = {"failed", "cancelled", "canceled", "error", "timeout"}

            while True:
                attempts += 1
                elapsed = time.time() - start_ts
                if elapsed > max_wait_seconds:
                    raise TimeoutError(f"Polling excedeu {max_wait_seconds}s; último status={last_status}, progress={last_progress}")

                resp = requests.get(operation_url, headers=headers)
                try:
                    payload = resp.json()
                except Exception:
                    raise RuntimeError(f"Não foi possível decodificar JSON do status (HTTP {resp.status_code}): {resp.text}")

                status = (payload.get("status") or "").lower()
                progress = payload.get("percentage") or payload.get("progress") or payload.get("percentComplete")
                print(f"[Vulns] CREM Status da tarefa: {status} | progress={progress} | tent.{attempts}")

                # Detecta mudança de status/progresso
                if status != last_status or progress != last_progress:
                    last_status, last_progress = status, progress
                    last_change_ts = time.time()

                # Sucesso
                if status in terminal_success:
                    download_url = (
                        payload.get("resourceLocation")
                        or payload.get("resultLocation")
                        or payload.get("resourceUri")
                        or payload.get("resultUri")
                    )
                    if not download_url:
                        raise RuntimeError(f"Job finalizou sem URL de download. Resposta: {payload}")
                    return download_url

                # Falha terminal
                if status in terminal_fail:
                    raise RuntimeError(f"Exportação não concluída (status: {status}). Resposta: {payload}")

                # Circuit breaker: preso sem progresso por stuck_minutes
                stuck_seconds = time.time() - last_change_ts
                if status in {"running", "inprogress", "in_progress", "queued", "accepted"} and (progress in (None, 0)):
                    if stuck_seconds > stuck_minutes * 60:
                        if restarts < max_restarts:
                            restarts += 1
                            print(f"[Vulns] CREM Job preso há {int(stuck_seconds)}s; reiniciando export (#{restarts})...")
                            operation_url = start_export()
                            # Reinicia contadores locais, mantém relógio global p/ timeout total
                            attempts = 0
                            backoff = poll_interval
                            last_status = None
                            last_progress = None
                            last_change_ts = time.time()
                            continue
                        else:
                            raise TimeoutError(f"Job preso em '{status}' sem progresso por {stuck_seconds:.0f}s após {restarts} reinícios.")

                # Aguardar próximo poll com backoff (máx 60s)
                time.sleep(backoff)
                backoff = min(backoff + 5, 60)

        # 1) Inicia export
        operation_url = start_export()
        # 2) Poll até finalizar (com circuit breaker)
        download_url = poll_until_done(operation_url)

        # 3) Baixa o ZIP
        qs = parse_qs(urlparse(download_url).query)
        is_presigned_s3 = any(k.lower().startswith('x-amz-') for k in qs.keys())

        dl_headers = {'Accept': 'application/zip,application/json'}
        if not is_presigned_s3:
            dl_headers['Authorization'] = f'Bearer {token}'

        dl_resp = requests.get(download_url, headers=dl_headers, allow_redirects=True)
        if dl_resp.status_code >= 400:
            raise RuntimeError(f"Falha ao baixar export: HTTP {dl_resp.status_code}, body={dl_resp.text}")
        content = dl_resp.content

        # 4) Descompacta e lê JSONs
        all_items = []
        with zipfile.ZipFile(BytesIO(content)) as z:
            for filename in z.namelist():
                if filename.lower().endswith(".json"):
                    with z.open(filename) as f:
                        data = json.load(f)
                        items = data.get("items", [])
                        for item in items:
                            # Limpa campos pesados/sensíveis
                            item.pop("ip", None)
                            # Enxuga cveRecords
                            if "cveRecords" in item and isinstance(item["cveRecords"], list):
                                for cve in item["cveRecords"]:
                                    if isinstance(cve, dict):
                                        for campo in ("protectionRules", "mitigationOption"):
                                            cve.pop(campo, None)
                            all_items.append(item)
                    print(f"[Vulns] Lido {len(items)} itens de {filename}")

        if not all_items:
            raise ValueError("Nenhum item encontrado dentro dos arquivos JSON do export.")

        # 5) DataFrame com uma linha por CVE
        df = pd.DataFrame(all_items)
        if "cveRecords" not in df.columns:
            # Caso raro: export sem lista de CVEs
            return df

        df = df.explode("cveRecords").reset_index(drop=True)
        cve_df = pd.concat([df.drop(columns=["cveRecords"]), df["cveRecords"].apply(pd.Series)], axis=1)
        return cve_df

    except Exception as e:
        # Em qualquer erro, devolve uma string, não lança
        return f"ERRO na coleta_vulnerabilidades: {e.__class__.__name__}: {e}"
