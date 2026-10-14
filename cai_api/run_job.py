"""Dispara uma execução do Job e (opcionalmente) aguarda o resultado.
  python cai_api/run_job.py                  # dispara e retorna o id da execução
  python cai_api/run_job.py --wait           # aguarda; código de saída 0 = sucesso, 1 = falha
  python cai_api/run_job.py --status <run_id>   # consulta uma execução
  python cai_api/run_job.py --logs <run_id>     # tenta baixar os logs"""
import argparse
import json

import cai_common as C

ap = argparse.ArgumentParser()
ap.add_argument("--wait", action="store_true")
ap.add_argument("--timeout", type=int, default=1800)
ap.add_argument("--status", metavar="RUN_ID")
ap.add_argument("--logs", metavar="RUN_ID")
a = ap.parse_args()

pid = C.project_id()
jid = C.job_id(pid)
if a.status:
    print(json.dumps(C.api("GET", f"/projects/{pid}/jobs/{jid}/runs/{a.status}"), indent=2))
    raise SystemExit(0)
if a.logs:
    out = C.api("GET", f"/projects/{pid}/jobs/{jid}/runs/{a.logs}/logs", quiet=True)
    print(out.get("logs", json.dumps(out, indent=2)) if out else "logs indisponíveis por esta API; veja a UI do CAI.")
    raise SystemExit(0)

run = C.api("POST", f"/projects/{pid}/jobs/{jid}/runs", {})
rid = run["id"]
print(f"execução iniciada: run_id={rid} status={run.get('status')}")
if a.wait:
    final = C.wait_run(pid, jid, rid, a.timeout)
    ok = C.is_success(C.run_state(final))
    print("RESULTADO:", "sucesso" if ok else f"falha ({C.run_state(final)})")
    raise SystemExit(0 if ok else 1)
