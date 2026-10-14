"""Cria (ou atualiza) o Job do CAI que roda bootstrap.py -> eval_job.py.
  python cai_api/setup_job.py                         # cron */15, runtime Python 3.10 Workbench Standard
  python cai_api/setup_job.py --manual                # sem agendamento
  python cai_api/setup_job.py --cron "*/5 * * * *" --cpu 2 --memory 4
  python cai_api/setup_job.py --dry-run               # mostra o payload sem enviar
As variáveis do projeto (Project Settings > Advanced) e o env.sh já valem para o Job; use --env só para extras."""
import argparse
import json

import cai_common as C

ap = argparse.ArgumentParser()
ap.add_argument("--name", default=C._env("CAI_JOB_NAME", default="genai-eval"))
ap.add_argument("--script", default="bootstrap.py")
ap.add_argument("--cron", default="*/15 * * * *")
ap.add_argument("--manual", action="store_true", help="sem agendamento")
ap.add_argument("--cpu", type=float, default=1)
ap.add_argument("--memory", type=float, default=2, help="GB")
ap.add_argument("--timeout", type=int, help="tempo máximo da execução, em segundos")
ap.add_argument("--kernel", default="Python 3.10")
ap.add_argument("--editor", default="Workbench", help="Workbench | JupyterLab")
ap.add_argument("--edition", default="Standard")
ap.add_argument("--runtime-id", help="image_identifier da runtime (sobrescreve a escolha automática)")
ap.add_argument("--env", action="append", default=[], metavar="CHAVE=VALOR")
ap.add_argument("--dry-run", action="store_true")
a = ap.parse_args()

pid = C.project_id()
body = {"name": a.name, "script": a.script, "cpu": a.cpu, "memory": a.memory,
        "runtime_identifier": C.pick_runtime(a.kernel, a.editor, a.edition, a.runtime_id)}
if not a.manual:
    body["schedule"] = a.cron
if a.timeout:
    body["timeout"] = a.timeout
if a.env:
    body["environment"] = dict(e.split("=", 1) for e in a.env)

existing = C.find_job(pid, a.name)
print(("atualizando" if existing else "criando"), "job:", json.dumps(body, ensure_ascii=False))
if a.dry_run:
    raise SystemExit(0)
job = (C.api("PATCH", f'/projects/{pid}/jobs/{existing["id"]}', body) if existing
       else C.api("POST", f"/projects/{pid}/jobs", body))
jid = (existing or job)["id"]
C.save_ids(project_id=pid, job_id=jid, job_name=a.name)
print(f"OK. job_id={jid}  (salvo em cai_api/.cai_ids.json)")
print("Para executar agora: python cai_api/run_job.py --wait")
