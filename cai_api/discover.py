"""Descobre os IDs necessários e mostra o estado do projeto/job.
  python cai_api/discover.py                 # projeto, job e últimas execuções
  python cai_api/discover.py --runtimes      # runtimes disponíveis (para escolher a Python 3.10)
  python cai_api/discover.py --export        # grava cai_api/.cai_ids.json e imprime exports de shell"""
import argparse
import json

import cai_common as C

ap = argparse.ArgumentParser()
ap.add_argument("--runtimes", action="store_true")
ap.add_argument("--export", action="store_true")
ap.add_argument("--runs", type=int, default=5)
a = ap.parse_args()

if a.runtimes:
    for r in C.list_runtimes():
        print(f'{r.get("kernel", "?"):14} {r.get("editor", "?"):12} {r.get("edition", "?"):10} '
              f'{str(r.get("full_version", "")):16} {r.get("image_identifier", r.get("id"))}')
    raise SystemExit(0)

pid = C.project_id()
print(f"projeto: {pid}")
job = C.find_job(pid)
if not job:
    print("job: (ainda não existe) -> python cai_api/setup_job.py")
else:
    print(f'job:     {job["id"]}  nome={job["name"]}  script={job.get("script")}  schedule={job.get("schedule") or "manual"}')
    runs = list(C.paged(f'/projects/{pid}/jobs/{job["id"]}/runs', "job_runs"))
    for r in sorted(runs, key=lambda x: str(x.get("created_at", "")), reverse=True)[:a.runs]:
        print(f'  run {r["id"]}  {r.get("status")}  {r.get("created_at", "")}')
    C.save_ids(project_id=pid, job_id=job["id"])
if a.export:
    ids = C.load_ids()
    print(f'export CAI_PROJECT_ID="{ids.get("project_id", "")}"\nexport CAI_JOB_ID="{ids.get("job_id", "")}"')
