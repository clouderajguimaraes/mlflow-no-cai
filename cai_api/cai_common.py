"""Cliente REST mínimo da API v2 do Cloudera AI (só precisa de `requests`).

Configuração (variáveis de ambiente ou arquivo cai_api/api.env.sh):
  CAI_DOMAIN        domínio do workbench (ex.: ml-xxxx.cloudera.site)   [dentro do CAI: CDSW_DOMAIN]
  CAI_API_KEY       API key (criada manualmente em User Settings)       [dentro do CAI: CDSW_APIV2_KEY]
  CAI_PROJECT_NAME  nome do projeto (padrão: mlflow-cai)  |  CAI_PROJECT_ID  (dentro do CAI: CDSW_PROJECT_ID)
  CAI_JOB_NAME      nome do job (padrão: genai-eval)      |  CAI_JOB_ID
  CAI_CA            (opcional) CA para validar o TLS do workbench
"""
import json
import os
import sys
import time

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from common import load_env_file  # noqa: E402

load_env_file(os.path.join(HERE, "api.env.sh"))
IDS_FILE = os.path.join(HERE, ".cai_ids.json")
TERMINAL_OK = ("SUCCEEDED", "SUCCESS")
TERMINAL_BAD = ("FAILED", "STOPPED", "TIMEOUT", "KILLED", "ERROR", "CANCEL")


class CaiError(SystemExit):
    pass


def die(msg):
    print(f"ERRO: {msg}", file=sys.stderr)
    raise CaiError(1)


def _env(*names, default=None):
    for n in names:
        if os.getenv(n):
            return os.environ[n]
    return default


def base_url():
    d = _env("CAI_DOMAIN", "CDSW_DOMAIN")
    if not d:
        die("defina CAI_DOMAIN (ex.: ml-xxxx.cloudera.site) em cai_api/api.env.sh")
    return (d if d.startswith("http") else "https://" + d).rstrip("/")


def api_key():
    k = _env("CAI_API_KEY", "CDSW_APIV2_KEY")
    if not k:
        die("defina CAI_API_KEY. Crie a chave em User Settings > API Keys (único passo manual).")
    return k


def api(method, path, body=None, params=None, ok=(200, 201, 202, 204), quiet=False):
    r = requests.request(method, f"{base_url()}/api/v2{path}", params=params, json=body, timeout=60,
                         headers={"Authorization": f"Bearer {api_key()}"},
                         verify=os.getenv("CAI_CA") or True)
    if r.status_code not in ok:
        if quiet:
            return None
        die(f"{method} {path} -> HTTP {r.status_code}: {r.text[:500]}")
    return r.json() if r.text.strip() else {}


def paged(path, key, params=None):
    """Itera todas as páginas de uma listagem (`next_page_token`)."""
    params, token = dict(params or {}, page_size=100), None
    while True:
        if token:
            params["page_token"] = token
        data = api("GET", path, params=params)
        yield from data.get(key, [])
        token = data.get("next_page_token")
        if not token:
            return


def load_ids():
    return json.load(open(IDS_FILE)) if os.path.isfile(IDS_FILE) else {}


def save_ids(**kw):
    ids = {**load_ids(), **{k: v for k, v in kw.items() if v}}
    json.dump(ids, open(IDS_FILE, "w"), indent=2)


def project_id():
    pid = _env("CAI_PROJECT_ID", "CDSW_PROJECT_ID")
    if pid:
        return pid
    name = _env("CAI_PROJECT_NAME", default="mlflow-cai")
    for p in paged("/projects", "projects"):
        if p.get("name") == name:
            save_ids(project_id=p["id"], project_name=name)
            return p["id"]
    die(f"projeto '{name}' não encontrado. Ajuste CAI_PROJECT_NAME ou defina CAI_PROJECT_ID.")


def find_job(pid, name=None):
    name = name or _env("CAI_JOB_NAME", default="genai-eval")
    for j in paged(f"/projects/{pid}/jobs", "jobs"):
        if j.get("name") == name:
            return j
    return None


def job_id(pid):
    jid = _env("CAI_JOB_ID")
    if jid:
        return jid
    j = find_job(pid)
    if not j:
        die("job não encontrado. Rode antes: python cai_api/setup_job.py")
    save_ids(job_id=j["id"], job_name=j["name"])
    return j["id"]


def list_runtimes():
    return list(paged("/runtimes", "runtimes"))


def pick_runtime(kernel="Python 3.10", editor="Workbench", edition="Standard", runtime_id=None):
    rts = list_runtimes()
    if runtime_id:
        return runtime_id
    cand = [r for r in rts if kernel.lower() in str(r.get("kernel", "")).lower()
            and editor.lower() in str(r.get("editor", "")).lower()
            and edition.lower() in str(r.get("edition", "")).lower()
            and str(r.get("status", "ENABLED")).upper() in ("ENABLED", "AVAILABLE", "")]
    if not cand:
        die(f"nenhuma runtime '{kernel} / {editor} / {edition}' encontrada. "
            "Liste com: python cai_api/discover.py --runtimes e informe --runtime-id.")
    cand.sort(key=lambda r: str(r.get("full_version", r.get("version", ""))))
    return cand[-1].get("image_identifier") or cand[-1].get("id")


def run_state(run):
    return str(run.get("status", "")).upper()


def is_terminal(status):
    return any(t in status for t in TERMINAL_OK + TERMINAL_BAD)


def is_success(status):
    return any(t in status for t in TERMINAL_OK)


def wait_run(pid, jid, rid, timeout=1800, every=10):
    t0 = time.time()
    while True:
        run = api("GET", f"/projects/{pid}/jobs/{jid}/runs/{rid}")
        st = run_state(run)
        print(f"  status: {st or '?'}  ({int(time.time() - t0)}s)", flush=True)
        if is_terminal(st):
            return run
        if time.time() - t0 > timeout:
            die(f"timeout aguardando a execução {rid}")
        time.sleep(every)
