"""Teste local dos scripts contra um servidor simulado (não acessa o CAI). Uso: python cai_api/selftest_api.py"""
import json, os, subprocess, sys, threading
from http.server import BaseHTTPRequestHandler, HTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
STATE = {"jobs": [], "polls": 0, "bodies": []}


class H(BaseHTTPRequestHandler):
    def _send(self, obj, code=200):
        b = json.dumps(obj).encode(); self.send_response(code); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)

    def _body(self):
        n = int(self.headers.get("Content-Length", 0)); return json.loads(self.rfile.read(n) or b"{}")

    def log_message(self, *a): pass

    def do_GET(self):
        assert self.headers["Authorization"] == "Bearer K"
        p = self.path.split("?")[0]
        if p == "/api/v2/projects": return self._send({"projects": [{"id": "p1", "name": "mlflow-cai"}]})
        if p == "/api/v2/runtimes":
            return self._send({"runtimes": [
                {"image_identifier": "img-py39", "kernel": "Python 3.9", "editor": "Workbench", "edition": "Standard", "full_version": "2024.1", "status": "ENABLED"},
                {"image_identifier": "img-py310-old", "kernel": "Python 3.10", "editor": "Workbench", "edition": "Standard", "full_version": "2023.12", "status": "ENABLED"},
                {"image_identifier": "img-py310-new", "kernel": "Python 3.10", "editor": "Workbench", "edition": "Standard", "full_version": "2024.05", "status": "ENABLED"},
                {"image_identifier": "img-py310-jup", "kernel": "Python 3.10", "editor": "JupyterLab", "edition": "Standard", "full_version": "2024.05", "status": "ENABLED"}]})
        if p == "/api/v2/projects/p1/jobs": return self._send({"jobs": STATE["jobs"]})
        if p.endswith("/runs"): return self._send({"job_runs": [{"id": "r1", "status": "ENGINE_SUCCEEDED", "created_at": "2026"}]})
        if "/runs/" in p:
            STATE["polls"] += 1
            return self._send({"id": "r1", "status": "ENGINE_RUNNING" if STATE["polls"] < 2 else "ENGINE_SUCCEEDED"})
        self._send({}, 404)

    def do_POST(self):
        b = self._body(); p = self.path
        if p.endswith("/jobs"):
            STATE["bodies"].append(b); job = {**b, "id": "j1"}; STATE["jobs"].append(job); return self._send(job)
        if p.endswith("/runs"): return self._send({"id": "r1", "status": "ENGINE_SCHEDULING"})
        self._send({}, 404)

    def do_PATCH(self):
        b = self._body(); STATE["bodies"].append(b); STATE["jobs"][0].update(b); self._send(STATE["jobs"][0])


srv = HTTPServer(("127.0.0.1", 0), H)
threading.Thread(target=srv.serve_forever, daemon=True).start()
env = {**os.environ, "CAI_DOMAIN": f"http://127.0.0.1:{srv.server_port}", "CAI_API_KEY": "K", "CAI_PROJECT_NAME": "mlflow-cai"}
for k in ("CAI_PROJECT_ID", "CAI_JOB_ID", "CDSW_PROJECT_ID"): env.pop(k, None)
run = lambda *a: subprocess.run([sys.executable, os.path.join(HERE, a[0]), *a[1:]], env=env, capture_output=True, text=True)

r = run("setup_job.py"); assert r.returncode == 0, r.stderr
assert STATE["bodies"][0]["runtime_identifier"] == "img-py310-new" and STATE["bodies"][0]["schedule"] == "*/15 * * * *"
assert STATE["bodies"][0]["script"] == "bootstrap.py"
r = run("setup_job.py", "--manual", "--cpu", "2"); assert r.returncode == 0 and "atualizando" in r.stdout
assert "schedule" not in STATE["bodies"][1] and len(STATE["jobs"]) == 1
r = run("discover.py"); assert "j1" in r.stdout and "ENGINE_SUCCEEDED" in r.stdout, r.stdout
r = run("run_job.py", "--wait"); assert r.returncode == 0 and "sucesso" in r.stdout, r.stdout + r.stderr
env["CAI_API_KEY"] = "ERRADA"
print("OK: selftest_api passou")
