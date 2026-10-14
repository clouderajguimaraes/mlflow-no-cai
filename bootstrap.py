"""Script de entrada do Job do CAI: instala dependências (só quando o requirements.txt mudou)
e em seguida executa a avaliação (eval_job.main) no mesmo processo.
Compatível com runtimes PBJ (o script roda como célula, sem __file__)."""
import hashlib
import os
import subprocess
import sys

try:
    ROOT = os.path.dirname(os.path.abspath(__file__))
except NameError:                      # runtime PBJ: __file__ não existe; o Job roda na raiz do projeto
    ROOT = os.getcwd()
os.chdir(ROOT)
sys.path.insert(0, ROOT)

digest = hashlib.sha256(open("requirements.txt", "rb").read()).hexdigest()
marker = os.path.join(os.getenv("STATE_DIR", "state"), ".requirements.sha")
if not (os.path.isfile(marker) and open(marker).read() == digest):
    print("bootstrap: instalando dependências...", flush=True)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-r", "requirements.txt"])
    os.makedirs(os.path.dirname(marker), exist_ok=True)
    open(marker, "w").write(digest)
else:
    print("bootstrap: dependências já instaladas.", flush=True)

import eval_job  # noqa: E402  (importado só depois do pip install)

rc = eval_job.main()
if rc:                                 # só sai com erro quando necessário (gate com FAIL_ON_GATE=1)
    raise SystemExit(rc)