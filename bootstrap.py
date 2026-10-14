"""Script de entrada do Job do CAI: instala dependências (só quando o requirements.txt mudou)
e em seguida executa a avaliação (eval_job.main) no mesmo processo."""
import hashlib
import os
import subprocess
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())
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

sys.exit(eval_job.main())
