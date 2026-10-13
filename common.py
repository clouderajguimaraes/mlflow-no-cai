"""Configuração e utilitários compartilhados (Kafka, variáveis de ambiente, logging)."""
import json
import logging
import os
import re


def load_env_file(path=None):
    """Carrega linhas `export CHAVE=valor` de um env.sh (opcional).
    Variáveis já definidas no ambiente (ex.: variáveis do projeto CAI) têm prioridade."""
    path = path or os.getenv("ENV_FILE") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "env.sh")
    if not os.path.isfile(path):
        return
    for line in open(path, encoding="utf-8"):
        m = re.match(r"^\s*export\s+([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line.rstrip("\n"))
        if not m:
            continue
        key, val = m.group(1), m.group(2).strip()
        if val[:1] in "\"'" and val.count(val[0]) >= 2:
            val = val[1:val.index(val[0], 1)]
        else:
            val = val.split(" #")[0].strip()
        os.environ.setdefault(key, val)


load_env_file()

TOPIC = os.getenv("KAFKA_TOPIC", "genai.agent.spans")


def setup_logging():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return logging.getLogger("genai-eval")


def env_int(name, default):
    return int(os.getenv(name, default))


def env_float(name, default):
    return float(os.getenv(name, default))


def kafka_conf(extra=None):
    """Configuração Kafka a partir de variáveis de ambiente (ver env.sh.example)."""
    conf = {"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]}
    if os.getenv("KAFKA_SECURITY_PROTOCOL"):          # SASL_SSL | SSL | SASL_PLAINTEXT | PLAINTEXT
        conf["security.protocol"] = os.environ["KAFKA_SECURITY_PROTOCOL"]
    if os.getenv("KAFKA_SASL_MECHANISM"):             # PLAIN | SCRAM-SHA-512 | GSSAPI
        conf["sasl.mechanism"] = os.environ["KAFKA_SASL_MECHANISM"]
        if os.getenv("KAFKA_SASL_USERNAME"):
            conf["sasl.username"] = os.environ["KAFKA_SASL_USERNAME"]
            conf["sasl.password"] = os.environ["KAFKA_SASL_PASSWORD"]
    if os.getenv("KAFKA_SSL_CA"):
        conf["ssl.ca.location"] = os.environ["KAFKA_SSL_CA"]
    conf.update(extra or {})
    return conf


def messages_to_text(msgs, roles=None):
    """Extrai texto de gen_ai.input/output.messages ([{role, parts:[{type:'text', content}]}]) ou de string."""
    if isinstance(msgs, str):
        try:
            msgs = json.loads(msgs)
        except Exception:
            return msgs
    out = []
    for m in msgs or []:
        if roles and m.get("role") not in roles:
            continue
        if isinstance(m.get("content"), str):
            out.append(m["content"])
        for p in m.get("parts", []):
            if p.get("type", "text") == "text":
                out.append(p.get("content", ""))
    return "\n".join(out).strip()
