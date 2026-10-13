"""CAI Job: Kafka -> agrupa spans por trace_id -> 4 métricas -> MLflow + parquet/Iceberg -> commit do offset.

Pensado para execução agendada (ex.: cron */15 * * * *). Veja README.md e env.sh.example.
"""
import fcntl
import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from common import TOPIC, env_float, env_int, kafka_conf, messages_to_text, setup_logging
import detectors as D

log = setup_logging()

MAX_MSGS = env_int("MAX_MSGS", 5000)             # para de ler após N mensagens...
MAX_SECONDS = env_int("MAX_SECONDS", 120)        # ...ou T segundos...
IDLE_SECONDS = env_int("IDLE_SECONDS", 15)       # ...ou se o tópico ficar sem mensagens novas
PENDING_MAX_MIN = env_int("PENDING_MAX_MIN", 30)  # tempo máx. que um trace sem span raiz espera
SAMPLE_RATE = env_float("JUDGE_SAMPLE_RATE", 1.0)  # fração dos traces enviada aos juízes
SINK = os.getenv("SINK", "parquet")              # parquet | iceberg
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "out")
STATE_DIR = os.getenv("STATE_DIR", "state")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "ai_governance.llm_eval_scores")
EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT", "genai-agent-quality")
GATE = os.getenv("QUALITY_GATE", "")             # ex.: "pii_entities/rate<=0,toxicity/rate<=0.05,context_precision/mean>=0.5"
FAIL_ON_GATE = os.getenv("FAIL_ON_GATE", "0") == "1"
GROUP = os.getenv("KAFKA_GROUP", "cai-genai-eval")

COLUMNS = [("run_id", "string"), ("evaluated_at", "timestamp"), ("trace_time", "timestamp"), ("trace_id", "string"),
           ("agent", "string"), ("model", "string"), ("question_masked", "string"), ("answer_masked", "string"),
           ("toxicity", "double"), ("pii_entities", "double"), ("pii_types", "string"),
           ("not_advice_violation", "double"), ("context_precision", "double"), ("judge_sampled", "boolean"),
           ("latency_ms", "double"), ("input_tokens", "double"), ("output_tokens", "double"), ("demo_fault", "string")]
SCHEMA = ", ".join(f"{n} {t}" for n, t in COLUMNS)


# ------------------------------------------------------------------ 1. leitura do Kafka
def consume():
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer(kafka_conf({"group.id": GROUP, "enable.auto.commit": False, "auto.offset.reset": "earliest"}))
    c.subscribe([TOPIC])
    spans, offsets, t0, last = [], {}, time.time(), time.time()
    while len(spans) < MAX_MSGS and time.time() - t0 < MAX_SECONDS and time.time() - last < IDLE_SECONDS:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        last = time.time()
        s = json.loads(m.value())
        s.setdefault("_first_seen", last)
        spans.append(s)
        offsets[m.partition()] = m.offset() + 1           # próximo offset a commitar

    def commit():
        if offsets:
            c.commit(offsets=[TopicPartition(TOPIC, p, o) for p, o in offsets.items()], asynchronous=False)
    return spans, offsets, commit, c


# ------------------------------------------------------------------ 2. agrupamento por trace
def _pending_path():
    return os.path.join(STATE_DIR, "pending.jsonl")


def load_pending():
    if not os.path.isfile(_pending_path()):
        return []
    return [json.loads(l) for l in open(_pending_path(), encoding="utf-8") if l.strip()]


def save_pending(spans):
    os.makedirs(STATE_DIR, exist_ok=True)
    tmp = _pending_path() + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for s in spans:
            f.write(json.dumps(s, ensure_ascii=False) + "\n")
    os.replace(tmp, _pending_path())


def dedupe(spans):
    seen, out = set(), []
    for s in spans:
        k = (s.get("trace_id"), s.get("span_id"))
        if k not in seen:
            seen.add(k)
            out.append(s)
    return out


def group_traces(spans, now=None):
    """Retorna (DataFrame de traces completos, spans pendentes, nº de traces descartados)."""
    now = now or time.time()
    by = {}
    for s in spans:
        by.setdefault(s["trace_id"], []).append(s)
    rows, pending, dropped = [], [], 0
    for tid, ss in by.items():
        root = next((s for s in ss if s.get("attributes", {}).get("gen_ai.operation.name") == "invoke_agent"), None)
        if root is None:                                   # span raiz ainda não chegou: espera um pouco
            age = now - min(s.get("_first_seen", now) for s in ss)
            if age < PENDING_MAX_MIN * 60:
                pending += ss
            else:
                dropped += 1
            continue
        at = root["attributes"]
        q = messages_to_text(at.get("gen_ai.input.messages"), {"user"})
        a = messages_to_text(at.get("gen_ai.output.messages"), {"assistant"})
        if not (q and a):
            dropped += 1
            continue
        docs, in_tok, out_tok = [], 0, 0
        for s in sorted(ss, key=lambda x: x.get("start_time_unix_nano", 0)):
            sa = s.get("attributes", {})
            docs += [d.get("content", "") for d in sa.get("gen_ai.retrieval.documents", [])]
            in_tok += int(sa.get("gen_ai.usage.input_tokens", 0) or 0)
            out_tok += int(sa.get("gen_ai.usage.output_tokens", 0) or 0)
        st, en = root.get("start_time_unix_nano"), root.get("end_time_unix_nano")
        rows.append({"trace_id": tid, "agent": at.get("gen_ai.agent.name", "unknown"),
                     "model": at.get("gen_ai.request.model"), "inputs": q, "predictions": a, "contexts": docs,
                     "trace_time": datetime.fromtimestamp(st / 1e9, tz=timezone.utc).replace(tzinfo=None) if st else pd.NaT,
                     "latency_ms": (en - st) / 1e6 if st and en else np.nan,
                     "input_tokens": in_tok or np.nan, "output_tokens": out_tok or np.nan,
                     "demo_fault": at.get("app.demo.fault")})
    return pd.DataFrame(rows), pending, dropped


# ------------------------------------------------------------------ 3. métricas
def sampled(trace_id):
    import hashlib
    return int(hashlib.sha1(trace_id.encode()).hexdigest(), 16) % 100 < SAMPLE_RATE * 100


def build_metrics():
    from mlflow.metrics import MetricValue, make_metric

    def mv(scores, thr):
        s = np.array(scores, dtype=float)
        ok = s[~np.isnan(s)]                              # taxa e média só sobre o que foi avaliado
        agg = {"mean": float(ok.mean()) if len(ok) else float("nan"),
               "rate": float((ok >= thr).mean()) if len(ok) else float("nan")}
        return MetricValue(scores=list(s), aggregate_results=agg)

    def tox(predictions):
        return mv([D.toxicity_score(p) for p in predictions], 0.5)

    def pii(predictions, inputs):
        return mv([float(len(D.find_pii(p)) + len(D.find_pii(i))) for p, i in zip(predictions, inputs)], 1)

    def adv(predictions, inputs, judge_sampled):
        return mv([D.judge_advice(D.mask(i), D.mask(p)) if s else np.nan
                   for p, i, s in zip(predictions, inputs, judge_sampled)], 1)

    def ctx(inputs, contexts, judge_sampled):
        return mv([D.context_precision(D.mask(i), [D.mask(c) for c in cs]) if (s and len(cs)) else np.nan
                   for i, cs, s in zip(inputs, contexts, judge_sampled)], 0.5)

    return [make_metric(eval_fn=f, greater_is_better=g, name=n)
            for f, g, n in [(tox, False, "toxicity"), (pii, False, "pii_entities"),
                            (adv, False, "not_advice_violation"), (ctx, True, "context_precision")]]


def _col(tbl, name):
    for c in (f"{name}/v1/score", f"{name}/score"):
        if c in tbl:
            return tbl[c].values
    for c in tbl.columns:
        if c.startswith(name + "/") and c.endswith("/score"):
            return tbl[c].values
    raise KeyError(f"coluna de score de '{name}' não encontrada em {list(tbl.columns)}")


def rate(series, thr, above=True):
    s = pd.Series(series).dropna()
    return float((s >= thr).mean() if above else (s < thr).mean()) if len(s) else float("nan")


def per_agent_metrics(out):
    m = {}
    for ag, g in out.groupby("agent"):
        m.update({f"by_agent/{ag}/n_traces": float(len(g)),
                  f"by_agent/{ag}/toxicity_rate": rate(g.toxicity, .5), f"by_agent/{ag}/pii_rate": rate(g.pii_entities, 1),
                  f"by_agent/{ag}/not_advice_rate": rate(g.not_advice_violation, 1),
                  f"by_agent/{ag}/context_precision_mean": float(g.context_precision.mean())})
    return m


def validate_truth(out):
    """Se os traces trazem `app.demo.fault` (simulador), mede precisão/recall de cada detector."""
    t = out[out.demo_fault.notna()]
    res = {}
    checks = {"pii": ("pii_entities", lambda s: s > 0), "toxic": ("toxicity", lambda s: s >= .5),
              "advice": ("not_advice_violation", lambda s: s >= 1), "bad_retrieval": ("context_precision", lambda s: s < .5)}
    for fault, (col, pred) in checks.items():
        sub = t[t[col].notna()]
        if sub.empty:
            continue
        actual, det = (sub.demo_fault == fault).values, pred(sub[col]).values
        tp, fp, fn = (actual & det).sum(), (~actual & det).sum(), (actual & ~det).sum()
        if tp + fn:
            res[f"validation/{fault}_recall"] = float(tp / (tp + fn))
        if tp + fp:
            res[f"validation/{fault}_precision"] = float(tp / (tp + fp))
    return res


def check_gate(metrics):
    """QUALITY_GATE='metrica<=x,metrica>=y'. Retorna (passou, [falhas])."""
    fails = []
    for part in filter(None, (p.strip() for p in GATE.split(","))):
        m = re.match(r"^([\w/.\-]+)\s*(<=|>=|<|>)\s*([\d.]+)$", part)
        if not m:
            log.warning("regra de gate inválida ignorada: %s", part)
            continue
        name, op, lim = m.group(1), m.group(2), float(m.group(3))
        v = metrics.get(name)
        if v is None or np.isnan(v):
            continue
        ok = {"<=": v <= lim, ">=": v >= lim, "<": v < lim, ">": v > lim}[op]
        if not ok:
            fails.append(f"{name}={v:.3f} (regra {op}{lim})")
    return not fails, fails


# ------------------------------------------------------------------ 4. gravação
def write_rows(pdf, run_id):
    if SINK == "iceberg":
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("genai-eval").getOrCreate()
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_TABLE.split('.')[0]}")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} ({SCHEMA}) USING iceberg")
        clean = pdf.astype(object).where(pdf.notna(), None)
        spark.createDataFrame(clean, schema=SCHEMA).writeTo(ICEBERG_TABLE).append()
        return ICEBERG_TABLE
    d = os.path.join(OUTPUT_DIR, f"dt={datetime.now(timezone.utc):%Y-%m-%d}")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f"{run_id}.parquet")
    pdf.to_parquet(path, index=False)
    return path


def process_batch(df, run_id):
    """Avalia um DataFrame de traces e devolve (res, out). Requer MLflow com run ativa."""
    import mlflow
    df = df.copy()
    df["judge_sampled"] = df.trace_id.map(sampled)
    res = mlflow.evaluate(data=df, model=None, predictions="predictions", extra_metrics=build_metrics())
    tbl = res.tables["eval_results_table"]
    out = pd.DataFrame({
        "run_id": run_id, "evaluated_at": datetime.now(timezone.utc).replace(tzinfo=None), "trace_time": df.trace_time,
        "trace_id": df.trace_id, "agent": df.agent, "model": df.model,
        "question_masked": df.inputs.map(D.mask), "answer_masked": df.predictions.map(D.mask),
        "toxicity": _col(tbl, "toxicity"), "pii_entities": _col(tbl, "pii_entities"),
        "pii_types": [",".join(sorted({k for k, _, _ in D.find_pii(i) + D.find_pii(p)})) for i, p in zip(df.inputs, df.predictions)],
        "not_advice_violation": _col(tbl, "not_advice_violation"), "context_precision": _col(tbl, "context_precision"),
        "judge_sampled": df.judge_sampled, "latency_ms": df.latency_ms, "input_tokens": df.input_tokens,
        "output_tokens": df.output_tokens, "demo_fault": df.demo_fault})
    return res, out[[n for n, _ in COLUMNS]]


# ------------------------------------------------------------------ job
def main():
    import mlflow
    os.makedirs(STATE_DIR, exist_ok=True)
    lock = open(os.path.join(STATE_DIR, "job.lock"), "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log.warning("Outra execução ainda está em andamento; saindo sem processar.")
        return 0

    spans, offsets, commit, consumer = consume()
    n_new = len(spans)
    spans = dedupe(load_pending() + spans)
    df, pending, dropped = group_traces(spans)
    log.info("lidas %d mensagens novas | %d traces completos | %d pendentes | %d descartados",
             n_new, len(df), len({s['trace_id'] for s in pending}), dropped)
    if df.empty:
        save_pending(pending)
        commit()
        consumer.close()
        log.info("Nada novo para avaliar.")
        return 0

    mlflow.set_experiment(EXPERIMENT)
    run_id = uuid.uuid4().hex[:12]
    with mlflow.start_run(run_name=f"eval-{run_id}") as run:
        res, out = process_batch(df, run_id)
        m = {k: float(v) for k, v in res.metrics.items()}
        m.update({"n_spans": len(spans), "n_traces": len(df), "n_judged": int(out.judge_sampled.sum()),
                  "n_pending": len({s['trace_id'] for s in pending}), "n_dropped_incomplete": dropped,
                  "judge_errors": D.STATS["judge_errors"], "latency_ms/mean": float(out.latency_ms.mean()),
                  "latency_ms/p95": float(out.latency_ms.quantile(.95)), "tokens_total": float(out.input_tokens.sum() + out.output_tokens.sum())})
        m.update(per_agent_metrics(out))
        if out.demo_fault.notna().any():
            m.update(validate_truth(out))
        ok, fails = check_gate(m)
        if GATE:
            m["quality_gate_pass"] = float(ok)
            mlflow.set_tags({"quality_gate": "PASS" if ok else "FAIL", "quality_gate_failures": "; ".join(fails)[:250]})
        mlflow.log_metrics({k: v for k, v in m.items() if not np.isnan(v)})
        mlflow.log_params({"kafka_topic": TOPIC, "kafka_group": GROUP, "offsets_next": json.dumps(offsets),
                           "judge_mode_advice": D._mode("advice"), "judge_mode_context": D._mode("context"),
                           "judge_sample_rate": SAMPLE_RATE, "sink": SINK})
        mlflow.set_tags({"window_start": str(out.trace_time.min()), "window_end": str(out.trace_time.max())})
        dest = write_rows(out, run_id)                       # 1) grava os detalhes...
        mlflow.log_table(out, "scores_masked.json")
        flagged = out[(out.pii_entities > 0) | (out.toxicity >= .5) | (out.not_advice_violation >= 1) | (out.context_precision < .5)]
        if len(flagged):
            mlflow.log_table(flagged, "flagged_traces.json")
        log.info("detalhes gravados em %s | run %s", dest, run.info.run_id)
    save_pending(pending)                                    # 2) guarda traces incompletos...
    commit()                                                 # 3) ...e só então commita o offset
    consumer.close()
    log.info("OK: %d traces avaliados | gate=%s %s", len(df), "PASS" if ok else "FAIL", "; ".join(fails))
    return 2 if (FAIL_ON_GATE and not ok) else 0


if __name__ == "__main__":
    sys.exit(main())
