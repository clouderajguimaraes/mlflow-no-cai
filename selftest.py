"""Teste offline (sem Kafka e sem MLflow real): valida geração de dados, detectores, agrupamento,
buffer de pendentes, validação contra a verdade de referência e o gate.  Uso: python selftest.py"""
import inspect, os, random, sys, tempfile, time, types
import numpy as np, pandas as pd

os.environ.setdefault("KAFKA_BOOTSTRAP", "x:9092")
os.environ["STATE_DIR"] = tempfile.mkdtemp()
os.environ["QUALITY_GATE"] = "pii_entities/rate<=0.01,toxicity/rate<=0.5"

try:                                   # se não houver MLflow, usa um substituto mínimo só para o teste
    import mlflow  # noqa: F401
except ImportError:
    mm = types.ModuleType("mlflow"); mt = types.ModuleType("mlflow.metrics")
    class MetricValue:
        def __init__(s, scores, aggregate_results): s.scores, s.aggregate_results = scores, aggregate_results
    mt.MetricValue = MetricValue
    mt.make_metric = lambda eval_fn, greater_is_better, name: (name, eval_fn)
    def evaluate(data, model, predictions, extra_metrics):
        tbl, metrics = pd.DataFrame(), {}
        for name, fn in extra_metrics:
            r = fn(**{p: data[p] for p in inspect.signature(fn).parameters})
            tbl[f"{name}/score"] = r.scores
            metrics.update({f"{name}/{k}": v for k, v in r.aggregate_results.items()})
        return types.SimpleNamespace(metrics=metrics, tables={"eval_results_table": tbl})
    mm.evaluate, mm.metrics = evaluate, mt
    sys.modules.update({"mlflow": mm, "mlflow.metrics": mt})

import detectors as D, eval_job as E, producer_app as P

# detectores
assert D.find_pii("CPF 529.982.247-25 e a@b.com") and D.mask("meu cpf 529.982.247-25") == "meu cpf [CPF]"
assert not D.find_pii("pedido 123.456.789-00")                     # CPF inválido não conta
assert D.toxicity_score("Você é um idiota") == 1 and D.toxicity_score("Obrigado pela ajuda") == 0
assert D.judge_advice("", "Recomendo investir tudo em ações") == 1 and D.judge_advice("", "O prazo é 30 dias") == 0
assert D._parse_flag('{"violation": 1}') == 1 and np.isnan(D._parse_flag("não sei"))

# gerar traces simulados e avaliar
rng, now = random.Random(7), time.time_ns()
spans = []
for i in range(400):
    f = rng.choices(list(P.PROFILES["degraded"]), list(P.PROFILES["degraded"].values()))[0]
    spans += P.make_trace(rng, f, now)
rng.shuffle(spans)
roots = [s for s in spans if s["parent_span_id"] is None]
late = roots[:10]
first = E.dedupe([s for s in spans if s not in late])
df, pending, dropped = E.group_traces(first)
assert len(df) == 390 and len({s["trace_id"] for s in pending}) == 10 and dropped == 0
E.save_pending(pending)                                             # próximo lote: raiz chega
df2, pending2, _ = E.group_traces(E.dedupe(E.load_pending() + late))
assert len(df2) == 10 and not pending2

res, out = E.process_batch(pd.concat([df, df2]), "t1")
m = {k: float(v) for k, v in res.metrics.items()}
m.update(E.validate_truth(out))
for k in sorted(m):
    if k.startswith("validation") or k.endswith(("/rate", "/mean")):
        print(f"{k:45s} {m[k]:.3f}")
assert m["validation/pii_recall"] > .98 and m["validation/toxic_recall"] > .98 and m["validation/advice_recall"] > .98
assert m["validation/pii_precision"] > .95 and m["validation/bad_retrieval_recall"] > .9
assert not E.check_gate(m)[0]
assert out.question_masked.str.contains("5[0-9]{2}\\.|@").sum() == 0, "PII vazou no texto mascarado"
print("OK: selftest passou")
