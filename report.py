"""Consulta as últimas runs do experimento e resume o último arquivo de detalhes.
Uso: python report.py [--last 10]"""
import argparse, glob, os
import mlflow, pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--last", type=int, default=10)
a = ap.parse_args()
exp = mlflow.get_experiment_by_name(os.getenv("MLFLOW_EXPERIMENT", "genai-agent-quality"))
runs = mlflow.search_runs([exp.experiment_id], order_by=["start_time DESC"], max_results=a.last)
cols = ["run_id", "start_time", "metrics.n_traces", "metrics.toxicity/rate", "metrics.pii_entities/rate",
        "metrics.not_advice_violation/rate", "metrics.context_precision/mean", "metrics.latency_ms/p95", "tags.quality_gate"]
pd.set_option("display.width", 250)
print(runs[[c for c in cols if c in runs]].round(3).to_string(index=False))
files = sorted(glob.glob(os.path.join(os.getenv("OUTPUT_DIR", "out"), "dt=*", "*.parquet")))
if files:
    d = pd.read_parquet(files[-1])
    print(f"\nÚltimo detalhe: {files[-1]} ({len(d)} traces)")
    print(d.groupby("agent")[["toxicity", "pii_entities", "not_advice_violation", "context_precision"]].mean().round(3))
