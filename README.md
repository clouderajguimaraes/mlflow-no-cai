# Qualidade e segurança de agentes LLM no Cloudera AI (PoC)

Avalia agentes LLM a partir de spans OpenTelemetry `gen_ai.*` publicados em Kafka, usando **MLflow 2.19** no Cloudera AI (CAI).
Quatro métricas: **Toxicity, PII, Not advice, Context precision**, mais métricas operacionais (latência, tokens).

```
Agente (OTel gen_ai.*) ─► Kafka ─► CAI Job agendado ─► MLflow (1 run por janela: agregados, gate)
                                        └────────────► Parquet / Iceberg (1 linha por trace, texto mascarado)
```

## Conteúdo
| Arquivo | Função |
|---|---|
| `producer_app.py` | Simula o agente e publica spans no Kafka (CLI, loop contínuo ou Application do CAI) |
| `eval_job.py` | Job de avaliação (rode agendado) |
| `detectors.py` | PII, toxicity e os juízes (regras locais ou LLM via HTTP) |
| `report.py` | Resume as últimas runs e o último detalhe |
| `selftest.py` | Teste offline (sem Kafka/MLflow) |
| `env.sh.example` | Todas as variáveis de configuração |

## Como rodar
1. Runtime Python 3.10 (Workbench/JupyterLab). `pip install -r requirements.txt`.
2. `cp env.sh.example env.sh` e preencha o Kafka. Crie o tópico (ex.: 3 partições). O usuário precisa de permissão (Ranger) no tópico e no consumer group.
3. Teste a conexão:
   ```python
   from confluent_kafka.admin import AdminClient; from common import kafka_conf
   print(list(AdminClient(kafka_conf()).list_topics(timeout=10).topics))
   ```
4. Publique dados: `python producer_app.py --n 200 --profile degraded` (perfis: `healthy`, `degraded`, `incident`; ou `--mix`).
5. Avalie: `python eval_job.py`. Consulte: `python report.py`.
6. Rode `python selftest.py` para validar os detectores sem infraestrutura.

## Como funciona o Job
1. Lê do Kafka a partir do último offset commitado do consumer group, até `MAX_MSGS`, `MAX_SECONDS` ou `IDLE_SECONDS` sem mensagens.
2. Agrupa spans por `trace_id`. Traces sem span raiz (`invoke_agent`) ficam em um buffer (`state/pending.jsonl`) e são reavaliados na próxima execução, até `PENDING_MAX_MIN`.
3. Calcula as métricas e (opcionalmente) amostra o que vai aos juízes. O texto enviado aos juízes é **sempre mascarado** (CPF, CNPJ, cartão, e-mail, telefone).
4. Grava a run no MLflow e os detalhes no parquet/Iceberg. **Só então** commita o offset (nada se perde se o Job falhar).
- Um arquivo de trava impede duas execuções simultâneas.

## Onde ver os resultados
- **MLflow (experimento `genai-agent-quality`)**: uma run por janela com métricas agregadas (`toxicity/rate`, `pii_entities/rate`, `not_advice_violation/rate`, `context_precision/mean`, `latency_ms/p95`, `by_agent/<agente>/...`), parâmetros (offsets), tags (`window_start`, `quality_gate`) e artefatos `scores_masked.json` e `flagged_traces.json` (só os casos problemáticos).
- **Parquet** em `out/dt=AAAA-MM-DD/<run>.parquet` (ou tabela Iceberg com `SINK=iceberg`).
- Via API: `mlflow.search_runs([...])` (veja `report.py`).

## Agendamento (CAI Jobs)
Jobs > New Job: script `eval_job.py`, agendamento recorrente com cron `*/15 * * * *`, mesma Runtime do teste, variáveis no projeto, alerta por e-mail em falha. Mantenha `MAX_SECONDS` bem abaixo do intervalo. Com `QUALITY_GATE` e `FAIL_ON_GATE=1`, o Job falha (e alerta) quando uma regra de qualidade é violada.

## Juízes: regras locais ou LLM
Por padrão, **Not advice** e **Context precision** usam regras locais (linha de base, sem LLM). Para usar LLMs, defina `JUDGE_MODE=http` e as URLs (ver `env.sh.example`); cada juiz pode ter endpoint/modelo próprios, por exemplo um modelo de guardrail (formato `llamaguard`) para *not advice* e um modelo instruct para *context precision*. Falhas do juiz viram `NaN` (e contam em `judge_errors`), sem derrubar o Job. Prompts customizáveis em `JUDGE_PROMPT_ADVICE` / `JUDGE_PROMPT_CONTEXT` (use `{question}`, `{answer}`, `{chunk}`; chaves literais em dobro).

## Validação com o simulador
Os traces simulados carregam `app.demo.fault` (verdade de referência). O Job calcula precisão e recall de cada detector (`validation/*`) para provar que as métricas detectam o que deveriam. Em dados reais esse atributo não existe e a validação é ignorada.

## Limitações conhecidas
- É monitoramento posterior (batch), não um filtro em tempo real. Guardrails que bloqueiam respostas precisam rodar dentro do agente.
- Os nomes dos atributos de retrieval (`gen_ai.retrieval.documents`) são uma suposição; confirme no tópico real.
- Os detectores locais são linhas de base: léxico de toxicidade, regex/checksum de PII e regras de aconselhamento. Para produção, use Presidio/Detoxify e juízes LLM, validados com casos rotulados.
- MLflow 2.19 não ingere OTLP nem mostra a Trace UI para spans vindos do Kafka. Isso fica para versões 3.x.
