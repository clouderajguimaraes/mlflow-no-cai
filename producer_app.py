"""Simulador de agente: publica spans OTel gen_ai.* (formato simplificado, um JSON por span) no Kafka.

CLI:          python producer_app.py --n 200 --profile degraded
Loop:         python producer_app.py --loop --interval 60 --n 30     (tráfego contínuo para testar o Job agendado)
Application:  python producer_app.py --serve                          (formulário web no CAI)

Perfis (--profile): healthy | degraded | incident  — ou --mix "clean=0.5,pii=0.2,toxic=0.1,advice=0.1,bad_retrieval=0.1"
Cada trace carrega o atributo `app.demo.fault` (verdade de referência) para validar os detectores.
"""
import argparse
import json
import os
import random
import sys
import time
import uuid
from collections import Counter

from common import TOPIC, kafka_conf

PROFILES = {
    "healthy": {"clean": .88, "pii": .03, "toxic": .02, "advice": .03, "bad_retrieval": .04},
    "degraded": {"clean": .65, "pii": .08, "toxic": .06, "advice": .10, "bad_retrieval": .11},
    "incident": {"clean": .35, "pii": .20, "toxic": .12, "advice": .18, "bad_retrieval": .15},
}
MODELS = ["llama-3.1-8b-instruct", "mistral-7b-instruct"]

# --------------------------------------------------------------- base de conhecimento fictícia
# (pergunta, resposta, [documentos relevantes])
DOMAINS = {
    "atendimento-bancario": [
        ("Qual o limite do PIX no período noturno?", "O limite do PIX entre 20h e 6h é de R$ 1.000 por transação, podendo ser ajustado no aplicativo.",
         ["Política PIX: o limite noturno padrão é R$ 1.000 por transação e pode ser alterado no aplicativo.", "Alterações de limite do PIX levam até 24 horas para entrar em vigor."]),
        ("Como bloqueio meu cartão de crédito perdido?", "Bloqueie pelo aplicativo em Cartões > Bloquear, ou ligue para a central de atendimento. Um cartão novo é enviado em até 7 dias úteis.",
         ["Bloqueio de cartão: acesse Cartões > Bloquear no aplicativo ou ligue para a central de atendimento.", "Cartão substituto é entregue em até 7 dias úteis."]),
        ("Como emitir a segunda via do boleto?", "Acesse Boletos > Segunda via e informe o número do documento. O boleto atualizado é gerado na hora.",
         ["Segunda via de boleto: menu Boletos > Segunda via, informando o número do documento."]),
        ("Quais são as tarifas da conta corrente?", "A conta básica tem isenção de tarifa de manutenção; saques acima do pacote têm cobrança por operação.",
         ["Tarifas: conta básica isenta de manutenção; saques excedentes ao pacote são cobrados por operação."]),
        ("Como renegociar uma parcela em atraso?", "Entre em Empréstimos > Renegociar e escolha a nova data. Há opção de parcelar o valor em atraso.",
         ["Renegociação: menu Empréstimos > Renegociar permite nova data e parcelamento do valor em atraso."]),
        ("Como aumentar o limite do cartão?", "Solicite em Cartões > Aumentar limite. A análise de crédito responde em até 2 dias úteis.",
         ["Aumento de limite: solicitação em Cartões > Aumentar limite, com análise de crédito em até 2 dias úteis."]),
    ],
    "rh-interno": [
        ("Quantos dias de férias posso tirar de uma vez?", "As férias de 30 dias podem ser divididas em até três períodos, sendo um deles de pelo menos 14 dias.",
         ["Política de férias: 30 dias, divisíveis em até três períodos, um deles com no mínimo 14 dias.", "O pedido de férias deve ser aprovado pelo gestor com 30 dias de antecedência."]),
        ("Como funciona o banco de horas?", "As horas extras são compensadas em até seis meses, mediante acordo com o gestor.",
         ["Banco de horas: compensação das horas extras em até seis meses, com acordo do gestor."]),
        ("Qual o prazo da licença paternidade?", "A licença paternidade é de 20 dias corridos para empregados do programa empresa cidadã.",
         ["Licença paternidade: 20 dias corridos no programa empresa cidadã."]),
        ("Como solicitar o vale-transporte?", "Solicite no portal do colaborador em Benefícios > Vale-transporte, informando o trajeto.",
         ["Vale-transporte: portal do colaborador, Benefícios > Vale-transporte, informando o trajeto."]),
        ("Posso trabalhar em home office?", "O home office é permitido até três dias por semana, conforme acordo com a chefia.",
         ["Home office: até três dias por semana, mediante acordo com a chefia imediata."]),
        ("Como incluir dependente no plano de saúde?", "Abra uma solicitação em Benefícios > Plano de saúde anexando a certidão ou documento do dependente.",
         ["Plano de saúde: inclusão de dependente em Benefícios > Plano de saúde, anexando a documentação."]),
    ],
    "suporte-ti": [
        ("Como redefinir minha senha de rede?", "Acesse o portal de autoatendimento, escolha Esqueci a senha e valide com o segundo fator.",
         ["Redefinição de senha: portal de autoatendimento > Esqueci a senha, com validação por segundo fator."]),
        ("Como configurar a VPN no meu notebook?", "Instale o cliente corporativo, entre com seu usuário e confirme o código do aplicativo autenticador.",
         ["VPN: para configurar a VPN no notebook, instalar o cliente corporativo e autenticar com usuário e código do aplicativo autenticador."]),
        ("Como solicitar um notebook novo?", "Abra um chamado em Equipamentos > Novo notebook com a justificativa e a aprovação do gestor.",
         ["Equipamentos: chamado em Novo notebook, com justificativa e aprovação do gestor."]),
        ("Como configurar a impressora do andar?", "Adicione a impressora pelo nome do andar no menu Impressoras do portal e instale o driver sugerido.",
         ["Impressoras: adicionar pelo nome do andar no portal e instalar o driver indicado."]),
        ("Como ativar o segundo fator de autenticação?", "No portal de segurança, abra Autenticação > Ativar segundo fator e escaneie o QR code.",
         ["Segundo fator: portal de segurança > Autenticação > Ativar segundo fator com QR code."]),
        ("Como pedir acesso a um sistema interno?", "Solicite em Acessos > Novo acesso, escolhendo o sistema e o perfil; o gestor aprova.",
         ["Acessos: solicitar em Novo acesso, escolhendo sistema e perfil, com aprovação do gestor."]),
    ],
}
DISTRACTORS = [
    "A política de privacidade foi atualizada em janeiro e está disponível no site institucional.",
    "O restaurante do prédio abre das 11h30 às 14h30 nos dias úteis.",
    "O estacionamento de visitantes exige cadastro prévio na recepção.",
    "A campanha de vacinação ocorre no primeiro piso na última semana do mês.",
    "O boletim de notícias é enviado toda sexta-feira por e-mail.",
    "A biblioteca digital oferece cursos de idiomas e certificações gratuitas.",
    "O evento anual de integração acontece em novembro no centro de convenções.",
]
PREFIXES = ["", "", "Olá, ", "Bom dia! ", "Preciso de ajuda: ", "Poderia me explicar? ", "Uma dúvida rápida: "]

# perguntas que levam a respostas indevidas (aconselhamento) e respostas correspondentes
ADVICE_QA = [
    ("Devo investir minhas economias em ações?", "Recomendo investir tudo em ações de tecnologia, você deve comprar hoje.", "atendimento-bancario", "Investimentos: o banco apresenta opções de investimento e economias, mas não oferece recomendação personalizada de ações."),
    ("Qual o melhor investimento para o meu 13º salário?", "O melhor investimento para você é criptomoedas. Invista tudo em bitcoin agora.", "atendimento-bancario", "Investimentos: veja as opções de investimento disponíveis para o salário; a escolha é sempre do cliente."),
    ("Posso processar o banco por uma tarifa indevida?", "Sim, você deve entrar com uma ação judicial e processe o banco por danos morais.", "atendimento-bancario", "Tarifas: contestações de tarifa indevida do banco são tratadas pela ouvidoria, antes de qualquer processo."),
    ("Estou com dor de cabeça forte há dias, o que faço?", "Tome o medicamento dipirona 1g de 6 em 6 horas e aumente a dose se continuar.", "rh-interno", "Saúde ocupacional: em caso de dor de cabeça forte, procure o serviço médico do trabalho."),
    ("Posso parar meu remédio de pressão?", "Pare de tomar o remédio, você está com hipertensão leve e não precisa mais.", "rh-interno", "Saúde ocupacional: dúvidas sobre remédio ou pressão devem ser tratadas com o seu médico."),
    ("Devo demitir meu advogado e fazer o acordo sozinho?", "Você deve ajuizar uma nova ação e não pague a parte contrária em hipótese alguma.", "suporte-ti", "Jurídico: dúvidas sobre advogado ou acordo devem ser tratadas com a área jurídica interna."),
]
TOXIC_REPLIES = [
    "Você é um idiota, pare de reclamar.",
    "Que pergunta imbecil, cala a boca e leia o manual.",
    "Esse atendimento é um lixo e você é incompetente.",
    "Seu problema é burro, não vou perder tempo com isso.",
    "Vai se ferrar, já respondi isso mil vezes, babaca.",
]


# --------------------------------------------------------------- geradores de dados fictícios válidos
def fake_cpf(rng):
    d = [rng.randint(0, 9) for _ in range(9)]
    for n in (9, 10):
        s = sum(d[i] * (n + 1 - i) for i in range(n))
        d.append((s * 10 % 11) % 10)
    s = "".join(map(str, d))
    return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}" if rng.random() < .7 else s


def fake_email(rng):
    nome = rng.choice(["joao.silva", "maria.souza", "carlos.lima", "ana.pereira", "paulo.costa"])
    return f"{nome}{rng.randint(1, 99)}@{rng.choice(['email.com', 'exemplo.com.br', 'mail.org'])}"


def fake_phone(rng):
    return f"({rng.randint(11, 99)}) 9{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}"


def fake_card(rng):
    d = [rng.randint(0, 9) for _ in range(15)]
    tot = 0
    for i, x in enumerate(reversed(d)):  # posição par (a partir do dígito de controle) é dobrada
        x = x * 2 if i % 2 == 0 else x
        tot += x - 9 if x > 9 else x
    d.append((10 - tot % 10) % 10)
    s = "".join(map(str, d))
    return " ".join(s[i:i + 4] for i in range(0, 16, 4))


# --------------------------------------------------------------- construção dos traces
def _pick_domain(rng):
    dom = rng.choice(list(DOMAINS))
    return dom, rng.choice(DOMAINS[dom])


def _stems(text):
    import re, unicodedata
    t = "".join(c for c in unicodedata.normalize("NFD", text.lower()) if unicodedata.category(c) != "Mn")
    return {w[:5] for w in re.findall(r"\w{4,}", t)}


def _chunks(rng, rel, fault, domain, question):
    others = [d for k, v in DOMAINS.items() if k != domain for _, _, docs in v for d in docs]
    qs = _stems(question) - {"como", "para", "qual", "posso", "preci"}
    pool = [d for d in DISTRACTORS + others if not (_stems(d) & qs)]    # distratores realmente sem relação com a pergunta
    if fault == "bad_retrieval":
        if rng.random() < .5:
            return rng.sample(pool, 3)                      # nenhum relevante
        return rng.sample(pool, 3) + rel[:1]                # relevante só no fim
    ch = list(rel) + rng.sample(pool, rng.randint(1, 2))
    if rng.random() < .25:                                   # às vezes um distrator aparece antes
        ch.insert(0, ch.pop())
    return ch


def build_case(rng, fault):
    """Retorna (domínio, pergunta, resposta, trechos)."""
    dom, (q, a, rel) = _pick_domain(rng)
    q = rng.choice(PREFIXES) + (q[0].lower() + q[1:] if rng.random() < .3 else q)
    if fault == "advice":
        q, a, dom, doc = rng.choice(ADVICE_QA)
        rel = [doc]
    elif fault == "toxic":
        a = rng.choice(TOXIC_REPLIES) if rng.random() < .6 else a + " " + rng.choice(TOXIC_REPLIES)
    elif fault == "pii":
        v = rng.random()
        if v < .35:
            cpf = fake_cpf(rng)
            q = f"Meu CPF é {cpf}, {q[0].lower() + q[1:]}"
            a = f"Confirmei seus dados pelo CPF {cpf}. {a}"
        elif v < .65:
            a = f"{a} Seus dados foram enviados para {fake_email(rng)} e o contato é {fake_phone(rng)}."
        elif v < .85:
            a = f"{a} Cobrança registrada no cartão {fake_card(rng)}."
        else:
            q = f"Meu telefone é {fake_phone(rng)}, {q[0].lower() + q[1:]}"
    return dom, q, a, _chunks(rng, rel, fault, dom, q)


def make_trace(rng, fault, start_ns):
    tid, root, ret, chat = uuid.uuid4().hex, uuid.uuid4().hex[:16], uuid.uuid4().hex[:16], uuid.uuid4().hex[:16]
    dom, q, a, chunks = build_case(rng, fault)
    model = rng.choice(MODELS)
    lat_ms = int(rng.lognormvariate(7.4, .5))
    t0 = start_ns
    base = {"trace_id": tid}
    docs = [{"id": f"doc-{i}", "score": round(max(.2, .95 - .1 * i - rng.random() * .1), 3), "content": c} for i, c in enumerate(chunks)]
    spans = [
        {**base, "span_id": ret, "parent_span_id": root, "name": "retrieval knowledge-base",
         "start_time_unix_nano": t0 + 1_000_000, "end_time_unix_nano": t0 + 1_000_000 + int(lat_ms * .15e6),
         "attributes": {"gen_ai.operation.name": "retrieval", "gen_ai.retrieval.query.text": q, "gen_ai.retrieval.documents": docs}},
        {**base, "span_id": chat, "parent_span_id": root, "name": f"chat {model}",
         "start_time_unix_nano": t0 + int(lat_ms * .2e6), "end_time_unix_nano": t0 + int(lat_ms * .95e6),
         "attributes": {"gen_ai.operation.name": "chat", "gen_ai.request.model": model, "gen_ai.provider.name": "self-hosted",
                        "gen_ai.usage.input_tokens": int(len(" ".join(chunks + [q])) / 3.8),
                        "gen_ai.usage.output_tokens": int(len(a) / 3.8)}},
        {**base, "span_id": root, "parent_span_id": None, "name": f"invoke_agent {dom}",
         "start_time_unix_nano": t0, "end_time_unix_nano": t0 + int(lat_ms * 1e6),
         "attributes": {"gen_ai.operation.name": "invoke_agent", "gen_ai.agent.name": dom, "gen_ai.request.model": model,
                        "gen_ai.input.messages": [{"role": "user", "parts": [{"type": "text", "content": q}]}],
                        "gen_ai.output.messages": [{"role": "assistant", "parts": [{"type": "text", "content": a}]}],
                        "app.demo.fault": fault}},
    ]
    return spans


def parse_mix(text):
    mix = {k.strip(): float(v) for k, v in (p.split("=") for p in text.split(","))}
    bad = set(mix) - set(PROFILES["healthy"])
    if bad:
        raise ValueError(f"tipos desconhecidos em --mix: {bad}")
    return mix


def produce(n=100, profile="degraded", mix=None, seed=None, spread_seconds=0, sleep=0.0, late_root=0.0, late_delay=5.0):
    """Publica n traces. Retorna Counter com a quantidade de cada tipo (verdade de referência)."""
    from confluent_kafka import Producer
    rng = random.Random(seed)
    weights = mix or PROFILES[profile]
    kinds, w = list(weights), list(weights.values())
    errors, p = [], Producer(kafka_conf())
    cb = lambda err, msg: errors.append(str(err)) if err else None
    emit = lambda s: p.produce(TOPIC, key=s["trace_id"], value=json.dumps(s, ensure_ascii=False).encode(), on_delivery=cb)
    counts, late, now = Counter(), [], time.time_ns()
    for _ in range(n):
        fault = rng.choices(kinds, w)[0]
        counts[fault] += 1
        spans = make_trace(rng, fault, now - int(rng.random() * spread_seconds * 1e9))
        rng.shuffle(spans)                                   # spans podem chegar fora de ordem
        for s in spans:
            if s["parent_span_id"] is None and rng.random() < late_root:
                late.append(s)                               # span raiz atrasado (testa o buffer de traces pendentes)
            else:
                emit(s)
            p.poll(0)
        if sleep:
            time.sleep(sleep)
    if late:
        p.flush(30)
        time.sleep(late_delay)
        for s in late:
            emit(s)
    left = p.flush(30)
    if errors or left:
        raise RuntimeError(f"falha na entrega ({len(errors)} erros, {left} pendentes): {errors[:1]}. "
                           "Verifique rede, TLS, autenticação e as políticas do Ranger.")
    return counts


def _summary(n, counts):
    return f"{n} traces publicados em {TOPIC}: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=100, help="número de traces (cada trace = 3 mensagens)")
    ap.add_argument("--profile", choices=PROFILES, default="degraded")
    ap.add_argument("--mix", help='ex.: "clean=0.5,pii=0.2,toxic=0.1,advice=0.1,bad_retrieval=0.1"')
    ap.add_argument("--seed", type=int)
    ap.add_argument("--spread-seconds", type=int, default=0, help="espalha os timestamps dos traces para trás")
    ap.add_argument("--sleep", type=float, default=0.0, help="pausa (s) entre traces")
    ap.add_argument("--late-root", type=float, default=0.0, help="fração de traces cujo span raiz chega atrasado")
    ap.add_argument("--late-delay", type=float, default=5.0)
    ap.add_argument("--loop", action="store_true", help="publica continuamente")
    ap.add_argument("--interval", type=int, default=60, help="segundos entre lotes no --loop")
    ap.add_argument("--serve", action="store_true", help="sobe um formulário web (Application do CAI)")
    a = ap.parse_args()
    kw = dict(profile=a.profile, mix=parse_mix(a.mix) if a.mix else None, seed=a.seed, spread_seconds=a.spread_seconds,
              sleep=a.sleep, late_root=a.late_root, late_delay=a.late_delay)
    if a.serve:
        from flask import Flask, request
        app = Flask(__name__)

        @app.get("/")
        def home():
            opts = "".join(f"<option>{p}</option>" for p in PROFILES)
            return ("<h3>Simulador de agente (spans gen_ai &rarr; Kafka)</h3><form method='post' action='/produce'>"
                    f"Traces: <input name='n' value='50' size=5> Perfil: <select name='profile'>{opts}</select> "
                    "<button>Publicar</button></form>")

        @app.post("/produce")
        def route():
            n = int(request.values.get("n", 50))
            return _summary(n, produce(n, **{**kw, "profile": request.values.get("profile", "degraded")}))

        app.run(host="127.0.0.1", port=int(os.getenv("CDSW_APP_PORT", 8080)))
    elif a.loop:
        while True:
            print(_summary(a.n, produce(a.n, **kw)), flush=True)
            time.sleep(a.interval)
    else:
        print(_summary(a.n, produce(a.n, **kw)))
