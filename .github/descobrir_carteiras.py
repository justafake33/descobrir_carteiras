"""
descobrir_carteiras.py
======================
Pipeline de descoberta de carteiras via GMGN Smart Money API.

Modos de uso:
  python descobrir_carteiras.py              → filtra e mostra candidatas do dia
  python descobrir_carteiras.py --acumular   → adiciona ao histórico diário (sem filtro)
  python descobrir_carteiras.py --relatorio  → ranking mensal a partir do histórico

Fluxo de descoberta contínua (recomendado):
  1. Rodar --acumular diariamente (pode ser automático via Task Scheduler)
  2. Rodar --relatorio ao fim do mês para ver quais carteiras apareceram mais
  3. Passar as top candidatas pelo Smart Money Profile → TP/SL → dashboard
"""

import os, sys, time, uuid, socket, requests, requests.adapters, csv
from datetime import datetime, date
from collections import Counter, defaultdict

# ── Forçar IPv4 ───────────────────────────────────────────────────────────────
_orig_gai = socket.getaddrinfo
def _gai_ipv4(h, p, f=0, t=0, pr=0, fl=0):
    return _orig_gai(h, p, socket.AF_INET, t, pr, fl)
socket.getaddrinfo = _gai_ipv4
SESSION = requests.Session()

# ── Config ────────────────────────────────────────────────────────────────────
GMGN_API_KEY = os.environ.get("GMGN_API_KEY", "")
GMGN_BASE    = "https://openapi.gmgn.ai"
CHAIN        = "sol"

PAGES_TRADES  = 5   # páginas × 100 trades = até 500 makers por rodada
MIN_APARICOES = 1   # no modo --acumular inclui todos (aparição única já é sinal)

# Filtros — só usados no modo padrão (não no --acumular)
MIN_WIN_RATE        = 0.50
MIN_TRADES_30D      = 20
MAX_FOLLOWERS       = 2000
MIN_WALLET_AGE_DAYS = 60
EXCLUIR_KOL         = True
TAGS_LIXO = {"kol", "influencer", "sniper", "bundler", "bot"}

# Arquivos
OUTPUT_CSV    = "candidatas_carteiras.csv"
HISTORICO_CSV = "historico_makers.csv"

# Campos salvos no histórico
CAMPOS_HIST = ["data", "address", "win_rate", "followers", "trades_30d",
               "pnl_30d", "pnl_ratio", "sol_balance", "wallet_age_days",
               "tags", "twitter"]

# ── HTTP ──────────────────────────────────────────────────────────────────────
def _get(path, extra_params=None, timeout=15):
    base_pairs = [("timestamp", str(int(time.time()))),
                  ("client_id", str(uuid.uuid4()))]
    if extra_params is None:
        params = base_pairs
    elif isinstance(extra_params, dict):
        params = base_pairs + list(extra_params.items())
    else:
        params = base_pairs + list(extra_params)

    url = f"{GMGN_BASE}{path}"
    try:
        r = SESSION.get(url, headers={"X-APIKEY": GMGN_API_KEY},
                        params=params, timeout=timeout)
        if r.status_code == 429:
            reset = None
            try: reset = r.json().get("reset_at")
            except: pass
            w = max(float(reset) - time.time(), 3) if reset else 15
            print(f"  [429] rate limit — aguardando {w:.0f}s...")
            time.sleep(min(w, 60))
            return None
        if r.status_code != 200:
            print(f"  [ERRO] HTTP {r.status_code} — {r.text[:150]}")
            return None
        body = r.json()
        if body.get("code") != 0:
            print(f"  [ERRO] code={body.get('code')} — "
                  f"{body.get('msg') or body.get('message') or body.get('error')}")
            return None
        return body.get("data")
    except Exception as e:
        print(f"  [EXC] {e}")
        return None

# ── Etapa 1: Coletar makers únicos dos trades ─────────────────────────────────
def coletar_makers(min_aparicoes=None):
    if min_aparicoes is None:
        min_aparicoes = MIN_APARICOES
    print("\n=== ETAPA 1: Coletando makers de /v1/user/smartmoney ===")
    contagem = Counter()
    maker_info_map = {}
    offset = 0
    for page in range(1, PAGES_TRADES + 1):
        data = _get("/v1/user/smartmoney",
                    {"chain": CHAIN, "limit": "100", "offset": str(offset)})
        if data is None:
            print(f"  Página {page} falhou"); break
        trades = data if isinstance(data, list) else (
            data.get("list") or data.get("activities") or data.get("items") or [])
        if not trades:
            print(f"  Sem trades na página {page}"); break
        for t in trades:
            maker = t.get("maker") or t.get("wallet_address") or ""
            if not maker: continue
            contagem[maker] += 1
            if maker not in maker_info_map:
                maker_info_map[maker] = t.get("maker_info") or {}
        print(f"  Página {page}: {len(trades)} trades | {len(contagem)} makers únicos")
        if len(trades) < 100: break
        offset += 100
        time.sleep(0.8)

    ativos = {m: info for m, info in maker_info_map.items()
              if contagem[m] >= min_aparicoes}
    print(f"  {len(ativos)} makers com ≥{min_aparicoes} aparição(ões)")
    return ativos

# ── Etapa 2: Stats individuais ────────────────────────────────────────────────
def buscar_stats_todos(enderecos, silencioso=False):
    if not silencioso:
        print(f"\n=== ETAPA 2: Stats de {len(enderecos)} wallets ===")
    resultado = {}
    for i, addr in enumerate(enderecos, 1):
        data = _get("/v1/user/wallet_stats",
                    {"chain": CHAIN, "wallet_address": addr, "period": "30d"})
        if data:
            resultado[addr] = data
        if not silencioso:
            print(f"  [{i:3}/{len(enderecos)}] {addr[:16]}... {'✓' if data else '✗'}")
        time.sleep(0.5)
    return resultado

# ── Extrair campos padronizados ───────────────────────────────────────────────
def extrair_stats(address, maker_info, raw):
    common = raw.get("common") or {} if raw else {}

    tags_feed  = set(t.lower() for t in (maker_info.get("tags") or []))
    tags_stats = set(t.lower() for t in (common.get("tags") or []))
    tags = tags_feed | tags_stats

    is_kol  = bool(tags & {"kol", "influencer", "renowned"})
    is_lixo = bool(tags & TAGS_LIXO)

    s = raw or {}
    pnl_stat   = s.get("pnl_stat") or {}
    win_rate   = float(pnl_stat.get("winrate") or pnl_stat.get("win_rate") or
                       s.get("winrate") or 0)
    followers  = int(common.get("follow_count") or 0)
    trades_30d = int(s.get("buy") or s.get("buy_count") or 0)
    pnl_30d    = float(s.get("realized_profit") or 0)
    pnl_ratio  = float(s.get("realized_profit_pnl") or s.get("pnl") or 0)
    sol_balance = float(s.get("native_balance") or 0)

    created_at = common.get("created_at")
    age_days   = None
    if created_at:
        try: age_days = (time.time() - float(created_at)) / 86400
        except: pass

    return {
        "address":         address,
        "tags":            ",".join(sorted(tags)),
        "twitter":         common.get("twitter_username") or "",
        "is_kol":          is_kol,
        "is_lixo":         is_lixo,
        "win_rate":        round(win_rate, 3),
        "followers":       followers,
        "trades_30d":      trades_30d,
        "pnl_30d":         round(pnl_30d, 2),
        "pnl_ratio":       round(pnl_ratio, 3),
        "sol_balance":     round(sol_balance, 2),
        "wallet_age_days": round(age_days, 0) if age_days else None,
        "_raw_stats_ok":   bool(raw),
    }

# ── Filtro (modo padrão) ──────────────────────────────────────────────────────
def filtrar(wallets):
    print(f"\n=== ETAPA 3: Filtrando {len(wallets)} wallets ===")
    ctrs = dict(kol=0, lixo=0, nova=0, win_baixo=0, sem_trades=0,
                muitos_seg=0, sem_stats=0)
    ok = []
    for s in wallets:
        if not s["_raw_stats_ok"]:
            ctrs["sem_stats"] += 1; continue
        if EXCLUIR_KOL and (s["is_kol"] or s["is_lixo"]):
            ctrs["kol"] += 1; continue
        if s["wallet_age_days"] is not None and s["wallet_age_days"] < MIN_WALLET_AGE_DAYS:
            ctrs["nova"] += 1; continue
        if s["win_rate"] < MIN_WIN_RATE:
            ctrs["win_baixo"] += 1; continue
        if s["trades_30d"] < MIN_TRADES_30D:
            ctrs["sem_trades"] += 1; continue
        if s["followers"] > MAX_FOLLOWERS:
            ctrs["muitos_seg"] += 1; continue
        ok.append(s)

    print(f"  Descartadas: {sum(ctrs.values())}")
    for motivo, n in ctrs.items():
        if n: print(f"    {motivo}: {n}")
    print(f"  ✅ {len(ok)} passaram")
    return sorted(ok, key=lambda x: x["win_rate"], reverse=True)[:50]

def score(s):
    sc  = s["win_rate"] * 40
    sc += 20 if s["followers"] < 100 else (12 if s["followers"] < 500 else 5)
    sc += 10 if s["trades_30d"] >= 50 else 0
    sc += 10 if s["pnl_30d"] > 0 else 0
    sc += 5  if s["pnl_ratio"] > 1.0 else 0
    return round(sc, 1)

# ── Modo --acumular ───────────────────────────────────────────────────────────
def modo_acumular():
    """Coleta makers do dia e acrescenta ao historico_makers.csv."""
    hoje = date.today().isoformat()
    print(f"\n📥 Modo acumulação — {hoje}")
    print(f"   Arquivo: {HISTORICO_CSV}\n")

    # Verificar se já rodou hoje
    if os.path.exists(HISTORICO_CSV):
        with open(HISTORICO_CSV, "r", encoding="utf-8") as f:
            linhas = list(csv.DictReader(f))
        ja_hoje = [l for l in linhas if l.get("data") == hoje]
        if ja_hoje:
            print(f"⚠️  Já existem {len(ja_hoje)} entradas para hoje ({hoje}).")
            resp = input("   Sobrescrever? (s/N): ").strip().lower()
            if resp != "s":
                print("Cancelado."); return
            # Remove entradas de hoje para re-inserir
            linhas = [l for l in linhas if l.get("data") != hoje]
    else:
        linhas = []

    # Coleta (sem filtro de aparições mínimas — queremos todos)
    makers = coletar_makers(min_aparicoes=1)
    if not makers:
        print("⛔ Nenhum maker coletado."); return

    stats_map = buscar_stats_todos(list(makers.keys()), silencioso=True)
    print(f"  Stats obtidos: {len(stats_map)}/{len(makers)}")

    novas = []
    for addr, minfo in makers.items():
        raw = stats_map.get(addr) or {}
        s   = extrair_stats(addr, minfo, raw)
        if not s["_raw_stats_ok"]:
            continue  # sem stats = não acumula
        if s["is_kol"] or s["is_lixo"]:
            continue  # exclui lixo mesmo no modo acumular
        linha = {"data": hoje}
        for campo in CAMPOS_HIST[1:]:  # skip "data"
            linha[campo] = s.get(campo, "")
        novas.append(linha)

    # Escreve tudo (histórico anterior + novas entradas de hoje)
    arquivo_existe = os.path.exists(HISTORICO_CSV)
    with open(HISTORICO_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS_HIST, extrasaction="ignore")
        w.writeheader()
        w.writerows(linhas)   # histórico anterior (sem hoje)
        w.writerows(novas)    # entradas de hoje

    print(f"\n✅ {len(novas)} makers adicionados para {hoje}")
    total = len(linhas) + len(novas)
    datas = sorted({l["data"] for l in linhas} | {hoje})
    print(f"   Total no histórico: {total} entradas | {len(datas)} dias")
    print(f"   Dias registrados: {', '.join(datas[-5:])}{'...' if len(datas) > 5 else ''}")

# ── Modo --relatorio ──────────────────────────────────────────────────────────
def modo_relatorio():
    """Lê historico_makers.csv e gera ranking por frequência + qualidade."""
    if not os.path.exists(HISTORICO_CSV):
        print(f"❌ {HISTORICO_CSV} não encontrado. Rode --acumular primeiro.")
        return

    with open(HISTORICO_CSV, "r", encoding="utf-8") as f:
        linhas = list(csv.DictReader(f))

    if not linhas:
        print("Histórico vazio."); return

    datas = sorted({l["data"] for l in linhas})
    total_dias = len(datas)
    print(f"\n📊 RELATÓRIO — {total_dias} dias ({datas[0]} → {datas[-1]})")
    print(f"   Total de entradas: {len(linhas)}\n")

    # Agregar por endereço
    agg = defaultdict(lambda: {
        "dias": 0, "win_rates": [], "pnls": [], "trades": [],
        "followers": 0, "tags": "", "twitter": "", "age": None
    })

    for l in linhas:
        addr = l["address"]
        a    = agg[addr]
        a["dias"] += 1
        try:    a["win_rates"].append(float(l["win_rate"]))
        except: pass
        try:    a["pnls"].append(float(l["pnl_30d"]))
        except: pass
        try:    a["trades"].append(int(l["trades_30d"]))
        except: pass
        # Usa valor mais recente para campos não-temporais
        if l["followers"]:
            try: a["followers"] = int(l["followers"])
            except: pass
        a["tags"]    = l.get("tags", "")
        a["twitter"] = l.get("twitter", "")
        if l.get("wallet_age_days"):
            try: a["age"] = float(l["wallet_age_days"])
            except: pass

    # Calcular métricas agregadas
    resultados = []
    for addr, a in agg.items():
        if not a["win_rates"]: continue
        freq     = a["dias"] / total_dias          # 0-1: fração de dias que apareceu
        avg_win  = sum(a["win_rates"]) / len(a["win_rates"])
        avg_pnl  = sum(a["pnls"]) / len(a["pnls"]) if a["pnls"] else 0
        avg_trd  = sum(a["trades"]) / len(a["trades"]) if a["trades"] else 0

        # Score mensal: frequência pesa muito (consistência > performance pontual)
        sc_mensal = (freq * 40) + (avg_win * 30) + (10 if avg_pnl > 0 else 0)
        sc_mensal += 10 if a["dias"] >= max(3, total_dias // 5) else 0  # bônus consistência

        resultados.append({
            "address":      addr,
            "dias":         a["dias"],
            "freq_pct":     round(freq * 100, 1),
            "avg_win_rate": round(avg_win, 3),
            "avg_pnl_30d":  round(avg_pnl, 2),
            "avg_trades":   round(avg_trd, 0),
            "followers":    a["followers"],
            "tags":         a["tags"],
            "twitter":      a["twitter"],
            "age_days":     a["age"],
            "score_mensal": round(sc_mensal, 1),
        })

    resultados = sorted(resultados, key=lambda x: x["score_mensal"], reverse=True)

    # Filtros mínimos para o relatório
    MIN_DIAS    = max(2, total_dias // 7)   # apareceu em ≥1/7 dos dias
    MIN_WIN_REL = 0.45

    qualificados = [r for r in resultados
                    if r["dias"] >= MIN_DIAS and r["avg_win_rate"] >= MIN_WIN_REL
                    and r["followers"] <= MAX_FOLLOWERS]

    print(f"Filtro mínimo: ≥{MIN_DIAS} dias de aparição | win≥{MIN_WIN_REL*100:.0f}% | seg≤{MAX_FOLLOWERS}")
    print(f"Qualificados: {len(qualificados)} de {len(resultados)} carteiras únicas\n")

    print(f"{'#':<3} {'Endereço':<46} {'Dias':>5} {'Freq':>6} {'Win%':>5} "
          f"{'PnL médio':>10} {'Seg':>5} {'Score':>7}")
    print("-" * 90)
    for i, r in enumerate(qualificados[:20], 1):
        print(f"{i:<3} {r['address']:<46} {r['dias']:>5} "
              f"{r['freq_pct']:>5.1f}% {r['avg_win_rate']*100:>4.0f}% "
              f"${r['avg_pnl_30d']:>9,.0f} {r['followers']:>5} "
              f"{r['score_mensal']:>7}")

    # Salva CSV do relatório
    rel_csv = f"relatorio_makers_{datas[-1]}.csv"
    campos_rel = ["score_mensal", "address", "dias", "freq_pct", "avg_win_rate",
                  "avg_pnl_30d", "avg_trades", "followers", "age_days", "tags", "twitter"]
    with open(rel_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos_rel, extrasaction="ignore")
        w.writeheader(); w.writerows(qualificados)
    print(f"\n✅ Relatório salvo em {rel_csv}")
    print("\nPRÓXIMOS PASSOS:")
    print("1. Verificar twitter/tags das top candidatas (evitar KOLs disfarçados)")
    print("2. Rodar Smart Money Profile (gmgn-portfolio stats) nas top 5")
    print("3. Backtester TP/SL → adicionar ao webhook Helius em quarentena")

# ── Modo padrão (snapshot do dia) ─────────────────────────────────────────────
def modo_padrao():
    print(f"🔍 Pipeline de descoberta — {datetime.now().strftime('%d/%m/%Y %H:%M')}")

    makers = coletar_makers()
    if not makers:
        print("⛔ Nenhum maker coletado."); return

    enderecos = list(makers.keys())
    stats_map = buscar_stats_todos(enderecos)

    stats_list = []
    for addr, minfo in makers.items():
        raw = stats_map.get(addr) or {}
        s   = extrair_stats(addr, minfo, raw)
        stats_list.append(s)
        ok  = "✓" if s["_raw_stats_ok"] else "✗"
        print(f"  {addr[:16]}... {ok}  win={s['win_rate']*100:.0f}%  "
              f"seg={s['followers']}  30d={s['trades_30d']}  "
              f"age={s['wallet_age_days']}d  tags=[{s['tags'][:25]}]")

    candidatas = filtrar(stats_list)
    if not candidatas:
        print("\n⚠️  Nenhuma passou. Top 10 sem filtros:")
        sem_kol = [s for s in stats_list if not s["is_kol"] and not s["is_lixo"]]
        for s in sorted(sem_kol, key=lambda x: x["win_rate"], reverse=True)[:10]:
            print(f"  {s['address'][:16]}  win={s['win_rate']*100:.0f}%  "
                  f"30d={s['trades_30d']}  seg={s['followers']}  "
                  f"age={s['wallet_age_days']}d  "
                  f"stats={'ok' if s['_raw_stats_ok'] else 'AUSENTE'}")
        return

    resultados = sorted([{**s, "score": score(s)} for s in candidatas],
                        key=lambda x: x["score"], reverse=True)

    print(f"\n{'='*80}\nTOP 20 CANDIDATAS\n{'='*80}")
    print(f"{'#':<3} {'Endereço':<46} {'Win%':>5} {'Seg':>5} "
          f"{'30d':>4} {'PnL':>8} {'Score':>6}")
    print("-" * 80)
    for i, r in enumerate(resultados[:20], 1):
        print(f"{i:<3} {r['address']:<46} {r['win_rate']*100:>4.0f}% "
              f"{r['followers']:>5} {r['trades_30d']:>4} "
              f"${r['pnl_30d']:>7,.0f} {r['score']:>6}")

    campos = ["score", "address", "win_rate", "followers", "trades_30d",
              "pnl_30d", "pnl_ratio", "wallet_age_days", "tags", "twitter"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore")
        w.writeheader(); w.writerows(resultados)
    print(f"\n✅ {len(resultados)} candidatas salvas em {OUTPUT_CSV}")

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not GMGN_API_KEY:
        print("❌ Falta GMGN_API_KEY\n   Windows: set GMGN_API_KEY=gmgn_xxxxx")
        sys.exit(1)

    modo = sys.argv[1] if len(sys.argv) > 1 else ""
    if modo == "--acumular":
        modo_acumular()
    elif modo == "--relatorio":
        modo_relatorio()
    else:
        modo_padrao()
