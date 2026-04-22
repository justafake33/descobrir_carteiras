import os, sys, time, uuid, socket, requests, csv
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

PAGES_TRADES  = 5
MIN_APARICOES = 1

MIN_WIN_RATE        = 0.50
MIN_TRADES_30D      = 20
MAX_FOLLOWERS       = 2000
MIN_WALLET_AGE_DAYS = 60
EXCLUIR_KOL         = True
TAGS_LIXO = {"kol", "influencer", "sniper", "bundler", "bot"}

OUTPUT_CSV    = "candidatas_carteiras.csv"
HISTORICO_CSV = "historico_makers.csv"

CAMPOS_HIST = ["data", "address", "win_rate", "followers", "trades_30d",
               "pnl_30d", "pnl_ratio", "sol_balance", "wallet_age_days",
               "tags", "twitter"]

# ── HTTP ──────────────────────────────────────────────────────────────────────
def _get(path, extra_params=None, timeout=15, retries=3):
    base_pairs = [("timestamp", str(int(time.time()))),
                  ("client_id", str(uuid.uuid4()))]
    if extra_params is None:
        params = base_pairs
    elif isinstance(extra_params, dict):
        params = base_pairs + list(extra_params.items())
    else:
        params = base_pairs + list(extra_params)

    url = f"{GMGN_BASE}{path}"
    for tentativa in range(retries):
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
                continue
            if r.status_code == 200:
                body = r.json()
                if body.get("code") == 0:
                    return body.get("data")
                print(f"  [ERRO] code={body.get('code')} — "
                      f"{body.get('msg') or body.get('message') or body.get('error')}")
            else:
                print(f"  [ERRO] HTTP {r.status_code} — {r.text[:150]}")
        except Exception as e:
            print(f"  [TENTATIVA {tentativa+1}] erro: {e}")
            time.sleep(2)

    return None

# ── Coletar makers ────────────────────────────────────────────────────────────
def coletar_makers(min_aparicoes=1):
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
            if not maker:
                continue
            contagem[maker] += 1
            if maker not in maker_info_map:
                maker_info_map[maker] = t.get("maker_info") or {}

        print(f"  Página {page}: {len(trades)} trades | {len(contagem)} makers únicos")
        if len(trades) < 100:
            break
        offset += 100
        time.sleep(0.8)

    ativos = {m: maker_info_map[m] for m in contagem if contagem[m] >= min_aparicoes}
    print(f"  {len(ativos)} makers com ≥{min_aparicoes} aparição(ões)")
    return ativos

# ── Stats individuais ─────────────────────────────────────────────────────────
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

# ── Modo --acumular ───────────────────────────────────────────────────────────
def modo_acumular():
    hoje = date.today().isoformat()
    print(f"\n📥 Modo acumulação — {hoje}")
    print(f"   Arquivo: {HISTORICO_CSV}\n")

    if os.path.exists(HISTORICO_CSV):
        with open(HISTORICO_CSV, "r", encoding="utf-8") as f:
            linhas = list(csv.DictReader(f))
        ja_hoje = [l for l in linhas if l.get("data") == hoje]
        if ja_hoje:
            print(f"⚠️  Já existem {len(ja_hoje)} entradas para hoje — sobrescrevendo automaticamente.")
        linhas = [l for l in linhas if l.get("data") != hoje]
    else:
        linhas = []

    makers = coletar_makers(min_aparicoes=1)
    if not makers:
        print("⛔ Nenhum maker coletado — API sem dados ou sem chave.")
        sys.exit(1)

    stats_map = buscar_stats_todos(list(makers.keys()), silencioso=True)
    print(f"  Stats obtidos: {len(stats_map)}/{len(makers)}")

    novas = []
    for addr, minfo in makers.items():
        raw = stats_map.get(addr) or {}
        s   = extrair_stats(addr, minfo, raw)
        if not s["_raw_stats_ok"]:
            continue
        if s["is_kol"] or s["is_lixo"]:
            continue
        linha = {"data": hoje}
        for campo in CAMPOS_HIST[1:]:
            linha[campo] = s.get(campo, "")
        novas.append(linha)

    with open(HISTORICO_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS_HIST, extrasaction="ignore")
        w.writeheader()
        w.writerows(linhas)
        w.writerows(novas)

    print(f"\n✅ {len(novas)} makers adicionados para {hoje}")
    total = len(linhas) + len(novas)
    datas = sorted({l["data"] for l in linhas} | {hoje})
    print(f"   Total no histórico: {total} entradas | {len(datas)} dias")
    print(f"   Dias registrados: {', '.join(datas[-5:])}{'...' if len(datas) > 5 else ''}")

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not GMGN_API_KEY:
        print("❌ Falta GMGN_API_KEY\n   Defina a variável de ambiente GMGN_API_KEY")
        sys.exit(1)

    modo = sys.argv[1] if len(sys.argv) > 1 else ""
    if modo == "--acumular":
        modo_acumular()
    else:
        print("Uso: python descobrir_carteiras.py --acumular")
        sys.exit(1)
