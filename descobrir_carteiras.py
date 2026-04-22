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
def _get(path, extra_params=None, timeout=15):
    base_pairs = [("timestamp", str(int(time.time()))),
                  ("client_id", str(uuid.uuid4()))]

    params = base_pairs + (list(extra_params.items()) if isinstance(extra_params, dict) else extra_params or [])

    try:
        r = SESSION.get(f"{GMGN_BASE}{path}", headers={"X-APIKEY": GMGN_API_KEY}, params=params, timeout=timeout)

        if r.status_code != 200:
            print(f"[ERRO] HTTP {r.status_code}")
            return None

        body = r.json()
        if body.get("code") != 0:
            return None

        return body.get("data")

    except Exception as e:
        print(f"[EXC] {e}")
        return None

# ── Coletar makers ────────────────────────────────────────────────────────────
def coletar_makers(min_aparicoes=1):
    contagem = Counter()
    maker_info_map = {}
    offset = 0

    for _ in range(PAGES_TRADES):
        data = _get("/v1/user/smartmoney", {"chain": CHAIN, "limit": "100", "offset": str(offset)})
        if not data:
            break

        trades = data if isinstance(data, list) else data.get("items", [])

        for t in trades:
            maker = t.get("maker") or ""
            if not maker:
                continue
            contagem[maker] += 1
            if maker not in maker_info_map:
                maker_info_map[maker] = t.get("maker_info") or {}

        offset += 100
        time.sleep(0.5)

    return {m: maker_info_map[m] for m in contagem if contagem[m] >= min_aparicoes}

# ── Stats ─────────────────────────────────────────────────────────────────────
def buscar_stats_todos(enderecos):
    resultado = {}
    for addr in enderecos:
        data = _get("/v1/user/wallet_stats", {"chain": CHAIN, "wallet_address": addr, "period": "30d"})
        if data:
            resultado[addr] = data
        time.sleep(0.3)
    return resultado

# ── Extrair stats ─────────────────────────────────────────────────────────────
def extrair_stats(address, maker_info, raw):
    common = raw.get("common") or {} if raw else {}

    return {
        "address": address,
        "win_rate": float(raw.get("winrate", 0)) if raw else 0,
        "followers": int(common.get("follow_count", 0)),
        "trades_30d": int(raw.get("buy", 0)) if raw else 0,
        "pnl_30d": float(raw.get("realized_profit", 0)) if raw else 0,
        "pnl_ratio": float(raw.get("pnl", 0)) if raw else 0,
        "sol_balance": float(raw.get("native_balance", 0)) if raw else 0,
        "wallet_age_days": 0,
        "tags": "",
        "twitter": "",
        "_raw_stats_ok": bool(raw),
        "is_kol": False,
        "is_lixo": False
    }

# ── Modo acumular (CORRIGIDO) ─────────────────────────────────────────────────
def modo_acumular():
    hoje = date.today().isoformat()
    print(f"\n📥 Modo acumulação — {hoje}")

    if os.path.exists(HISTORICO_CSV):
        with open(HISTORICO_CSV, "r", encoding="utf-8") as f:
            linhas = list(csv.DictReader(f))

        # REMOVE entradas do dia automaticamente
        linhas = [l for l in linhas if l.get("data") != hoje]
        print("Sobrescrevendo automaticamente (modo cloud)")

    else:
        linhas = []

    makers = coletar_makers()
    if not makers:
        return

    stats_map = buscar_stats_todos(list(makers.keys()))

    novas = []
    for addr, minfo in makers.items():
        s = extrair_stats(addr, minfo, stats_map.get(addr))

        if not s["_raw_stats_ok"]:
            continue

        linha = {"data": hoje}
        for campo in CAMPOS_HIST[1:]:
            linha[campo] = s.get(campo, "")

        novas.append(linha)

    with open(HISTORICO_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS_HIST)
        w.writeheader()
        w.writerows(linhas)
        w.writerows(novas)

    print(f"✅ {len(novas)} adicionados")

# ── Entry ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not GMGN_API_KEY:
        print("Falta API KEY")
        sys.exit(1)

    modo = sys.argv[1] if len(sys.argv) > 1 else ""

    if modo == "--acumular":
        modo_acumular()
