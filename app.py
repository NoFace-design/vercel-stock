import asyncio, re, time, os, math, calendar, hashlib
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote
import httpx, feedparser, numpy as np, pandas as pd, yfinance as yf, pandas_ta as ta
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

TICKERS = ["NVDA", "AAPL", "AMZN", "MU", "AVGO", "TSM", "INTC", "META", "MSFT", "AMD", "TSLA"]
ALIASES = {"NVDA": ["nvidia"], "AAPL": ["apple", "iphone"], "AMZN": ["amazon", "aws"], "MU": ["micron"],
           "AVGO": ["broadcom"], "TSM": ["tsmc", "taiwan semiconductor"], "INTC": ["intel"],
           "META": ["meta platforms", "facebook", "instagram"], "MSFT": ["microsoft", "azure"],
           "AMD": ["advanced micro devices"], "TSLA": ["tesla"]}
RX = {t: re.compile(r"\b(" + "|".join(map(re.escape, [t.lower()] + a)) + r")\b", re.I) for t, a in ALIASES.items()}

NIT = os.getenv("NITTER_HOST", "https://nitter.net")
def gn(site):  # Google News RSS scoped to one outlet (those outlets have no open RSS)
    return f"https://news.google.com/rss/search?q=site:{site}+(stocks+OR+earnings+OR+markets)+when:1d&hl=en-US&gl=US&ceid=US:en"
FEEDS = {
    # Americas: wires, markets desks, press-release wires
    "Reuters": ("Americas", gn("reuters.com")), "Bloomberg": ("Americas", gn("bloomberg.com")), "AP News": ("Americas", gn("apnews.com")),
    "Wall Street Journal": ("Americas", "https://feeds.a.dj.com/rss/RSSMarketsMain.xml"),
    "CNBC": ("Americas", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    "MarketWatch": ("Americas", "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    "Barron's": ("Americas", gn("barrons.com")), "Yahoo Finance": ("Americas", "https://finance.yahoo.com/news/rssindex"),
    "Seeking Alpha": ("Americas", "https://seekingalpha.com/market_currents.xml"),
    "Benzinga": ("Americas", "https://www.benzinga.com/feed"), "Investing.com": ("Americas", "https://www.investing.com/rss/news.rss"),
    "Business Wire": ("Americas", gn("businesswire.com")), "PR Newswire": ("Americas", "https://www.prnewswire.com/rss/news-releases-list.rss"),
    "GlobeNewswire": ("Americas", gn("globenewswire.com")),
    # Primary sources (no editorial slant, earliest possible)
    "SEC 8-K Filings": ("Primary", "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&count=100&output=atom"),
    "Federal Reserve": ("Primary", "https://www.federalreserve.gov/feeds/press_all.xml"),
    "ECB": ("Primary", "https://www.ecb.europa.eu/rss/press.html"),
    # Europe
    "Financial Times": ("Europe", gn("ft.com")), "BBC Markets": ("Europe", "https://feeds.bbci.co.uk/news/business/rss.xml"),
    "Sky News": ("Europe", "https://feeds.skynews.com/feeds/rss/business.xml"), "DW Business": ("Europe", "https://rss.dw.com/rdf/rss-en-bus"),
    "Euronews": ("Europe", gn("euronews.com")), "S&P Global": ("Global", gn("spglobal.com")),
    # Asia (supply-chain and chip coverage lands here first)
    "Nikkei Asia": ("Asia", "https://asia.nikkei.com/rss/feed/nar"), "SCMP": ("Asia", "https://www.scmp.com/rss/92/feed"),
    "Caixin": ("Asia", gn("caixinglobal.com")), "Straits Times": ("Asia", "https://www.straitstimes.com/news/business/rss.xml"),
    "Korea Herald": ("Asia", gn("koreaherald.com")), "Focus Taiwan (CNA)": ("Asia", gn("focustaiwan.tw")),
    "DigiTimes": ("Asia", gn("digitimes.com")), "Economic Times": ("Asia", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    "Mint": ("Asia", gn("livemint.com")), "Japan Times": ("Asia", gn("japantimes.co.jp")),
    # Middle East
    "Arab News": ("Middle East", gn("arabnews.com")), "Gulf News": ("Middle East", gn("gulfnews.com")),
    "The National (UAE)": ("Middle East", gn("thenationalnews.com")), "Al Arabiya": ("Middle East", gn("english.alarabiya.net")),
    # Real-time breaking wires (fast, unverified)
    "Watcher.Guru": ("Breaking", f"{NIT}/WatchersGuru/rss"), "UnusualWhales": ("Breaking", f"{NIT}/unusual_whales/rss"),
    "Deitaone": ("Breaking", f"{NIT}/Deitaone/rss"),
}
SEC_UA = {"User-Agent": "TradingDashboard/1.0 (replace-with-your-email@example.com)"}

POS = set("surge surges soar soars jump jumps rally rallies beat beats record upgrade upgraded bullish gain gains rise rises growth strong boost profit outperform breakthrough approve approval wins deal partnership expands buyback raises tops climbs".split())
NEG = set("plunge plunges fall falls drop drops slump miss misses downgrade downgraded bearish loss losses cut cuts weak lawsuit probe investigation recall ban bans fine tariff layoffs layoff decline declines crash warns warning fears risk selloff tumble tumbles sinks slides halts delay delays".split())
MKT = re.compile(r"stock|share|market|earnings|nasdaq|s&p|dow\b|fed\b|rate|inflation|chip|semiconductor|\bai\b|revenue|profit|yield|etf|ipo|tariff|treasury|analyst|guidance|forecast|\$", re.I)
POL = re.compile(r"election|trump|biden|harris|kremlin|hamas|opinion|editorial|senator|campaign|impeach|democrat|republican|parliament|governor|protest|royal|celebrity", re.I)

def sentiment(text):
    w = re.findall(r"[a-z']+", text.lower()); s = 0
    for i, x in enumerate(w):
        v = (x in POS) - (x in NEG)
        if v and i and w[i - 1] in ("not", "no", "fails", "fail"): v = -v
        s += v
    sc = max(-1.0, min(1.0, s / 2))
    return sc, ("Positive" if sc > 0 else "Negative" if sc < 0 else "Neutral")

NEWS = {}
STATUS = {}
async def pull(client, name, meta):
    region, url = meta
    try:
        r = await client.get(url, headers=SEC_UA if "sec.gov" in url else None); r.raise_for_status()
        feed = feedparser.parse(r.text)
        STATUS[name] = {"name": name, "region": region, "ok": len(feed.entries) > 0}
    except Exception:
        STATUS[name] = {"name": name, "region": region, "ok": False}; return
    for e in feed.entries[:30]:
        title = re.sub(r"\s+", " ", e.get("title", "")).strip()
        if not title: continue
        tk = [t for t, rx in RX.items() if rx.search(title)]
        if not tk and not (MKT.search(title) and not POL.search(title)): continue
        pp = e.get("published_parsed")
        ts = calendar.timegm(pp) if pp else time.time()
        key = hashlib.md5(title.lower().encode()).hexdigest()
        if key in NEWS: continue
        sc, lab = sentiment(title)
        NEWS[key] = {"source": name, "title": title, "link": e.get("link", ""), "ts": ts, "score": sc, "label": lab, "tickers": tk, "region": region}

LAST = 0.0
LOCK = asyncio.Lock()
async def refresh_news():
    # Fetch every feed right now. Runs only when someone asks; calls within 8s share one fetch.
    global LAST
    async with LOCK:
        if time.time() - LAST < 8: return
        async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as c:
            await asyncio.gather(*(pull(c, n, u) for n, u in FEEDS.items()))
        cut = time.time() - 172800
        for k in [k for k, v in NEWS.items() if v["ts"] < cut]: NEWS.pop(k, None)
        LAST = time.time()

app = FastAPI()

CACHE = {}
TF = {"15M": ("15m", "30d", None), "1H": ("60m", "180d", None), "4H": ("60m", "360d", "4h"), "1D": ("1d", "2y", None)}
def load(t, tf):
    k = (t, tf)
    if k in CACHE and time.time() - CACHE[k][0] < 10: return CACHE[k][1]
    iv, per, rs = TF[tf]
    df = yf.download(t, period=per, interval=iv, progress=False, auto_adjust=False, threads=False)
    if df is None or df.empty: raise ValueError("no market data returned")
    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
    df.columns = [str(c).lower() for c in df.columns]
    df = df[["open", "high", "low", "close", "volume"]].astype(float).dropna()
    if rs: df = df.resample(rs).agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()
    CACHE[k] = (time.time(), df); return df

def T(f, *a, **k):
    try: return f(*a, **k)
    except Exception: return None
def sv(x, col=0):
    try:
        s = x.iloc[:, col] if isinstance(x, pd.DataFrame) else x
        return float(s.dropna().iloc[-1])
    except Exception: return None
def cmp(a, b): return None if a is None or b is None else (a > b) - (a < b)
def band(x, lo, hi): return None if x is None or lo is None or hi is None else (1 if x < lo else -1 if x > hi else 0)

NPAT = 17
def patterns(df):
    # pure-Python candlestick detectors (no TA-Lib); latest signal in last 3 bars wins
    O, H, L, C = (df[k].values for k in ("open", "high", "low", "close"))
    res = {}
    def at(i):
        o, h, l, c = O[i], H[i], L[i], C[i]; po, pc = O[i-1], C[i-1]
        b = abs(c - o); r = h - l + 1e-9; lo = min(o, c) - l; hi = h - max(o, c)
        tr = np.sign(C[i-1] - C[i-4]); s = {}
        if b < 0.1 * r and tr != 0: s["Doji"] = int(-tr)
        if b > 0 and lo >= 2 * b and hi <= 0.3 * b + 0.05 * r:
            if tr < 0: s["Hammer"] = 1
            elif tr > 0: s["Hanging Man"] = -1
        if b > 0 and hi >= 2 * b and lo <= 0.3 * b + 0.05 * r:
            if tr > 0: s["Shooting Star"] = -1
            elif tr < 0: s["Inverted Hammer"] = 1
        if pc < po and c > o and o <= pc and c >= po: s["Bullish Engulfing"] = 1
        if pc > po and c < o and o >= pc and c <= po: s["Bearish Engulfing"] = -1
        pb = abs(pc - po)
        if pb > 0 and b < 0.6 * pb and max(o, c) < max(po, pc) and min(o, c) > min(po, pc):
            if pc < po and c > o: s["Bullish Harami"] = 1
            elif pc > po and c < o: s["Bearish Harami"] = -1
        mid = (po + pc) / 2
        if pc < po and c > o and o < pc and mid < c < po: s["Piercing Line"] = 1
        if pc > po and c < o and o > pc and po < c < mid: s["Dark Cloud Cover"] = -1
        o2, c2 = O[i-2], C[i-2]; b2 = abs(c2 - o2)
        if c2 < o2 and pb < 0.3 * b2 and c > o and c > (o2 + c2) / 2: s["Morning Star"] = 1
        if c2 > o2 and pb < 0.3 * b2 and c < o and c < (o2 + c2) / 2: s["Evening Star"] = -1
        big = all(abs(C[j] - O[j]) > 0.4 * (H[j] - L[j] + 1e-9) for j in (i, i-1, i-2))
        if big and all(C[j] > O[j] for j in (i, i-1, i-2)) and C[i] > C[i-1] > C[i-2]: s["Three White Soldiers"] = 1
        if big and all(C[j] < O[j] for j in (i, i-1, i-2)) and C[i] < C[i-1] < C[i-2]: s["Three Black Crows"] = -1
        if b > 0.95 * r: s["Marubozu"] = 1 if c > o else -1
        return s
    for i in (-3, -2, -1): res.update(at(i))
    return res

def signals(df):
    o, h, l, c, v = (df[k] for k in ("open", "high", "low", "close", "volume"))
    p = float(c.iloc[-1]); out = []
    def add(n, g, s, w=1.0):
        if s is not None: out.append({"name": n, "group": g, "signal": int(s), "weight": w})
    for L in (5, 10, 20, 50, 100, 200):
        if len(df) > L + 5:
            for n, f in (("SMA", ta.sma), ("EMA", ta.ema), ("WMA", ta.wma), ("HMA", ta.hma)):
                add(f"{n}({L})", "Trend", cmp(p, sv(T(f, c, length=L))))
    if len(df) > 80:
        tk = (h.rolling(9).max() + l.rolling(9).min()) / 2; kj = (h.rolling(26).max() + l.rolling(26).min()) / 2
        sa = ((tk + kj) / 2).shift(26); sb = ((h.rolling(52).max() + l.rolling(52).min()) / 2).shift(26)
        a, b = sa.iloc[-1], sb.iloc[-1]
        add("Ichimoku Tenkan/Kijun", "Trend", cmp(tk.iloc[-1], kj.iloc[-1]))
        add("Ichimoku Cloud", "Trend", 1 if p > max(a, b) else -1 if p < min(a, b) else 0)
    ps = T(ta.psar, h, l, c)
    if ps is not None: add("Parabolic SAR", "Trend", 1 if pd.notna(ps.iloc[-1, 0]) else -1)
    add("Supertrend", "Trend", sv(T(ta.supertrend, h, l, c), 1))
    ax = T(ta.adx, h, l, c); adx = sv(ax, 0)
    add("ADX/DI", "Trend", None if adx is None else (cmp(sv(ax, 1), sv(ax, 2)) if adx > 20 else 0))
    dc = T(ta.donchian, h, l, c); add("Donchian Channel", "Trend", cmp(p, sv(dc, 1)))
    add("RSI(14)", "Momentum", band(sv(T(ta.rsi, c, length=14)), 30, 70))
    add("RSI(7)", "Momentum", band(sv(T(ta.rsi, c, length=7)), 30, 70))
    m = T(ta.macd, c); add("MACD", "Momentum", cmp(sv(m, 0), sv(m, 2)))
    st = T(ta.stoch, h, l, c); add("Stoch %K zone", "Momentum", band(sv(st, 0), 20, 80)); add("Stoch K/D", "Momentum", cmp(sv(st, 0), sv(st, 1)))
    add("CCI(20)", "Momentum", band(sv(T(ta.cci, h, l, c)), -100, 100))
    add("Williams %R", "Momentum", band(sv(T(ta.willr, h, l, c)), -80, -20))
    add("ROC(10)", "Momentum", cmp(sv(T(ta.roc, c, length=10)), 0))
    ts = T(ta.tsi, c); add("TSI", "Momentum", cmp(sv(ts, 0), sv(ts, 1)))
    add("Awesome Osc", "Momentum", cmp(sv(T(ta.ao, h, l)), 0))
    bb = T(ta.bbands, c, length=20); add("Bollinger Bands", "Volatility", band(p, sv(bb, 0), sv(bb, 2)))
    kc = T(ta.kc, h, l, c); add("Keltner Channel", "Volatility", band(p, sv(kc, 0), sv(kc, 2)))
    cv = sv((h - l).ewm(span=10).mean().pct_change(10))
    add("Chaikin Volatility", "Volatility", None if cv is None or len(c) < 12 else (cmp(p, float(c.iloc[-11])) if cv > 0 else 0))
    ob = T(ta.obv, c, v); add("OBV vs EMA20", "Volume", cmp(sv(ob), sv(T(ta.ema, ob, length=20))))
    cm = sv(T(ta.cmf, h, l, c, v)); add("CMF(20)", "Volume", None if cm is None else (1 if cm > .05 else -1 if cm < -.05 else 0))
    add("MFI(14)", "Volume", band(sv(T(ta.mfi, h, l, c, v, length=14)), 20, 80))
    vw = (((h + l + c) / 3) * v).rolling(20).sum() / v.rolling(20).sum(); add("VWAP(20)", "Volume", cmp(p, sv(vw)))
    base = len(out)
    for n, sg in patterns(df).items():
        if sg: add(n, "Pattern", sg, 0.5)
    atr = sv(T(ta.atr, h, l, c, length=14)) or float((h - l).tail(14).mean())
    return out, atr, base + NPAT

def news_score(t):
    now = time.time()
    own = [i for i in NEWS.values() if t in i["tickers"] and now - i["ts"] < 86400]
    mk = [i for i in NEWS.values() if not i["tickers"] and now - i["ts"] < 43200]
    def wavg(L):
        num = den = 0.0
        for i in L:
            w = math.exp(-(now - i["ts"]) / 21600); num += w * i["score"]; den += w
        return num / den if den else 0.0
    s = 0.75 * wavg(own) + 0.25 * wavg(mk) if own else 0.5 * wavg(mk)
    return max(-1.0, min(1.0, s)), len(own)

f2 = lambda x: None if x is None or not np.isfinite(x) else round(float(x), 2)

def hist_signals(df):
    # vectorised signals for every historical bar (about 20 core indicators)
    o, h, l, c, v = (df[k] for k in ("open", "high", "low", "close", "volume"))
    S = {}
    sg = lambda up, dn: pd.Series(np.where(up, 1, np.where(dn, -1, 0)), index=df.index)
    def put(name, fn):
        try: S[name] = fn()
        except Exception: pass
    for L in (20, 50, 200):
        put(f"SMA{L}", lambda L=L: np.sign(c - ta.sma(c, L))); put(f"EMA{L}", lambda L=L: np.sign(c - ta.ema(c, L)))
    put("SMA50/200 trend", lambda: np.sign(ta.sma(c, 50) - ta.sma(c, 200)))
    put("RSI14", lambda: sg(ta.rsi(c, length=14) < 30, ta.rsi(c, length=14) > 70))
    put("MACD", lambda: np.sign(ta.macd(c).iloc[:, 0] - ta.macd(c).iloc[:, 2]))
    put("Stochastic", lambda: sg(ta.stoch(h, l, c).iloc[:, 0] < 20, ta.stoch(h, l, c).iloc[:, 0] > 80))
    put("CCI", lambda: sg(ta.cci(h, l, c) < -100, ta.cci(h, l, c) > 100))
    put("Williams %R", lambda: sg(ta.willr(h, l, c) < -80, ta.willr(h, l, c) > -20))
    put("ROC10", lambda: np.sign(ta.roc(c, length=10)))
    put("Awesome Osc", lambda: np.sign(ta.ao(h, l)))
    put("Bollinger", lambda: sg(c < ta.bbands(c, length=20).iloc[:, 0], c > ta.bbands(c, length=20).iloc[:, 2]))
    put("ADX/DI", lambda: np.sign(ta.adx(h, l, c).iloc[:, 1] - ta.adx(h, l, c).iloc[:, 2]) * (ta.adx(h, l, c).iloc[:, 0] > 20))
    put("OBV", lambda: np.sign(ta.obv(c, v) - ta.ema(ta.obv(c, v), length=20)))
    put("CMF", lambda: sg(ta.cmf(h, l, c, v) > .05, ta.cmf(h, l, c, v) < -.05))
    put("MFI", lambda: sg(ta.mfi(h, l, c, v, length=14) < 20, ta.mfi(h, l, c, v, length=14) > 80))
    put("Supertrend", lambda: ta.supertrend(h, l, c).iloc[:, 1])
    return pd.DataFrame(S).fillna(0)

def tone_study(t, c):
    # best-effort: GDELT daily news tone vs later price moves (no free long headline archive exists)
    q = quote(f'"{ALIASES[t][0]}" sourcelang:english')
    j = httpx.get(f"https://api.gdeltproject.org/api/v2/doc/doc?query={q}&mode=timelinetone&timespan=52w&format=json", timeout=15).json()
    d = j["timeline"][0]["data"]
    tone = pd.Series([x["value"] for x in d], index=pd.to_datetime([x["date"][:8] for x in d]))
    tone = tone.groupby(tone.index.normalize()).mean()
    dev = tone - tone.rolling(30).mean()
    px = c.copy(); px.index = pd.to_datetime(px.index).tz_localize(None).normalize()
    x = pd.concat([dev, px.shift(-5) / px - 1], axis=1, join="inner").dropna(); x.columns = ["dev", "f5"]
    pos, neg = x[x.dev > 0], x[x.dev <= 0]
    return {"days": int(len(x)), "corr": f2(x.dev.corr(x.f5)), "up_pos": f2((pos.f5 > 0).mean() * 100), "up_neg": f2((neg.f5 > 0).mean() * 100), "tone_now": f2(dev.dropna().iloc[-1])}

def _history(t):
    df = yf.download(t, period="max", interval="1d", progress=False, auto_adjust=True, threads=False)
    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
    df.columns = [str(x).lower() for x in df.columns]
    df = df[["open", "high", "low", "close", "volume"]].astype(float).dropna().tail(3800)
    if len(df) < 400: raise ValueError("not enough history")
    c = df["close"]; S = hist_signals(df); score = 50 + 50 * S.mean(axis=1); now = float(score.iloc[-1])
    out = {"bars": len(df), "years": f2(len(df) / 252), "from": str(df.index[0])[:10], "score_now": f2(now), "analogs": []}
    f10 = c.shift(-10) / c - 1; out["base_up10"] = f2((f10.dropna() > 0).mean() * 100)
    for H in (5, 10, 20):
        x = (c.shift(-H) / c - 1)[((score - now).abs() <= 10)].dropna()
        if len(x) >= 20:
            out["analogs"].append({"h": H, "n": int(len(x)), "up": f2((x > 0).mean() * 100), "avg": f2(x.mean() * 100), "p5": f2(x.quantile(.05) * 100), "p95": f2(x.quantile(.95) * 100)})
            if H == 10: out["p_up"] = f2(((x > 0).sum() + 10) / (len(x) + 20) * 100)
    rel = []
    for n in S.columns:
        sn = S[n].iloc[-1]
        if sn == 0: continue
        x = f10[S[n] == sn].dropna()
        if len(x) >= 30:
            hit = (x > 0).mean() if sn > 0 else (x < 0).mean()
            rel.append({"name": n, "dir": "Bullish" if sn > 0 else "Bearish", "hit": f2(hit * 100), "n": int(len(x)), "avg": f2(x.mean() * 100)})
    out["reliability"] = sorted(rel, key=lambda r: -abs(r["hit"] - 50))[:10]
    y = c.groupby(c.index.year).last().pct_change().dropna() * 100
    out["yearly"] = [{"y": int(k), "r": f2(v)} for k, v in y.tail(15).items()]
    mr = (c.groupby([c.index.year, c.index.month]).last().pct_change().dropna() * 100)
    mv = mr[mr.index.get_level_values(1) == pd.Timestamp.now().month]
    out["month"] = {"m": pd.Timestamp.now().strftime("%B"), "avg": f2(mv.mean()), "up": f2((mv > 0).mean() * 100), "n": int(len(mv))}
    dr = c.pct_change().dropna(); dd = c / c.cummax() - 1
    out.update({"vol": f2(dr.std() * np.sqrt(252) * 100), "maxdd": f2(dd.min() * 100), "now_dd": f2(dd.iloc[-1] * 100), "best_day": f2(dr.max() * 100), "worst_day": f2(dr.min() * 100)})
    try: out["news"] = tone_study(t, c)
    except Exception: out["news"] = None
    return out

HC = {}
def history(t):
    if t in HC and time.time() - HC[t][0] < 3600: return HC[t][1]
    try: out = _history(t)
    except Exception as e: out = {"error": str(e)}
    HC[t] = (time.time(), out); return out

@app.get("/api/analysis/{ticker}")
async def analysis(ticker: str, tf: str = "1D"):
    ticker = ticker.upper()
    if ticker not in TICKERS or tf not in TF: raise HTTPException(404, "unknown ticker/timeframe")
    try: _, df, hist = await asyncio.gather(refresh_news(), asyncio.to_thread(load, ticker, tf), asyncio.to_thread(history, ticker))
    except Exception as e: raise HTTPException(502, f"data error: {e}")
    if len(df) < 60: raise HTTPException(502, "not enough bars")
    sigs, atr, scanned = await asyncio.to_thread(signals, df)
    tw = sum(s["weight"] for s in sigs)
    tech = 50 + 50 * sum(s["weight"] * s["signal"] for s in sigs) / tw
    ns, nn = news_score(ticker)
    hp = hist.get("p_up")
    nb = 50 + 50 * ns
    up = max(3.0, min(97.0, (0.55 * tech + 0.25 * nb + 0.20 * hp) if hp is not None else (0.7 * tech + 0.3 * nb)))
    p = float(df["close"].iloc[-1]); sup = float(df["low"].tail(20).min()); res = float(df["high"].tail(20).max())
    lo = max(p - 0.8 * atr, sup - 0.1 * atr); hi = min(lo + 0.6 * atr, p)
    if hi <= lo: lo, hi = p - 0.6 * atr, p - 0.1 * atr
    tp_lo = min(p + 1.5 * atr, res) if res > p + 0.5 * atr else p + 1.5 * atr
    tp_hi = max(p + 2.5 * atr, tp_lo + 0.5 * atr)
    stop = min(sup - 0.25 * atr, p - 1.5 * atr)
    verdict = ("STRONG BUY / BULLISH BREAKOUT", "green") if up >= 65 else ("HIGH RISK / BEARISH DOWN TREND", "red") if up <= 35 else ("NEUTRAL RANGE", "yellow")
    r = lambda x: round(float(x), 2)
    return {"ticker": ticker, "tf": tf, "price": r(p), "atr": r(atr), "asof": str(df.index[-1]),
            "tech_score": r(tech), "news_score": r(ns), "news_count": nn,
            "prob_up": r(up), "prob_down": r(100 - up), "verdict": verdict[0], "color": verdict[1],
            "targets": {"buy": [r(lo), r(hi)], "take_profit": [r(tp_lo), r(tp_hi)], "stop": r(stop), "support": r(sup), "resistance": r(res)},
            "counts": {"bullish": sum(s["signal"] > 0 for s in sigs), "bearish": sum(s["signal"] < 0 for s in sigs),
                       "neutral": sum(s["signal"] == 0 for s in sigs), "scanned": scanned},
            "history": hist, "hist_prob": hp, "fired": [f'{s["name"]} ({s["group"]}): {"Bull" if s["signal"] > 0 else "Bear" if s["signal"] < 0 else "Neutral"}' for s in sigs if s["signal"] != 0][:40]}

@app.get("/api/news")
async def news(ticker: str = "", limit: int = 60):
    await refresh_news()
    items = sorted(NEWS.values(), key=lambda x: -x["ts"])
    if ticker: items = [i for i in items if ticker.upper() in i["tickers"]]
    return items[:limit]

@app.get("/api/sources")
async def sources(): return list(STATUS.values())

@app.get("/api/tickers")
async def tickers(): return TICKERS

@app.get("/", response_class=HTMLResponse)
async def index(): return (Path(__file__).parent / "public" / "index.html").read_text(encoding="utf-8")

# run: uvicorn app:app --host 0.0.0.0 --port 8000
