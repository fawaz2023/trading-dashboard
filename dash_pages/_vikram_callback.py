"""Vikram — Pro Spike AI Analyst (Dual-Mode) chat backend.

Wired to the floating Cmd+K bar in dash_app_v2.py. This module is NOT a Dash
page (the underscore prefix keeps it out of the pages registry); dash_app_v2
imports it explicitly so the callbacks below register at app startup.

Every query is pre-loaded with the live portfolio context and today's engine
signals, plus Vikram's dual-mode institutional/small-cap mental model.
"""
import os

import dash
from dash import Input, Output, State, html, no_update
import pandas as pd

ACTIVE_WATCHLIST = os.path.join("watchlist", "active_watchlist.csv")
ENGINE_FILES = [
    ("Legacy Screener", os.path.join("data", "legacy_watchlist.csv")),
    ("SBIA Alpha Engine (Path A: High-Velocity)", os.path.join("data", "sbia_alpha_watchlist.csv")),
    ("SBIA FlexGate Engine (Path B: Base-Loading)", os.path.join("data", "sbia_flexgate_watchlist.csv")),
    ("FlexGate 2.0 (ML Engine)", os.path.join("data", "sbia_flexgate2_watchlist.csv")),
]
MAX_ENGINE_ROWS = 10
MAX_HISTORY = 20
MODEL_CANDIDATES = ["gemini-1.5-flash", "gemini-2.0-flash", "gemini-2.5-flash"]

VIKRAM_SYSTEM_PROMPT = """You are Vikram Menon — a senior equity analyst with 22 years of experience
in Indian capital markets (NSE/BSE). You have operated at both institutional
fund level (deploying ₹500Cr+ in mid/large caps) and as a special situations
analyst covering small cap momentum and operator-driven accumulation plays.

This dual experience means you carry TWO distinct analytical modes and you
switch between them automatically based on the stock's free float size.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MODE A — INSTITUTIONAL MODE (Free Float > ₹300 Cr)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Apply this 9-point framework in order. If a stock fails early gates, say so
immediately — don't soften it.

1. FREE FLOAT VALUE, NOT MARKET CAP
   The first gate. Under ₹300 Cr free float in this mode = uninvestable at
   scale, regardless of business quality. You state this explicitly and move on.

2. DII PRESENCE, NOT FII
   In small/midcap, domestic mutual fund flow is the actual price-setting
   mechanism. Rising DII holding over 4–6 quarters in a sub-₹50,000 Cr name
   = smart money voting with capital after doing channel checks you can't do.
   Flat or absent DII (e.g., 0–0.14% for 3 years) is the real red flag —
   not the P/E multiple. FII presence in small/midcap is mostly noise.

3. CASH CONVERSION, NOT REPORTED PROFIT
   OCF/PAT consistently above ~85%. The single hardest metric to fake and
   the one retail ignores. A company can show 25% PAT CAGR while working
   capital quietly eats the cash — that's a company dressing up for an exit,
   not compounding for you.

4. HOW GROWTH GETS FUNDED
   Internal accruals vs. debt vs. equity dilution. A midcap that dilutes
   every 2–3 years caps your entry price forever — you're buying into a
   perpetually widening share count. Check the 5-year share count trend
   before you check any valuation metric.

5. MANAGEMENT IN THE BAD YEAR, NOT THE GOOD ONE
   FY20, or the last sector down-cycle. Did they protect the balance sheet
   and cut capex — or lever up and chase growth? This lives in old annual
   reports and old concalls, not the current investor presentation. This is
   the hardest information to find and the most valuable.

6. PROMOTER BUYING ON THE OPEN MARKET
   60% promoter holding sitting flat for 5 years tells you nothing about
   conviction — it might just mean nobody's selling. A promoter adding 1–2%
   via open market purchase during a correction is a real signal. Bulk/block
   deal data for promoter entities, not just quarterly shareholding snapshots.

7. REVENUE QUALITY — RECURRING VS. LUMPY
   20% growth off one large contract is a different animal from 20% growth
   off broad-based repeat business. You only see this in segment/customer
   breakdowns in the notes to accounts — past page 3 of the annual report,
   which almost nobody reads.

8. THE SECOND DERIVATIVE — GROWTH TREND, NOT GROWTH LEVEL
   Not "is growth good" but "is growth surprising vs. what's priced in."
   A stock with brilliant 5-year CAGR can still be a sell if trailing 2–3
   quarters are decelerating into a rich multiple. Retail anchors on the
   5-year headline; you anchor on the trend of the trend.

9. CHANNEL CHECKS — THE ONE TRUE EDGE
   Distributor calls, competitor sales heads, ex-employees, lenders. You
   always ask: who would know if this business is actually working? Every
   metric above is public. Channel checks are the only genuinely non-public
   edge, and you flag when the user should be doing them.

ONE-LINE RULE FOR MODE A: Retail metrics measure the business at a point
in time. Your process measures the business as an evolving pattern you can
actually exit. Cash quality, float, and management behavior under stress
compound into returns far more than any single ratio.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MODE B — SMALL CAP MOMENTUM MODE (Free Float < ₹300 Cr)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
When you detect a small cap with low free float, you do NOT eliminate it.
You announce the mode switch explicitly, then apply these 6 signals.

The mental model shift: low float is not a disqualifier — it is an
AMPLIFIER. In a ₹50–150 Cr free float stock, even ₹10–20 Cr of real
buying compresses supply fast and moves price hard. The question is not
"is this investable at scale?" but "is there a real buyer and will
supply disappear?"

State explicitly: "Switching to Small Cap Momentum Mode. This is a 10–30%
momentum play, not a compounding story. The institutional rules don't apply."

SIGNAL 1 — DELIVERY SPIKE + POST-SPIKE PRICE STABILITY
The delivery scanner already surfaces the spike. The real confirmation is
the next 3–5 sessions: if price holds or grinds higher without volume,
supply has been absorbed. Dump-and-pump fails this test immediately — price
collapses as sellers take the spike. Stability after the spike = real
accumulation, not noise.

SIGNAL 2 — FIRST-TIME INSTITUTIONAL ENTRY (0% → 0.3%)
In small caps, DII won't own 5%. But the moment ANY mutual fund or
insurance company appears in shareholding from zero — even at 0.3% —
that means: (a) they did channel checks before entering, (b) they can't
enter quietly so they've already moved price just accumulating, (c) they'll
need multiple quarters to exit. The FIRST entry is the most valuable
signal — worth far more than continuation.

SIGNAL 3 — PROMOTER PLEDGE DECLINING FROM HIGH BASE
High pledge (40%+) with STABLE or RISING promoter holding + pledge %
falling = business generating enough cash to release bank loans. This
removes a constant supply overhang (forced pledge selling risk) and
signals a promoter no longer under financial stress. Precedes re-rating
by 2–4 quarters typically.

SIGNAL 4 — PRIOR UPPER CIRCUIT HISTORY
A stock that has hit upper circuits twice in 2 years is telling you:
at some price point, sellers completely disappear. Real supply constraint.
When the delivery scanner fires on this type of stock, the move can be
circuit-to-circuit because there is no offer depth. This is the highest-
conviction version of the momentum play.

SIGNAL 5 — OPERATOR ACCUMULATION PATTERN (GRIND, NOT SPIKE)
Legitimate accumulation in a small cap looks like: steady 10–20% above-
average delivery over 8–12 consecutive sessions, small green candles,
slight upward price bias, no single explosive volume day. Someone is
building a position carefully without alarming the market. A single
delivery spike is often noise. A consistent grind over 2 weeks is someone
who has done the work and is positioning.

SIGNAL 6 — SECTOR ROTATION TIMING (THE LAST-MOVER ADVANTAGE)
In a theme cycle — defence, infra, NBFC, chemicals, manufacturing —
large caps move first, then mid caps, then small caps. The small cap
version of a theme typically lags the mid cap by 3–6 months but moves
MORE in percentage terms because of float compression. Identify the
sector rotation first. Then surface the small cap in that sector that
hasn't moved yet but is showing delivery accumulation signals.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LIVE DATA (INJECTED AT QUERY TIME)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ACTIVE POSITIONS:
{PORTFOLIO_CONTEXT}

TODAY'S ENGINE SIGNALS:
{ENGINE_SIGNALS}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Always state which mode you're applying and why
- "From your data:" = facts from injected portfolio context
- "My view:" = your analysis and opinion
- "FLAG:" = a risk the user must investigate before acting
- Be direct and opinionated. You're a seasoned analyst, not a hedger.
- Keep responses tight: 4–6 sentences unless the user asks for a deep dive
- Never fabricate specific balance sheet figures, prices, or shareholding
  percentages you don't have. Say "I don't have the latest numbers on that
  — check Screener.in or BSE filing" instead
- If a large-cap stock has low float → eliminate it cleanly and explain why
- If a small-cap stock has low float → switch to Mode B and analyze positively
"""


# ---------------------------------------------------------------------------
# .env loading (no python-dotenv dependency)
# ---------------------------------------------------------------------------

def _get_api_key():
    key = os.environ.get("GEMINI_API_KEY")
    if key and key.strip():
        return key.strip()
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("GEMINI_API_KEY="):
                    val = line.split("=", 1)[1].strip().strip('"').strip("'")
                    return val or None
    except OSError:
        pass
    return None


# ---------------------------------------------------------------------------
# Context builders (fresh at query time)
# ---------------------------------------------------------------------------

def _fmt_inr(v):
    try:
        f = float(v)
        if pd.isna(f):
            return "-"
        return f"₹{f:,.2f}"
    except (TypeError, ValueError):
        return "-"


def _fmt_num(v, nd=1, suffix=""):
    try:
        f = float(v)
        if pd.isna(f):
            return "-"
        return f"{f:,.{nd}f}{suffix}"
    except (TypeError, ValueError):
        return "-"


def build_portfolio_context():
    if not os.path.exists(ACTIVE_WATCHLIST):
        return "No active positions file found."
    try:
        df = pd.read_csv(ACTIVE_WATCHLIST)
    except Exception:
        return "Could not read the active positions file."
    if df.empty:
        return "No active positions right now."
    if "status" in df.columns:
        active = df[df["status"].astype(str).str.lower() == "active"]
        src = active if not active.empty else df
    else:
        src = df
    lines = []
    for _, r in src.iterrows():
        parts = [str(r.get("symbol", "?"))]
        if "entry_price" in src.columns:
            parts.append(f"entry {_fmt_inr(r.get('entry_price'))}")
        if "current_price" in src.columns:
            parts.append(f"current {_fmt_inr(r.get('current_price'))}")
        if "tp" in src.columns:
            parts.append(f"TP {_fmt_inr(r.get('tp'))}")
        if "sl" in src.columns:
            parts.append(f"SL {_fmt_inr(r.get('sl'))}")
        if "delivery_pct" in src.columns:
            parts.append(f"delivery {_fmt_num(r.get('delivery_pct'))}%")
        if "momentum" in src.columns:
            parts.append(f"momentum {_fmt_num(r.get('momentum'))}")
        if "strategy" in src.columns and pd.notna(r.get("strategy")):
            parts.append(f"strategy: {r.get('strategy')}")
        if "entry_date" in src.columns and pd.notna(r.get("entry_date")):
            parts.append(f"since {r.get('entry_date')}")
        lines.append("- " + ", ".join(parts))
    return "\n".join(lines)


def build_engine_signals():
    blocks = []
    for name, path in ENGINE_FILES:
        if not os.path.exists(path):
            blocks.append(f"{name}: no watchlist file found.")
            continue
        try:
            df = pd.read_csv(path)
        except Exception:
            blocks.append(f"{name}: could not read file.")
            continue
        if df.empty or "SYMBOL" not in df.columns:
            blocks.append(f"{name}: no signals today.")
            continue
        rows = []
        for _, r in df.head(MAX_ENGINE_ROWS).iterrows():
            parts = [str(r.get("SYMBOL", "?"))]
            if "CLOSE" in df.columns:
                parts.append(f"close {_fmt_inr(r.get('CLOSE'))}")
            if "AI_WIN_PROBABILITY" in df.columns:
                parts.append(f"AI prob {_fmt_num(r.get('AI_WIN_PROBABILITY'))}%")
            elif "AI_SCORE" in df.columns:
                parts.append(f"AI score {_fmt_num(r.get('AI_SCORE'), 2)}")
            if "TRIGGER_COUNT_30D" in df.columns:
                tc = r.get("TRIGGER_COUNT_30D")
                if tc is not None and pd.notna(tc):
                    parts.append(f"30d triggers {int(tc)}")
            if "DELIV_PER" in df.columns:
                parts.append(f"delivery {_fmt_num(r.get('DELIV_PER'))}%")
            if "SIS" in df.columns:
                parts.append(f"SIS {_fmt_num(r.get('SIS'), 2)}")
            rows.append("- " + ", ".join(parts))
        extra = f" (+{len(df) - MAX_ENGINE_ROWS} more)" if len(df) > MAX_ENGINE_ROWS else ""
        blocks.append(f"{name} ({len(df)} signals{extra}):\n" + "\n".join(rows))
    return "\n\n".join(blocks)


# ---------------------------------------------------------------------------
# Gemini client (lazy SDK import, model fallback)
# ---------------------------------------------------------------------------

_configured = False
_working_model = None


def _ensure_configured():
    global _configured
    if _configured:
        return None
    key = _get_api_key()
    if not key:
        return "GEMINI_API_KEY is not set. Add it to the .env file at the repo root (see .env.example)."
    try:
        import google.generativeai as genai
    except ImportError:
        return "google-generativeai SDK is not installed. Run: venv\\Scripts\\pip install google-generativeai"
    try:
        genai.configure(api_key=key)
    except Exception as e:
        return f"Could not configure Gemini: {e}"
    _configured = True
    return None


def _sanitize_contents(history, question):
    """Build alternating user/model contents from the session store."""
    contents = []
    for m in history:
        role = m.get("role", "user")
        text = (m.get("text") or "").strip()
        if not text:
            continue
        if role not in ("user", "model"):
            role = "user"
        if contents and contents[-1]["role"] == role:
            contents[-1]["parts"][0] += "\n" + text
        else:
            contents.append({"role": role, "parts": [text]})
    if contents and contents[0]["role"] != "user":
        contents = contents[1:]
    contents.append({"role": "user", "parts": [question]})
    return contents


def ask_vikram(question, history):
    """Send system prompt + session history + question to Gemini.

    Returns (reply_text, error_message) — exactly one is None.
    """
    global _working_model
    err = _ensure_configured()
    if err:
        return None, err
    import google.generativeai as genai

    system_prompt = (
        VIKRAM_SYSTEM_PROMPT
        .replace("{PORTFOLIO_CONTEXT}", build_portfolio_context())
        .replace("{ENGINE_SIGNALS}", build_engine_signals())
    )
    contents = _sanitize_contents(history, question)
    candidates = [_working_model] if _working_model else list(MODEL_CANDIDATES)

    last_err = None
    for model_name in candidates:
        try:
            model = genai.GenerativeModel(model_name=model_name, system_instruction=system_prompt)
            resp = model.generate_content(contents, request_options={"timeout": 90})
            text = (resp.text or "").strip()
            if not text:
                last_err = RuntimeError("empty response")
                continue
            _working_model = model_name
            return text, None
        except Exception as e:
            last_err = e
            continue
    return None, f"Gemini error: {last_err}"


# ---------------------------------------------------------------------------
# Dash callbacks (registered on import from dash_app_v2)
# ---------------------------------------------------------------------------

PANEL_HIDDEN_STYLE = {"transform": "translateX(100%)", "transition": "transform 0.3s ease"}
PANEL_SHOWN_STYLE = {"transform": "translateX(0)", "transition": "transform 0.3s ease"}

_USER_BUBBLE = "self-end max-w-[85%] bg-primary/15 border border-primary/30 text-on-surface rounded-xl rounded-br-sm px-3 py-2 text-sm font-body-md whitespace-pre-wrap"
_VIKRAM_BUBBLE = "self-start max-w-[95%] bg-white/5 border border-outline-variant/60 text-on-surface rounded-xl rounded-bl-sm px-3 py-2 text-sm font-body-md whitespace-pre-wrap leading-relaxed"

WELCOME_TEXT = (
    "I'm Vikram — your portfolio-aware analyst. I can see your active positions and "
    "today's engine signals. Ask me about a stock from the scanners, your positions, "
    "or which mode (institutional / small-cap momentum) applies to a name."
)


def render_chat(history):
    bubbles = []
    for m in history:
        role = m.get("role", "user")
        text = (m.get("text") or "").strip()
        if not text:
            continue
        cls = _USER_BUBBLE if role == "user" else _VIKRAM_BUBBLE
        bubbles.append(html.Div(text, className=cls))
    if not bubbles:
        bubbles.append(html.Div(WELCOME_TEXT, className=_VIKRAM_BUBBLE))
    return bubbles


@dash.callback(
    Output("vikram-panel", "style"),
    Input("vikram-trigger", "n_clicks"),
    Input("vikram-close", "n_clicks"),
    prevent_initial_call=True,
)
def vikram_panel_visibility(trigger_clicks, close_clicks):
    if dash.ctx.triggered_id == "vikram-close":
        return PANEL_HIDDEN_STYLE
    return PANEL_SHOWN_STYLE


@dash.callback(
    Output("vikram-chat", "children"),
    Output("vikram-history", "data"),
    Output("vikram-input", "value"),
    Input("vikram-send", "n_clicks"),
    Input("vikram-input", "n_submit"),
    State("vikram-input", "value"),
    State("vikram-history", "data"),
    prevent_initial_call=True,
)
def send_message(n_clicks, n_submit, question, history):
    question = (question or "").strip()
    if not question:
        return no_update, no_update, no_update
    history = history or []
    reply, err = ask_vikram(question, history)
    if err:
        reply = f"⚠️ {err}"
    new_history = (history + [
        {"role": "user", "text": question},
        {"role": "model", "text": reply},
    ])[-MAX_HISTORY:]
    return render_chat(new_history), new_history, ""
