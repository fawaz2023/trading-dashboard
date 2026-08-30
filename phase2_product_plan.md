# Phase 2: SBIA Terminal — Public SaaS Product Plan

## The One-Line Vision
A subscription-based Indian institutional screener that surfaces what smart money is doing in NSE/BSE — before retail notices.

---

## Tier Architecture

| Feature | Free | Free Trial (14 days) | Pro (₹999/mo) | Institutional (₹4,999/mo) |
|---|---|---|---|---|
| 12-Condition Screener (top 3 only) | ✅ | ✅ Full | ✅ Full | ✅ Full |
| SBIA Institutional Engine signals | ❌ | ✅ | ✅ | ✅ |
| FlexGate 2.0 AI signals | ❌ | ✅ | ✅ | ✅ |
| Verify Conditions (stock lookup) | ✅ Limited | ✅ Full | ✅ Full | ✅ Full |
| Trading Simulation Ledger | ❌ | ✅ | ✅ | ✅ |
| Win Rate / Backtest history | ❌ | ✅ | ✅ | ✅ |
| **Expert Picks (Analyst Notes)** | ❌ | ✅ | ✅ | ✅ |
| **Daily Commentary feed** | ❌ | ✅ | ✅ | ✅ |
| **Exit Announcements** | ❌ | ✅ | ✅ | ✅ |
| Data Health / Pipeline status | ❌ | ❌ | ❌ | ✅ |
| API access (CSV/JSON export) | ❌ | ❌ | ❌ | ✅ |
| Daily email digest | ❌ | ✅ | ✅ | ✅ |
| Seats / Sub-accounts | 1 | 1 | 1 | 5 |

> **Free Trial logic:** 14-day full Pro access, no credit card required at signup. On day 15, access gates to Free tier unless they subscribe. Card required only at conversion — this is the highest-converting pattern for SaaS products.

---

## Phase 2 Build — 4 Stages

---

## ⭐ New Feature: Expert Picks + Daily Commentary

This is the most differentiated, highest-value feature of the entire product. It turns a screener into an **analyst terminal**. Here's how it works:

### Expert Picks — How it works architecturally

You (the analyst) have a private **Admin Panel** at `/admin/expert-picks` where you:
1. Write a pick: choose a symbol, write your thesis (why you like it), set a target and stop-loss
2. Click "Publish" — this writes a row to the `expert_picks` database table
3. Every paid user's dashboard instantly shows the pick in a dedicated **"FZ Expert Picks"** section with a premium card design — **your name, your thesis, your conviction**

### Daily Commentary — The "Analyst Feed"

A lightweight daily notes system, like a private Twitter/Substack feed visible only to subscribers:
- You post from `/admin/commentary` — just a text box + publish button
- Subscribers see it as a **pinned banner at the top of the dashboard every day**
- Older posts scroll into a **"Commentary Archive"** feed below
- This is what keeps users coming back *daily* even when there are no new signals
- Example entry: *"26 Aug — Markets looking indecisive ahead of F&O expiry. Watching CARBORUNIV closely for a breakout above 1120. Holding existing positions."*

### Exit Announcements — Special Design ✨

When you close an Expert Pick position, instead of it just disappearing, users see a **special exit card** with:
- A large **P&L badge** (green if profitable, red if stopped out) with a ticker animation
- Your exit reasoning text: *"Exited CARBORUNIV at ₹1,180 — target achieved in 12 days. +6.6% return."*
- The card stays visible for **7 days** before archiving, styled differently from active picks (greyed out but with a clear outcome stamp)
- This builds **credibility and accountability** — users see the full track record, not just the wins

### Database Schema for this Feature
```sql
-- expert_picks table
id, symbol, entry_date, entry_price, thesis_text, 
target_price, stop_loss, status (ACTIVE/EXITED/STOPPED),
exit_date, exit_price, exit_note, published_by

-- daily_commentary table  
id, post_date, content_text, pinned, published_by
```

> [!CAUTION]
> **SEBI WARNING — Read this carefully before publishing Expert Picks publicly.**
> Publishing specific stock picks with entry/exit prices to paying subscribers is classified as **Investment Advisory Services** under SEBI (Investment Advisers) Regulations, 2013. Running this without a SEBI RIA (Registered Investment Adviser) license can result in fines and criminal prosecution. This is not theoretical — SEBI has actively pursued enforcement in 2024–2026 against unregistered Telegram/Discord channels doing exactly this.
>
> **Your options:**
> 1. **Get SEBI RIA registration** (significant paperwork, net worth requirement of ₹5L for individuals)
> 2. **Reframe as "educational content"** — label picks clearly as "research for educational purposes only, not investment advice" (consult a lawyer on how defensible this is)
> 3. **Launch Expert Picks only after taking legal advice**
>
> The screener (algo-generated signals) is less risky legally because no human is making the recommendation. Expert Picks where *you personally* pick stocks for paying customers is the risky part.

---

### Stage 1: Auth Layer (Foundation — must do first)
The entire product gates on this. Nothing else matters without it.

- **Recommended stack:** Flask-Login OR Supabase (hosted Postgres + Auth)
  - Supabase is strongly preferred — it gives you auth, database, and row-level security out of the box, with a generous free tier
- **What to build:**
  - `/login`, `/register`, `/forgot-password` pages
  - User table: `id, email, plan_tier, subscription_status, created_at`
  - JWT session tokens (Supabase handles this automatically)
  - Dash callback guard: every `layout()` function checks `current_user.plan_tier` before rendering premium content

> [!IMPORTANT]
> Do NOT build auth yourself from scratch. Use Supabase or Auth0. Rolling your own auth for a financial product is a security disaster waiting to happen.

---

### Stage 2: Payment Integration
- **Recommended:** Razorpay (India-native, supports UPI, cards, net banking)
  - Alternatives: Cashfree, PayU
  - Stripe works but has friction for Indian UPI users
- **What to build:**
  - Razorpay subscription plans (monthly/annual)
  - Webhook endpoint: listens for `payment.captured` → upgrades user in Supabase
  - `/billing` page showing plan, next renewal, invoice history
  - Cancellation flow (required by RBI for recurring payments)

> [!WARNING]
> Razorpay requires GST registration + business PAN to activate subscriptions. Plan for 2-3 weeks of paperwork if you don't already have this.

---

### Stage 3: Hosting & Deployment
Your current setup (local Windows machine + ngrok/cloudflare tunnel) is fine for personal use but **cannot serve paying customers**. You need a real server.

**Recommended path:**

```
Hetzner VPS (€5/mo, Germany) OR DigitalOcean Droplet ($6/mo)
  └── Ubuntu 22.04
      └── Gunicorn (production WSGI server) → serves the Dash app
      └── Nginx (reverse proxy + SSL termination)
      └── Certbot (free Let's Encrypt SSL certificate)
      └── GitHub Actions (auto-deploy on push to main)
      └── Cron job (replaces Windows Task Scheduler for daily data update)
```

> [!NOTE]
> The daily data pipeline (`auto_update_smart.py`) runs fine on Linux with a simple `crontab` entry. The only thing to rewrite is the `.bat` file → a `.sh` shell script.

---

### Stage 4: Product Polish (before public launch)
- **Landing page:** A proper marketing page at the root `/` that explains the product to non-users before they sign up (not the dashboard itself)
- **Onboarding flow:** After signup, a quick 3-step walkthrough showing the user what each section does
- **Rate limiting:** Prevent API/data scraping from free tier users
- **Admin panel:** A private `/admin` page for you to see total users, MRR, churn, and flag broken signals

---

## Technology Decision: Keep Dash or Switch?

| Option | Pros | Cons |
|---|---|---|
| **Keep Dash (current)** | Already built, Python-native, no context switch | Auth/login in Dash is hacky (not designed for it), scales poorly with many users |
| **Next.js frontend + FastAPI backend** | Built-for-auth, scales beautifully, modern ecosystem | Requires rebuilding all 3 pages in React |
| **Flask + Jinja2 templates** | Simple, auth is trivial, same language | Less interactive than Dash, no reactive callbacks |

**Honest recommendation:** For Phase 2, seriously consider a **FastAPI backend + Next.js frontend** split. The Dash framework was not designed to handle user sessions, payment states, and admin logic — you will fight it constantly. Your Python data pipeline stays exactly as-is; it just writes CSVs that an API then serves. But this is a bigger lift.

If you want to ship fast and validate demand first, **stay in Dash** with Supabase auth bolted on. You can always migrate later once you have paying customers.

---

## Realistic Timeline

| Stage | Estimated Time |
|---|---|
| Stage 1: Auth Layer | 2–3 weeks |
| Stage 2: Razorpay Integration | 1–2 weeks (+ paperwork time) |
| Stage 3: Server Setup & Deploy | 1 week |
| Stage 4: Polish & Landing Page | 1–2 weeks |
| **Total to launch** | **~6–8 weeks of focused work** |

---

## Open Questions Before We Start

1. **Business entity:** Do you have a registered company/GST number? Required for Razorpay subscriptions.
2. **Pricing validation:** Have you tested if real users will pay ₹999/month? Consider a "founding member" waitlist first.
3. **Tech preference:** Are you comfortable with Python-only (Dash + Flask), or open to a JavaScript frontend (Next.js)?
4. **Data refresh SLA:** Paying customers will expect a specific refresh time (e.g., "data updated by 7:30 PM daily"). What's your commitment?
5. **SEBI compliance:** You are displaying signals that could influence trading decisions. Consult a lawyer about whether this requires SEBI registration as an Investment Advisor.
