"""ConvictionScorer — Layer 2 of the Vikram v3 architecture.

Pure scoring logic over the fundamentals dict from FundamentalFetcher.
Stock classes by MARKET CAP:
  S < ₹500 Cr · M ₹500–10,000 Cr · L > ₹10,000 Cr

Class M (the screener's primary target) gets the full 5-metric score.
Class S gets pledge + FCF only. Class L gets no score — disclaimer only.
"""
import math


def classify(market_cap_cr):
    if market_cap_cr is None:
        return "U"
    if market_cap_cr > 10000:
        return "L"
    if market_cap_cr >= 500:
        return "M"
    return "S"


def _gate_scores(fund):
    """Per-metric /10 fundamental-gate scores (the anchors used in the
    mandatory summary table). Returns {metric: (score/10 or None)}."""
    gate = {}
    ol = fund.get("op_lev_ratio")
    if fund.get("op_lev_inflecting"):
        gate["op_leverage"] = 10 if ol and ol > 3 else 8
    elif ol is not None:
        if ol > 3:
            gate["op_leverage"] = 10
        elif ol >= 2:
            gate["op_leverage"] = 8
        elif ol > 1:
            gate["op_leverage"] = 5
        else:
            gate["op_leverage"] = 2
    ratio = fund.get("fcf_pat_ratio")
    if ratio is not None:
        if ratio > 3.0 or ratio < 1 / 3.0:
            gate["fcf_quality"] = 0
        elif ratio > 0.95:
            gate["fcf_quality"] = 10
        elif ratio >= 0.80:
            gate["fcf_quality"] = 8
        elif ratio >= 0.50:
            gate["fcf_quality"] = 5
        else:
            gate["fcf_quality"] = 2
    direction = fund.get("pledge_direction")
    pledge = fund.get("pledge_trend") or []
    if direction is not None:
        if direction == "falling" and len(pledge) >= 2:
            gate["pledge_trend"] = 10
        elif direction == "falling":
            gate["pledge_trend"] = 8
        elif direction == "flat":
            gate["pledge_trend"] = 7
        else:
            gate["pledge_trend"] = 3 if (pledge[-1] or 0) < 2 else 0
    cov = fund.get("interest_coverage_trend")
    if cov is not None:
        gate["interest_coverage"] = {"improving": 10, "stable": 4, "deteriorating": 2}.get(cov, 4)
    roice = fund.get("roice_pct")
    if roice is not None:
        if roice > 30:
            gate["roice"] = 10
        elif roice >= 20:
            gate["roice"] = 8
        elif roice >= 10:
            gate["roice"] = 6
        elif roice >= 0:
            gate["roice"] = 3
        else:
            gate["roice"] = 0
    return gate


def fundamental_strength(fund):
    """Fundamentals-only 0-100 score from the five gate metrics.

    Used for Class L (no accumulation conviction) and as a fallback when the
    full conviction score cannot apply. Averages the available /10 metrics
    (x10); vetoes drive it to 0.
    """
    gate = _gate_scores(fund)
    avail = [v for v in gate.values() if v is not None]
    if len(avail) < 2:
        return gate, None, "INSUFFICIENT_DATA"
    veto = False
    pledge = fund.get("pledge_trend") or [0.0]
    if fund.get("pledge_direction") == "rising" and (pledge[-1] or 0) >= 0.5:
        veto = True
    ratio = fund.get("fcf_pat_ratio")
    if ratio is not None and not math.isclose(ratio, 0) and (ratio > 3.0 or ratio < 1 / 3.0):
        veto = True
    if veto:
        return gate, 0, "VETO"
    score = round(sum(avail) / len(avail) * 10)
    if score >= 75:
        rating = "STRONG"
    elif score >= 45:
        rating = "MODERATE"
    else:
        rating = "WEAK"
    return gate, score, rating


class ConvictionScorer:
    def score(self, fund):
        """fund: dict from FundamentalFetcher.fetch(). Returns scoring dict."""
        if not fund or fund.get("error"):
            return {
                "stock_class": "U",
                "veto": False,
                "veto_reasons": [],
                "score": None,
                "rating": "FUNDAMENTALS_UNAVAILABLE",
                "boosters": [],
                "drags": [],
                "display_badge": f"❓ Fundamentals unavailable ({fund.get('error', 'no data') if fund else 'no data'})",
            }

        mcap = fund.get("market_cap_cr")
        stock_class = classify(mcap)
        base = {
            "stock_class": stock_class,
            "market_cap_cr": mcap,
            "veto": False,
            "veto_reasons": [],
            "score": None,
            "rating": None,
            "boosters": [],
            "drags": [],
            "display_badge": None,
        }

        if stock_class == "L":
            gate, fs_score, fs_rating = fundamental_strength(fund)
            base["rating"] = "LARGE_CAP_DISCLAIMER"
            base["fundamental_score"] = fs_score
            base["fundamental_rating"] = fs_rating
            base["gate"] = gate
            if fs_score is not None:
                base["display_badge"] = f"⚠️ Large-cap · Fundamentals {fs_score}/100 ({fs_rating}) — rebalancing noise; verify separately"
            else:
                base["display_badge"] = "⚠️ Large-cap signal — likely rebalancing noise; verify separately"
            return base

        score = 50
        boosters, drags = [], []
        veto_reasons = []

        # ---- vetoes (hard, any class) ----
        pledge = fund.get("pledge_trend") or [0.0]
        if fund.get("pledge_direction") == "rising" and (pledge[-1] or 0) >= 0.5:
            veto_reasons.append(f"Promoter pledge rising QoQ ({fund.get('pledge_trend')})")

        ratio = fund.get("fcf_pat_ratio")
        if ratio is not None and not math.isclose(ratio, 0) and (ratio > 3.0 or ratio < 1 / 3.0):
            veto_reasons.append(f"FCF/PAT 3yr cumulative divergence {ratio:.2f}x (outside [0.33, 3.0])")

        # ---- pledge points ----
        if fund.get("pledge_direction") == "falling" and (pledge[-1] or 0) >= 1.0:
            score += 10
            boosters.append("Pledge falling QoQ (+10)")

        # ---- FCF/PAT points ----
        if ratio is not None:
            if 0.8 <= ratio <= 1.2:
                score += 10
                boosters.append(f"OCF ≈ PAT (3yr ratio {ratio:.2f}, +10)")
            elif 0.0 < ratio <= 3.0 and not (ratio > 2.0 or ratio < 0.5):
                score -= 5
                drags.append(f"FCF/PAT divergence 20–100% (ratio {ratio:.2f}, -5)")

        # ---- Class M full metrics ----
        if stock_class == "M":
            if fund.get("op_lev_inflecting"):
                score += 25
                boosters.append(f"Operating leverage inflecting ({fund.get('op_lev_ratio', 0):.1f}x, +25)")
            roice = fund.get("roice_pct")
            if roice is not None:
                if roice > 20:
                    score += 15
                    boosters.append(f"RoICE {roice}% (+15)")
                elif roice >= 10:
                    score += 8
                    boosters.append(f"RoICE {roice}% (+8)")
            cov = fund.get("interest_coverage_trend")
            if cov == "improving":
                score += 10
                boosters.append("Interest coverage improving (+10)")
            elif cov == "deteriorating":
                score -= 10
                drags.append("Interest coverage deteriorating (-10)")

        if veto_reasons:
            base["veto"] = True
            base["veto_reasons"] = veto_reasons
            base["score"] = 0
            base["rating"] = "VETO"
            base["boosters"] = boosters
            base["drags"] = drags
            base["display_badge"] = f"🚫 VETO: {veto_reasons[0]}"
            return base

        score = max(0, min(100, score))
        if score >= 75:
            rating = "HIGH_CONVICTION"
            badge = f"⚡ {score} | ✅ No Vetoes"
        elif score >= 45:
            rating = "MODERATE"
            badge = f"✅ {score} | No Vetoes"
        else:
            rating = "LOW"
            badge = f"⚠️ {score} | Weak fundamentals"
        if stock_class == "M" and fund.get("op_lev_inflecting"):
            badge += " | 🚀 Op Lev"
        base["score"] = score
        base["rating"] = rating
        base["boosters"] = boosters
        base["drags"] = drags
        base["display_badge"] = badge
        return base
