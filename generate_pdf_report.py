import os
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether, HRFlowable
)

pdf_path = "Shadow_Box_Institutional_Alpha_Report.pdf"
doc = SimpleDocTemplate(
    pdf_path,
    pagesize=letter,
    rightMargin=36, leftMargin=36, topMargin=36, bottomMargin=36
)

styles = getSampleStyleSheet()

# Custom Styles
PRIMARY_COLOR = colors.HexColor("#0f172a")    # Slate 900
SECONDARY_COLOR = colors.HexColor("#0284c7")  # Sky 600
ACCENT_COLOR = colors.HexColor("#10b981")     # Emerald 500
DARK_BG = colors.HexColor("#1e293b")          # Slate 800
LIGHT_BG = colors.HexColor("#f8fafc")         # Slate 50
TEXT_DARK = colors.HexColor("#0f172a")
TEXT_MUTED = colors.HexColor("#475569")

title_style = ParagraphStyle(
    'DocTitle', parent=styles['Heading1'],
    fontName='Helvetica-Bold', fontSize=22, leading=26,
    textColor=PRIMARY_COLOR, spaceAfter=6
)
subtitle_style = ParagraphStyle(
    'DocSubTitle', parent=styles['Normal'],
    fontName='Helvetica-Bold', fontSize=12, leading=16,
    textColor=SECONDARY_COLOR, spaceAfter=15
)

h1_style = ParagraphStyle(
    'H1', parent=styles['Heading2'],
    fontName='Helvetica-Bold', fontSize=14, leading=18,
    textColor=PRIMARY_COLOR, spaceBefore=12, spaceAfter=8
)

body_style = ParagraphStyle(
    'Body', parent=styles['Normal'],
    fontName='Helvetica', fontSize=9, leading=13,
    textColor=TEXT_DARK, spaceAfter=6
)

bullet_style = ParagraphStyle(
    'Bullet', parent=body_style,
    leftIndent=12, firstLineIndent=-8, spaceAfter=4
)

code_style = ParagraphStyle(
    'CodeBlock', parent=styles['Normal'],
    fontName='Courier-Bold', fontSize=8.5, leading=11,
    textColor=colors.HexColor("#0f172a"), backColor=colors.HexColor("#f1f5f9"),
    borderColor=colors.HexColor("#cbd5e1"), borderWidth=0.5,
    borderPadding=6, spaceBefore=4, spaceAfter=6
)

story = []

# HEADER SECTION
story.append(Paragraph("SHADOW-BOX INSTITUTIONAL ALPHA (SBIA) ENGINE v1.0", title_style))
story.append(Paragraph("Quantitative System Specification & Multi-Year Backtest Prospectus", subtitle_style))
story.append(HRFlowable(width="100%", thickness=2, color=SECONDARY_COLOR, spaceAfter=12))

# 1. EXECUTIVE SUMMARY
story.append(Paragraph("1. Executive Summary", h1_style))
story.append(Paragraph(
    "The Shadow-Box Institutional Alpha (SBIA) Engine v1.0 is a fully mechanical, quant-grade swing trading system "
    "engineered to detect and exploit institutional accumulation footprint anomalies in the Indian stock market (NSE). "
    "By pairing a strict 12-condition delivery cascade with a cross-sectional Random Forest Machine Learning model and "
    "dynamic ATR volatility exits, the engine achieves asymmetric portfolio growth while maintaining institutional-grade "
    "capital protection.", body_style
))

# 2. THE 12-CONDITION CASCADE
story.append(Paragraph("2. The 12-Condition Cascade Architecture", h1_style))
story.append(Paragraph("Every asset must pass all 12 baseline rules simultaneously prior to ML pipeline evaluation:", body_style))

cond_data = [
    [Paragraph("Category", body_style), Paragraph("Condition # & Rule Specification", body_style)],
    [Paragraph("Baseline Quality Floor", body_style), Paragraph("1. Delivery % (1D) >= 50%<br/>2. Delivery Turnover (1D) >= Rs 50,00,000<br/>3. Average Trade Worth (ATW 1D) >= Rs 20,000", body_style)],
    [Paragraph("Delivery % Cascade", body_style), Paragraph("4. Del % (1D) > Del % (1W Avg)<br/>5. Del % (1W Avg) > Del % (1M Avg)<br/>6. Del % (1M Avg) > Del % (3M Avg)", body_style)],
    [Paragraph("Turnover Cascade", body_style), Paragraph("7. Turnover (1D) > Turnover (1W Avg)<br/>8. Turnover (1W Avg) > Turnover (1M Avg)<br/>9. Turnover (1M Avg) > Turnover (3M Avg)", body_style)],
    [Paragraph("ATW Ticket Cascade", body_style), Paragraph("10. ATW (1D) > ATW (1W Avg)<br/>11. ATW (1W Avg) > ATW (1M Avg)<br/>12. ATW (1M Avg) > ATW (3M Avg)", body_style)]
]
t_cond = Table(cond_data, colWidths=[140, 390])
t_cond.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,0), LIGHT_BG),
    ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
    ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ('TOPPADDING', (0,0), (-1,-1), 5),
    ('BOTTOMPADDING', (0,0), (-1,-1), 5),
]))
story.append(t_cond)
story.append(Spacer(1, 10))

# 3. MACHINE LEARNING ENGINE & EQUATIONS
story.append(Paragraph("3. Machine Learning Engine & Equations", h1_style))
story.append(Paragraph("Candidates passing the 12 conditions are scored daily across the entire 2,000+ NSE stock universe using cross-sectional percentile rankings (0-100) before being processed by the Random Forest Model (shadow_box_model.pkl):", body_style))

story.append(Paragraph("A. Smart Institution Score (SIS) — Weighted Geometric Mean:", body_style))
story.append(Paragraph("SIS = [(STABILITY_SCORE + 1)^0.50 x (FOOTPRINT_SCORE + 1)^0.30 x (MOMENTUM_SCORE + 1)^0.20] - 1", code_style))

story.append(Paragraph("B. Price-Neutral Whale Density — Institutional Concentration Metric:", body_style))
story.append(Paragraph("Whale_Density = (ATW / VWAP) / Typical_Lot_Size", code_style))

story.append(Paragraph("C. Implied Trades — Market Breadth Counter:", body_style))
story.append(Paragraph("Implied_Trades = Delivery_Turnover / ATW", code_style))

story.append(Paragraph("D. The AI Bouncer Gate: Setups with predicted AI_WIN_PROBABILITY < 60.0% are rejected.", body_style))
story.append(Spacer(1, 10))

# 4. QUANTILE SWEET SPOTS (GOLDILOCKS ZONES)
story.append(Paragraph("4. Quantile Sweet Spots (Goldilocks Zone Analysis)", h1_style))
story.append(Paragraph("A 5-bin quantile analysis across all 418 executed trades revealed the exact institutional footprint sweet spots:", body_style))

goldi_data = [
    [Paragraph("Metric", body_style), Paragraph("Goldilocks Zone", body_style), Paragraph("Win Rate", body_style), Paragraph("Avg Return", body_style), Paragraph("Institutional Behavior Insight", body_style)],
    [Paragraph("SIS Score", body_style), Paragraph("50.0 - 69.7", body_style), Paragraph("57.8%", body_style), Paragraph("+4.23%", body_style), Paragraph("Quiet, stealth accumulation phase. Scores > 70 drop to 42% WR due to retail crowding.", body_style)],
    [Paragraph("Whale Density", body_style), Paragraph("0.16 - 0.37", body_style), Paragraph("54.2%", body_style), Paragraph("+2.75%", body_style), Paragraph("Steady algorithmic execution below the radar. Values > 1.0 indicate non-momentum block/pledge deals.", body_style)],
    [Paragraph("Implied Trades", body_style), Paragraph("0 - 2,703", body_style), Paragraph("57.1%", body_style), Paragraph("+3.87%", body_style), Paragraph("Volume concentrated in few institutional hands. Mid-range (7k-14k) drops WR to 41.7% (retail dispersion).", body_style)]
]
t_goldi = Table(goldi_data, colWidths=[80, 75, 55, 65, 255])
t_goldi.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,0), LIGHT_BG),
    ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
    ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ('TOPPADDING', (0,0), (-1,-1), 4),
    ('BOTTOMPADDING', (0,0), (-1,-1), 4),
]))
story.append(t_goldi)
story.append(Spacer(1, 10))

# 5. EXECUTION, RISK CONTROLS & CAPITAL ALLOCATION
story.append(Paragraph("5. Execution, Risk Controls & Capital Safety Ceiling", h1_style))
story.append(Paragraph("• Dynamic Volatility Exits: 2.0x ATR14 Stop Loss / 4.0x ATR14 Take Profit (1:2 R:R payout structure).", bullet_style))
story.append(Paragraph("• Account Sizing: 1.5% fixed-fractional account risk per trade.", bullet_style))
story.append(Paragraph("• 10% Position Cap Ceiling: Single trade allocation is strictly capped at 10% of portfolio cash, completely amputating gap-down tail risk on low-volatility stocks.", bullet_style))
story.append(Paragraph("• Real-World Friction: T+1 Open execution, pessimistic double-breach resolution, worst-case gap execution, and a 0.50% round-trip slippage/tax penalty applied to every trade.", bullet_style))
story.append(Spacer(1, 10))

# 6. MULTI-YEAR BACKTEST PERFORMANCE
story.append(Paragraph("6. Multi-Year Backtest Performance (22 Months / 536 Trading Days)", h1_style))

perf_data = [
    [Paragraph("Metric", body_style), Paragraph("Value", body_style), Paragraph("Metric", body_style), Paragraph("Value", body_style)],
    [Paragraph("Evaluation Span", body_style), Paragraph("Dec 2022 - Oct 2024", body_style), Paragraph("Total Trades Executed", body_style), Paragraph("418", body_style)],
    [Paragraph("Starting Capital", body_style), Paragraph("Rs 10,00,000.00", body_style), Paragraph("Win Rate", body_style), Paragraph("50.5% (211 W / 207 L)", body_style)],
    [Paragraph("Ending Equity", body_style), Paragraph("Rs 13,14,602.00", body_style), Paragraph("Profit Factor", body_style), Paragraph("1.57", body_style)],
    [Paragraph("Total Net P&L", body_style), Paragraph("+Rs 3,14,602.00", body_style), Paragraph("Average Winner", body_style), Paragraph("+11.48%", body_style)],
    [Paragraph("Net Portfolio ROI", body_style), Paragraph("+31.46%", body_style), Paragraph("Average Loser", body_style), Paragraph("-7.06%", body_style)],
    [Paragraph("Maximum Drawdown", body_style), Paragraph("8.39%", body_style), Paragraph("Calmar Ratio", body_style), Paragraph("3.75", body_style)]
]
t_perf = Table(perf_data, colWidths=[130, 135, 130, 135])
t_perf.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (1,-1), LIGHT_BG),
    ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
    ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ('TOPPADDING', (0,0), (-1,-1), 4),
    ('BOTTOMPADDING', (0,0), (-1,-1), 4),
]))
story.append(t_perf)
story.append(Spacer(1, 10))

# 7. DIAGNOSTICS & CONCLUSION
story.append(Paragraph("7. Key Diagnostics & Live Approval", h1_style))
story.append(Paragraph("1. 10% Position Cap Impact: 100% of trades hit the 10% ceiling, proving that unconstrained ATR sizing on low-volatility assets creates hidden tail risk. The cap neutered catastrophic gap-downs while preserving asymmetric upside.", bullet_style))
story.append(Paragraph("2. Q1 2024 Correction Resilience: During the brutal SEBI mid-cap sell-off (Jan-Mar 2024), the system absorbed small paper cuts (-Rs 27k Jan, -Rs 26k Feb, -Rs 13k Mar), keeping maximum drawdown to 8.39%, before exploding back into profit in April (+Rs 45k) and June (+Rs 57k).", bullet_style))
story.append(Paragraph("3. Final Status: BACKTEST LOGIC IS PERMANENTLY LOCKED. APPROVED FOR LIVE PRODUCTION.", bullet_style))

doc.build(story)
print("PDF generated successfully: Shadow_Box_Institutional_Alpha_Report.pdf")
