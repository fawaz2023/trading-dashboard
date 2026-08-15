import os
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether, HRFlowable
)

pdf_path = "Shadow_Box_Production_Deployment_Plan.pdf"
doc = SimpleDocTemplate(
    pdf_path,
    pagesize=letter,
    rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40
)

styles = getSampleStyleSheet()

# Custom Styles
PRIMARY_COLOR = colors.HexColor("#0f172a")    # Slate 900
SECONDARY_COLOR = colors.HexColor("#2563eb")  # Blue 600
ACCENT_COLOR = colors.HexColor("#f59e0b")     # Amber 500
TEXT_COLOR = colors.HexColor("#334155")       # Slate 700
LIGHT_BG = colors.HexColor("#f8fafc")         # Slate 50

title_style = ParagraphStyle(
    "CustomTitle", parent=styles["Heading1"], fontSize=18, leading=22,
    textColor=PRIMARY_COLOR, spaceAfter=16, alignment=1, fontName="Helvetica-Bold"
)

h1_style = ParagraphStyle(
    "Heading1Custom", parent=styles["Heading2"], fontSize=14, leading=18,
    textColor=SECONDARY_COLOR, spaceBefore=18, spaceAfter=8, fontName="Helvetica-Bold"
)

h2_style = ParagraphStyle(
    "Heading2Custom", parent=styles["Heading3"], fontSize=12, leading=16,
    textColor=PRIMARY_COLOR, spaceBefore=12, spaceAfter=6, fontName="Helvetica-Bold"
)

body_style = ParagraphStyle(
    "CustomBody", parent=styles["Normal"], fontSize=10, leading=16,
    textColor=TEXT_COLOR, spaceAfter=8, fontName="Helvetica"
)

bullet_style = ParagraphStyle(
    "CustomBullet", parent=body_style, leftIndent=20, bulletIndent=10, spaceAfter=6
)

alert_style = ParagraphStyle(
    "Alert", parent=body_style, textColor=colors.red, backColor=colors.HexColor("#fee2e2"),
    borderPadding=10, spaceBefore=12, spaceAfter=12, fontName="Helvetica-Bold"
)

story = []

# Title
story.append(Paragraph("Shadow-Box Institutional Alpha", title_style))
story.append(Paragraph("PRODUCTION DEPLOYMENT PLAN", ParagraphStyle("SubTitle", parent=title_style, fontSize=14, textColor=SECONDARY_COLOR)))
story.append(Spacer(1, 10))
story.append(HRFlowable(width="100%", thickness=2, color=PRIMARY_COLOR, spaceAfter=20))

# Warning
story.append(Paragraph("⚠️ USER REVIEW REQUIRED", alert_style))
story.append(Paragraph("We must mathematically validate the final three tweaks in isolation before pushing them to the live dashboard. Proceeding without validation introduces unnecessary risk.", body_style))

# Phase 1
story.append(Paragraph("Phase 1: Validate the Final Tweaks (101_FINAL_TWEAKED_backtest.py)", h1_style))
story.append(Paragraph("Before going live, we must prove that the final insights improve the system. We will create a new backtest based on 95_ULTIMATE_FLEXGATE and inject exactly three new constraints:", body_style))

story.append(Paragraph("• <b>1. Seasonal Position Sizing:</b>", bullet_style))
story.append(Paragraph("Green Months (Nov, Dec, Apr, May): 10% Portfolio Cap", ParagraphStyle("SubBul1", parent=bullet_style, leftIndent=40)))
story.append(Paragraph("Red Months (Feb, Jul, Sep, Oct): 5% Portfolio Cap", ParagraphStyle("SubBul2", parent=bullet_style, leftIndent=40)))

story.append(Paragraph("• <b>2. Algorithmic Footprint Filter:</b>", bullet_style))
story.append(Paragraph("Require Delivery Turnover Spike Ratio > 1.5x AND ATW Spike Ratio < 1.1x to ensure pure algorithmic drip-buying.", ParagraphStyle("SubBul3", parent=bullet_style, leftIndent=40)))

story.append(Paragraph("• <b>3. Quiet Momentum Ceiling:</b>", bullet_style))
story.append(Paragraph("Reject setups where the relative Momentum Score is > 30 (ensuring we avoid obvious retail breakouts).", ParagraphStyle("SubBul4", parent=bullet_style, leftIndent=40)))

story.append(Paragraph("We will run this against the existing dataset. If the Profit Factor and Win Rate improve, the logic is permanently locked.", body_style))

# Phase 2
story.append(Paragraph("Phase 2: Lock the Final Parameters", h1_style))
story.append(Paragraph("Once validated, we will update the master Backtest Lineage Report with the final numbers. This document becomes the immutable 'Constitution' of the system. No further parameter adjustments will be made without a full re-test.", body_style))

# Phase 3
story.append(Paragraph("Phase 3: Build the Live Streamlit Dashboard", h1_style))
story.append(Paragraph("We will build the live production dashboard on top of the existing calculate_active_signals.py architecture. The dashboard will feature:", body_style))
story.append(Paragraph("• <b>SBIA Live Watchlist:</b> The primary screener running the locked 101_FINAL_TWEAKED logic.", bullet_style))
story.append(Paragraph("• <b>Legacy Watchlist:</b> The original 12-condition screener running in parallel for historical comparison.", bullet_style))
story.append(Paragraph("• <b>Live Seasonal Sizing Widget:</b> A dynamic indicator recommending the maximum safe position size based on the current calendar month.", bullet_style))

# Phase 4
story.append(Paragraph("Phase 4: Download 2025–2026 Live Data", h1_style))
story.append(Paragraph("In parallel with dashboard construction, we will execute the data downloader scripts to fetch the newest NSE Bhavcopy and Delivery files up to the current date in 2026. This will populate the live dashboard with current context and allow for immediate forward-testing.", body_style))

doc.build(story)
print(f"PDF successfully generated at {os.path.abspath(pdf_path)}")
