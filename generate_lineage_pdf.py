import os
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, HRFlowable
)

pdf_path = "c:/Users/fawaz/Desktop/trading_dashboard/ShadowBox_Backtest_Lineage_Report.pdf"
doc = SimpleDocTemplate(
    pdf_path,
    pagesize=letter,
    rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40
)

styles = getSampleStyleSheet()

# Custom Styles
PRIMARY_COLOR = colors.HexColor("#0f172a")    # Slate 900
SECONDARY_COLOR = colors.HexColor("#2563eb")  # Blue 600
TEXT_COLOR = colors.HexColor("#334155")       # Slate 700

title_style = ParagraphStyle(
    "CustomTitle", parent=styles["Heading1"], fontSize=18, leading=22,
    textColor=PRIMARY_COLOR, spaceAfter=16, alignment=1, fontName="Helvetica-Bold"
)
h1_style = ParagraphStyle(
    "Heading1Custom", parent=styles["Heading2"], fontSize=14, leading=18,
    textColor=SECONDARY_COLOR, spaceBefore=18, spaceAfter=8, fontName="Helvetica-Bold"
)
body_style = ParagraphStyle(
    "CustomBody", parent=styles["Normal"], fontSize=10, leading=16,
    textColor=TEXT_COLOR, spaceAfter=8, fontName="Helvetica"
)
bullet_style = ParagraphStyle(
    "CustomBullet", parent=body_style, leftIndent=20, bulletIndent=10, spaceAfter=6
)

story = []

story.append(Paragraph("Shadow-Box Institutional Alpha", title_style))
story.append(Paragraph("THE COMPLETE BACKTEST LINEAGE", ParagraphStyle("SubTitle", parent=title_style, fontSize=14, textColor=SECONDARY_COLOR)))
story.append(Spacer(1, 10))
story.append(HRFlowable(width="100%", thickness=2, color=PRIMARY_COLOR, spaceAfter=20))

story.append(Paragraph("This document serves as the permanent historical record of the system's entire mathematical evolution. It spans dozens of iterations and tens of thousands of lines of code, tracking the engine from its primitive beginnings all the way to the final optimized AI institutional scanner.", body_style))

# ERA 1
story.append(Paragraph("ERA 1: The Early Momentum Scanners (Scripts 08 - 24)", h1_style))
story.append(Paragraph("The foundation of the system, focusing strictly on basic price and delivery momentum.", body_style))
story.append(Paragraph("• <b>08_simple_strategy_backtest.py:</b> The very first attempt at a quantitative backtest.", bullet_style))
story.append(Paragraph("• <b>10_mentor_exact_8conditions.py:</b> Integration of strict '8-Condition Mentor Rules'.", bullet_style))
story.append(Paragraph("• <b>21_complete_strategy_with_atw.py:</b> Breakthrough. Introduced Average Trade Worth (ATW).", bullet_style))

# ERA 2
story.append(Paragraph("ERA 2: The Cascade Architecture (Scripts 25 - 69)", h1_style))
story.append(Paragraph("The introduction of progressive, multi-timeframe dominance.", body_style))
story.append(Paragraph("• <b>25_complete_11_conditions_backtest.py:</b> The birth of the cascade (1D > 1W > 1M > 3M).", bullet_style))
story.append(Paragraph("• <b>61_final_realistic_backtest.py:</b> Added real-world friction and strict fixed-fractional sizing.", bullet_style))
story.append(Paragraph("• <b>69_full_performance_backtest.py:</b> The peak of the pure 11-condition logic without Machine Learning.", bullet_style))

# ERA 3
story.append(Paragraph("ERA 3: The Hybrid & Strict Realism Phase (Scripts 70 - 91)", h1_style))
story.append(Paragraph("Optimizing execution and eradicating 'fake' signals.", body_style))
story.append(Paragraph("• <b>80_hybrid_daily_weekly_backtest.py:</b> Blended daily momentum triggers with macro weekly structure.", bullet_style))
story.append(Paragraph("• <b>91_STRICT_realistic_backtest.py:</b> The Reality Check. Implemented brutal T+1 open execution rules.", bullet_style))

# ERA 4
story.append(Paragraph("ERA 4: The Machine Learning & AI Integration (Scripts 92 - 94)", h1_style))
story.append(Paragraph("Replacing rigid condition floors with statistical probabilities and Cross-Sectional Ranking.", body_style))
story.append(Paragraph("• <b>92_ML_COMPOUND_backtest.py:</b> The first system to use shadow_box_model.pkl.", bullet_style))
story.append(Paragraph("• <b>93_ULTIMATE_patched_backtest.py:</b> The 12-Condition ML Bouncer. Gated entries behind an AI_WIN_PROBABILITY >= 60.0%. Result: 418 Trades | 50.5% Win Rate | 1.57 Profit Factor | +31.46% ROI.", bullet_style))

# ERA 5
story.append(Paragraph("ERA 5: The Flex-Gate & Institutional Fingerprints (Scripts 95 - 100)", h1_style))
story.append(Paragraph("The finalized, mathematically optimized production engine.", body_style))
story.append(Paragraph("• <b>96_PATTERNS_institutional_fingerprint.py:</b> Discovered true drivers (Goldilocks Zone, ICT Temporal Clustering, Whale Density Spikes).", bullet_style))
story.append(Paragraph("• <b>95_FLEXGATE_accumulation_backtest.py:</b> Introduced 'Flex-Gates'. Result: 87 Trades | 55.2% Win Rate | 4.84 Profit Factor.", bullet_style))
story.append(Paragraph("• <b>95_ULTIMATE_FLEXGATE_backtest.py:</b> The Final Form. Dropped 200-SMA. Result: 355 Trades | 53.0% Win Rate | 3.49 Profit Factor | +62.71% ROI.", bullet_style))
story.append(Paragraph("• <b>100_VERIFY_and_DISCOVER.py:</b> Statistically proven seasonality edge and volume spike divergences.", bullet_style))

doc.build(story)
print(f"PDF successfully generated at {os.path.abspath(pdf_path)}")
