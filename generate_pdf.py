import os
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, ListFlowable, ListItem

def create_pdf():
    pdf_filename = "Shadow_Box_Institutional_Alpha_Report.pdf"
    doc = SimpleDocTemplate(pdf_filename, pagesize=letter,
                            rightMargin=50, leftMargin=50,
                            topMargin=50, bottomMargin=50)

    styles = getSampleStyleSheet()
    
    # Custom Styles
    title_style = ParagraphStyle(
        'TitleStyle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.darkblue,
        spaceAfter=15,
        alignment=1 # Center
    )
    
    header_style = ParagraphStyle(
        'HeaderStyle',
        parent=styles['Heading2'],
        fontSize=13,
        textColor=colors.black,
        spaceBefore=15,
        spaceAfter=10
    )
    
    sub_header_style = ParagraphStyle(
        'SubHeaderStyle',
        parent=styles['Heading3'],
        fontSize=11,
        textColor=colors.dimgrey,
        spaceBefore=10,
        spaceAfter=5
    )
    
    body_style = ParagraphStyle(
        'BodyStyle',
        parent=styles['Normal'],
        fontSize=10,
        spaceAfter=8,
        leading=14
    )
    
    bullet_style = ParagraphStyle(
        'BulletStyle',
        parent=styles['Normal'],
        fontSize=10,
        leading=14,
        leftIndent=15
    )
    
    bold_body_style = ParagraphStyle(
        'BoldBodyStyle',
        parent=styles['Normal'],
        fontSize=10,
        spaceAfter=8,
        leading=14,
        fontName='Helvetica-Bold'
    )

    story = []

    # 1. HEADER & EXECUTIVE SUMMARY
    story.append(Paragraph("1. HEADER & EXECUTIVE SUMMARY", header_style))
    story.append(Paragraph("<b>System Name:</b> SHADOW-BOX INSTITUTIONAL ALPHA (SBIA) ENGINE v1.0", body_style))
    story.append(Paragraph("<b>Architecture:</b> 12-Condition Progressive Cascade + Random Forest ML (Shadow Box) + 1.5% Fixed Risk Compounding", body_style))
    story.append(Paragraph("<b>Objective:</b> Systematic capture of institutional momentum anomalies using delivery footprint cascades, cross-sectional Machine Learning filters, and dynamic volatility risk controls.", body_style))
    story.append(Spacer(1, 10))

    # 2. THE 12-CONDITION CASCADE
    story.append(Paragraph("2. THE 12-CONDITION CASCADE (EXACT RULES)", header_style))
    story.append(Paragraph("All 12 rules must pass simultaneously under the following four distinct categories:", body_style))
    
    story.append(Paragraph("<b>BASELINE FILTERS (Minimum Quality Floor):</b>", sub_header_style))
    story.append(Paragraph("1. Delivery % (1D) >= 50%", bullet_style))
    story.append(Paragraph("2. Delivery Turnover (1D) >= ₹50,00,000", bullet_style))
    story.append(Paragraph("3. ATW (Average Trade Worth) (1D) >= ₹20,000", bullet_style))
    
    story.append(Paragraph("<b>DELIVERY % CASCADE (Accumulation Accelerating):</b>", sub_header_style))
    story.append(Paragraph("4. Delivery % (1D) > Delivery % (1W avg)", bullet_style))
    story.append(Paragraph("5. Delivery % (1W) > Delivery % (1M avg)", bullet_style))
    story.append(Paragraph("6. Delivery % (1M) > Delivery % (3M avg)", bullet_style))
    
    story.append(Paragraph("<b>DELIVERY TURNOVER CASCADE (Footprint Growing):</b>", sub_header_style))
    story.append(Paragraph("7. Turnover (1D) > Turnover (1W avg)", bullet_style))
    story.append(Paragraph("8. Turnover (1W) > Turnover (1M avg)", bullet_style))
    story.append(Paragraph("9. Turnover (1M) > Turnover (3M avg)", bullet_style))
    
    story.append(Paragraph("<b>ATW CASCADE (Institutional Ticket Size Growing):</b>", sub_header_style))
    story.append(Paragraph("10. ATW (1D) > ATW (1W avg)", bullet_style))
    story.append(Paragraph("11. ATW (1W) > ATW (1M avg)", bullet_style))
    story.append(Paragraph("12. ATW (1M) > ATW (3M avg)", bullet_style))
    story.append(Spacer(1, 15))

    # 3. THE MACHINE LEARNING BRAIN
    story.append(Paragraph("3. THE MACHINE LEARNING BRAIN (EXACT MATH)", header_style))
    
    story.append(Paragraph("<b>CROSS-SECTIONAL RANKING:</b>", bold_body_style))
    story.append(Paragraph("The system calculates 1W/1M ratios for Delivery %, Turnover, and ATW, then ranks them daily across all NSE stocks (0 to 100 percentiles) to get MOMENTUM_SCORE, FOOTPRINT_SCORE, and STABILITY_SCORE.", body_style))
    
    story.append(Paragraph("<b>SIS (Smart Institution Score):</b>", bold_body_style))
    story.append(Paragraph("A weighted geometric mean tracking setup durability.<br/><b>Formula:</b> SIS = [ (STABILITY_SCORE + 1)^0.50 * (FOOTPRINT_SCORE + 1)^0.30 * (MOMENTUM_SCORE + 1)^0.20 ] - 1", body_style))
    
    story.append(Paragraph("<b>PRICE-NEUTRAL WHALE DENSITY:</b>", bold_body_style))
    story.append(Paragraph("Measures the concentration of institutional block buying, normalized against share price to prevent large-cap bias.<br/><b>Formula:</b> Whale_Density = (ATW / VWAP) / Typical_Lot_Size", body_style))
    
    story.append(Paragraph("<b>IMPLIED TRADES:</b>", bold_body_style))
    story.append(Paragraph("Estimates the number of institutional-grade individual trades executed during the day.<br/><b>Formula:</b> Implied_Trades = Delivery_Turnover / ATW", body_style))
    
    story.append(Paragraph("<b>THE AI GATE:</b>", bold_body_style))
    story.append(Paragraph("The 3 features above are fed into a Random Forest Classifier. Any stock scoring an AI Win Probability of < 60.0% is instantly rejected.", body_style))
    story.append(Spacer(1, 15))

    # 4. EXECUTION & RISK CONTROLS
    story.append(Paragraph("4. EXECUTION & RISK CONTROLS", header_style))
    story.append(Paragraph("<b>Dynamic Volatility Exits:</b> 2.0x ATR14 Stop Loss / 4.0x ATR14 Take Profit (1:2 R:R).", bullet_style))
    story.append(Paragraph("<b>Position Sizing:</b> 1.5% fixed fractional risk of current equity per trade.", bullet_style))
    story.append(Paragraph("<b>Capital Safety Ceiling:</b> 10% Portfolio Max Cap per trade to amputate gap-down tail risk on low-volatility stocks.", bullet_style))
    story.append(Paragraph("<b>Realism:</b> T+1 Morning Open execution, pessimistic double-breach resolution, 0.50% round-trip slippage/tax penalty.", bullet_style))
    story.append(Spacer(1, 15))

    # 5. BACKTEST PERFORMANCE SUMMARY
    story.append(Paragraph("5. BACKTEST PERFORMANCE SUMMARY (22 MONTHS / Dec 2022 - Oct 2024)", header_style))
    story.append(Paragraph("<b>Starting Capital:</b> ₹10,00,000", bullet_style))
    story.append(Paragraph("<b>Ending Equity:</b> ₹13,14,602", bullet_style))
    story.append(Paragraph("<b>Net Profit / ROI:</b> +₹3,14,602 (+31.46%)", bullet_style))
    story.append(Paragraph("<b>Maximum Drawdown:</b> 8.39% (Peak-to-Trough)", bullet_style))
    story.append(Paragraph("<b>Total Trades Executed:</b> 418", bullet_style))
    story.append(Paragraph("<b>Win Rate:</b> 50.5% (211 Wins / 207 Losses)", bullet_style))
    story.append(Paragraph("<b>Profit Factor:</b> 1.57", bullet_style))
    story.append(Paragraph("<b>Average Winning Trade:</b> +11.48%", bullet_style))
    story.append(Paragraph("<b>Average Losing Trade:</b> -7.06%", bullet_style))
    story.append(Spacer(1, 15))
    
    # 6. BASELINE SANITY FILTERS & GOLDILOCKS ZONES (Ablation Findings)
    story.append(Paragraph("6. BASELINE SANITY FILTERS & THE GOLDILOCKS ZONES", header_style))
    story.append(Paragraph("Based on strict ML pattern recognition and quantile binning, all watchlist signals must pass these structural sanity filters to avoid traps:", body_style))
    
    story.append(Paragraph("<b>BASELINE SANITY FILTERS:</b>", sub_header_style))
    story.append(Paragraph("1. Delivery Turnover (in Crore) Must be > ₹100.0 Crores (rejects illiquid small-caps).", bullet_style))
    story.append(Paragraph("2. Implied Trades Must be > 21,000 (ensures underlying trading activity).", bullet_style))
    story.append(Paragraph("3. Whale_Density (Price-Normalized) Must be between 3.5 and 50.0 (rejects isolated, mathematical anomalies).", bullet_style))
    
    story.append(Paragraph("<b>THE GOLDILOCKS ZONES (Highest Win-Rate Quintiles):</b>", sub_header_style))
    
    story.append(Paragraph("<b>1. Smart Institutional Score (SIS)</b>", bold_body_style))
    story.append(Paragraph("<b>Goldilocks Zone:</b> 50.0 to 69.7", bullet_style))
    story.append(Paragraph("<b>Insight:</b> You don't want the absolute highest SIS scores (the 70-90 range actually drops to a 42% win rate). A glowing green score across every timeframe is often a 'crowded trade' where retail is already pouring in. The true institutional edge is found in the 50-70 zone — the quiet accumulation phase.", bullet_style))

    story.append(Paragraph("<b>2. Whale Density (Price-Normalized)</b>", bold_body_style))
    story.append(Paragraph("<b>Goldilocks Zone:</b> 0.16 to 0.37 (Average trade size is 16% to 37% of a standard lot at VWAP)", bullet_style))
    story.append(Paragraph("<b>Insight:</b> Extreme whale density (1.0+) has a lower win rate (49-51%). Massive block trades are often pre-arranged off-exchange transfers or promoter pledging that don't lead to momentum. The sweet spot represents steady, relentless algorithmic accumulation.", bullet_style))

    story.append(Paragraph("<b>3. Implied Trades</b>", bold_body_style))
    story.append(Paragraph("<b>Goldilocks Zone:</b> 0 to 2,700 Implied Trades (Highly concentrated hands)", bullet_style))
    story.append(Paragraph("<b>Insight:</b> For a given Delivery Turnover, you want it concentrated in as few hands as possible. When Implied Trades are in the middle (7,000 to 14,000), the win rate crashes because the turnover is being spread across thousands of retail participants instead of concentrated institutional buyers.", bullet_style))
    
    story.append(Paragraph("<b>THE FINAL ML GATE:</b>", sub_header_style))
    story.append(Paragraph("Pass the surviving stocks that clear the baseline and sanity filters into <b>shadow_box_model.pkl</b>. Maintain the strict filter of <b>AI_WIN_PROBABILITY >= 60.0%</b>.", body_style))

    # Build PDF
    doc.build(story)
    print(f"PDF generated successfully: {pdf_filename}")

if __name__ == "__main__":
    create_pdf()
