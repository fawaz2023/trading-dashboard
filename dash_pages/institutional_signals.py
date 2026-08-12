import dash
from dash import html, dcc
import pandas as pd
import os

dash.register_page(__name__, path='/institutional-signals', name='Institutional Signals', title='Pro Spike - Institutional Signals')

def layout():
    header_section = html.Div(
        className="px-4 md:px-6 pt-6 pb-2 w-full flex flex-col gap-6",
        children=[
            html.Section(
                className="flex flex-col gap-1",
                children=[
                    html.H2("Institutional Signals", className="font-display-lg text-headline-lg md:text-[36px] text-on-surface tracking-tight"),
                    html.P("High-conviction data signals for professional trading.", className="font-body-md text-on-surface-variant")
                ]
            ),
            # AI Generative Summary Layer (2026 UX Principle)
            html.Div(
                className="glass-panel p-4 rounded-xl flex items-start gap-4 border border-primary/20 bg-primary/5",
                children=[
                    html.Div(
                        className="p-2 rounded-full bg-primary/10 text-primary glow-accent",
                        children=[html.Span("auto_awesome", className="material-symbols-outlined text-lg")]
                    ),
                    html.Div(
                        className="flex flex-col gap-1.5",
                        children=[
                            html.P(
                                [
                                    html.Strong("NVDA ", className="text-on-surface"),
                                    "is experiencing a ",
                                    html.Span("345% volume spike", className="text-primary font-semibold"),
                                    ", correlating with a recent institutional options sweep and positive sector sentiment."
                                ],
                                className="font-body-md text-on-surface-variant leading-relaxed"
                            ),
                            html.Button(
                                "Generate full trade thesis →",
                                className="text-primary text-sm font-semibold hover:underline self-start mt-1 cursor-pointer hover:text-primary-fixed transition-colors"
                            )
                        ]
                    )
                ]
            )
        ]
    )

    filter_bar = html.Div(
        className="sticky top-0 z-20 bg-[#10141a]/95 backdrop-blur-xl border-y border-white/10 px-4 md:px-6 py-3 flex flex-wrap justify-between items-center gap-4 w-full",
        children=[
            html.Div(
                className="flex flex-wrap gap-2",
                children=[
                    html.Div(className="relative glass-panel rounded px-3 py-1.5 flex items-center gap-2 cursor-pointer hover:bg-white/5 transition-colors", children=[
                        html.Span("Sector:", className="text-[10px] text-on-surface-variant uppercase tracking-wider font-semibold"),
                        html.Span("Technology", className="text-label-sm text-on-surface"),
                        html.Span("arrow_drop_down", className="material-symbols-outlined text-[14px] text-on-surface-variant")
                    ]),
                    html.Div(className="relative glass-panel rounded px-3 py-1.5 flex items-center gap-2 cursor-pointer hover:bg-white/5 transition-colors", children=[
                        html.Span("Market Cap:", className="text-[10px] text-on-surface-variant uppercase tracking-wider font-semibold"),
                        html.Span("Large (>10B)", className="text-label-sm text-on-surface"),
                        html.Span("arrow_drop_down", className="material-symbols-outlined text-[14px] text-on-surface-variant")
                    ]),
                    html.Div(className="relative glass-panel rounded px-3 py-1.5 flex items-center gap-2 cursor-pointer hover:bg-white/5 transition-colors", children=[
                        html.Span("Exchange:", className="text-[10px] text-on-surface-variant uppercase tracking-wider font-semibold"),
                        html.Span("NASDAQ", className="text-label-sm text-on-surface"),
                        html.Span("arrow_drop_down", className="material-symbols-outlined text-[14px] text-on-surface-variant")
                    ])
                ]
            ),
            html.Div(
                className="flex items-center gap-2",
                children=[
                    html.Button(className="flex items-center gap-2 px-3 py-1.5 glass-panel rounded text-on-surface text-label-sm hover:bg-white/10 transition-colors", children=[
                        html.Span("tune", className="material-symbols-outlined text-[16px]"),
                        "Filter"
                    ]),
                    html.Button(className="flex items-center gap-2 px-3 py-1.5 bg-primary/10 border border-primary text-primary rounded text-label-sm hover:bg-primary/20 transition-colors shadow-[0_0_15px_rgba(90,240,179,0.2)]", children=[
                        html.Span("send", className="material-symbols-outlined text-[16px]"),
                        "Send to AI Terminal"
                    ])
                ]
            )
        ]
    )

    # Mock Data Rows based on the Stitch HTML
    mock_data = [
        {"sym": "NVDA", "name": "Nvidia Corp", "icon": "memory", "price": "$1,245.60", "surge": "+345%", "surge_color": "text-primary", "deliv": "12.4M", "turnover": "$15.4B", "blocks": "42", "score": 98, "score_color": "text-primary", "bar_bg": "bg-primary", "bar_shadow": "shadow-[0_0_8px_rgba(90,240,179,0.8)]"},
        {"sym": "CRWD", "name": "CrowdStrike", "icon": "cloud", "price": "$312.45", "surge": "+210%", "surge_color": "text-primary", "deliv": "4.2M", "turnover": "$1.3B", "blocks": "18", "score": 92, "score_color": "text-primary", "bar_bg": "bg-primary", "bar_shadow": "shadow-[0_0_8px_rgba(90,240,179,0.8)]"},
        {"sym": "AAPL", "name": "Apple Inc", "icon": "devices", "price": "$189.20", "surge": "+45%", "surge_color": "text-secondary", "deliv": "45.1M", "turnover": "$8.5B", "blocks": "12", "score": 75, "score_color": "text-secondary", "bar_bg": "bg-secondary", "bar_shadow": ""},
        {"sym": "TSLA", "name": "Tesla Inc", "icon": "electric_car", "price": "$175.34", "surge": "-12%", "surge_color": "text-error", "deliv": "88.3M", "turnover": "$15.5B", "blocks": "6", "score": 42, "score_color": "text-on-surface-variant", "bar_bg": "bg-on-surface-variant", "bar_shadow": ""},
        {"sym": "PLTR", "name": "Palantir Tech", "icon": "dataset", "price": "$24.80", "surge": "+185%", "surge_color": "text-primary", "deliv": "15.2M", "turnover": "$376M", "blocks": "24", "score": 88, "score_color": "text-primary", "bar_bg": "bg-primary", "bar_shadow": "shadow-[0_0_8px_rgba(90,240,179,0.8)]"},
    ]

    table_rows = []
    for row in mock_data:
        # Determine icon background based on whether it's a primary or neutral icon
        icon_bg = "bg-secondary-container/20 text-secondary border border-secondary/30" if row["sym"] != "AAPL" else "bg-surface-bright text-on-surface-variant border border-white/10"
        
        table_rows.append(
            html.Tr(
                className="border-b border-white/5 hover:bg-white/5 transition-all duration-300 cursor-pointer group hover:z-20 hover:scale-[1.01] hover:shadow-[0_4px_20px_rgba(90,240,179,0.08)] hover:border-primary/30 relative z-10 bg-[#10141a]/50",
                children=[
                    html.Td(
                        className="py-1.5 px-4",
                        children=[
                            html.Div(
                                className="flex items-center gap-3",
                                children=[
                                    html.Div(
                                        className=f"w-6 h-6 rounded-full flex items-center justify-center {icon_bg}",
                                        children=[html.Span(row["icon"], className="material-symbols-outlined text-[14px]")]
                                    ),
                                    html.Div(
                                        className="flex flex-col leading-tight",
                                        children=[
                                            html.Span(row["sym"], className="text-on-surface font-semibold group-hover:text-primary transition-colors text-sm"),
                                            html.Span(row["name"], className="text-[11px] text-on-surface-variant font-body-md")
                                        ]
                                    )
                                ]
                            )
                        ]
                    ),
                    html.Td(row["price"], className="py-1.5 px-4 text-right text-on-surface text-sm"),
                    html.Td(row["surge"], className=f"py-1.5 px-4 text-right {row['surge_color']} text-sm"),
                    html.Td(row["deliv"], className="py-1.5 px-4 text-right text-on-surface text-sm"),
                    html.Td(row["turnover"], className="py-1.5 px-4 text-right text-on-surface text-sm font-data-mono"),
                    html.Td(row["blocks"], className="py-1.5 px-4 text-right text-on-surface text-sm"),
                    html.Td(
                        className="py-1.5 px-4 text-center",
                        children=[
                            html.Div(
                                className="flex items-center justify-center gap-2",
                                children=[
                                    html.Div(str(row["score"]), className=f"{row['score_color']} font-bold text-sm"),
                                    html.Div(
                                        className="w-12 h-1 bg-surface rounded-full overflow-hidden border border-white/10",
                                        children=[
                                            html.Div(className=f"h-full {row['bar_bg']} {row['bar_shadow']}", style={"width": f"{row['score']}%"})
                                        ]
                                    )
                                ]
                            )
                        ]
                    )
                ]
            )
        )

    data_table = html.Section(
        className="glass-panel rounded-lg overflow-hidden flex flex-col w-full",
        children=[
            html.Div(
                className="overflow-x-auto",
                children=[
                    html.Table(
                        className="w-full text-left border-collapse min-w-[800px]",
                        children=[
                            html.Thead(
                                html.Tr(
                                    className="border-b border-white/5 bg-white/5",
                                    children=[
                                        html.Th("Symbol", className="py-2 px-4 font-label-sm text-label-sm text-on-surface-variant uppercase tracking-wider"),
                                        html.Th("Last Price", className="py-2 px-4 font-label-sm text-label-sm text-on-surface-variant uppercase tracking-wider text-right"),
                                        html.Th("Volume Surge (%)", className="py-2 px-4 font-label-sm text-label-sm text-on-surface-variant uppercase tracking-wider text-right"),
                                        html.Th("Delivery Volume", className="py-2 px-4 font-label-sm text-label-sm text-on-surface-variant uppercase tracking-wider text-right"),
                                        html.Th("Delivery Turnover", className="py-2 px-4 font-label-sm text-label-sm text-on-surface-variant uppercase tracking-wider text-right"),
                                        html.Th("Block Deals", className="py-2 px-4 font-label-sm text-label-sm text-on-surface-variant uppercase tracking-wider text-right"),
                                        html.Th("Edge Score", className="py-2 px-4 font-label-sm text-label-sm text-on-surface-variant uppercase tracking-wider text-center")
                                    ]
                                )
                            ),
                            html.Tbody(className="font-data-md text-data-md", children=table_rows)
                        ]
                    )
                ]
            ),
            # Pagination Footer
            html.Div(
                className="py-2 px-4 border-t border-white/10 flex justify-between items-center bg-surface-container-low/30",
                children=[
                    html.Span("Showing 1-5 of 142 signals", className="text-[11px] text-on-surface-variant"),
                    html.Div(
                        className="flex gap-1",
                        children=[
                            html.Button(
                                className="w-6 h-6 rounded border border-white/10 flex items-center justify-center text-on-surface-variant hover:bg-white/10 hover:text-on-surface transition-colors disabled:opacity-50",
                                children=[html.Span("chevron_left", className="material-symbols-outlined text-[14px]")]
                            ),
                            html.Button(
                                className="w-6 h-6 rounded border border-white/10 flex items-center justify-center text-on-surface-variant hover:bg-white/10 hover:text-on-surface transition-colors",
                                children=[html.Span("chevron_right", className="material-symbols-outlined text-[14px]")]
                            )
                        ]
                    )
                ]
            )
        ]
    )

    return html.Div(
        className="flex flex-col w-full h-full relative",
        children=[
            header_section,
            filter_bar,
            html.Div(
                className="flex flex-col w-full h-full p-2 md:p-4",
                children=[data_table]
            )
        ]
    )
