import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import pandas as pd
import os

dash.register_page(__name__, path='/', name='Institutional Signals')

# --- 1. Data Loading ---
CSV_PATH = "data/active_signals_ranked.csv"
def load_data():
    if os.path.exists(CSV_PATH):
        df = pd.read_csv(CSV_PATH)
        df['STABILITY_RAW'] = pd.to_numeric(df['STABILITY_RAW'], errors='coerce')
        df['TRIGGER_COUNT_30D'] = pd.to_numeric(df['TRIGGER_COUNT_30D'], errors='coerce')
        return df
    return pd.DataFrame()

df = load_data()

# --- 2. AG-Grid Column Definitions ---
column_defs = [
    {"field": "SYMBOL", "headerName": "Symbol", "width": 120, "pinned": "left", "cellStyle": {"fontWeight": "bold"}},
    {"field": "EXCHANGE", "headerName": "Exch", "width": 80},
    {"field": "CLOSE", "headerName": "Close", "width": 100, "type": "numericColumn", "valueFormatter": {"function": "value.toFixed(2)"}},
    {"field": "AI_SCORE", "headerName": "AI Score", "width": 110, "type": "numericColumn", "valueFormatter": {"function": "(value * 1).toFixed(2)"}},
    {"field": "COMBINED_SCORE", "headerName": "Comb Score", "width": 110, "type": "numericColumn", "valueFormatter": {"function": "(value * 1).toFixed(2)"}},
    {"field": "STABILITY_RAW", "headerName": "Stability", "width": 110, "type": "numericColumn", "valueFormatter": {"function": "value.toFixed(2)"}},
    {"field": "TRIGGER_COUNT_30D", "headerName": "Triggers", "width": 100, "type": "numericColumn"},
    {"field": "MOMENTUM_RAW", "headerName": "Momentum", "width": 110, "type": "numericColumn"},
    {"field": "FOOTPRINT_RAW", "headerName": "Footprint", "width": 110, "type": "numericColumn"},
    {"field": "DELIV_PER", "headerName": "Deliv %", "width": 100, "type": "numericColumn"},
    {"field": "ATW", "headerName": "ATW", "width": 90, "type": "numericColumn"},
    {"field": "DATE", "headerName": "Trigger Date", "width": 130}
]

grid_options = {
    "rowClassRules": {
        "bg-success text-white font-weight-bold": "params.data.TRIGGER_COUNT_30D === 1 && params.data.STABILITY_RAW > 3.16",
        "bg-danger text-white": "params.data.TRIGGER_COUNT_30D > 2"
    }
}

# --- 3. Layout ---
layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H2("Institutional Screener", style={"fontWeight": "800", "letterSpacing": "-0.5px"}), width=8),
        dbc.Col(dbc.Button("Refresh Data", className="btn-premium", id="refresh-btn-inst"), width=4, align="end")
    ], className="mb-4"),
    
    # KPI Cards Row
    dbc.Row([
        dbc.Col([
            html.Div([
                html.Div("Total Signals", className="kpi-label"),
                html.Div(str(len(df)), id="total-signals-inst", className="kpi-value success")
            ], className="glass-card")
        ], width=4),
        dbc.Col([
            html.Div([
                html.Div("Trading Days", className="kpi-label"),
                html.Div(str(df['DATE'].nunique() if 'DATE' in df.columns else 0), className="kpi-value warning")
            ], className="glass-card")
        ], width=4),
    ], className="mb-4"),
    
    # ML Rules Legend
    dbc.Row([
        dbc.Col([
            html.Div([
                html.Span("🟢 Accumulation (Stab > 3.16 & Trig == 1) ", className="ml-badge", style={"backgroundColor": "rgba(40, 167, 69, 0.2)", "color": "#28a745", "border": "1px solid rgba(40, 167, 69, 0.3)"}),
                html.Span("🔴 Distribution Trap (Trig > 2) ", className="ml-badge", style={"backgroundColor": "rgba(220, 53, 69, 0.2)", "color": "#dc3545", "border": "1px solid rgba(220, 53, 69, 0.3)"})
            ], style={"marginBottom": "20px"})
        ], width=12)
    ]),

    # AG-Grid Table
    dbc.Row([
        dbc.Col([
            html.Div([
                dag.AgGrid(
                    id="signals-grid-inst",
                    rowData=df.to_dict("records"),
                    columnDefs=column_defs,
                    defaultColDef={"resizable": True, "sortable": True, "filter": True},
                    dashGridOptions=grid_options,
                    style={"height": 600, "width": "100%"},
                    className="ag-theme-alpine-dark"
                )
            ], className="glass-card")
        ], width=12)
    ])
], fluid=True, className="mt-3")

# --- 4. Callbacks ---
@callback(
    Output("signals-grid-inst", "rowData"),
    Output("total-signals-inst", "children"),
    Input("refresh-btn-inst", "n_clicks"),
    prevent_initial_call=True
)
def refresh_data(n_clicks):
    fresh_df = load_data()
    return fresh_df.to_dict("records"), str(len(fresh_df))
