import dash
from dash import html, dcc, Input, Output
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import pandas as pd
import os

# --- 1. Data Loading ---
CSV_PATH = "data/active_signals_ranked.csv"
if os.path.exists(CSV_PATH):
    df = pd.read_csv(CSV_PATH)
    df['STABILITY_RAW'] = pd.to_numeric(df['STABILITY_RAW'], errors='coerce')
    df['TRIGGER_COUNT_30D'] = pd.to_numeric(df['TRIGGER_COUNT_30D'], errors='coerce')
else:
    df = pd.DataFrame()

# --- 2. App Initialization ---
# We use CYBORG as a base, but we will override it with our own premium CSS
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
app.title = "Institutional Screener - Dash"

# --- 3. Premium Custom CSS ---
# This replaces the flat Bootstrap theme with a modern glassmorphism terminal look
premium_css = """
<style>
    body {
        background-color: #0E1117;
        color: #FAFAFA;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    }
    
    /* Glassmorphism Cards */
    .glass-card {
        background: rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    }
    
    /* KPI Typography */
    .kpi-label {
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 1px;
        color: #8b949e;
        margin-bottom: 5px;
    }
    .kpi-value {
        font-size: 32px;
        font-weight: 700;
        color: #00FFCC;
    }
    
    /* Premium Buttons */
    .btn-premium {
        background: linear-gradient(135deg, #6c5ce7 0%, #8e7bff 100%) !important;
        border: none !important;
        color: white !important;
        font-weight: 600;
        padding: 10px 24px;
        border-radius: 8px;
        transition: all 0.3s ease;
    }
    .btn-premium:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 15px rgba(108, 92, 231, 0.4);
    }
    
    /* AG-Grid Alpine Dark Customization */
    .ag-theme-alpine-dark {
        --ag-background-color: #121417;
        --ag-header-background-color: #1a1d24;
        --ag-odd-row-background-color: #121417;
        --ag-header-foreground-color: #8b949e;
        --ag-foreground-color: #FAFAFA;
        --ag-border-color: #2d333b;
        --ag-row-hover-color: rgba(108, 92, 231, 0.1);
        font-family: 'Inter', sans-serif;
    }
    .ag-theme-alpine-dark .ag-header {
        border-bottom: 1px solid #2d333b;
    }
    
    /* ML Rule Badges */
    .ml-badge {
        padding: 6px 12px;
        border-radius: 6px;
        font-size: 12px;
        font-weight: 600;
        margin-right: 10px;
    }
</style>
"""

# --- 4. AG-Grid Column Definitions ---
column_defs = [
    {"field": "SYMBOL", "headerName": "Symbol", "width": 120, "pinned": "left", "cellStyle": {"fontWeight": "bold"}},
    {"field": "EXCHANGE", "headerName": "Exch", "width": 80},
    {"field": "CLOSE", "headerName": "Close", "width": 100, "type": "numericColumn", "valueFormatter": {"function": "value.toFixed(2)"}},
    {"field": "MOMENTUM_RAW", "headerName": "Momentum", "width": 110, "type": "numericColumn"},
    {"field": "FOOTPRINT_RAW", "headerName": "Footprint", "width": 110, "type": "numericColumn"},
    {"field": "STABILITY_RAW", "headerName": "Stability", "width": 110, "type": "numericColumn", "valueFormatter": {"function": "value.toFixed(2)"}},
    {"field": "DELIV_PER", "headerName": "Deliv %", "width": 100, "type": "numericColumn"},
    {"field": "ATW", "headerName": "ATW", "width": 90, "type": "numericColumn"},
    {"field": "TRIGGER_COUNT_30D", "headerName": "Triggers", "width": 100, "type": "numericColumn"}
]

grid_options = {
    "rowClassRules": {
        "bg-success text-white font-weight-bold": "params.data.TRIGGER_COUNT_30D === 1 && params.data.STABILITY_RAW > 3.16",
        "bg-danger text-white": "params.data.TRIGGER_COUNT_30D > 2"
    }
}

# --- 5. Layout ---
layout = html.Div([
    # Inject CSS
    dcc.Markdown(premium_css, dangerously_allow_html=True),
    
    dbc.Container([
        # Header
        dbc.Row([
            dbc.Col(html.H2("Institutional Screener", style={"fontWeight": "800", "letterSpacing": "-0.5px"}), width=8),
            dbc.Col(dbc.Button("Refresh Data", className="btn-premium", id="refresh-btn"), width=4, align="end")
        ], className="mt-4 mb-4"),
        
        # KPI Cards Row
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Div("Total Signals", className="kpi-label"),
                    html.Div(str(len(df)), className="kpi-value")
                ], className="glass-card")
            ], width=3),
            # Add more KPI cards here later (e.g., Win Rate, Avg ROI)
        ], className="mb-4"),
        
        # ML Rules Legend
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Span("🟢 Accumulation (Stab > 3.16 & Trig == 1) ", className="ml-badge", style={"backgroundColor": "rgba(40, 167, 69, 0.2)", "color": "#28a745", "border": "1px solid #28a745"}),
                    html.Span("🔴 Distribution Trap (Trig > 2) ", className="ml-badge", style={"backgroundColor": "rgba(220, 53, 69, 0.2)", "color": "#dc3545", "border": "1px solid #dc3545"})
                ], style={"marginBottom": "15px"})
            ], width=12)
        ]),

        # AG-Grid Table (Inside a glass card)
        dbc.Row([
            dbc.Col([
                html.Div([
                    dag.AgGrid(
                        id="signals-grid",
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
    ], fluid=True)
])

app.layout = layout

# --- 6. Callbacks ---
@app.callback(
    Output("signals-grid", "rowData"),
    Input("refresh-btn", "n_clicks"),
    prevent_initial_call=True
)
def refresh_data(n_clicks):
    if os.path.exists(CSV_PATH):
        fresh_df = pd.read_csv(CSV_PATH)
        return fresh_df.to_dict("records")
    return dash.no_update

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
