import dash
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc

# Initialize the multi-page Dash app
app = Dash(__name__, use_pages=True, pages_folder="dash_pages", external_stylesheets=[dbc.themes.CYBORG])
app.title = "Institutional Screener"

# Define the glassmorphism sidebar
sidebar = html.Div(
    [
        html.H2("FZ Standard", className="display-6"),
        html.Hr(style={"borderColor": "rgba(255,255,255,0.1)", "marginBottom": "2rem"}),
        html.Nav(
            [
                dcc.Link(
                    f"{page['name']}", 
                    href=page["relative_path"],
                    className="sidebar-nav-link"
                )
                for page in dash.page_registry.values()
            ],
            className="nav flex-column"
        ),
    ],
    className="sidebar"
)

# Main layout shell
app.layout = html.Div([
    dcc.Location(id="url"),
    sidebar,
    html.Div(
        dash.page_container, 
        className="content"
    )
])

if __name__ == '__main__':
    # Stop the previous simple server if needed and run this one
    app.run(debug=True, host='0.0.0.0')
