# app.py - creates Dash app and wires global callbacks (navbar/sidebar/mode)
from dash import Dash, html, dcc, Input, Output, State, ctx
import dash_bootstrap_components as dbc
import dash

from data.data_loader import initialize_data
import data.store as store
from layouts.navbar import get_navbar
from layouts.sidebar import sidebar
from utils.constants import DEFAULT_MODE

# Initialize data and store
santrips_dict, selected_model = initialize_data()
store.santrips_dict = santrips_dict
store.selected_model = selected_model

# Create app
app = Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)
app.title = "CalibViz"

# Shared layout (dash.page_container will load pages)
app.layout = html.Div(
    [
        dcc.Location(id="url"),
        dcc.Store(id="mode-store", data=DEFAULT_MODE),
        get_navbar(),
        sidebar,
        dash.page_container,
    ]
)

# --- Global callbacks (sidebar toggle, mode buttons, highlight nav buttons) ---

# Highlight active link buttons in navbar
@app.callback(
    Output("btn-home", "outline"),
    Output("btn-tour", "outline"),
    Output("btn-emp", "outline"),
    Input("url", "pathname"),
)
def highlight_button(pathname):
    home, tour, emp = True, True, True
    if pathname == "/":
        home = False
    elif pathname == "/tour-type-page" or pathname == "/tour-type":
        tour = False
    elif pathname == "/employee-tour-type-page" or pathname == "/employee":
        emp = False
    return home, tour, emp

# open/close sidebar offcanvas
@app.callback(
    Output("sidebar-panel", "is_open"),
    Input("sidebar-toggle", "n_clicks"),
    State("sidebar-panel", "is_open"),
)
def toggle_sidebar(n, is_open):
    if n:
        return not is_open
    return is_open

# mode switcher (Trip vs Dest)
@app.callback(
    Output("mode-store", "data"),
    Output("btn-mode-trip", "outline"),
    Output("btn-mode-dest", "outline"),
    Output("btn-mode-trip", "color"),
    Output("btn-mode-dest", "color"),
    Input("btn-mode-trip", "n_clicks"),
    Input("btn-mode-dest", "n_clicks"),
    State("mode-store", "data"),
)
def set_mode(n_trip, n_dest, current):
    trg = ctx.triggered_id
    mode = current or DEFAULT_MODE
    if trg == "btn-mode-trip":
        mode = "trip"
    elif trg == "btn-mode-dest":
        mode = "dest"
    if mode == "trip":
        return mode, False, True, "primary", "secondary"
    else:
        return mode, True, False, "secondary", "primary"

# --- Populate scenario dropdown ---
@app.callback(
    Output("scenario-dd", "options"),
    Output("scenario-dd", "value"),
    Input("url", "pathname"),
    prevent_initial_call=False
)
def populate_scenario_dropdown(_):
    scenarios = sorted(store.santrips_dict.keys())
    default_scenario = scenarios[0] if scenarios else None
    opts = [{'label': s, 'value': s} for s in scenarios]
    return opts, default_scenario
