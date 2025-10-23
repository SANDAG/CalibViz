import dash_bootstrap_components as dbc
from dash import html

sidebar = dbc.Offcanvas(
    [
        html.H5("Scenario Model", className="mb-3"),
        dbc.Button("Trip Mode Choice", id="btn-mode-trip", color="primary", className="mb-2", n_clicks=0, style={"width":"100%"}),
        dbc.Button("Destination Choice", id="btn-mode-dest", color="secondary", outline=True, className="mb-2", n_clicks=0, style={"width":"100%"}),
        html.Hr()
    ],
    id="sidebar-panel",
    title="Choose Metric",
    is_open=False,
    placement="start",
)
