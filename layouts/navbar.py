from dash import html, dcc
import dash_bootstrap_components as dbc

def get_navbar():
    return html.Div(
        [
            html.Button("☰", id="sidebar-toggle", n_clicks=0,
                        style={'marginRight': '12px', 'fontSize': '20px',
                               'background':'transparent','color':'white','border':'none','cursor':'pointer'}),

            html.Img(src='/assets/sandag-logo.png',
                     style={'height': '40px', 'marginLeft': 'auto', 'marginRight': '20px'}),

            html.Div("ABM3 Calibration Visualizer",
                     style={'fontSize': '22px', 'fontWeight': 'bold', 'flex': '1', 'alignSelf': 'center'}),

            html.Div(
                [
                    html.Label("Scenario", style={'color': 'white', 'marginRight': '8px'}),
                    dcc.Dropdown(id='scenario-dd', options=[], value=None, clearable=False, persistence=True, style={'width': '280px'})
                ],
                style={'display': 'flex', 'alignItems': 'center', 'gap': '8px',
                       'marginRight': '20px', 'minWidth': '300px'}
            ),

            dbc.ButtonGroup(
                [
                    dcc.Link(dbc.Button("Aggregated Tour Type", id="btn-home", outline=True, size="sm", style={'color': 'white'}), href="/"),
                    dcc.Link(dbc.Button("Disaggregated Tour Type", id="btn-tour", outline=True, size="sm", style={'color': 'white'}), href="/tour-type-page"),
                    dcc.Link(dbc.Button("Employee Trips", id="btn-emp", outline=True, size="sm", style={'color': 'white'}), href="/employee-tour-type-page"),
                ]
            ),
        ],
        style={'display': 'flex', 'justifyContent': 'space-between', 'alignItems': 'center',
               'padding': '10px 20px', 'backgroundColor': "#4c6f92", 'borderBottom': '1px solid #dee2e6'}
    )
