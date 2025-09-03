import os
import pandas as pd
import numpy as np
import dash
from dash import dcc, html, dash_table, Dash, Input, Output, State, callback_context
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import dash_leaflet as dl
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv, find_dotenv
from config import load_survey_data, load_model_data


# === Detect App environment and read environment variables ===
dotenv_path = find_dotenv()
if dotenv_path:
    load_dotenv(dotenv_path)
env = os.getenv("ENV")
if env == "Azure":
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")
elif env == "Local":
    scenario_list_str = os.getenv("SCENARIO_LIST")
    survey = os.getenv("SURVEY_PATH")
    selected_model = os.getenv("SELECTED_MODEL")
else:
    raise ValueError("Environment variable 'ENV' must be set to either 'Azure' or 'Local'.")
print(f"Running in environment: {env}")


# === Load survey and model data ===
# load survey data from Databricks
survey_data = load_survey_data()

# load model data from input environment
if env == "Azure":
    pass    #need to update later
else:
    # get scenario dictionary and save metadata and model data for each scenario
    scenario_list = scenario_list_str.split(",") if scenario_list_str else []
    scenario_dict = {path : {} for path in scenario_list}
    model_data = load_model_data(scenario_dict, selected_model, env)


# === Process airport trip mode choice data ===
def process_santrips(trip_data, aggregator, emp):
    # ignore employee trips if emp is set to False
    if emp == False:
        trip_data = trip_data.query("tour_type != 'emp'").copy()
    # include ONLY employee trips if emp is set to True
    else:
        trip_data = trip_data.query("tour_type == 'emp'").copy()

    # map model and survey arrival mode to WSP mode split
    arrival_mode_to_wsp = {
            "drop_off": "Drop-off/Pick up",
            "shuttle": "Shared Shuttle Van",
            "public_transit": "Public Transportation",
            "park_escort": "Drop-off/Pick up",
            "parked_on_site":"Personal Car Parked",
            "parked_off_site":"Personal Car Parked",
            'parked_employee':"Personal Car Parked",
            'parked_unknown':"Personal Car Parked",
            "rental_car":"Rental Car",
            "tnc":"UBER/Lyft",
            "taxi":"Taxi",
            "active_transportation":"Walk"
    }
    trip_data['arrival_mode'] = trip_data['arrival_mode'].replace(arrival_mode_to_wsp)

    # rename columns for clarity
    trip_data = trip_data.rename(columns={'weight_person_trip': 'trip'})

    """
    Group the trip data by the specified aggregator (e.g., arrival_mode) and tour type
    """
    # group by user-input aggregator (e.g., arrival_mode) and tour type and calculate percentage of trips by tour type
    trip_by_mode = trip_data.groupby([aggregator,'tour_type'])['trip'].sum().reset_index()
    trip_by_mode['trip_pct'] = trip_by_mode['trip'] / trip_by_mode.groupby('tour_type')['trip'].transform('sum') * 100

    # create total row from trip_by_mode and calculate percentage of trips
    trip_mode_totals = trip_by_mode.groupby(aggregator)['trip'].sum().reset_index()
    trip_mode_totals['tour_type'] = 'Total'
    trip_mode_totals['trip_pct'] = trip_mode_totals['trip'] / trip_mode_totals['trip'].sum() * 100

    if emp == False:
        # concatenate the total row to the trip_by_mode DataFrame
        trip_by_dTour_aggMode = pd.concat([trip_by_mode, trip_mode_totals], ignore_index=True)
    else:
        trip_by_dTour_aggMode = trip_by_mode.copy()

    # create a new column for the general tour type
    trip_by_dTour_aggMode['tour_type_general'] = trip_by_dTour_aggMode['tour_type'].apply(
    lambda x: 'resident' if str(x).startswith('res_') else
              'visitor' if str(x).startswith('vis_') else
              'employee' if str(x).startswith('emp') else
              'Total' if str(x) == 'Total' else
              x
    )

    # Ensure all modes in unique_modes are present in merged_df for each tour_type
    unique_modes = list(set(arrival_mode_to_wsp.values()))
    all_tour_types = trip_by_dTour_aggMode['tour_type'].unique()
    rows_to_add = []

    for tour_type in all_tour_types:
        existing_modes = set(trip_by_dTour_aggMode.loc[trip_by_dTour_aggMode['tour_type'] == tour_type, 'arrival_mode'])
        missing_modes = set(unique_modes) - existing_modes
        for mode in missing_modes:
            # Find the general tour type for this tour_type
            general_type = trip_by_dTour_aggMode.loc[trip_by_dTour_aggMode['tour_type'] == tour_type, 'tour_type_general'].iloc[0]
            rows_to_add.append({
                'arrival_mode': mode,
                'tour_type': tour_type,
                'trip': 0.0,
                'trip_pct': 0.0,
                'tour_type_general': general_type
            })
        trip_by_dTour_aggMode = pd.concat([trip_by_dTour_aggMode, pd.DataFrame(rows_to_add)], ignore_index=True)
    
    if emp == False:
        # calculate total trip by general tour type and by arrival mode
        trip_by_geTour_aggMode = trip_by_dTour_aggMode.query("tour_type_general != 'Total'").copy()
        trip_by_geTour_aggMode = trip_by_geTour_aggMode.groupby(['tour_type_general', aggregator])['trip'].sum().reset_index()
        
        # calculate trip percentage by general tour type and by arrival mode
        total_trips_by_geTour = trip_by_geTour_aggMode.groupby(['tour_type_general'])['trip'].sum().reset_index()
        trip_by_geTour_aggMode = trip_by_geTour_aggMode.merge(total_trips_by_geTour, on='tour_type_general', suffixes=('_by_mode', '_total'))
        trip_by_geTour_aggMode['trip_pct'] = trip_by_geTour_aggMode['trip_by_mode'] / trip_by_geTour_aggMode['trip_total'] * 100

        # combine trips of all general tour types and calculate overall total
        trip_geMode_totals = trip_by_dTour_aggMode.query("tour_type == 'Total'").copy()
        trip_geMode_totals = trip_geMode_totals.rename(columns={'trip': 'trip_by_mode'}).drop(['tour_type'], axis=1)
        trip_geMode_totals['trip_total'] = trip_geMode_totals['trip_by_mode'].sum()
        trip_by_geTour_aggMode = pd.concat([trip_by_geTour_aggMode, trip_geMode_totals], ignore_index=True)
        return trip_by_dTour_aggMode, trip_by_geTour_aggMode
    else:
        return trip_by_dTour_aggMode

    

def merge_summarized_trip_data(model, survey, aggregator2):
    return model.merge(survey, on=aggregator2, how='right', suffixes=('_model', '_survey'))


# Define result dictionary, aggregation, and employee trip inclusion
santrips_dict = {}
aggregator = 'arrival_mode'
emp = False  # Set to True to include employee trips

# Process survey data
# load trip by arrival mode and by tour type (i.e., market segment)
survey_santrips, ge_survey_santrips = process_santrips(survey_data["santrips"], aggregator, emp)
survey_emp_santrips = process_santrips(survey_data["santrips"], aggregator, True)

# Process model data and merge with survey data
for path, data in model_data.items():
    # get scenario name and id
    scenario_name = str(data['metadata']['scenario_id']) + ': ' + data['metadata']['scenario_name']

    # load trip by arrival mode and by tour type (i.e., market segment)
    model_santrips, ge_model_santrips = process_santrips(data["santrips"], aggregator, emp)
    model_emp_santrips = process_santrips(data["santrips"], aggregator, True)

    # get merged DataFrames for trip w/wo employee trips by arrival mode and by tour type (i.e., market segment)
    merge_df = merge_summarized_trip_data(model_santrips, survey_santrips, ['tour_type', aggregator])
    merge_df_general = merge_summarized_trip_data(ge_model_santrips, ge_survey_santrips, ['tour_type_general', aggregator])
    merge_df_emp = merge_summarized_trip_data(model_emp_santrips, survey_emp_santrips, ['tour_type', aggregator])

    # store merged DataFrames in the dictionary
    santrips_dict[scenario_name] = {
        "model": "airport.SAN",
        "merge_df": merge_df,
        "merge_df_general": merge_df_general,
        "merge_df_emp": merge_df_emp
    }

# === Establish Dash App ===
# Ensure necessary data exist
try:
    santrips_dict
except NameError:
    print("santrips_dict not found")

# Scenario list for the navbar dropdown
scenarios = sorted(santrips_dict.keys())
default_scenario = scenarios[0] if scenarios else None

# Common category order for plots
X_ORDER = [
    'Drop-off/Pick up', 'UBER/Lyft', 'Taxi', 'Personal Car Parked',
    'Shared Shuttle Van', 'Rental Car', 'Walk', 'Public Transportation'
]

# --- App ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.title = "CalibViz"

# --- Navbar with Scenario dropdown ---
def get_navbar():
    return html.Div(
        [
            html.Img(src='/assets/sandag-logo.png',
                     style={'height': '40px', 'marginLeft': 'auto', 'marginRight': '20px'}),
            html.Div("ABM3 Calibration Visualizer",
                     style={'fontSize': '22px', 'fontWeight': 'bold', 'flex': '1', 'alignSelf': 'center'}),
            html.Div(
                [
                    html.Label("Scenario", style={'color': 'white', 'marginRight': '8px'}),
                    dcc.Dropdown(
                        id='scenario-dd',
                        options=[{'label': s, 'value': s} for s in scenarios],
                        value=default_scenario,
                        clearable=False,
                        persistence=True,
                        style={'width': '280px'}
                    )
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
            )
        ],
        style={'display': 'flex', 'justifyContent': 'space-between', 'alignItems': 'center',
               'padding': '10px 20px', 'backgroundColor': "#4c6f92", 'borderBottom': '1px solid #dee2e6'}
    )

# --- Summary Card ---
def generate_summary_card(df: pd.DataFrame):
    if df is None or df.empty:
        return dbc.Alert("No data for this scenario.", color="warning", className="mt-2")

    # Aggregate and compute percentages robustly
    df_no_total = df.query("tour_type_general != 'Total'").copy()
    tbl = df_no_total.groupby('tour_type_general')[['trip_by_mode_model', 'trip_by_mode_survey']].sum().reset_index()
    tot_model = float(tbl['trip_by_mode_model'].sum())
    tot_survey = float(tbl['trip_by_mode_survey'].sum())
    tbl['trip_pct_model'] = (tbl['trip_by_mode_model'] / (tot_model if tot_model else 1.0)) * 100.0
    tbl['trip_pct_survey'] = (tbl['trip_by_mode_survey'] / (tot_survey if tot_survey else 1.0)) * 100.0

    # Add total row to tbl
    total_row = {
        'tour_type_general': 'Total',
        'trip_by_mode_model': tbl['trip_by_mode_model'].sum(),
        'trip_by_mode_survey': tbl['trip_by_mode_survey'].sum(),
        'trip_pct_model': tbl['trip_pct_model'].sum(),
        'trip_pct_survey': tbl['trip_pct_survey'].sum()
    }
    tbl = pd.concat([tbl, pd.DataFrame([total_row])], ignore_index=True)

    cell_style = {"padding": "8px 20px", "minWidth": "120px", "textAlign": "right", 'border': '1px solid black'}

    rows = []
    for _, row in tbl.iterrows():
        rows.append(
            html.Tr(
                [
                    html.Td(row['tour_type_general'], style=cell_style),
                    html.Td(f"{row['trip_by_mode_survey']:.1f}", style=cell_style),
                    html.Td(f"{row['trip_by_mode_model']:.1f}", style=cell_style),
                    html.Td(f"{row['trip_pct_survey']:.2f}%", style=cell_style),
                    html.Td(f"{row['trip_pct_model']:.2f}%", style=cell_style),
                ]
            )
        )

    table = dbc.Table(
        [
            html.Thead(
                html.Tr(
                    [
                        html.Th("Tour Type", style=cell_style),
                        html.Th("Survey Weighted Trips", style=cell_style),
                        html.Th("Model Weighted Trips", style=cell_style),
                        html.Th("Survey Trip %", style=cell_style),
                        html.Th("Model Trip %", style=cell_style),
                    ]
                )
            ),
            html.Tbody(rows),
        ],
        bordered=True, striped=True, hover=True, size="sm",
        style={'border': '1px solid black', 'borderCollapse': 'collapse'}
    )

    note = html.P("Only weighted person trips are included in this visualizer", style={"marginTop": "10px", "fontStyle": "italic"})

    return dbc.Card(
        [
            dbc.CardHeader("Trip Summary by Departing Air Passenger Market", style={"fontWeight": "bold"}),
            dbc.CardBody(
                [
                    table,
                    note
                ]
            )
        ],
        style={"margin-bottom": "20px"}
    )

# --- Pages ---
summary_layout = html.Div(
    [
        get_navbar(),
        html.Div(
            [
                html.H3(id="summary-model-title", style={"display": "inline-block"}),
                html.Div(id="summary-card", style={"marginBottom": "20px"}),

                html.Label("Select Aggregated Tour Type:"),
                dcc.Dropdown(id='general-tour-type-dropdown', options=[], value=None, clearable=False,
                             style={'width': '300px', 'margin-bottom': '20px'}),

                html.Button("Show Weighted Person Trips", id='toggle-btn', n_clicks=0, style={'margin-bottom': '20px'}),
                dcc.Graph(id='general-bar-chart')
            ],
            style={'padding': '20px'}
        )
    ]
)

tour_type_layout = html.Div(
    [
        get_navbar(),
        html.Div(
            [
                html.H3(id="tour-model-title", style={"display": "inline-block"}),
                html.Br(),
                html.Label("Select Disaggregated Tour Type:"),
                dcc.Dropdown(id='tour-type-dropdown', options=[], value=None, clearable=False,
                             style={'width': '300px', 'margin-bottom': '20px'}),
                html.Button("Show Weighted Person Trips", id='toggle-btn-tour', n_clicks=0, style={'margin-bottom': '20px'}),
                dcc.Graph(id='bar-chart')
            ],
            style={'padding': '20px'}
        )
    ]
)

employee_tour_type_layout = html.Div(
    [
        get_navbar(),
        html.Div(
            [
                html.H3(id="emp-model-title", style={"display": "inline-block"}),
                html.Br(),
                html.Label("Employee Trips by Tour Type:"),
                dcc.Dropdown(id='employee-tour-type-dropdown', options=[], value=None, clearable=False,
                             style={'width': '300px', 'margin-bottom': '20px'}),
                html.Button("Show Weighted Person Trips", id='toggle-btn-emp', n_clicks=0, style={'margin-bottom': '20px'}),
                dcc.Graph(id='employee-bar-chart')
            ],
            style={'padding': '20px'}
        )
    ]
)

# --- Routing & validation layout ---
app.layout = html.Div([dcc.Location(id="url"), html.Div(id="page-content")])

@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    if pathname == "/tour-type-page":
        return tour_type_layout
    elif pathname == "/employee-tour-type-page":
        return employee_tour_type_layout
    return summary_layout

# --- Helpers ---
def _empty_fig(title: str = ""):
    return px.bar(title=title)

def _get_scenario_data_safe(scenario):
    if not scenario or scenario not in santrips_dict:
        raise PreventUpdate
    return santrips_dict[scenario]

# --- Fan-out: set titles, summary card, and dropdown options/values for all pages ---
@app.callback(
    Output("summary-model-title", "children"),
    Output("summary-card", "children"),
    Output("general-tour-type-dropdown", "options"),
    Output("general-tour-type-dropdown", "value"),
    Input("scenario-dd", "value"),
    Input("url", "pathname"),
)

def refresh_summary_for_scenario(scenario, pathname):
    if pathname not in ("/", None):  # only when on summary page
        raise PreventUpdate

    d = _get_scenario_data_safe(scenario)
    merge_df_general = d["merge_df_general"]

    # scenario_title = f"Scenario: {scenario}"
    model_title = f"Model: {selected_model}"
    summary_card = generate_summary_card(merge_df_general)

    gen_vals = merge_df_general['tour_type_general'].dropna().unique().tolist()
    gen_opts = [{'label': t, 'value': t} for t in gen_vals]
    gen_val = 'Total' if 'Total' in gen_vals else (gen_vals[0] if gen_vals else None)

    return model_title, summary_card, gen_opts, gen_val

# --- Highlight active button ---
@app.callback(
    Output("btn-home", "outline"),
    Output("btn-tour", "outline"),
    Output("btn-emp", "outline"),
    Input("url", "pathname"),
)
def highlight_button(pathname):
    # Default: all outlined (not active)
    home, tour, emp = True, True, True

    if pathname == "/":
        home = False   # remove outline → filled button
    elif pathname == "/tour-type-page":
        tour = False
    elif pathname == "/employee-tour-type-page":
        emp = False

    return home, tour, emp


# --- TOUR PAGE: titles, dropdown ---
@app.callback(
    Output("tour-model-title", "children"),
    Output("tour-type-dropdown", "options"),
    Output("tour-type-dropdown", "value"),
    Input("scenario-dd", "value"),
    Input("url", "pathname"),
)
def refresh_tour_for_scenario(scenario, pathname):
    if pathname != "/tour-type-page":
        raise PreventUpdate

    d = _get_scenario_data_safe(scenario)
    merge_df = d["merge_df"]

    # scenario_title = f"Scenario: {scenario}"
    model_title = f"Model: {selected_model}"

    tour_vals = merge_df['tour_type'].dropna().unique().tolist()
    tour_opts = [{'label': t, 'value': t} for t in tour_vals]
    tour_val = 'Total' if 'Total' in tour_vals else (tour_vals[0] if tour_vals else None)

    return model_title, tour_opts, tour_val


# --- EMPLOYEE PAGE: titles, dropdown ---
@app.callback(
    Output("emp-model-title", "children"),
    Output("employee-tour-type-dropdown", "options"),
    Output("employee-tour-type-dropdown", "value"),
    Input("scenario-dd", "value"),
    Input("url", "pathname"),
)
def refresh_emp_for_scenario(scenario, pathname):
    if pathname != "/employee-tour-type-page":
        raise PreventUpdate

    d = _get_scenario_data_safe(scenario)
    merge_df_emp = d["merge_df_emp"]

    # scenario_title = f"Scenario: {scenario}"
    model_title = f"Model: {selected_model}"

    emp_vals = merge_df_emp['tour_type'].dropna().unique().tolist()
    emp_opts = [{'label': t, 'value': t} for t in emp_vals]
    emp_val = emp_vals[0] if emp_vals else None

    return model_title, emp_opts, emp_val

# --- Bar Charts ---
bar_color_sequence = ["#ff7f0e","#4461e2"]  # survey vs. model

@app.callback(
    Output('general-bar-chart', 'figure'),
    Output('toggle-btn', 'children'),
    Input('scenario-dd', 'value'),
    Input('general-tour-type-dropdown', 'value'),
    Input('toggle-btn', 'n_clicks')
)
def update_general_bar_chart(scenario, selected_general_tour_type, n_clicks):
    if not scenario or scenario not in santrips_dict or not selected_general_tour_type:
        return _empty_fig("No data"), "Show Weighted Person Trips"

    show_weighted = (n_clicks % 2 == 1)
    df = _get_scenario_data_safe(scenario)["merge_df_general"]
    filtered = df[df['tour_type_general'] == selected_general_tour_type]

    if filtered.empty:
        return _empty_fig("No data"), ("Show Percentage of Trips" if show_weighted else "Show Weighted Person Trips")

    if show_weighted:
        value_vars = ["trip_by_mode_survey", "trip_by_mode_model"]
        y_label = "Person Trips"
        btn_text = "Show Percentage of Trips"
    else:
        value_vars = ["trip_pct_survey", "trip_pct_model"]
        y_label = "Percentage of Trips"
        btn_text = "Show Weighted Person Trips"

    fig = px.bar(
        filtered.melt(id_vars=aggregator, value_vars=value_vars, var_name="Source", value_name=y_label),
        x=aggregator, y=y_label, color="Source", barmode="group",
        category_orders={aggregator: X_ORDER},
        color_discrete_sequence=bar_color_sequence,
        title=f"Model vs Survey {y_label} by Mode ({selected_general_tour_type})"
    )
    return fig, btn_text

@app.callback(
    Output('bar-chart', 'figure'),
    Output('toggle-btn-tour', 'children'),
    Input('scenario-dd', 'value'),
    Input('tour-type-dropdown', 'value'),
    Input('toggle-btn-tour', 'n_clicks')
)
def update_bar_chart(scenario, selected_tour_type, n_clicks):
    if not scenario or scenario not in santrips_dict or not selected_tour_type:
        return _empty_fig("No data"), "Show Weighted Person Trips"

    show_weighted = (n_clicks % 2 == 1)
    df = _get_scenario_data_safe(scenario)["merge_df"]
    filtered = df[df['tour_type'] == selected_tour_type]

    if filtered.empty:
        return _empty_fig("No data"), ("Show Percentage of Trips" if show_weighted else "Show Weighted Person Trips")

    if show_weighted:
        value_vars = ["trip_survey", "trip_model"]
        y_label = "Person Trips"
        btn_text = "Show Percentage of Trips"
    else:
        value_vars = ["trip_pct_survey", "trip_pct_model"]
        y_label = "Percentage of Trips"
        btn_text = "Show Weighted Person Trips"

    fig = px.bar(
        filtered.melt(id_vars=aggregator, value_vars=value_vars, var_name="Source", value_name=y_label),
        x=aggregator, y=y_label, color="Source", barmode="group",
        category_orders={aggregator: X_ORDER},
        color_discrete_sequence=bar_color_sequence,
        title=f"Model vs Survey {y_label} by Mode ({selected_tour_type})"
    )
    return fig, btn_text

@app.callback(
    Output('employee-bar-chart', 'figure'),
    Output('toggle-btn-emp', 'children'),
    Input('scenario-dd', 'value'),
    Input('employee-tour-type-dropdown', 'value'),
    Input('toggle-btn-emp', 'n_clicks')
)
def update_employee_bar_chart(scenario, selected_tour_type, n_clicks):
    if not scenario or scenario not in santrips_dict or not selected_tour_type:
        return _empty_fig("No data"), "Show Weighted Person Trips"

    show_weighted = (n_clicks % 2 == 1)
    df = _get_scenario_data_safe(scenario)["merge_df_emp"]
    filtered = df[df['tour_type'] == selected_tour_type]

    if filtered.empty:
        return _empty_fig("No data"), ("Show Percentage of Trips" if show_weighted else "Show Weighted Person Trips")

    if show_weighted:
        value_vars = ["trip_survey", "trip_model"]
        y_label = "Weighted Person Trips"
        btn_text = "Show Percentage of Trips"
    else:
        value_vars = ["trip_pct_survey", "trip_pct_model"]
        y_label = "Percentage of Weighted Person Trips"
        btn_text = "Show Weighted Person Trips"

    fig = px.bar(
        filtered.melt(id_vars=aggregator, value_vars=value_vars, var_name="Source", value_name=y_label),
        x=aggregator, y=y_label, color="Source", barmode="group",
        category_orders={aggregator: X_ORDER},
        color_discrete_sequence=bar_color_sequence,
        title=f"Model vs Survey {y_label} by Mode (Employee - {selected_tour_type})"
    )
    return fig, btn_text

# --- Run ---
if __name__ == '__main__':
    app.run(debug=True, port=8050)