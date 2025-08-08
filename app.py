import os
import pandas as pd
import dash
from dash import Dash, html, dash_table, dcc
from dash import Input, Output, State
import plotly.express as px
import dash_leaflet as dl
import numpy as np
import plotly.graph_objects as go
from dash import callback_context
import dash_bootstrap_components as dbc
from dotenv import load_dotenv, find_dotenv



# Load .env file
dotenv_path = find_dotenv()
if dotenv_path:
    load_dotenv(dotenv_path)

# Detect App environment and read environment variables
env = os.getenv("ENV")

if env == "Azure":
    scenario_id = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    scenario_id = os.getenv("DATABRICKS_HTTP_PATH")
    scenario_id = os.getenv("DATABRICKS_TOKEN")
elif env == "Local":
    scenario_id = os.getenv("SCENARIO_ID")
    scenario_path = os.getenv("SCENARIO_PATH")
    survey = os.getenv("SURVEY_PATH")
    selected_model = os.getenv("SELECTED_MODEL")
    scenario_name = scenario_path.split('\\')[-1]
else:
    raise ValueError("Environment variable 'ENV' must be set to either 'Azure' or 'Local'.")
print(f"Running in environment: {env}")

# Load survey and model data based on environment
from config import load_survey_data, load_model_data
model_data = load_model_data(scenario_id, scenario_path, selected_model, env)
survey_data = load_survey_data()

# Process trip mode choice data
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

# Load trip by arrival mode and by tour type (i.e., market segment)
aggregator = 'arrival_mode'
emp = False  # Set to True to include employee trips
model_santrips, ge_model_santrips = process_santrips(model_data["santrips"], aggregator, emp)
survey_santrips, ge_survey_santrips = process_santrips(survey_data["santrips"], aggregator, emp)
model_emp_santrips = process_santrips(model_data["santrips"], aggregator, True)
survey_emp_santrips = process_santrips(survey_data["santrips"], aggregator, True)

# Cet merged DataFrames for trip w/wo employee trips by arrival mode and by tour type (i.e., market segment)
merge_df = merge_summarized_trip_data(model_santrips, survey_santrips, ['tour_type', aggregator])
merge_df_general = merge_summarized_trip_data(ge_model_santrips, ge_survey_santrips, ['tour_type_general', aggregator])
merge_df_emp = merge_summarized_trip_data(model_emp_santrips, survey_emp_santrips, ['tour_type', aggregator])


# Create app with pages enabled
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "CalibViz"

# === NAVIGATION BAR ===
def get_navbar():
    return html.Div([
        # === SANDAG Logo ===
        html.Img(
            src='/assets/sandag-logo.png',
            style={
                'height': '40px',
                'marginLeft': 'auto',
                'marginRight': '20px'
            }
        ),
        html.Div("ABM3 Calibration Visualizer", style={
            'fontSize': '22px',
            'fontWeight': 'bold',
            'flex': '1',
            'alignSelf': 'center'
        }),
        dbc.ButtonGroup([
            dcc.Link(dbc.Button("Aggregated Tour Type", color="primary", outline=True, size="sm"), href="/"),
            dcc.Link(dbc.Button("Disaggregated Tour Type", color="primary", outline=True, size="sm"), href="/tour-type-page"),
            dcc.Link(dbc.Button("Employee Trips", color="primary", outline=True, size="sm"), href="/employee-tour-type-page"),
        ])
    ], style={
        'display': 'flex',
        'justifyContent': 'space-between',
        'alignItems': 'center',
        'padding': '10px 20px',
        'backgroundColor': "#4c6f92",
        'borderBottom': '1px solid #dee2e6'
    })



# === Summary Card ===
def generate_summary_card(df):
    # Create summary table
    df = df.groupby('tour_type_general')[['trip_by_mode_model','trip_by_mode_survey']].sum().reset_index()
    df['trip_pct_model'] = df['trip_by_mode_model'] / df['trip_by_mode_model'].iloc[0] * 100
    df['trip_pct_survey'] = df['trip_by_mode_survey'] / df['trip_by_mode_survey'].iloc[0] * 100
    
    # Define a common style for all cells
    cell_style = {"padding": "8px 20px", "minWidth": "120px", "textAlign": "right", 'border': '1px solid black'}

    rows = []
    for _, row in df.iterrows():
        rows.append(html.Tr([
            html.Td(row['tour_type_general'], style=cell_style),
            html.Td(f"{row['trip_by_mode_model']:.1f}", style=cell_style),
            html.Td(f"{row['trip_pct_model']:.2f}%", style=cell_style),
            html.Td(f"{row['trip_by_mode_survey']:.1f}", style=cell_style),
            html.Td(f"{row['trip_pct_survey']:.2f}%", style=cell_style)
        ]))
    table = dbc.Table([
        html.Thead(html.Tr([
            html.Th("Tour Type", style=cell_style),
            html.Th("Model Weighted Trips", style=cell_style),
            html.Th("Model Trip %", style=cell_style),
            html.Th("Survey Weighted Trips", style=cell_style),
            html.Th("Survey Trip %", style=cell_style)
        ])),
        html.Tbody(rows)
    ], bordered=True, striped=True, hover=True, size="sm", style={'border': '1px solid black', 'borderCollapse': 'collapse'})
    card = dbc.Card([
        dbc.CardHeader(
            "Trip Summary by Departing Air Passenger Market",
            style={"fontWeight": "bold"}
        ),
        dbc.CardBody(table)
    ], style={"margin-bottom": "20px"})
    return card

# === PAGE 1: Aggregated Tour Type Page ===
summary_layout = html.Div([
    get_navbar(),
    html.Div([
        html.H3(f"Scenario: {scenario_name}", style={"marginRight": "40px", "display": "inline-block"}),
        html.H3(f"Model: {selected_model}", style={"display": "inline-block"}),

        generate_summary_card(merge_df_general),

        html.Label("Select Aggregated Tour Type:"),
        dcc.Dropdown(
            id='general-tour-type-dropdown',
            options=[{'label': t, 'value': t} for t in merge_df_general['tour_type_general'].unique()],
            value='Total',
            clearable=False,
            style={'width': '300px', 'margin-bottom': '20px'}
        ),

        html.Button("Show Weighted Person Trips", id='toggle-btn', n_clicks=0, style={'margin-bottom': '20px'}),
        dcc.Graph(id='general-bar-chart')
    ], style={'padding': '20px'})
])

# === PAGE 2: Disaggregated Tour Type Chart Page ===
tour_type_layout = html.Div([
    get_navbar(),
    html.Div([
        html.H3(f"Scenario: {scenario_name}", style={"marginRight": "40px", "display": "inline-block"}),
        html.H3(f"Model: {selected_model}", style={"display": "inline-block"}),
        html.Br(),
        html.Label("Select Disaggregated Tour Type:"),
        dcc.Dropdown(
            id='tour-type-dropdown',
            options=[{'label': t, 'value': t} for t in merge_df['tour_type'].unique()],
            value='Total',
            clearable=False,
            style={'width': '300px', 'margin-bottom': '20px'}
        ),

        html.Button("Show Weighted Person Trips", id='toggle-btn-tour', n_clicks=0, style={'margin-bottom': '20px'}),
        dcc.Graph(id='bar-chart')
    ], style={'padding': '20px'})
])


# === PAGE 3: Employee Trips Chart Page ===
employee_tour_type_layout = html.Div([
    get_navbar(),
    html.Div([
        html.H3(f"Scenario: {scenario_name}", style={"marginRight": "40px", "display": "inline-block"}),
        html.H3(f"Model: {selected_model}", style={"display": "inline-block"}),
        html.Br(),
        html.Label("Employee Trips by Tour Type:"),
        dcc.Dropdown(
            id='employee-tour-type-dropdown',
            options=[{'label': t, 'value': t} for t in merge_df_emp['tour_type'].unique()],
            value='emp',
            clearable=False,
            style={'width': '300px', 'margin-bottom': '20px'}
        ),

        html.Button("Show Weighted Person Trips", id='toggle-btn-emp', n_clicks=0, style={'margin-bottom': '20px'}),
        dcc.Graph(id='employee-bar-chart')
    ], style={'padding': '20px'})
])


# === APP LAYOUT with Routing ===
app.layout = html.Div([
    dcc.Location(id="url"),
    html.Div(id="page-content")
])


# === ROUTING CALLBACK ===
@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname")
)
def display_page(pathname):
    if pathname == "/tour-type-page":
        return tour_type_layout
    elif pathname == "/employee-tour-type-page":
        return employee_tour_type_layout
    else:
        return summary_layout

# Callback for General Tour Type Chart (on summary page)
@app.callback(
    Output('general-bar-chart', 'figure'),
    Output('toggle-btn', 'children'),
    Input('general-tour-type-dropdown', 'value'),
    Input('toggle-btn', 'n_clicks')
)
def update_general_bar_chart(selected_general_tour_type, n_clicks):
    show_weighted = n_clicks % 2 == 1
    filtered_df = merge_df_general[merge_df_general['tour_type_general'] == selected_general_tour_type]

    if show_weighted:
        value_vars = ["trip_by_mode_model", "trip_by_mode_survey"]
        y_label = "Person Trips"
        btn_text = "Show Percentage of Trips"
    else:
        value_vars = ["trip_pct_model", "trip_pct_survey"]
        y_label = "Percentage of Trips"
        btn_text = "Show Weighted Person Trips"

    x_axis_order = ['Drop-off/Pick up', 'UBER/Lyft', 'Taxi', 'Personal Car Parked','Shared Shuttle Van','Rental Car','Walk','Public Transportation']

    fig = px.bar(
        filtered_df.melt(id_vars=aggregator, value_vars=value_vars, var_name="Source", value_name=y_label),
        x=aggregator, y=y_label, color="Source", barmode="group",
        category_orders={'arrival_mode': x_axis_order},
        title=f"Model vs Survey {y_label} by Mode ({selected_general_tour_type})"
    )
    return fig, btn_text


# Callback for Tour Type Page Chart
@app.callback(
    Output('bar-chart', 'figure'),
    Output('toggle-btn-tour', 'children'),
    Input('tour-type-dropdown', 'value'),
    Input('toggle-btn-tour', 'n_clicks')
)
def update_bar_chart(selected_tour_type, n_clicks):
    show_weighted = n_clicks % 2 == 1
    filtered_df = merge_df[merge_df['tour_type'] == selected_tour_type]

    if show_weighted:
        value_vars = ["trip_model", "trip_survey"]
        y_label = "Person Trips"
        btn_text = "Show Percentage of Trips"
    else:
        value_vars = ["trip_pct_model", "trip_pct_survey"]
        y_label = "Percentage of Trips"
        btn_text = "Show Weighted Person Trips"
        
    x_axis_order = ['Drop-off/Pick up', 'UBER/Lyft', 'Taxi', 'Personal Car Parked','Shared Shuttle Van','Rental Car','Walk','Public Transportation']

    fig = px.bar(
        filtered_df.melt(id_vars=aggregator, value_vars=value_vars, var_name="Source", value_name=y_label),
        x=aggregator, y=y_label, color="Source", barmode="group",
        category_orders={'arrival_mode': x_axis_order},
        title=f"Model vs Survey {y_label} by Mode ({selected_tour_type})"
    )
    return fig, btn_text

# Callback for Employee Trips by Tour Type Page
@app.callback(
    Output('employee-bar-chart', 'figure'),
    Output('toggle-btn-emp', 'children'),
    Input('employee-tour-type-dropdown', 'value'),
    Input('toggle-btn-emp', 'n_clicks')
)
def update_employee_bar_chart(selected_tour_type, n_clicks):
    show_weighted = n_clicks % 2 == 1
    filtered_df = merge_df_emp[merge_df_emp['tour_type'] == selected_tour_type]

    if show_weighted:
        value_vars = ["trip_model", "trip_survey"]
        y_label = "Weighted Person Trips"
        btn_text = "Show Percentage of Trips"
    else:
        value_vars = ["trip_pct_model", "trip_pct_survey"]
        y_label = "Percentage of Weighted Person Trips"
        btn_text = "Show Weighted Person Trips"

    x_axis_order = ['Drop-off/Pick up', 'UBER/Lyft', 'Taxi', 'Personal Car Parked',
                    'Shared Shuttle Van', 'Rental Car', 'Walk', 'Public Transportation']

    fig = px.bar(
        filtered_df.melt(id_vars=aggregator, value_vars=value_vars,
                         var_name="Source", value_name=y_label),
        x=aggregator, y=y_label, color="Source", barmode="group",
        category_orders={'arrival_mode': x_axis_order},
        title=f"Model vs Survey {y_label} by Mode (Employee - {selected_tour_type})"
    )
    return fig, btn_text


if __name__ == '__main__':
    app.run(debug=True, port=8051)