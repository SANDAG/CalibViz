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
def process_santrips(trip_data, aggregator):
    # map model and survey arrival mode to WSP mode split
    arrival_mode_to_wsp = {
            "drop_off": "Drop-off/Pick up",
            "shuttle": "Shared Shuttle Van",
            "public_transit": "Public Transportation",
            "park_escort": "Drop-off/Pick up",
            "parked_on_site":"Personal Car Parked",
            "parked_off_site":"Personal Car Parked",
            # 'parked_employee':"Personal Car Parked",  # ignore employee trips for now
            # 'parked_unknown':"Personal Car Parked",   # ignore employee trips for now
            "rental_car":"Rental Car",
            "tnc":"UBER/Lyft",
            "taxi":"Taxi",
            "active_transportation":"Walk"
    }
    trip_data['arrival_mode'] = trip_data['arrival_mode'].replace(arrival_mode_to_wsp)

    """
    Group the trip data by the specified aggregator (e.g., arrival_mode) and tour type
    """
    # group by user-input aggregator (e.g., arrival_mode) and tour type and calculate percentage of trips by tour type
    trip_by_mode = trip_data.groupby([aggregator,'tour_type'])['weight_person_trip'].sum().reset_index()
    trip_by_mode['trip_pct'] = trip_by_mode['weight_person_trip'] / trip_by_mode.groupby('tour_type')['weight_person_trip'].transform('sum') * 100

    # create total row from trip_by_mode and calculate percentage of trips
    trip_mode_totals = trip_by_mode.groupby(aggregator)['weight_person_trip'].sum().reset_index()
    trip_mode_totals['tour_type'] = 'Total'
    trip_mode_totals['trip_pct'] = trip_mode_totals['weight_person_trip'] / trip_mode_totals['weight_person_trip'].sum() * 100

    # concatenate the total row to the trip_by_mode DataFrame
    trip_by_tour_mode = pd.concat([trip_by_mode, trip_mode_totals], ignore_index=True)

    # create a new column for the general tour type
    trip_by_tour_mode['tour_type_general'] = trip_by_tour_mode['tour_type'].apply(
    lambda x: 'resident' if str(x).startswith('res_') else
              'visitor' if str(x).startswith('vis_') else
              'employee' if str(x).startswith('emp') else
              'Total' if str(x) == 'Total' else
              x
    )
    
    return trip_by_tour_mode

# Load trip by arrival mode and by tour type (i.e., market segment)
aggregator = 'arrival_mode'
model_santrips = process_santrips(model_data["santrips"], aggregator)
survey_santrips = process_santrips(survey_data["santrips"], aggregator)

# Merge the two DataFrames and maintain aggrator columns from survey data
merged_df = model_santrips.merge(survey_santrips, on=[aggregator, 'tour_type'], how='right', suffixes=('_model', '_survey'))

# Create merged DataFrame for trip mode by general tour type
merged_df_general = merged_df.groupby(['tour_type_general_survey'])[['weight_person_trip_model','weight_person_trip_survey']].sum().reset_index()
merged_df_general['trip_pct_model'] =  merged_df_general['weight_person_trip_model'] / merged_df_general.query("tour_type_general_survey != 'Total'")['weight_person_trip_model'].sum() * 100
merged_df_general['trip_pct_survey'] =  merged_df_general['weight_person_trip_survey'] / merged_df_general.query("tour_type_general_survey != 'Total'")['weight_person_trip_survey'].sum() * 100






# Create app
app = dash.Dash(__name__)
app.title = "CalibViz"

# Add a button to switch between percentage and weighted person trips in the bar chart
app.layout = html.Div([
    html.H2("SANDAG ABM Calibration Visualizer"),
    html.H3(f"Scenario: {scenario_name}", style={"margin-right": "40px", "display": "inline-block"}),
    html.H3(f"Model: {selected_model}", style={"display": "inline-block"}),
    html.Label("Select Tour Type:"),
    dcc.Dropdown(
        id='tour-type-dropdown',
        options=[{'label': t, 'value': t} for t in merged_df['tour_type'].unique()],
        value='Total',
        clearable=False,
        style={
            'width': '300px',
            'height': '40px',
            'margin-bottom': '20px',
            'position': 'relative',
            'left': '0px',
            'top': '0px'
        }
    ),
    html.Button(
        "Show Weighted Person Trips",
        id='toggle-btn',
        n_clicks=0,
        style={'margin-bottom': '20px'}
    ),
    dcc.Graph(id='bar-chart')
])

@app.callback(
    Output('bar-chart', 'figure'),
    Output('toggle-btn', 'children'),
    Input('tour-type-dropdown', 'value'),
    Input('toggle-btn', 'n_clicks')
)
def update_bar_chart(selected_tour_type, n_clicks):
    show_weighted = n_clicks % 2 == 1
    filtered_df = merged_df[merged_df['tour_type'] == selected_tour_type]
    if show_weighted:
        value_vars = ["weight_person_trip_model", "weight_person_trip_survey"]
        y_label = "Weighted Person Trips"
        btn_text = "Show Percentage of Trips"
    else:
        value_vars = ["trip_pct_model", "trip_pct_survey"]
        y_label = "Percentage of Trips"
        btn_text = "Show Weighted Person Trips"

    fig = px.bar(
        filtered_df.melt(
            id_vars=aggregator,
            value_vars=value_vars,
            var_name="Source",
            value_name=y_label
        ),
        x=aggregator,
        y=y_label,
        color="Source",
        barmode="group",
        title=f"Model vs Survey {y_label} by Mode ({selected_tour_type})"
    )
    return fig, btn_text

# Add a card to display weighted person trips and trip_pct by tour type and total
def generate_summary_card(df):
    
    # Define a common style for all cells
    cell_style = {"padding": "8px 20px", "minWidth": "120px", "textAlign": "right", 'border': '1px solid black'}

    rows = []
    for _, row in df.iterrows():
        rows.append(html.Tr([
            html.Td(row['tour_type_general_survey'], style=cell_style),
            html.Td(f"{row['weight_person_trip_model']:.1f}", style=cell_style),
            html.Td(f"{row['trip_pct_model']:.2f}%", style=cell_style),
            html.Td(f"{row['weight_person_trip_survey']:.1f}", style=cell_style),
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

summary_card = generate_summary_card(merged_df_general)

# Insert the card into the app layout (before the dropdown)
app.layout.children.insert(3, summary_card)

if __name__ == '__main__':
    app.run(debug=True, port=8050)