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
def process_santrips(trip_data):
    # group by trip mode and tour type
    trip_by_mode = trip_data.groupby(['trip_mode','tour_type'])['weight_person_trip'].sum().reset_index()

    # create total row from trip_by_mode
    trip_mode_totals = trip_by_mode.groupby('trip_mode')['weight_person_trip'].sum().reset_index()
    trip_mode_totals['tour_type'] = 'Total'
    trip_by_mode_with_total = pd.concat([trip_by_mode, trip_mode_totals], ignore_index=True)

    return trip_by_mode_with_total

# Load trip by mode choice and by primary purpose (i.e., market segment)
model_santrips = process_santrips(model_data["santrips"])
survey_santrips = process_santrips(survey_data["santrips"])

# Merge the two DataFrames
merged_df = model_santrips.merge(survey_santrips, on=['trip_mode', 'tour_type'], suffixes=('_model', '_survey'))

# === Bar Chart: trip mode choice and trip by primary purpose ===
# Create app
app = dash.Dash(__name__)
app.title = "CalibViz"

# Set the external stylesheets for Dash Bootstrap Components
app.layout = html.Div([
    html.H2("Comparison of Model vs. Survey Person Trips by Mode"),
    html.H3(f"Scenario: {scenario_name}"),
    html.H3(f"Model: {selected_model}"),
    html.Label("Select Tour Type:"),
    dcc.Dropdown(
        id='tour-type-dropdown',
        options=[{'label': t, 'value': t} for t in merged_df['tour_type'].unique()],
        value='Total',  # default selection
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
    dcc.Graph(id='bar-chart')
])

@app.callback(
    Output('bar-chart', 'figure'),
    Input('tour-type-dropdown', 'value')
)
def update_bar_chart(selected_tour_type):
    filtered_df = merged_df[merged_df['tour_type'] == selected_tour_type]
    fig = px.bar(
        filtered_df.melt(
            id_vars="trip_mode",
            value_vars=["weight_person_trip_model", "weight_person_trip_survey"],
            var_name="Source",
            value_name="Trips"
        ),
        x="trip_mode",
        y="Trips",
        color="Source",
        barmode="group",
        title=f"Model vs Survey Weighted Person Trips by Mode ({selected_tour_type})"
    )
    return fig

if __name__ == '__main__':
    app.run(debug=True)