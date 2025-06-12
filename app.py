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
if "DATABRICKS_HOST" in os.environ: # need to change this to match azure environment
    ENV = "Azure"
    scenario_id = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    scenario_id = os.getenv("DATABRICKS_HTTP_PATH")
    scenario_id = os.getenv("DATABRICKS_TOKEN")
else:
    ENV = "local"
    scenario_id = os.getenv("SCENARIO_ID")
    scenario = os.getenv("SCENARIO_PATH")
    survey = os.getenv("SURVEY_PATH")
    selected_model = os.getenv("SELECTED_MODEL")
    scenario_name = scenario.split('\\')[-1]

print(f"Running in environment: {ENV}")


# Load model data based on environment
if ENV == "local":
    from config_local import load_data
else:
    from config_azure import load_data
data = load_data(scenario, survey)

# Process trip mode choice data
def process_santrips(trip_data):
    # change airport trip modes to match them to arrival modes
    trip_data.loc[trip_data['arrival_mode']=='TAXI_LOC1', "trip_mode"] = "TAXI"
    trip_data.loc[(trip_data['arrival_mode']=='RIDEHAIL_LOC1') 
                        & (trip_data['trip_mode']== "SHARED2"), "trip_mode"] = "TNC_SINGLE"
    trip_data.loc[(trip_data['arrival_mode']=='RIDEHAIL_LOC1') 
                        & (trip_data['trip_mode']== "SHARED3"), "trip_mode"] = "TNC_SHARED"

    # create a generalized primary purpose column
    trip_data.loc[trip_data['primary_purpose'].str.contains('bus'), "primary_purpose"] = "Business"
    trip_data.loc[trip_data['primary_purpose'].str.contains('per'), "primary_purpose"] = "Personal"
    trip_data.loc[trip_data['primary_purpose'].str.contains('ext'), "primary_purpose"] = "External"
    trip_data.loc[trip_data['primary_purpose'].str.contains('emp'), "primary_purpose"] = "Employee"

    # group by trip mode and primary purpose
    trip_by_mode = trip_data.groupby(['trip_mode'])['trip_id'].count()
    trip_by_purpose_mode = trip_data.groupby(['primary_purpose','trip_mode'])['trip_id'].count()
    purpose_mode_df = trip_by_purpose_mode.unstack(fill_value=0)

    # create total row from trip_by_mode
    total_trips_row = pd.DataFrame(trip_by_mode).T
    total_trips_row.index = ['Total']

    # align columns
    for col in purpose_mode_df.columns:
        if col not in total_trips_row.columns:
            total_trips_row[col] = 0
    total_trips_row = total_trips_row[purpose_mode_df.columns]

    # combine
    combined_df = pd.concat([purpose_mode_df, total_trips_row])

    # add "Total Trips" column per row
    combined_df['total_by_purpose'] = combined_df.sum(axis=1)

    return combined_df

# Load trip by mode choice and by primary purpose (i.e., market segment)
model_santrips = process_santrips(data["final_santrips"])
survey_santrips = process_santrips(data["survey_santrips"])

# === Bar Chart: trip mode choice and trip by primary purpose ===
# Create a combined DataFrame for plotting
def prepare_comparison_df(model_df, survey_df):
    # remove 'total_by_purpose' column for plotting
    model = model_df.drop(columns=['total_by_purpose'])
    survey = survey_df.drop(columns=['total_by_purpose'])
    # melt both DataFrames
    model_melt = model.reset_index().melt(id_vars='index', var_name='trip_mode', value_name='model_trips')
    survey_melt = survey.reset_index().melt(id_vars='index', var_name='trip_mode', value_name='survey_trips')
    # merge on purpose and trip_mode
    merged = pd.merge(model_melt, survey_melt, on=['index', 'trip_mode'])
    merged = merged.rename(columns={'index': 'purpose'})
    return merged

comparison_df = prepare_comparison_df(model_santrips, survey_santrips)

# Dash app for comparison
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H2("Compare Model vs Survey Trips by Mode and Purpose"),
    html.H3(f"Scenario: {scenario_name}"),
    html.H3(f"Model: {selected_model}"),
    html.Label("Primary Purpose:"),
    dcc.Dropdown(
        id='purpose_dropdown',
        options=[{'label': p, 'value': p} for p in comparison_df['purpose'].unique()],
        value='Total',  # default value
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

    dcc.Graph(id='comparison_bar_chart')
])

@app.callback(
    Output('comparison_bar_chart', 'figure'),
    Input('purpose_dropdown', 'value')
)
def update_comparison_chart(selected_purpose):
    df = comparison_df[comparison_df['purpose'] == selected_purpose]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df['trip_mode'],
        y=df['model_trips'],
        name='Model'
    ))
    fig.add_trace(go.Bar(
        x=df['trip_mode'],
        y=df['survey_trips'],
        name='Survey'
    ))
    fig.update_layout(
        barmode='group',
        title=f"'{selected_purpose}' Trips by Mode",
        xaxis_title='Trip Mode',
        yaxis_title='Number of Trips',
        xaxis_tickangle=-45
    )
    return fig

if __name__ == '__main__':
    app.run(debug=True)