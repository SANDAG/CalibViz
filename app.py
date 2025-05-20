import dash
from dash import dcc, html
from dash.dependencies import Input, Output
from dotenv import load_dotenv, find_dotenv
import os

# Only load if the .env file is present
dotenv_path = find_dotenv()
if dotenv_path:
    load_dotenv(dotenv_path)

# Read environment variables
model_data_rootdir = os.getenv("MODELDATA_ROOTDIR")
model_runs_list_str = os.getenv("MODELRUNS_LIST")

# Convert MODELRUNS_LIST from string to a list
if model_runs_list_str:
    model_runs_list = model_runs_list_str.split(",")
else:
    model_runs_list = []

# Function to simulate some action based on the model run
def perform_action(selected_model_run):
    return f"Action performed for model run: {selected_model_run}"

app = dash.Dash(__name__)
server = app.server  # This is required for Azure deployment

app.layout = html.Div(children=[
    html.H1("Hello Dash"),
    html.P("This is a simple Dash app for calibration visualization."),
    html.P(f"Model Data Root Directory: {model_data_rootdir}"),
    
    # Dropdown for model runs
    dcc.Dropdown(
        id='model-run-dropdown',
        options=[{'label': run, 'value': run} for run in model_runs_list],
        value=model_runs_list[0],  # Default value
        style={'width': '50%'}
    ),
    
    # Display the result after selecting a model run
    html.Div(id='action-output')
])

# Define the callback to handle changes in the dropdown
@app.callback(
    Output('action-output', 'children'),
    Input('model-run-dropdown', 'value')
)
def update_output(selected_model_run):
    # Perform some function based on the selected model run
    result = perform_action(selected_model_run)
    return result

if __name__ == "__main__":
    app.run(debug=True)
