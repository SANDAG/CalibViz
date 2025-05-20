import dash
from dash import dcc, html
from dash.dependencies import Input, Output
from dotenv import load_dotenv, find_dotenv
import os
import pandas as pd
import plotly.express as px

# Only load if the .env file is present
dotenv_path = find_dotenv()
if dotenv_path:
    load_dotenv(dotenv_path)

# Read environment variables
model_data_rootdir = os.getenv("MODELDATA_ROOTDIR")
model_runs_list_str = os.getenv("MODELRUNS_LIST")
model_data_path = os.getenv("MODELDATA_PATH")
model_data_file = os.getenv("MODELDATA_FILE")

# Convert MODELRUNS_LIST from string to a list
if model_runs_list_str:
    model_runs_list = model_runs_list_str.split(",")
else:
    model_runs_list = []

# Function to read the data file for the selected model run
def load_model_data_file(file_path):
    """Load a data file into a pandas DataFrame based on its extension."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File does not exist: {file_path}")

    _, ext = os.path.splitext(file_path)  # Get the file extension

    ext = ext.lower()
    if ext == ".csv":
        return pd.read_csv(file_path)
    elif ext == ".json":
        return pd.read_json(file_path)
    elif ext in [".xls", ".xlsx"]:
        return pd.read_excel(file_path)
    elif ext == ".parquet":
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


app = dash.Dash(__name__)
server = app.server  # This is required for Azure deployment

app.layout = html.Div(children=[
    html.H1("Dash CalibViz"),
    html.P("This is a simple Dash app for calibration visualization."),
    html.P(f"Model Data Root Directory: {model_data_rootdir}"),
    
    # Dropdown for model runs
    dcc.Dropdown(
        id='model-run-dropdown',
        options=[{'label': run, 'value': run} for run in model_runs_list],
        value=model_runs_list[0],  # Default value
        style={'width': '50%'}
    ),
    
    # Display the histogram of the selected variable
    dcc.Graph(id='distribution-chart')
])

# Define the callback to handle changes in the dropdown
@app.callback(
    Output('distribution-chart', 'figure'),
    Input('model-run-dropdown', 'value')
)
def update_chart(selected_model_run):
    # Read the CSV corresponding to the selected model run
    try:
        file_path = os.path.join(model_data_rootdir, selected_model_run, model_data_path, model_data_file) # Full path to file
        df = load_model_data_file(file_path)
        
        if df is None:
            return {
                'data': [],
                'layout': {'title': f"No CSV files found for model run: {selected_model_run}"}
            }
        
        # Assuming the CSV has a column named 'variable_name' (replace with your column name)
        if 'PopEmpDenPerMi' not in df.columns:
            return {
                'data': [],
                'layout': {'title': f"Column 'PopEmpDenPerMi' not found in the file for model run: {selected_model_run}"}
            }
        
        # Create a histogram using Plotly
        fig = px.histogram(df, x='PopEmpDenPerMi', title=f"Distribution of 'PopEmpDenPerMi' for {selected_model_run}")
        return fig
    except Exception as e:
        return {
            'data': [],
            'layout': {'title': f"Error: {str(e)}"}
        }


if __name__ == "__main__":
    app.run(debug=True)
