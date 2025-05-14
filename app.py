import dash
from dash import html

app = dash.Dash(__name__)
server = app.server  # This is required for Azure deployment

app.layout = html.Div(children=[
    html.H1("Hello Dash"),
    html.P("This is a simple Dash app for calibration visualization.")
])

if __name__ == "__main__":
    app.run(debug=True)
