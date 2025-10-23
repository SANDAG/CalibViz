# pages/employee.py
import dash
from dash import html, dcc, Input, Output
import plotly.express as px

from utils.callbacks_common import _get_scenario_data_safe, _keys_for_mode, _empty_fig, _sorted_categories
from utils.constants import BAR_COLOR_SEQUENCE
import data.store as store

dash.register_page(__name__, path="/employee-tour-type-page", name="Employee Trips")

layout = html.Div(
    [
        html.H3(id="emp-model-title", style={"display": "inline-block"}),
        html.Br(),
        html.Label("Employee Trips by Tour Type:"),
        dcc.Dropdown(id='employee-tour-type-dropdown', options=[], value=None, clearable=False, style={'width': '300px', 'margin-bottom': '20px'}),
        html.Button("Show Weighted Person Trips", id='toggle-btn-emp', n_clicks=0, style={'margin-bottom': '20px'}),
        dcc.Graph(id='employee-bar-chart')
    ],
    style={'padding': '20px'}
)

@dash.callback(
    Output("emp-model-title", "children"),
    Output("employee-tour-type-dropdown", "options"),
    Output("employee-tour-type-dropdown", "value"),
    Input("scenario-dd", "value"),
    Input("mode-store", "data"),
)
def refresh_emp_for_scenario(scenario, mode):
    if not scenario:
        raise dash.exceptions.PreventUpdate
    d = _get_scenario_data_safe(scenario, store.santrips_dict)
    _, _, df_emp_key, _, _, mode_label = _keys_for_mode(mode)
    df_emp = d[df_emp_key]

    model_title = f"Model: {store.selected_model} • {mode_label}"
    vals = df_emp['tour_type'].dropna().unique().tolist() if not df_emp.empty else []
    opts = [{'label': t, 'value': t} for t in vals]
    val = vals[0] if vals else None
    return model_title, opts, val

@dash.callback(
    Output('employee-bar-chart', 'figure'),
    Output('toggle-btn-emp', 'children'),
    Input('scenario-dd', 'value'),
    Input('employee-tour-type-dropdown', 'value'),
    Input('toggle-btn-emp', 'n_clicks'),
    Input('mode-store', 'data'),
)
def update_employee_bar_chart(scenario, selected_tour_type, n_clicks, mode):
    if not scenario or scenario not in store.santrips_dict or not selected_tour_type:
        return _empty_fig("No data"), "Show Weighted Person Trips"

    _, _, df_emp_key, aggregator_col, cat_order, _ = _keys_for_mode(mode)
    df = _get_scenario_data_safe(scenario, store.santrips_dict)[df_emp_key]
    filtered = df[df['tour_type'] == selected_tour_type]
    if filtered.empty:
        btn = "Show Percentage of Trips" if (n_clicks % 2 == 1) else "Show Weighted Person Trips"
        return _empty_fig("No data"), btn

    show_weighted = (n_clicks % 2 == 1)
    if show_weighted:
        value_vars = ["trip_survey", "trip_model"]; y_label = "Weighted Person Trips"; btn_text = "Show Percentage of Trips"
    else:
        value_vars = ["trip_pct_survey", "trip_pct_model"]; y_label = "Percentage of Weighted Person Trips"; btn_text = "Show Weighted Person Trips"

    orders = {aggregator_col: (cat_order if cat_order else _sorted_categories(filtered, aggregator_col))}

    fig = px.bar(
        filtered.melt(id_vars=aggregator_col, value_vars=value_vars, var_name="Source", value_name=y_label),
        x=aggregator_col, y=y_label, color="Source", barmode="group",
        category_orders=orders, color_discrete_sequence=BAR_COLOR_SEQUENCE,
        title=f"Model vs Survey {y_label} by {aggregator_col} (Employee - {selected_tour_type})"
    )
    return fig, btn_text
