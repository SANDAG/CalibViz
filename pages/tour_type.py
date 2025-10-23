# pages/tour_type.py
import dash
from dash import html, dcc, Input, Output
import plotly.express as px

from utils.callbacks_common import _get_scenario_data_safe, _keys_for_mode, _empty_fig, _sorted_categories
from utils.constants import BAR_COLOR_SEQUENCE
import data.store as store

dash.register_page(__name__, path="/tour-type-page", name="Disaggregated Tour Type")

layout = html.Div(
    [
        html.H3(id="tour-model-title", style={"display": "inline-block"}),
        html.Br(),
        html.Label("Select Disaggregated Tour Type:"),
        dcc.Dropdown(id='tour-type-dropdown', options=[], value=None, clearable=False, style={'width': '300px', 'margin-bottom': '20px'}),
        html.Button("Show Weighted Person Trips", id='toggle-btn-tour', n_clicks=0, style={'margin-bottom': '20px'}),
        dcc.Graph(id='bar-chart')
    ],
    style={'padding': '20px'}
)

@dash.callback(
    Output("tour-model-title", "children"),
    Output("tour-type-dropdown", "options"),
    Output("tour-type-dropdown", "value"),
    Input("scenario-dd", "value"),
    Input("mode-store", "data"),
)
def refresh_tour_for_scenario(scenario, mode):
    if not scenario:
        raise dash.exceptions.PreventUpdate
    d = _get_scenario_data_safe(scenario, store.santrips_dict)
    df_key, _, _, _, _, mode_label = _keys_for_mode(mode)
    df = d[df_key]

    model_title = f"Model: {store.selected_model} • {mode_label}"
    tour_vals = df['tour_type'].dropna().unique().tolist() if not df.empty else []
    tour_opts = [{'label': t, 'value': t} for t in tour_vals]
    tour_val = 'Total' if 'Total' in tour_vals else (tour_vals[0] if tour_vals else None)
    return model_title, tour_opts, tour_val

@dash.callback(
    Output('bar-chart', 'figure'),
    Output('toggle-btn-tour', 'children'),
    Input('scenario-dd', 'value'),
    Input('tour-type-dropdown', 'value'),
    Input('toggle-btn-tour', 'n_clicks'),
    Input('mode-store', 'data'),
)
def update_bar_chart(scenario, selected_tour_type, n_clicks, mode):
    if not scenario or scenario not in store.santrips_dict or not selected_tour_type:
        return _empty_fig("No data"), "Show Weighted Person Trips"

    df_key, _, _, aggregator_col, cat_order, _ = _keys_for_mode(mode)
    df = _get_scenario_data_safe(scenario, store.santrips_dict)[df_key]
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
        title=f"Model vs Survey {y_label} by {aggregator_col} ({selected_tour_type})"
    )
    return fig, btn_text
