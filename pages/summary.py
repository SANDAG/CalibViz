# pages/summary.py
import dash
from dash import html, dcc, Input, Output
import plotly.express as px

from layouts.summary_card import generate_summary_card
from utils.callbacks_common import _get_scenario_data_safe, _keys_for_mode, _empty_fig, _sorted_categories
from utils.constants import BAR_COLOR_SEQUENCE
import data.store as store

dash.register_page(__name__, path="/", name="Aggregated Tour Type")

layout = html.Div(
    [
        html.H3(id="summary-model-title", style={"display": "inline-block"}),
        html.Div(id="summary-card", style={"marginBottom": "20px"}),
        html.Label("Select Aggregated Tour Type:"),
        dcc.Dropdown(id='general-tour-type-dropdown', options=[], value=None, clearable=False, style={'width': '300px', 'margin-bottom': '20px'}),
        html.Button("Show Weighted Person Trips", id='toggle-btn', n_clicks=0, style={'margin-bottom': '20px'}),
        dcc.Graph(id='general-bar-chart')
    ],
    style={'padding': '20px'}
)

# Page callbacks
@dash.callback(
    Output("summary-model-title", "children"),
    Output("summary-card", "children"),
    Output("general-tour-type-dropdown", "options"),
    Output("general-tour-type-dropdown", "value"),
    Input("scenario-dd", "value"),
    Input("mode-store", "data"),
)
def refresh_summary_for_scenario(scenario, mode):
    if not scenario:
        raise dash.exceptions.PreventUpdate
    d = _get_scenario_data_safe(scenario, store.santrips_dict)
    _, df_general_key, _, aggregator_col, _, mode_label = _keys_for_mode(mode)
    df_general = d[df_general_key]

    model_title = f"Model: {store.selected_model} • {mode_label}"
    summary_card = generate_summary_card(df_general)

    gen_vals = df_general['tour_type_general'].dropna().unique().tolist() if not df_general.empty else []
    gen_opts = [{'label': t, 'value': t} for t in gen_vals]
    gen_val = 'Total' if 'Total' in gen_vals else (gen_vals[0] if gen_vals else None)
    return model_title, summary_card, gen_opts, gen_val

@dash.callback(
    Output('general-bar-chart', 'figure'),
    Output('toggle-btn', 'children'),
    Input('scenario-dd', 'value'),
    Input('general-tour-type-dropdown', 'value'),
    Input('toggle-btn', 'n_clicks'),
    Input('mode-store', 'data'),
)
def update_general_bar_chart(scenario, selected_general_tour_type, n_clicks, mode):
    if not scenario or scenario not in store.santrips_dict or not selected_general_tour_type:
        return _empty_fig("No data"), "Show Weighted Person Trips"

    df_general_key = _keys_for_mode(mode)[1]
    aggregator_col = _keys_for_mode(mode)[3]
    cat_order = _keys_for_mode(mode)[4]

    df = _get_scenario_data_safe(scenario, store.santrips_dict)[df_general_key]
    filtered = df[df['tour_type_general'] == selected_general_tour_type]
    if filtered.empty:
        btn = "Show Percentage of Trips" if (n_clicks % 2 == 1) else "Show Weighted Person Trips"
        return _empty_fig("No data"), btn

    show_weighted = (n_clicks % 2 == 1)
    if show_weighted:
        value_vars = ["trip_by_mode_survey", "trip_by_mode_model"]; y_label = "Person Trips"; btn_text = "Show Percentage of Trips"
    else:
        value_vars = ["trip_pct_survey", "trip_pct_model"]; y_label = "Percentage of Trips"; btn_text = "Show Weighted Person Trips"

    orders = {aggregator_col: (cat_order if cat_order else _sorted_categories(filtered, aggregator_col))}

    fig = px.bar(
        filtered.melt(id_vars=aggregator_col, value_vars=value_vars, var_name="Source", value_name=y_label),
        x=aggregator_col, y=y_label, color="Source", barmode="group",
        category_orders=orders, color_discrete_sequence=BAR_COLOR_SEQUENCE,
        title=f"Model vs Survey {y_label} by {aggregator_col} ({selected_general_tour_type})"
    )
    return fig, btn_text
