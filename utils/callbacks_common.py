from dash.exceptions import PreventUpdate
import plotly.express as px
from utils.constants import X_ORDER

def _empty_fig(title=""):
    return px.bar(title=title)

def _get_scenario_data_safe(scenario, data_dict):
    if not scenario or scenario not in data_dict:
        raise PreventUpdate
    return data_dict[scenario]

def _keys_for_mode(mode):
    if mode == "dest":
        return ("merge_df2", "merge_df_general2", "merge_df_emp2", "origin_pmsa", None, "Destination Choice")
    return ("merge_df", "merge_df_general", "merge_df_emp", "arrival_mode", X_ORDER, "Trip Mode Choice")

def _sorted_categories(df, col):
    cats = df[col].dropna().astype(str).unique().tolist()
    return sorted(cats)
