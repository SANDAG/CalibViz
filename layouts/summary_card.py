import dash_bootstrap_components as dbc
from dash import html
import pandas as pd


def generate_summary_card(df: pd.DataFrame):
    if df is None or df.empty:
        return dbc.Alert("No data for this scenario.", color="warning", className="mt-2")

    # Aggregate and compute percentages robustly
    df_no_total = df.query("tour_type_general != 'Total'").copy()
    tbl = df_no_total.groupby('tour_type_general')[['trip_by_mode_model', 'trip_by_mode_survey']].sum().reset_index()
    tot_model = float(tbl['trip_by_mode_model'].sum())
    tot_survey = float(tbl['trip_by_mode_survey'].sum())
    tbl['trip_pct_model'] = (tbl['trip_by_mode_model'] / (tot_model if tot_model else 1.0)) * 100.0
    tbl['trip_pct_survey'] = (tbl['trip_by_mode_survey'] / (tot_survey if tot_survey else 1.0)) * 100.0

    # Add total row to tbl
    total_row = {
        'tour_type_general': 'Total',
        'trip_by_mode_model': tbl['trip_by_mode_model'].sum(),
        'trip_by_mode_survey': tbl['trip_by_mode_survey'].sum(),
        'trip_pct_model': tbl['trip_pct_model'].sum(),
        'trip_pct_survey': tbl['trip_pct_survey'].sum()
    }
    tbl = pd.concat([tbl, pd.DataFrame([total_row])], ignore_index=True)

    cell_style = {"padding": "8px 20px", "minWidth": "120px", "textAlign": "right", 'border': '1px solid black'}

    rows = []
    for _, row in tbl.iterrows():
        rows.append(
            html.Tr(
                [
                    html.Td(row['tour_type_general'], style=cell_style),
                    html.Td(f"{row['trip_by_mode_survey']:.1f}", style=cell_style),
                    html.Td(f"{row['trip_by_mode_model']:.1f}", style=cell_style),
                    html.Td(f"{row['trip_pct_survey']:.2f}%", style=cell_style),
                    html.Td(f"{row['trip_pct_model']:.2f}%", style=cell_style),
                ]
            )
        )

    table = dbc.Table(
        [
            html.Thead(
                html.Tr(
                    [
                        html.Th("Tour Type", style=cell_style),
                        html.Th("Survey Weighted Trips", style=cell_style),
                        html.Th("Model Weighted Trips", style=cell_style),
                        html.Th("Survey Trip %", style=cell_style),
                        html.Th("Model Trip %", style=cell_style),
                    ]
                )
            ),
            html.Tbody(rows),
        ],
        bordered=True, striped=True, hover=True, size="sm",
        style={'border': '1px solid black', 'borderCollapse': 'collapse'}
    )

    note = html.P("Only weighted person trips are included in this visualizer", style={"marginTop": "10px", "fontStyle": "italic"})

    return dbc.Card(
        [
            dbc.CardHeader("Trip Summary by Departing Air Passenger Market", style={"fontWeight": "bold"}),
            dbc.CardBody(
                [
                    table,
                    note
                ]
            )
        ],
        style={"margin-bottom": "20px"}
    )