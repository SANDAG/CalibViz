# data_loader.py - load environment, survey, and model data and prepare santrips_dict
import os
from dotenv import load_dotenv, find_dotenv, dotenv_values
from config import load_survey_data, load_model_data

def initialize_data():
    """
    Replicates original environment/config loading and model/survey data load.
    Returns:
        santrips_dict: dict keyed by scenario display name -> dataframes dict
        selected_model: str
    """
    # === Detect App environment and read environment variables ===
    env_vars = dotenv_values()  # returns dict from .env
    for key, value in env_vars.items():
        os.environ.setdefault(key, value)

    dotenv_path = find_dotenv()
    user = None
    if dotenv_path:
        load_dotenv(dotenv_path, override=False)
        user = os.getenv("USER_AGENT_ENTRY")

    env = os.getenv("ENV")
    if env == "Azure":
        # placeholder: Azure path handling if needed
        pass
    elif env == "Local":
        scenario_list_str = os.getenv("SCENARIO_LIST")
        survey = os.getenv("SURVEY_PATH")
        selected_model = os.getenv("SELECTED_MODEL")
    else:
        raise ValueError("Environment variable 'ENV' must be set to either 'Azure' or 'Local'.")

    # === Load survey and model data ===
    survey_data = load_survey_data(user)

    if env == "Azure":
        model_data = {}
    else:
        scenario_list = scenario_list_str.split(",") if scenario_list_str else []
        scenario_dict = {path: {} for path in scenario_list}
        model_data = load_model_data(scenario_dict, selected_model, env, user)

    # NOTE: we will replicate the original processing logic to produce santrips_dict
    # For brevity we call helper functions defined inline below (same logic as original app).
    import pandas as pd
    from copy import deepcopy

    def process_santrips(trip_data, aggregator, emp):
        # logic copied from original app.py
        arrival_mode_to_wsp = {
            "drop_off": "Drop-off/Pick up",
            "shuttle": "Shared Shuttle Van",
            "public_transit": "Public Transportation",
            "park_escort": "Drop-off/Pick up",
            "parked_on_site":"Personal Car Parked",
            "parked_off_site":"Personal Car Parked",
            'parked_employee':"Personal Car Parked",
            'parked_unknown':"Personal Car Parked",
            "rental_car":"Rental Car",
            "tnc":"UBER/Lyft",
            "taxi":"Taxi",
            "active_transportation":"Walk"
        }
        df = trip_data.copy()
        if emp == False:
            df = df.query("tour_type != 'emp'").copy()
        else:
            df = df.query("tour_type == 'emp'").copy()

        df['arrival_mode'] = df['arrival_mode'].replace(arrival_mode_to_wsp)
        df = df.rename(columns={'weight_person_trip': 'trip'})

        trip_by_mode = df.groupby([aggregator,'tour_type'])['trip'].sum().reset_index()
        trip_by_mode['trip_pct'] = trip_by_mode['trip'] / trip_by_mode.groupby('tour_type')['trip'].transform('sum') * 100

        trip_mode_totals = trip_by_mode.groupby(aggregator)['trip'].sum().reset_index()
        trip_mode_totals['tour_type'] = 'Total'
        trip_mode_totals['trip_pct'] = trip_mode_totals['trip'] / trip_mode_totals['trip'].sum() * 100

        if emp == False:
            trip_by_dTour_aggMode = pd.concat([trip_by_mode, trip_mode_totals], ignore_index=True)
        else:
            trip_by_dTour_aggMode = trip_by_mode.copy()

        trip_by_dTour_aggMode['tour_type_general'] = trip_by_dTour_aggMode['tour_type'].apply(
            lambda x: 'resident' if str(x).startswith('res_') else
                      'visitor' if str(x).startswith('vis_') else
                      'employee' if str(x).startswith('emp') else
                      'Total' if str(x) == 'Total' else
                      x
        )

        if aggregator == 'arrival_mode':
            unique_modes = list(set(arrival_mode_to_wsp.values()))
        elif aggregator == 'origin_pmsa':
            unique_modes = list(df['origin_pmsa'].unique())
        all_tour_types = trip_by_dTour_aggMode['tour_type'].unique()
        rows_to_add = []

        for tour_type in all_tour_types:
            existing_modes = set(trip_by_dTour_aggMode.loc[trip_by_dTour_aggMode['tour_type'] == tour_type, aggregator])
            missing_modes = set(unique_modes) - existing_modes
            for mode in missing_modes:
                general_type = trip_by_dTour_aggMode.loc[trip_by_dTour_aggMode['tour_type'] == tour_type, 'tour_type_general'].iloc[0]
                rows_to_add.append({
                    aggregator: mode,
                    'tour_type': tour_type,
                    'trip': 0.0,
                    'trip_pct': 0.0,
                    'tour_type_general': general_type
                })
        if rows_to_add:
            trip_by_dTour_aggMode = pd.concat([trip_by_dTour_aggMode, pd.DataFrame(rows_to_add)], ignore_index=True)

        if emp == False:
            trip_by_geTour_aggMode = trip_by_dTour_aggMode.query("tour_type_general != 'Total'").copy()
            trip_by_geTour_aggMode = trip_by_geTour_aggMode.groupby(['tour_type_general', aggregator])['trip'].sum().reset_index()
            total_trips_by_geTour = trip_by_geTour_aggMode.groupby(['tour_type_general'])['trip'].sum().reset_index()
            trip_by_geTour_aggMode = trip_by_geTour_aggMode.merge(total_trips_by_geTour, on='tour_type_general', suffixes=('_by_mode', '_total'))
            trip_by_geTour_aggMode['trip_pct'] = trip_by_geTour_aggMode['trip_by_mode'] / trip_by_geTour_aggMode['trip_total'] * 100

            trip_geMode_totals = trip_by_dTour_aggMode.query("tour_type == 'Total'").copy()
            trip_geMode_totals = trip_geMode_totals.rename(columns={'trip': 'trip_by_mode'}).drop(['tour_type'], axis=1)
            trip_geMode_totals['trip_total'] = trip_geMode_totals['trip_by_mode'].sum()
            trip_by_geTour_aggMode = pd.concat([trip_by_geTour_aggMode, trip_geMode_totals], ignore_index=True)
            return trip_by_dTour_aggMode, trip_by_geTour_aggMode
        else:
            return trip_by_dTour_aggMode

    def merge_summarized_trip_data(model, survey, aggregator):
        return model.merge(survey, on=aggregator, how='right', suffixes=('_model', '_survey'))

    # Build santrips_dict similar to original
    santrips_dict = {}
    aggregator = 'arrival_mode'
    aggregator2 = 'origin_pmsa'
    emp = False

    # Process survey data
    survey_arrival, ge_survey_arrival = process_santrips(survey_data["santrips"], aggregator, emp)
    survey_emp_arrival = process_santrips(survey_data["santrips"], aggregator, True)
    survey_pmsa, ge_survey_pmsa = process_santrips(survey_data["santrips"], aggregator2, emp)
    survey_emp_pmsa = process_santrips(survey_data["santrips"], aggregator2, True)

    for path, data in model_data.items():
        scenario_name = str(data['metadata']['scenario_id']) + ': ' + data['metadata']['scenario_name']

        model_arrival, ge_model_arrival = process_santrips(data["santrips"], aggregator, emp)
        model_emp_arrival = process_santrips(data["santrips"], aggregator, True)
        merge_df = merge_summarized_trip_data(model_arrival, survey_arrival, ['tour_type', aggregator])
        merge_df_general = merge_summarized_trip_data(ge_model_arrival, ge_survey_arrival, ['tour_type_general', aggregator])
        merge_df_emp = merge_summarized_trip_data(model_emp_arrival, survey_emp_arrival, ['tour_type', aggregator])

        model_pmsa, ge_model_pmsa = process_santrips(data["santrips"], aggregator2, emp)
        model_emp_pmsa = process_santrips(data["santrips"], aggregator2, True)
        merge_df2 = merge_summarized_trip_data(model_pmsa, survey_pmsa, ['tour_type', aggregator2])
        merge_df_general2 = merge_summarized_trip_data(ge_model_pmsa, ge_survey_pmsa, ['tour_type_general', aggregator2])
        merge_df_emp2 = merge_summarized_trip_data(model_emp_pmsa, survey_emp_pmsa, ['tour_type', aggregator2])

        santrips_dict[scenario_name] = {
            "model": "airport.SAN",
            "merge_df": merge_df,
            "merge_df_general": merge_df_general,
            "merge_df_emp": merge_df_emp,
            "merge_df2": merge_df2,
            "merge_df_general2": merge_df_general2,
            "merge_df_emp2": merge_df_emp2
        }

    # Fallback if no model_data found
    if not santrips_dict:
        # create a minimal example (so app won't crash)
        santrips_dict = {"No Data": {"merge_df": pd.DataFrame(), "merge_df_general": pd.DataFrame(), "merge_df_emp": pd.DataFrame(),
                                     "merge_df2": pd.DataFrame(), "merge_df_general2": pd.DataFrame(), "merge_df_emp2": pd.DataFrame()}}
        selected_model = selected_model if 'selected_model' in locals() else "NoModel"
    else:
        selected_model = selected_model if 'selected_model' in locals() else next(iter(santrips_dict.keys()))

    return santrips_dict, selected_model

