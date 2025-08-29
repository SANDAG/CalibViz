import os
import yaml
import pandas as pd
from pathlib import Path
from databricks import sql
from dotenv import load_dotenv

import warnings
warnings.filterwarnings("ignore")

# === Utility functions ===
# Configure Azure Databricks connection
def get_connection():
        return sql.connect(
            server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path = os.getenv("DATABRICKS_HTTP_PATH"),
            access_token = os.getenv("DATABRICKS_TOKEN")
            )

# Read table from Azure Databricks
def read_table(query, conn):
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

# Read scenario metadata
def read_metadata(scenario_path):
    meta_path = os.path.join(scenario_path, r"output\datalake_metadata.yaml")
    scenario_name = os.path.basename(scenario_path)
    if not Path(meta_path).exists():
        print(f"⚠️ Metadata file missing in {scenario_path}, assigning default scenario_id=999 and name='{scenario_name}'")
        return {
            "scenario_id": 999,
            "scenario_name": scenario_name,
            "scenario_yr": 2022
        }
    else:
        with open(meta_path, "r") as f:
            meta = yaml.safe_load(f)
    return {
        "scenario_id": int(meta.get("scenario_id")),
        "scenario_name": meta.get("scenario_title"),
        "scenario_yr": int(meta.get("scenario_year"))
    }

    
# === Load data ===
# Survey data
def load_survey_data():
     conn = get_connection()
     sd1 = read_table(f"""SELECT * FROM read_files('/Volumes/survey/sdia25/calibration/departing_trips_by_mode.csv')""", conn).drop('_rescued_data', axis=1)
     sd1 = sd1.rename(columns={'airport_access_mode':'arrival_mode', 'respondent_type':'primary_purpose', 'inbound_bool':'inbound', 'person_trips':'weight_person_trip','origin_pmsa_label':'opmsa'})
     
     return {
            "santrips": sd1,
        }

# Model data
def load_model_data(scenario_dict, selected_model, env):
    # load geo crosswalk
    conn = get_connection()
    mgra2pmsa_xref = read_table(f"""SELECT * FROM tam.geo.mgra15_taz15_pmsa_xref""", conn).rename(columns={'MGRA':'mgra','TAZ':'taz','PSEUDOMSA':'origin_pmsa'})

    if env == "Local":
        for scenario_path in scenario_dict.keys():
            print(f"Loading data from scenario: {scenario_path}")
            
            # load scenario metadata
            scenario_meta = read_metadata(scenario_path)

            # load model data and get trip tour type and origin pmsa
            sdia_trip = pd.read_csv(os.path.join(scenario_path, r"output\airport.SAN\final_santrips.csv")).rename(columns={'origin':'origin_mgra'})
            sdia_tour = pd.read_csv(os.path.join(scenario_path, r"output\airport.SAN\final_santours.csv"))[['tour_id','tour_type']]
            sdia_trip = sdia_trip.merge(mgra2pmsa_xref, left_on='origin_mgra', right_on='mgra', how='left')
            df1 = sdia_trip.merge(sdia_tour, on='tour_id')[['origin_mgra','origin_pmsa','trip_mode','arrival_mode','tour_type','inbound','weight_person_trip']]
            df1 = df1.query("inbound == True and tour_type != 'external'")  # constrain to inbound and non-external trips only, given the absence of outbound and external trips in the survey data

            # map model tour types to survey types
            tour_types_mapping = {
                                    'vis_per':'vis_nb',
                                    'vis_bus':'vis_bus',
                                    'emp':'emp',
                                    'res_per1':'res_nb',
                                    'res_per2':'res_nb',
                                    'res_per3':'res_nb',
                                    'res_per4':'res_nb',
                                    'res_per5':'res_nb',
                                    'res_per6':'res_nb',
                                    'res_per7':'res_nb',
                                    'res_per8':'res_nb',
                                    'res_bus1':'res_bus',
                                    'res_bus2':'res_bus',
                                    'res_bus3':'res_bus',
                                    'res_bus4':'res_bus',
                                    'res_bus5':'res_bus',
                                    'res_bus6':'res_bus',
                                    'res_bus7':'res_bus',
                                    'res_bus8':'res_bus'
                                }
            df1['tour_type'] = df1['tour_type'].replace(tour_types_mapping)

            # match model airport trip modes to arrival modes
            df1.loc[df1['arrival_mode']=='TAXI_LOC1', "trip_mode"] = "TAXI"
            df1.loc[(df1['arrival_mode']=='RIDEHAIL_LOC1') 
                    & (df1['trip_mode']== "SHARED2"), "trip_mode"] = "TNC_SINGLE"
            df1.loc[(df1['arrival_mode']=='RIDEHAIL_LOC1') 
                    & (df1['trip_mode']== "SHARED3"), "trip_mode"] = "TNC_SHARED"

            # map model arrival modes to survey modes
            arrival_mode_mapping = {
                                    'CURB_LOC1': 'drop_off',
                                    'HOTEL_COURTESY': 'shuttle',
                                    'KNR_LOC': 'public_transit',
                                    'KNR_MIX': 'public_transit',
                                    'KNR_PRM': 'public_transit',
                                    'PARK_ESCORT': 'drop_off',
                                    'PARK_LOC1': 'parked_on_site',
                                    'PARK_LOC4': 'parked_off_site',
                                    'PARK_LOC5': 'parked_off_site',
                                    'RENTAL': 'rental_car',
                                    'TAXI_LOC1':'taxi',
                                    'RIDEHAIL_LOC1':'tnc',
                                    'SHUTTLEVAN': 'shuttle',
                                    'TNC_LOC': 'public_transit',
                                    'TNC_MIX': 'public_transit',
                                    'TNC_PRM': 'public_transit',
                                    'WALK': 'active_transportation',
                                    'WALK_LOC': 'public_transit',
                                    'WALK_MIX': 'public_transit',
                                    'WALK_PRM': 'public_transit'
                                    }
            df1['arrival_mode'] = df1['arrival_mode'].replace(arrival_mode_mapping)

            # update scenario dictionary with metadata and loaded data
            scenario_dict[scenario_path]['metadata'] = scenario_meta
            scenario_dict[scenario_path]['santrips'] = df1

            print(f"Available data tables: {list(scenario_dict[scenario_path].keys())}")

        return scenario_dict
        
    elif env == "Azure":
        scenario_id = scenario_meta['scenario_id']
        # df1 = read_table(f"""SELECT * FROM tam.abm3.main__scenario ORDER BY scenario_id DESC""", conn)
        # df2 = read_table(f"""SELECT DISTINCT(model) FROM tam.abm3_reporting.tripcount__by_model""", conn)
        # df3 = read_table(f"""SELECT * FROM tam_dev.calibration.calib__tripcount_by_taz
        #                 WHERE scenario_id in ({scenario_id}) AND model in ('{model}') LIMIT 100""", conn)
        df4 = read_table(f"""SELECT * FROM tam_dev.calibration.calib__tripcount_by_mode_choice
                        WHERE scenario_id in ({scenario_id}) AND model in ('{selected_model}') LIMIT 100""", conn)
        return {
            "santrips": df4,
        }