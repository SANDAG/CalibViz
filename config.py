# config.py
import os
from dotenv import load_dotenv, find_dotenv
from databricks import sql
import pandas as pd

import warnings
warnings.filterwarnings("ignore")

# Utility functions
# Configure Azure Databricks connection
def get_connection():
        return sql.connect(
            server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path = os.getenv("DATABRICKS_HTTP_PATH"),
            access_token = os.getenv("DATABRICKS_TOKEN")
            )

# read table from Azure Databricks
def read_table(query, conn):
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()


# Establish Azure Databricks connection
conn = get_connection()

# Load survey data
def load_survey_data():
     sd1 = read_table(f"""SELECT * FROM read_files('/Volumes/survey/sdia25/calibration/departing_trips_by_mode.csv')""", conn).drop('_rescued_data', axis=1)
     sd1 = sd1.rename(columns={'airport_access_mode':'arrival_mode', 'respondent_type':'primary_purpose', 'inbound_bool':'inbound', 'person_weight':'weight_person_trip'})

     return {
            "santrips": sd1
        }

# Load model data based on environment
def load_model_data(scenario_id, scenario_path, selected_model, env):
    if env == "Local":
        sdia_trip = pd.read_csv(os.path.join(scenario_path, r"output\airport.SAN\final_santrips.csv"))
        sdia_tour = pd.read_csv(os.path.join(scenario_path, r"output\airport.SAN\final_santours.csv"))[['tour_id','tour_type']]
        df1 = sdia_trip.merge(sdia_tour, on='tour_id')[['trip_mode','arrival_mode','tour_type','inbound','weight_person_trip']]
        df1 = df1.query("inbound == True and tour_type != 'external'")  # constrain to inbound trip and non-external trip only due to the lack of outbound trip and external trip in survey data
        
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

        # map model arrival modes to survey modes
        arrival_mode_mapping = {
                                'CURB_LOC1': 'drop_off',
                                'HOTEL_COURTESY': 'shuttle',
                                'KNR_LOC': 'public_transit',
                                'KNR_MIX': 'public_transit',
                                'KNR_PRM': 'public_transit',
                                'PARK_ESCORT': 'park_escort',
                                'PARK_LOC1': 'park_on_site',
                                'PARK_LOC4': 'park_off_site',
                                'PARK_LOC5': 'park_off_site',
                                'RENTAL': 'rental_car',
                                'RIDEHAIL_LOC1': 'tnc',
                                'SHUTTLEVAN': 'shuttle',
                                'TAXI_LOC1': 'taxi',
                                'TNC_LOC': 'public_transit',
                                'TNC_MIX': 'public_transit',
                                'TNC_PRM': 'public_transit',
                                'WALK': 'active_transportation',
                                'WALK_LOC': 'public_transit',
                                'WALK_MIX': 'public_transit',
                                'WALK_PRM': 'public_transit'
                                }

        df1['arrival_mode'] = df1['arrival_mode'].replace(arrival_mode_mapping)

        # change airport trip modes to match them to arrival modes
        df1.loc[df1['arrival_mode']=='taxi', "trip_mode"] = "TAXI"
        df1.loc[(df1['arrival_mode']=='tnc') 
                & (df1['trip_mode']== "SHARED2"), "trip_mode"] = "TNC_SINGLE"
        df1.loc[(df1['arrival_mode']=='tnc') 
                & (df1['trip_mode']== "SHARED3"), "trip_mode"] = "TNC_SHARED"
        
        
        return {
            "santrips": df1,
        }
        
    elif env == "Azure":
        conn = get_connection()
        # df1 = read_table(f"""SELECT * FROM tam.abm3.main__scenario ORDER BY scenario_id DESC""", conn)
        # df2 = read_table(f"""SELECT DISTINCT(model) FROM tam.abm3_reporting.tripcount__by_model""", conn)
        # df3 = read_table(f"""SELECT * FROM tam_dev.calibration.calib__tripcount_by_taz
        #                 WHERE scenario_id in ({scenario_id}) AND model in ('{model}') LIMIT 100""", conn)
        df4 = read_table(f"""SELECT * FROM tam_dev.calibration.calib__tripcount_by_mode_choice
                        WHERE scenario_id in ({scenario_id}) AND model in ('{selected_model}') LIMIT 100""", conn)
        return {
            # "scenario_list": df1,
            # "model_list": df2,
            # "calib__tripcount_by_taz": df3,
            "santrips": df4,
        }