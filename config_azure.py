# config.py
import os
from dotenv import load_dotenv, find_dotenv
from databricks import sql

# === Connection setup ===
# Only load if the .env file is present
dotenv_path = find_dotenv()
if dotenv_path:
    load_dotenv(dotenv_path)

def get_connection():
    return sql.connect(
        server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path = os.getenv("DATABRICKS_HTTP_PATH"),
        access_token = os.getenv("DATABRICKS_TOKEN")
        )

def read_table(query, conn):
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()


#=== Main function ===
def load_data(scenario_id, model):
    conn = get_connection()
    df1 = read_table(f"""SELECT * FROM tam.abm3.main__scenario ORDER BY scenario_id DESC""", conn)
    df2 = read_table(f"""SELECT DISTINCT(model) FROM tam.abm3_reporting.tripcount__by_model""", conn)
    df3 = read_table(f"""SELECT * FROM tam_dev.calibration.calib__tripcount_by_taz
                     WHERE scenario_id in ({scenario_id}) AND model in ('{model}') LIMIT 100""", conn)
    df4 = read_table(f"""SELECT * FROM tam_dev.calibration.calib__tripcount_by_mode_choice
                     WHERE scenario_id in ({scenario_id}) AND model in ('{model}') LIMIT 100""", conn)
    return {
        "scenario_list": df1,
        "model_list": df2,
        "calib__tripcount_by_taz": df3,
        "calib__tripcount_by_mode_choice": df4,
    }