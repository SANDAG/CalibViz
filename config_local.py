# config_local.py

import os
import pandas as pd

def load_data(model, survey):
    df1 = pd.read_csv(os.path.join(model, r"output\airport.SAN\final_santrips.csv"))
    df2 = pd.read_csv(os.path.join(survey, r"output\airport.SAN\final_santrips.csv"))   # use other model data as a dummy survey data

    return {
        "final_santrips": df1,
        "survey_santrips": df2
    }