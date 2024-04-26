import os
import sys

# Matplotlib, Seaborn
import seaborn as sns
import matplotlib.pyplot as plt
print(plt.style.available)
plt.style.use('bmh')

# Filter warnings
import warnings
warnings.filterwarnings("ignore")

# Datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Basic & utils
import pandas as pd
import numpy as np
import joblib, json, pickle

# Sklearn * model
from sklearn import metrics 
from sklearn import model_selection
import xgboost


from database_connector import oraDB
conn, cur = oraDB.connect_CINS_SMY()
print('Connected DB CINS_SMY - conn')

conn_aly, cur_aly = oraDB.connect_DW_ANALYTICS()
print('Connected oraDW_ANALYTICS - conn_aly')


def download_or_reload(saved_file, query, update=False):
    if (not os.path.exists(saved_file)) or (update is True):
        cur.execute(query)
        result = cur.fetchall()
        column_names = [c[0] for c in cur.description]
        df = pd.DataFrame(result, columns=column_names)
        df.to_pickle(saved_file)
    df = pd.read_pickle(saved_file)
    print(f'Len {len(df)}')
    return df