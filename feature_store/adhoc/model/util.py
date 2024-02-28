import os
import sys
import pathlib

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
import lightgbm
import catboost

from oraDB import oraDB
conn, cur = oraDB.connect_CINS_SMY()
print('Connected DB CINS_SMY - conn')


def download_or_reload(saved_file, query, cursor=cur):
    if not os.path.exists(saved_file):
        cursor.execute(query)
        result = cursor.fetchall()
        column_names = [c[0] for c in cursor.description]
        df = pd.DataFrame(result, columns=column_names)
        df.to_pickle(saved_file)
    df = pd.read_pickle(saved_file)
    print(f'Len {len(df)}')
    return df