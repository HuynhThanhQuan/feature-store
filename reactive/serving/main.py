import os
import sys
import pathlib
import pandas as pd
import numpy as np
import pickle
import prepare_data
import preprocessing


if __name__ == "__main__":
    report_date = '01-09-2023'
    X = prepare_data.get_data(report_date)
    cust_id = X.index.tolist()
    X = preprocessing.transform(X)
    model = pickle.load(open('./model/model','rb'))
    score = model.predict_proba(X)
    score_df = pd.DataFrame({'score': score}, index=cust_id)
    score_df =  score_df.sort_values(by='score')
    score_df.to_parquet(f'./out/SCORE_{report_date}')
    