class modelPred:
    # Predict using the best model
    def predict(model, rpt_dt):

        import pandas as pd
        import joblib
        from oraDB import oraDB
        conn, cur = oraDB.connect()
        import os

        # Load dataset
        print("[Simulate] Load prediction dataset")
        print('Load preditionc pickle file')
        pred = pd.read_pickle('{}_pred_dataset.pkl'.format(rpt_dt), 'gzip')
        print(pred)

        print('Loading model and feature list...')
        # Load model
        print("[Simulate] Load Reactivate classification model ")
        alg = joblib.load('REACTIVE_CLASSIFICATION_{}'.format(model))
        # print(print)

        # Load feature list
        print("[Simulate] Load Feature list of Reactivate model")
        feat_list = joblib.load('feat_list_{}'.format(model))
        print(feat_list)

        print('Predicting for reporting date {}...'.format(rpt_dt))
        # Predict
        print(pred[feat_list])
        y_pred = pd.DataFrame(alg.predict(pred[feat_list]).astype(str), columns=['REACTIVATED_PRED'])
        y_prob = pd.DataFrame(alg.predict_proba(pred[feat_list]), columns=['REACTIVATED_PROB_0', 'REACTIVATED_PROB_1'])
        pred_rslt = pd.concat([pred['CUSTOMER_CDE'], y_pred, y_prob], axis=1)
        pred_rslt['REACTIVATED_PROB'] = pred_rslt.apply(lambda row: row['REACTIVATED_PROB_0'] if row['REACTIVATED_PRED']==0 else row['REACTIVATED_PROB_1'], axis=1)
        pred_rslt = pred_rslt[['CUSTOMER_CDE', 'REACTIVATED_PRED', 'REACTIVATED_PROB']]
        
        # Insert results
        print('[Simulate] Inserting REACTIVATE CLASSIFICATION results for reporting date {}...'.format(rpt_dt))
#         cur.execute("DELETE FROM CINS_MODEL_RSLT WHERE MODEL_NM = 'REACTIVATE CLASSIFICATION' AND RPT_DT = TO_DATE('{}', 'DD-MM-YY') AND PID = {}".format(rpt_dt, model))
#         conn.commit()

#         ins_query = f"""INSERT INTO CINS_MODEL_RSLT (MODEL_NM, PID, CUSTOMER_CDE, RSLT, RPT_DT, CONFIDENCE, ADD_TSTP)
#                 VALUES('REACTIVATE CLASSIFICATION', {model}, :1, :2, TO_DATE('{rpt_dt}', 'DD-MM-YY'), :3, CURRENT_TIMESTAMP)"""
#         cur.executemany(ins_query, [tuple(row) for row in pred_rslt.values])
#         conn.commit()
#         conn.close()
        
        # return print('Finished inserting REACTIVATE CLASSIFICATION results for reporting date {}...'.format(rpt_dt))
        print('[Simulate] Push prediction result into DB')
        print(pred_rslt)
        print('Predict Done')
        # return

#     # Rule-based product recommendation
#     def prod_rec(model, rpt_dt):
        
#         import pandas as pd
#         import joblib
#         from libs.oraDB import oraDB
#         conn, cur = oraDB.connect()
#         import os
        
        # Load dataset
        # pred = pd.read_pickle('/opt/bitnami/jupyterhub-singleuser/Reactivate/files/{}_pred_dataset.pkl'.format(rpt_dt), 'gzip')
        print('Product recommendation')
        print('Getting list of customers to reactivate for reporting date {}...'.format(rpt_dt))
        print('[Simulate] Push prediction result into DB')
        query = "SELECT CUSTOMER_CDE FROM CINS_MODEL_RSLT WHERE MODEL_NM = 'REACTIVATE CLASSIFICATION' AND RSLT = '1' AND PID = {} AND RPT_DT = TO_DATE('{}', 'DD-MM-YY')".format(model, rpt_dt)
        print(f'[Simulate] Run query {query}')
        pred_rslt = pd.read_pickle('pred_rslt.pkl')
        # pred_rslt = pd.read_sql(query, conn)
        # pred_rslt.to_pickle('pred_rslt.pkl')
        pred_rslt = pred_rslt.merge(pred, how='left', on='CUSTOMER_CDE')
        print(pred_rslt)
        
        # Get list of customers to reactivate
        cst_react = pred_rslt[['CUSTOMER_CDE'] + [col for col in pred_rslt.columns if col.endswith('_HOLD') or col.endswith('OTHER_BNK') or col.endswith('OTHER_BANK')]]
        
        
        print('[DEBUG] cst_react')
        print(cst_react)
        cst_react_pd_hold = cst_react.melt(id_vars=['CUSTOMER_CDE'], var_name='PD_HOLD', value_name='HOLD')
        print('[DEBUG] cst_react_pd_hold')
        print(cst_react_pd_hold)
        
        # Get feature list
        print(f'[Simulate] Run query {query} to get feature list')
        query=f"""SELECT FTR_NM, SUB_GRP
        FROM CINS_SMY.CINS_FTR_DIM
        WHERE MODEL_NM = 'REACTIVATE CLASSIFICATION'
        AND ACTIVE = 1
        AND (FTR_NM LIKE '%_HOLD'
        OR FTR_NM LIKE '%_OTHER_BNK'
        OR FTR_NM LIKE '%_OTHER_BANK')"""
        feat_list_df = pd.read_pickle('feat_list_df.pkl')
        # feat_list_df = pd.read_sql(query, conn)
        # feat_list_df.to_pickle('feat_list_df.pkl')
        print('[DEBUG] feat_list_df')
        print(feat_list_df)

        print('Recommending products...')
        # Get product recommendation results
        cst_pd_hold = cst_react_pd_hold.merge(feat_list_df, left_on='PD_HOLD', right_on='FTR_NM').groupby(['CUSTOMER_CDE', 'SUB_GRP']).sum().reset_index()
        cst_pd_hold['SUB_GRP'] = cst_pd_hold['SUB_GRP'].str.replace('_', ' ')
        
        print('[DEBUG] cst_pd_hold')
        print(cst_pd_hold)
        
        pd_rec = cst_pd_hold[cst_pd_hold['HOLD']==0][['CUSTOMER_CDE', 'SUB_GRP']].rename(columns={'SUB_GRP':'PD_REC'}).drop_duplicates().reset_index(drop=True)
        print('[DEBUG] pd_rec')
        print(pd_rec)

        print('[Simulate] Inserting product recommendation results of reporting date {}...'.format(rpt_dt))
        # Insert results
        # cur.execute("DELETE FROM CINS_MODEL_RSLT WHERE MODEL_NM = 'REACTIVATE' AND RPT_DT = TO_DATE('{}', 'DD-MM-YY') AND PID = {}".format(rpt_dt, model))
        # conn.commit()

        # ins_query = f"""INSERT INTO CINS_MODEL_RSLT (MODEL_NM, PID, CUSTOMER_CDE, RSLT, RPT_DT, CONFIDENCE, ADD_TSTP)
        # VALUES('REACTIVATE', {model}, :1, :2, TO_DATE('{rpt_dt}', 'DD-MM-YY'), NULL, CURRENT_TIMESTAMP)"""
        # cur.executemany(ins_query, [tuple(row) for row in pd_rec.values])
        # conn.commit()
        # conn.close()

        return print('---FINISHED RECOMMENDING PRODUCTS TO REACTIVATE INACTIVE CUSTOMERS FOR REPORTING DATE {} WITH MODEL #{}---'.format(rpt_dt, model))
    
    def all_pred(pid, rpt_dt):
        modelPred.predict(pid, rpt_dt)
        # modelPred.prod_rec(pid, rpt_dt)
        return