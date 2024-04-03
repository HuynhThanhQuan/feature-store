import pathlib
import os
import pickle

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier


import logging
logger = logging.getLogger(__name__)


def evaluate_metric(y, y_pred, y_prob):
    p,r,f,_ = metrics.precision_recall_fscore_support(y, y_pred)
    auc = metrics.roc_auc_score(y, y_prob[:,1])
    gini = 2 * auc - 1
    logger.info(f'Precision {p}, Recall: {r}, F1-score: {f} ')
    logger.info(f'AUC: {auc}, Gini: {gini}')
    logger.info(f'\n{metrics.classification_report(y, y_pred)}')


def evaluate(model, X, y, eval_metric=True):
    y_pred = model.predict(X)
    y_prob = model.predict_proba(X)
    if eval_metric:
        evaluate_metric(y, y_pred, y_prob)

    
class Trainer:
    def __init__(self, data_dict, ml_config):
        self.data_dict = data_dict
        self.model_path = ml_config['model_path']
        self.model = ml_config['model']
        self.init()
        
    def init(self):
        logger.debug('Init training model params')
        assert 'train' in self.data_dict.keys(), 'Not found train dataset '
        
        num1feat_cols = [
            'CASA_HOLD',
            ]
        num2feat_cols = [
            'AGE',
            'LOR',
            'CREDIT_SCORE',
            'CASA_BAL_SUM_NOW',
            'CASA_BAL_SUM_36M',
            'CASA_BAL_SUM_24M',
            'CASA_BAL_SUM_12M',
            'CASA_BAL_MAX_12M',
            'CASA_TXN_AMT_SUM_36M',
            'CASA_TXN_AMT_SUM_24M',
            'CASA_TXN_AMT_SUM_12M',
            # 'CASA_TXN_CT_36M',
            # 'CASA_TXN_CT_24M',
            'CASA_TXN_CT_12M',
            'CASA_ACCT_CT_36M',
            'CASA_ACCT_ACTIVE_CT_12M',
            'CASA_DAY_SINCE_LAST_TXN_CT_36M',
            ]

        catfeat_cols = ['AREA','PROFESSION','GEN_GRP','LIFE_STG']
        
        # Pipeline + Model
        num1_transformer = Pipeline(steps=[('imputer', SimpleImputer(strategy='constant', fill_value=0))])
        num2_transformer = Pipeline(steps=[('imputer', SimpleImputer(strategy='median')), ('scaler', StandardScaler())])
        categorical_transformer = Pipeline(steps=[('encoder', OneHotEncoder(handle_unknown='ignore'))])
        transformer = ColumnTransformer(
            transformers=[
                ('num1', num1_transformer, num1feat_cols),
                ('num2', num2_transformer, num2feat_cols),
                ('cat', categorical_transformer, catfeat_cols)]
        )
        
        # Init model
        if self.model == 'XGBoost':
            model = XGBClassifier()
        elif self.model == 'RandomForest':
            model = RandomForestClassifier()
        elif self.model == 'LogisticRegression':
            model = LogisticRegression()
        else:
            raise Exception(f'Unrecognize model {self.model}')
            
        self.pipeline = Pipeline([
            ("transform", transformer),
            ("model", model)
        ])
        logger.debug('Created training pipeline')

        
    def train(self):
        X_train, y_train = self.data_dict['train']
        X_valid, y_valid = self.data_dict['valid']
        X_test, y_test = self.data_dict['backtest']
        
        # Train
        logger.info('Training...')
        self.pipeline.fit(X_train, y_train)
        
        # Dump model
        pickle.dump(self.pipeline, open(self.model_path,'wb'))
        logger.debug(f'Dumped model at {self.model_path}')
        
        # Evaluate
        logger.info('Evaluate train metrics')
        evaluate(self.pipeline, X_train, y_train)
        logger.info('Evaluate valid metrics')
        evaluate(self.pipeline, X_valid, y_valid)
        logger.info('Evaluate test metrics')
        evaluate(self.pipeline, X_test, y_test)
        logger.info('Training finished')


class Predictor:
    def __init__(self,model_path):
        self.model_path = model_path
        self.model = pickle.load(open(self.model_path,'rb'))
        
    def score(self, X):
        logger.info('Scoring...')
        score = self.model.predict_proba(X)
        return score
    
    def evaluate(self, X, y):
        logger.info('Evaluating')
        evaluate(self.model, X, y)