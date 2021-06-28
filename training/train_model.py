# -*- coding: utf-8 -*-
"""
Created on Tue May 11 10:26:07 2021

@author: mayur
"""


import pandas as pd 
import warnings
from imblearn.under_sampling import RandomUnderSampler
from catboost import CatBoostClassifier, Pool


if __name__ == '__main__':
    pd.options.mode.chained_assignment = None
    warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
    
    Data = pd.read_pickle('../data/datasets/12mo_5min.pkl')  
    if 'repeat_c' in Data.columns:
        Data = Data.drop(['repeat_c'],axis=1)

    actrain = Data[Data.day<20]
    actest = Data[Data.day>=20]
    actrain = actrain.drop(['day'],axis=1)
    actest = actest.drop(['day'],axis=1)
    
    X_train = actrain.iloc[:,:-1]
    y_train = actrain.iloc[:,-1]
    X_test = actest.iloc[:,:-1]
    y_test = actest.iloc[:,-1]
    
    ros = RandomUnderSampler(random_state=0)
    X_resampled, y_resampled = ros.fit_resample(X_train, y_train) 
    X_resamptes, y_resamptes = ros.fit_resample(X_test, y_test) 
    
    dts_resamp = Pool(X_resampled, y_resampled,cat_features=['subtype','roadtype', 'type_y'])
    dataset =  Pool(X_train, y_train,cat_features=['subtype','roadtype', 'type_y'])
    evaldataset = Pool(X_test,y_test,cat_features=['subtype','roadtype', 'type_y'])
    
    model = CatBoostClassifier(
        loss_function = 'Logloss',
        iterations=500,
        eval_metric='AUC',
        custom_metric=['Precision','AUC'],
        random_seed=63,
        #learning_rate=0.03,
        #scale_pos_weight=1.5,
        #l2_leaf_reg=3,
        #bagging_temperature=1,
        #random_strength=1,
        #one_hot_max_size=2,
        #leaf_estimation_method='Newton',
        verbose=50) 
    model.fit(dts_resamp,
              eval_set=evaldataset,
              plot=True)
    
    model.save_model('../data/waze_prob_model.cbm')




