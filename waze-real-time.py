# -*- coding: utf-8 -*-
"""
Created on Tue Jun  1 16:03:44 2021

@author: mayur
"""

import requests
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
from datetime import timedelta
import time
import warnings
from catboost import CatBoostClassifier

# client ID de acesso à API
client_id = 'Fundacao+Getulio+Vargas'

# polígonos
WAZE_POLYGONS = {
    'Sao Paulo 1': {
        'city': 'São Paulo',
        'points': '-46.89388,-23.65469;-46.8972,-23.79037;-46.32591,-23.791;-46.32865,-23.64431;-46.89388,-23.65469',
    },
    'Sao Paulo 2': {
        'city': 'São Paulo',
        'points': '-46.9344,-23.4539;-46.91986,-23.55706;-46.27304,-23.52685;-46.30119,-23.35991;-46.9344,-23.4539',
    },
    'Sao Paulo 3': {
        'city': 'São Paulo',
        'points': '-46.2656,-23.52821;-46.92789,-23.5558;-46.90063,-23.65647;-46.31814,-23.64391;-46.2656,-23.52821',
    }
}

cities = ['Sao Paulo 1', 'Sao Paulo 2', 'Sao Paulo 3']


def waze_download(polygon_path):
    
    with open(polygon_path, 'r') as f:
        poly_data = json.load(f)
        
    responses = []
    for i in cities:
        #if d['city_name'].startswith('Sao Paulo'):# or d['city_name'] in ['Xalapa','Quito','Lima','Montivideo']:

        polygon_city = WAZE_POLYGONS[i]['points']
        url = 'https://world-georss.waze.com/rtserver/web/TGeoRSS?format=JSON&types=traffic&tk=ccp_partner&ccp_partner_name={}&polygon={}'.format(
            client_id, polygon_city)
        try:
            response = requests.get(url)
            responses.append(response.json())
        except Exception as e:
            # responses[poly_id] = None
            # capture_exception(e)
            print(e)

    alerts = []
    jams = []
    for data in responses:
        time_pub = data['endTime'][:-4]
        if 'alerts' in data.keys():
            alerts = alerts + data['alerts']
        if 'jams' in data.keys():
            jams = jams + data['jams']

    #import pdb; pdb.set_trace()

    return alerts, jams, time_pub
    
def process_alerts(alerts_list,pub):
    
    dfa = pd.DataFrame(alerts_list)
    dfa = dfa[dfa.city.isin(['São Paulo'])]#,'Xalapa','Quito','Montevideo','Miraflores'])]
    dfa['pub_utc_date'] = pub
    dfa = dfa[dfa.type=='ACCIDENT'] 
    dfa['geometry'] = gpd.GeoSeries(dfa['location'].apply(
                    lambda coord: Point((coord['x'],coord['y']))), crs='EPSG:4326').to_crs('EPSG:22523')
    dfa['longitude'] = dfa['location'].apply(lambda coord: coord['x'])
    dfa['latitude'] = dfa['location'].apply(lambda coord: coord['y'])
    
    dfa = dfa.drop(['location'],axis=1)
    dfa  = dfa.drop_duplicates()
    dfa = dfa.drop_duplicates(subset=['pub_utc_date','uuid'],keep='first')
    
    dfa = dfa.reset_index().set_index(['uuid'])
    dfa  =  dfa.sort_index()
    alerts_uuids  = dfa.index.unique()
    
    return dfa, alerts_uuids
    
    
def process_jams(jams_list,dfjtotal,pub):
    dfj = pd.DataFrame(jams_list)
    dfj = dfj[dfj.city.isin(['São Paulo'])]#,'Xalapa','Quito','Montevideo','Miraflores'])]
    dfj['pub_utc_date'] = pub
    
    dfj['geometry'] = gpd.GeoSeries(dfj.line.apply(
                    lambda x: LineString([(coord['x'],coord['y']) for coord in x])), crs='EPSG:4326').to_crs('EPSG:22523')
    
    dfj = dfj.drop(['line','segments'],axis=1)
    dfj = dfj.drop_duplicates()
    dfj = dfj.drop_duplicates(subset=['pub_utc_date','uuid'],keep='first')
    dfj['street'] = dfj.street.apply(str)
    
    dfjtotal = pd.concat([dfjtotal,dfj])
        
    #keeps only last 60 minutes of jam data in memory
    if len(dfjtotal.pub_utc_date.unique())>60:
        older_date = sorted(dfjtotal.pub_utc_date.unique().tolist())[0]
        dfjtotal = dfjtotal[dfjtotal.pub_utc_date != older_date] 
        
    return dfjtotal

def search_acc_jams(alerts_uuids,dfa,jams_ix):
    
    jam_acc = []
    # t=len(alerts_uuids)
    # i=0
    for au in alerts_uuids: 
    
        acid = dfa.loc[[au]]
        acid = acid.reset_index().set_index(['pub_utc_date', 'street'])
        # print(i,'/',t,au,len(acid),time.ctime())    
        # i+=1
        try:
            jrows = jams_ix.loc[jams_ix.index.intersection(acid.index)]
            jrows['d'] = jrows.geometry.apply(lambda x: acid.iloc[0].geometry.distance(x))
            jrows = jrows[jrows.d<=200]
        except:
            jrows =  []
        if len(jrows)>0:
            jrows =  jrows[jrows.d==jrows.d.min()]
            
            jrows = jrows.reset_index().drop_duplicates(subset=['pub_utc_date']).set_index(['pub_utc_date','street'])
            
            acid.loc[jrows.index,'juuid']  = jrows.uuid.apply(int)
            jam_acc.extend(acid.reset_index().loc[:,['pub_utc_date','uuid','juuid']].values.tolist())

    return jam_acc

def merge_acc_jams(dfa,jams_ix,jam_acc):
    # print(' preprocessing merge',time.ctime())
    accids2= dfa.reset_index().set_index(['pub_utc_date', 'uuid'])
    accids2= accids2.sort_index()
    jams_ix2 = jams_ix.reset_index().set_index(['pub_utc_date', 'uuid'])
    jams_ix2= jams_ix2.sort_index()

    jadf = pd.DataFrame(jam_acc)
    jadfnn = jadf.dropna()
    
    # print(' merging data',time.ctime())
    joined =[]
    if len(jadfnn)>0:
        for i,j in enumerate(jadfnn[1].unique()):
            # if i%100==0:
            #     print(i,time.ctime())
            if len(jadf)>0:
                keys = jadf[jadf[1]==j]
                aa = accids2.loc[list(keys[[0,1]].itertuples(index=False,name=None))]
                
                keys2=jams_ix2.index.intersection(list(keys[[0,2]].itertuples(index=False,name=None)))
                ja = jams_ix2.loc[keys2]
                ja = ja.reset_index().drop_duplicates(subset=['pub_utc_date']).set_index(['pub_utc_date','uuid'])
                
                result = pd.merge(aa.reset_index(), ja.reset_index(), on=["pub_utc_date", "street"]).set_index('index')
                joined.append(result)
    
    if len(joined)>0:
        dfjoined =pd.concat(joined) 
        dfjoined = dfjoined[~dfjoined.index.duplicated(keep='first')]
        notfoundf =  accids2[~accids2['index'].isin(dfjoined.index)]
        notfoundf = notfoundf.reset_index().set_index('index')
        notfoundf.rename(columns={'uuid':'uuid_x','type':'type_x','country':'country_x','city':'city_x','roadType':'roadType_x','pubMillis':'pubMillis_x','geometry':'geometry_x'}, inplace=True)
        all_df = pd.concat([dfjoined,notfoundf])
        all_df = all_df.sort_values('pub_utc_date')
    else:
        notfoundf =  accids2  
        notfoundf = notfoundf.reset_index().set_index('index')
        notfoundf.rename(columns={'uuid':'uuid_x','type':'type_x','country':'country_x','city':'city_x','roadType':'roadType_x','pubMillis':'pubMillis_x','geometry':'geometry_x'}, inplace=True)
        all_df = notfoundf
        all_df = all_df.sort_values('pub_utc_date')
        all_df['length'] = 0
        all_df['level'] = 0
        all_df['delay'] = 0
        all_df['type_y'] = 'NONE'
        
    return joined, all_df
    
def process_extra_features(all_df,jams_ix):
    
    all_first = all_df.loc[all_df.uuid_x.drop_duplicates(keep='first').index]
    all_first.length.fillna(0,inplace=True)
    all_first.level.fillna(0,inplace=True)
    all_first.delay.fillna(0,inplace=True)
    all_first['pub_utc_date0'] = pd.to_datetime(all_first['pub_utc_date'])
    all_first['pub_utc_date1'] =all_first.pub_utc_date0 - timedelta(minutes=1)
    all_first['pub_utc_date1'] = all_first['pub_utc_date1'].apply(lambda x: str(x))#+'.000')
    
    all_first['pub_utc_date5'] =all_first.pub_utc_date0 - timedelta(minutes=5)
    all_first['pub_utc_date5'] = all_first['pub_utc_date5'].apply(lambda x: str(x))#+'.000')
    
    all_first['pub_utc_date10'] =all_first.pub_utc_date0 - timedelta(minutes=10)
    all_first['pub_utc_date10'] = all_first['pub_utc_date10'].apply(lambda x: str(x))#+'.000')
    
    all_first['pub_utc_date20'] =all_first.pub_utc_date0 - timedelta(minutes=20)
    all_first['pub_utc_date20'] = all_first['pub_utc_date20'].apply(lambda x: str(x))#+'.000')
    
    all_first['pub_utc_date30'] =all_first.pub_utc_date0 - timedelta(minutes=30)
    all_first['pub_utc_date30'] = all_first['pub_utc_date30'].apply(lambda x: str(x))#+'.000')
    
    for dt  in ['1','5','10','20','30']:
        # ifo=0
        # inofo=0
        for row in all_first.iterrows():
            # break
            try:
                zzz = jams_ix.loc[(row[1]['pub_utc_date'+dt],row[1].street)]
                zzz['d'] = zzz.geometry.apply(lambda x: x.distance(row[1].geometry_x))
                zzz = zzz[zzz.d<=200]
                zzz =  zzz[zzz.d==zzz.d.min()]
                
                all_first.loc[row[0],'len'+dt] = row[1].length - zzz.length[0]
                all_first.loc[row[0],'lev'+dt] = row[1].level - zzz.level[0]
                all_first.loc[row[0],'del'+dt] = row[1].delay - zzz.delay[0]
                # ifo+=1
            except:
                all_first.loc[row[0],'len'+dt] = row[1].length - 0
                all_first.loc[row[0],'lev'+dt] = row[1].level - 0
                all_first.loc[row[0],'del'+dt] = row[1].delay - 0
    return all_first

def merge_extra_features(alerts_uuids,all_first,all_df,dfatotal):
    for au in alerts_uuids:
        
        first = all_first[all_first.uuid_x==au]
        
        cols = ['len1', 'lev1', 'del1', 'len5', 'lev5', 'del5', 'len10', 'lev10',
               'del10', 'len20','lev20', 'del20', 'len30', 'lev30', 'del30']
        zzz=all_df[all_df.uuid_x==first.uuid_x.iloc[0]]
        
        
        firstrep = first.loc[first.index.repeat(len(zzz)),cols]
        firstrep.index =  zzz.index 
        all_df.loc[firstrep.index,cols] = firstrep.loc[:,cols]
    
    for k in all_df.uuid_x.unique():
        # break
        temp = all_df[all_df.uuid_x==k]
        
        if temp.length.isnull().all():
            temp['length'] = 0
            temp['level'] = 0
            temp['delay'] = 0    
            temp['type_y'] = 'NONE'
        elif temp.length.isnull().any():
            # if i>5:
            #     break
            # i+=1
            temp['c'] = temp.length.mask(temp.length.ffill().notnull(),temp.length.interpolate(limit_area='inside'))
            temp['level'] = temp.level.mask(temp.level.ffill().notnull(),temp.level.interpolate(limit_area='inside'))
            temp['delay'] = temp.delay.mask(temp.delay.ffill().notnull(),temp.delay.interpolate(limit_area='inside'))
            temp['type_y'] = temp.type_y.mask(temp.type_y.ffill().notnull(),temp.type_y.bfill())
            
            temp['length'].fillna(0,inplace=True)
            temp['level'].fillna(0,inplace=True)
            temp['delay'].fillna(0,inplace=True)    
            temp['type_y'].fillna('NONE',inplace=True)    
        

        all_df.loc[temp.index,'length'] = temp['length']  
        all_df.loc[temp.index,'level'] = temp['level']  
        all_df.loc[temp.index,'delay'] = temp['delay']  
        all_df.loc[temp.index,'type_y'] = temp['type_y']
    #shows alerts from last 10 minutes
    dfatotal = pd.concat([dfatotal,all_df])
    if len(dfatotal.pub_utc_date.unique())>10:
        older_date = sorted(dfatotal.pub_utc_date.unique().tolist())[0]
        dfatotal = dfatotal[dfatotal.pub_utc_date != older_date] 
    
    all_df = dfatotal.drop_duplicates('uuid_x',keep='last').reset_index(drop=True)
    
    return all_df,dfatotal

def clean_and_predict_proba(all_df,model):
    #remove unwanted columns
    
    
    if len(joined)>0:
        x_predict = all_df.drop(['pub_utc_date','uuid_x','country_x','city_x','type_x',
                                 'magvar','street','pubMillis_x','reportDescription',
                                 'geometry_x','longitude','latitude','uuid_y','country_y',
                                 'city_y','speedKMH','turnType','endNode','speed',
                                 'roadType_y','pubMillis_y','blockingAlertUuid','startNode',
                                 'geometry_y'],axis=1)
    else:
        x_predict = all_df.drop(['pub_utc_date','uuid_x','country_x','city_x','type_x',
                                 'magvar','street','pubMillis_x','reportDescription',
                                 'geometry_x','longitude','latitude'],axis=1)
    
    x_predict.rename(columns={'nThumbsUp':'nthumbsup','reportRating':'reportrating','roadType_x':'roadtype'}, inplace=True)
    x_predict['subtype'] = x_predict['subtype'].fillna('not reported')
    x_predict['roadtype'] = x_predict.roadtype.fillna(0)
    x_predict['roadtype'] = x_predict.roadtype.apply(str)
    x_predict = x_predict[['subtype', 'roadtype', 'nthumbsup', 'reliability', 'reportrating',
       'confidence', 'length', 'level', 'delay', 'type_y', 'len1', 'lev1',
       'del1', 'len5', 'lev5', 'del5', 'len10', 'lev10', 'del10', 'len20',
       'lev20', 'del20', 'len30', 'lev30', 'del30']]
    
    
    
    y_prob = model.predict_proba(x_predict)   
    probdf = pd.DataFrame(y_prob)
    return probdf
             

if __name__ == '__main__':
    pd.options.mode.chained_assignment = None
    warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
    
    
    #Cities timezones
    # xalapa = 'America/Mexico_City'
    # quito = 'America/Guayaquil'
    # montevideo = 'America/Montevideo'
    # miraflores = 'America/Lima'
    saopaulo = 'America/Sao_Paulo'
    
    polygon_path = '../data/polygons.json' #polygon and url file path
    path = '../data/real_time/' #final csv save location
    model_path = "../data/waze_prob_model.cbm" #trained classification model path
    
    model = CatBoostClassifier()
    model.load_model(model_path)
    
    dfatotal = pd.DataFrame()
    pub = ''
    try:
        dfjtotal = pd.read_pickle('data/dfjtotal')
    except:
        dfjtotal = pd.DataFrame()
    
    while True:
      net=False  
      t0 = time.time()
      try:
        
        alerts,jams,pub = waze_download(polygon_path)
          
        
        dfa, alerts_uuids = process_alerts(alerts,pub)
        dfjtotal = process_jams(jams,dfjtotal,pub)
                
        jams_ix = dfjtotal.set_index(['pub_utc_date', 'street'])
        jams_ix = jams_ix.sort_index()
        
        
        jam_acc = search_acc_jams(alerts_uuids,dfa,jams_ix)
        joined, all_df = merge_acc_jams(dfa,jams_ix,jam_acc)
        
        all_first = process_extra_features(all_df,jams_ix)
        all_df,dfatotal = merge_extra_features(alerts_uuids, all_first, all_df,dfatotal)
        
        probdf = clean_and_predict_proba(dfatotal,model)
        
        
        # datas = pd.to_datetime(all_df['pub_utc_date'])
        #fix cities timezone
        datasp = pd.to_datetime(all_df[all_df.city_x=='São Paulo']['pub_utc_date'],utc=True).dt.tz_convert(saopaulo)
        # dataxa = pd.to_datetime(all_df[all_df.city_x=='Xalapa']['pub_utc_date'],utc=True).dt.tz_convert(xalapa)
        # dataqt = pd.to_datetime(all_df[all_df.city_x=='Quito']['pub_utc_date'],utc=True).dt.tz_convert(quito)
        # datamv = pd.to_datetime(all_df[all_df.city_x=='Montevideo']['pub_utc_date'],utc=True).dt.tz_convert(montevideo)
        # datamf = pd.to_datetime(all_df[all_df.city_x=='Miraflores']['pub_utc_date'],utc=True).dt.tz_convert(miraflores)
    
        # datas = pd.concat([datasp,dataxa,dataqt,datamv,datamf]).sort_index()
        datas = datasp.sort_index()
        
        
        csvtodisk = all_df[['longitude','latitude','street','city_x']]
        csvtodisk['prob'] = probdf[1].round(2)
        csvtodisk['hour'] = datas.apply(lambda x: x.hour)
        csvtodisk['minute'] = datas.apply(lambda x: x.minute)
        csvtodisk['day'] = datas.apply(lambda x: x.day)
        csvtodisk['month'] = datas.apply(lambda x: x.month)
        csvtodisk['year'] = datas.apply(lambda x: x.year)
        csvtodisk['weekday'] = datas.apply(lambda x: x.weekday())
        csvtodisk.rename(columns={'city_x':'city'}, inplace=True)
        
        csvtodisk.to_csv(path+'acc_realtime.csv',index=False)
        dfjtotal.to_pickle('data/dfjtotal')
      except Exception as inst:
          net=True
          print(inst)
          pass
      
      t1 = time.time()
      if net:
         print('download error',pub,' call',(t1-t0),'secs. next try in',60-(t1-t0),'secs')
      else:
          print('completed',pub,' call',(t1-t0),'secs. sleeping for',60-(t1-t0),'secs')
      time.sleep(60-(t1-t0))


#


