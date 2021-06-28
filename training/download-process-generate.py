# -*- coding: utf-8 -*-
"""
Created on Tue May 11 10:26:07 2021

@author: mayur
"""

# import logging
import pprint
import os
import boto3
import io
import pandas as pd
from retrying import retry
import time
# from pyproj import Proj
# from shapely.geometry import LineString
# from pyroutelib3 import Router  # Import the router
import pickle
import pytz
from shapely.geometry import shape, Point
import warnings
import geopandas as gpd
import json
import dateutil
from datetime import timedelta
from pandas import json_normalize, to_datetime
from geopandas import read_file
from calendar import monthrange as mr
from dask.distributed import Client


@retry(stop_max_delay=900*1000,
        wait_fixed=15 *1000)
def poll_status(_id,athena):
    '''
    poll query status
    '''
    result = athena.get_query_execution(
        QueryExecutionId = _id
    )

    # logging.info(pprint.pformat(result['QueryExecution']))
    state = result['QueryExecution']['Status']['State']
    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        raise Exception

@retry(stop_max_attempt_number=10)
def download_s3(s3,S3BUCKET_NAME,s3_key):
    try:
        obj = s3.Object(bucket_name=S3BUCKET_NAME, key=s3_key).get()
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
        return df
    except:
        raise Exception

def query_to_athena(t):
    filename,sql,S3BUCKET_NAME,DATABASE_NAME=t
    # filename,sql=t
    # boto3.set_stream_logger('botocore', level='WARNING')
    athena = boto3.client('athena')
    s3 = boto3.resource('s3')
    result = athena.start_query_execution(
        QueryString = sql,
        QueryExecutionContext = {
            'Database': DATABASE_NAME
        },
        ResultConfiguration = {
            'OutputLocation': 's3://' + S3BUCKET_NAME,
        },
        WorkGroup='EquipeCiro'
    )

    # logging.info(pprint.pformat(result))

    QueryExecutionId = result['QueryExecutionId']

    
    result = poll_status(QueryExecutionId,athena)
    
    

    # If folder doesn't exist, then create it.
    if not os.path.isdir('../data/athena_logs/'):
        os.makedirs('../data/athena_logs/')

    # save response
    with open('../data/athena_logs/'+filename + '.log', 'w') as f:
        f.write(pprint.pformat(result, indent = 4))

    # save query result from S3
    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        s3_key = QueryExecutionId + '.csv'
        # local_filename = filename + '.csv'
        # s3_key = '8a1812b8-f743-48f4-8755-8ff92690989b.csv'
        # s3.Bucket(S3BUCKET_NAME).download_file(s3_key, local_filename)
        
        # obj = s3.Object(bucket_name=S3BUCKET_NAME, key=s3_key).get()
        # df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
        df = download_s3(s3,S3BUCKET_NAME,s3_key)
        return df
    return pd.DataFrame()

def amazon_download(ddatas,qalerts,qjams):
    S3BUCKET_NAME1 = 'aws-athena-query-results-east-2'
    DATABASE_NAME1 = 'cities' 
    client = Client(n_workers=2,threads_per_worker=1)
    altup = ('alerts_'+str(ddatas.date()),qalerts,S3BUCKET_NAME1,DATABASE_NAME1)
    jatup = ('jams_'+str(ddatas.date()),qjams,S3BUCKET_NAME1,DATABASE_NAME1)
    fut = client.map(query_to_athena,[altup,jatup])
    results = client.gather(fut)
    alerts,jams = results
    client.close() 
    return alerts, jams
    
    
def process_alerts(alerts):
    
    alerts = alerts[alerts.type=='ACCIDENT'] 
    alerts = alerts.drop(['polygon_slug'],axis=1)
    accids  = alerts.drop_duplicates()
    accids = accids.drop_duplicates(subset=['pub_utc_date','uuid'],keep='first')
    accids = accids.sort_values('pub_utc_date')
    accids['pub_utc_dt'] = pd.to_datetime(accids.pub_utc_date, utc=True)
    accids['street'] = accids.street.apply(str)
    accids['coord'] = list(zip(accids['longitude'], accids['latitude']))
    accids['coord'] = gpd.GeoSeries(accids['coord'].apply(
            lambda x: Point(x)), crs='EPSG:4326').to_crs('EPSG:22523')
    accids = accids.reset_index().set_index(['uuid'])
    accids  =  accids.sort_index()
    alerts_uuids  = accids.index.unique()
    return accids, alerts_uuids
    
def process_jams(jams):
    #jams = jams[jams.city=="São Paulo"]
    jams = jams.drop(['polygon_slug'],axis=1)
    jams = jams.drop_duplicates()
    jams['street'] = jams.street.apply(str)
    
    jams_ix = jams.set_index(['pub_utc_date', 'street'])
    jams_ix = jams_ix.sort_index()
    jams_ix['ls'] =  gpd.GeoSeries(jams_ix.line_geojson.apply(
            lambda x: shape(json.loads(x))), crs='EPSG:4326').to_crs('EPSG:22523')
    return jams_ix

def search_acc_jams(alerts_uuids,accids,jams_ix):

    jam_acc = []
    # t=len(alerts_uuids)
    # i=0
    for au in alerts_uuids: 
        acid = accids.loc[[au]]
        acid = acid.reset_index().set_index(['pub_utc_date', 'street'])
        # print(i,'/',t,au,len(acid),time.ctime())    
        # i+=1
        try:
            jrows = jams_ix.loc[jams_ix.index.intersection(acid.index)]
            jrows['d'] = jrows.ls.apply(lambda x: acid.iloc[0].coord.distance(x))
            jrows = jrows[jrows.d<=200]
        except:
            jrows =  []
        if len(jrows)>0:
            jrows =  jrows[jrows.d==jrows.d.min()]
            jrows = jrows.reset_index().drop_duplicates(subset=['pub_utc_date']).set_index(['pub_utc_date','street'])
            acid.loc[jrows.index,'juuid']  = jrows.uuid.apply(int)
            jam_acc.extend(acid.reset_index().loc[:,['pub_utc_date','uuid','juuid']].values.tolist())
    return jam_acc

def merge_acc_jams(accids,jams_ix,jam_acc):

    accids2= accids.reset_index().set_index(['pub_utc_date', 'uuid'])
    accids2= accids2.sort_index()
    accids2 = accids2.drop(['longitude', 'latitude'],axis=1)
    
    jams_ix2 = jams_ix.reset_index().set_index(['pub_utc_date', 'uuid'])
    jams_ix2= jams_ix2.sort_index()
    jams_ix2  =  jams_ix2.drop(['line_geojson'],axis=1)
    
    
    
    jadf = pd.DataFrame(jam_acc)
    jadfnn = jadf.dropna()
    
    
    joined =[]
    for i,j in enumerate(jadfnn[1].unique()):
        # if i%100==0:
        #     print(i,time.ctime())
        keys = jadf[jadf[1]==j]
        aa = accids2.loc[list(keys[[0,1]].itertuples(index=False,name=None))]
        
        keys2=jams_ix2.index.intersection(list(keys[[0,2]].itertuples(index=False,name=None)))
        ja = jams_ix2.loc[keys2]
        ja = ja.reset_index().drop_duplicates(subset=['pub_utc_date']).set_index(['pub_utc_date','uuid'])
        
        result = pd.merge(aa.reset_index(), ja.reset_index(), on=["pub_utc_date", "street"]).set_index('index')
        joined.append(result)
    
    dfjoined =pd.concat(joined) 
    dfjoined = dfjoined[~dfjoined.index.duplicated(keep='first')]
    notfoundf =  accids2[~accids2['index'].isin(dfjoined.index)]
    notfoundf = notfoundf.reset_index().set_index('index')
    notfoundf.rename(columns={'uuid':'uuid_x','type':'type_x'}, inplace=True)
    
    all_df = pd.concat([dfjoined,notfoundf])
    all_df = all_df.sort_values('pub_utc_date')
    
    return all_df
    
def process_extra_features(all_df,jams_ix):
    all_first = all_df.loc[all_df.uuid_x.drop_duplicates(keep='first').index]
    all_first.length.fillna(0,inplace=True)
    all_first.level.fillna(0,inplace=True)
    all_first.delay.fillna(0,inplace=True)
    all_first['pub_utc_date0'] = pd.to_datetime(all_first['pub_utc_date'])
    all_first['pub_utc_date1'] =all_first.pub_utc_date0 - timedelta(minutes=1)
    all_first['pub_utc_date1'] = all_first['pub_utc_date1'].apply(lambda x: str(x)+'.000')
    
    all_first['pub_utc_date5'] =all_first.pub_utc_date0 - timedelta(minutes=5)
    all_first['pub_utc_date5'] = all_first['pub_utc_date5'].apply(lambda x: str(x)+'.000')
    
    all_first['pub_utc_date10'] =all_first.pub_utc_date0 - timedelta(minutes=10)
    all_first['pub_utc_date10'] = all_first['pub_utc_date10'].apply(lambda x: str(x)+'.000')
    
    all_first['pub_utc_date20'] =all_first.pub_utc_date0 - timedelta(minutes=20)
    all_first['pub_utc_date20'] = all_first['pub_utc_date20'].apply(lambda x: str(x)+'.000')
    
    all_first['pub_utc_date30'] =all_first.pub_utc_date0 - timedelta(minutes=30)
    all_first['pub_utc_date30'] = all_first['pub_utc_date30'].apply(lambda x: str(x)+'.000')
    
    for dt  in ['1','5','10','20','30']:
        # ifo=0
        # inofo=0
        for row in all_first.iterrows():
        
            try:
                zzz = jams_ix.loc[(row[1]['pub_utc_date'+dt],row[1].street)]
                zzz['d'] = zzz.ls.apply(lambda x: x.distance(row[1].coord))
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
    del all_df['ls']
    del all_first['ls']
    return all_df,all_first
    
def merge_extra_features(alerts_uuids,all_first,all_df):
    for au in alerts_uuids:
    
        first = all_first[all_first.uuid_x==au]
        
        cols = ['len1', 'lev1', 'del1', 'len5', 'lev5', 'del5', 'len10', 'lev10',
               'del10', 'len20','lev20', 'del20', 'len30', 'lev30', 'del30']
        zzz=all_df[all_df.uuid_x==first.uuid_x.iloc[0]]
        
        
        firstrep = first.loc[first.index.repeat(len(zzz)),cols]
        firstrep.index =  zzz.index
        all_df.loc[zzz.index,cols] = firstrep.loc[:,cols]
    return all_df

def search_vs(df_all_vs,all_df):
    
    teste_b = {}
    teste_d = {}
    t=0
    for n_line, line in df_all_vs.iterrows():
        # break
        # if t<64:
        # print(t,n_line,time.ctime())
        t+=1
        range_accids = (all_df['pub_utc_dt'] >= line['min_range']) & (all_df['pub_utc_dt'] <= line['max_range']) 
        
        candidates = all_df[range_accids]
        if len(candidates)>0:
            # start = router.findNode(line['geom.coordinates'][1],line['geom.coordinates'][0])
                candidates['dist2vs'] = candidates.coord.apply(lambda x: line['coord'].distance(x))
            # for  dradius in  [50,100,200,300,400,500]:
                dradius = 500 
                canddist = candidates[candidates.dist2vs<=dradius]
                # if len(canddist)>0:
                #     if dradius>200:
                #         # break
                #         print('searching routes',dradius,time.ctime())
                #         candu = canddist.groupby(['longitude', 'latitude']).size().reset_index()
                #         candu1 = candu[['longitude', 'latitude']].apply(lambda x: route_length(start,x,dradius+100), axis=1)
                #         for a in candu1:
                            
                #             if not a[2]:
                #                 canddist = canddist.drop(canddist[(canddist.latitude==a[0])&(canddist.longitude==a[1])].index)
                               
                if len(canddist)>0:
                    canddist['totalt'] = (line['occurred_from'].astimezone(pytz.utc)-canddist.pub_utc_dt).dt.total_seconds()/60
                    
                    teste_b[n_line] = canddist[['dist2vs','totalt','type_x','subtype']].reset_index().values.tolist()
                    # break
                    # print(n_line,'encontrado')
                else:
                    # print(n_line,'não encontrado')
                    abc1 = (line['occurred_from'].astimezone(pytz.utc)-candidates.pub_utc_dt).dt.total_seconds()/60
                    abc2 = candidates['dist2vs']
                    teste_d[n_line] = pd.concat([abc1, abc2], axis=1)
               
    
    with open('../data/datasets/mergevs_found_'+str(ddatas.date())+'.pkl', 'wb') as output_file:
        pickle.dump(teste_b, output_file)
    with open('../data/datasets/mergevs_notfound_'+str(ddatas.date())+'.pkl', 'wb') as output_file:
        pickle.dump(teste_d, output_file)
    
    teste_c=  {i : values for i,values in teste_b.items()}
    teste_e=  {i : values for i,values in teste_d.items()}
    
    return teste_c,teste_e
    
    
def merge_vs(teste_c,all_df):
    for k,v in teste_c.items():
        # print(k,time.ctime())  
    
        classe = 1 
            
        alert_index = []
        for v1 in v:
            alert_index.append(v1[0])
     
        all_df.loc[alert_index,'class'] = classe
    # i=0
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
        
        temp2 = temp.uuid_x.value_counts().reset_index()
        dictemp={}
        for row in temp2.itertuples(index=False,name=None):
            dictemp[row[0]] = list(range(row[1]))
        v2=[]    
        for k2,v2 in dictemp.items():
            temp.loc[temp.uuid_x==k2,'repeat_c'] = v2
            
        all_df.loc[temp.index,'repeat_c'] = temp['repeat_c']  
        all_df.loc[temp.index,'length'] = temp['length']  
        all_df.loc[temp.index,'level'] = temp['level']  
        all_df.loc[temp.index,'delay'] = temp['delay']  
        all_df.loc[temp.index,'type_y'] = temp['type_y'] 
    return all_df

def in_sp(dataframe, shp_file="../data/sp_polygon/35MUE250GC_SIR.shp"):

    SP_cities = read_file(shp_file)

    # Select Sao Paulo by coordenation, 0.72% aren't
    dataframe['coord'] = dataframe['geom.coordinates'].apply(
        lambda x: Point(x))

    sp_city = SP_cities[SP_cities["NM_MUNICIP"] == "SÃO PAULO"]["geometry"].values[0]

    is_in_sp_city = dataframe['coord'].apply(lambda x: x.within(sp_city))
    df_data_sp = dataframe[is_in_sp_city].copy()

    return df_data_sp

def parse_date(dataframe):
    # tzc = tz.gettz("America/Sao_Paulo")
    dataframe['occurred_from'] = to_datetime(dataframe['occurred_from'])
    dataframe['occurred_to'] = to_datetime(dataframe['occurred_to'])
    # by definitionin system, must be the same date...
    coherent_date = (dataframe['occurred_to']
                     == dataframe['occurred_from'])
    # 98.61% is coherent
    dataframe = dataframe[coherent_date]
    dataframe['year'] = [
        date.year for date in dataframe['occurred_from']]
    dataframe['month'] = [
        date.month for date in dataframe['occurred_from']]
    dataframe['day'] = [
        date.day for date in dataframe['occurred_from']]
    return dataframe

def process_vs(year):
    vs_o = VidaSegura("../data/vs_all.json", year)
    vs_o.read_json()
    vs_o.pre_processing()
    data = vs_o.processing
    sp_2019 = data[data['year'] == year]
    delta_time = timedelta(hours=3)
    sp_2019['max_range'] = to_datetime(sp_2019['occurred_from'] + delta_time, utc=True)
    sp_2019['min_range'] = to_datetime(sp_2019['occurred_from'] - delta_time, utc=True)
    sp_2019 = sp_2019.sort_values('occurred_from')
    df_all_vs = sp_2019.copy()
    df_all_vs['coord'] = gpd.GeoSeries(df_all_vs['coord'], crs='EPSG:4326').to_crs('EPSG:22523')
    df_all_vs.to_pickle('../data/df_all_vs.pkl')
    return df_all_vs

class VidaSegura():
    def __init__(self, file_name, year):
        self.file_json = file_name
        self.year = year
        self.data = None
        self.data_processing = None
    def read_json(self):
        with open(self.file_json, 'r') as f:
            file = json.load(f)
        self.df_normalize = json_normalize(file)
    def pre_processing(self):
        # Pre-processing
        df_data = parse_date(self.df_normalize)
        # removing nan columns
        df_data = df_data.dropna(axis=1, how='all')
        # selecting by shape
        df_sp = in_sp(df_data)
        self.processing = df_sp

if __name__ == '__main__':

    print('starting script',time.ctime())

    pd.options.mode.chained_assignment = None
    warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

    S3BUCKET_NAME1 = 'aws-athena-query-results-east-2'
    DATABASE_NAME1 = 'cities'          
    
    year = 2019
        
    try:
        df_all_vs = pd.read_pickle('../data/df_all_vs.pkl')    
    except:
        df_all_vs = process_vs(year)
    
        
    for mes in range(1, 13):
        fday=1
        # if mes==1:
        #     fday = 1
        ld = mr(year, mes)[1] + 1
        for diames in range(fday, ld):

            ddatas = dateutil.parser.parse(str(year) + "-" + str(mes) + "-" + str(diames) + "T00:00:00.000")
            
            qalerts = 'SELECT "pub_utc_date", "polygon_slug", "uuid", "street",' \
                                ' "type", "subtype", "roadtype", "longitude",'\
                                                ' "latitude", "nthumbsup", "reliability",'\
                                ' "reportrating", "confidence" '\
                                     'FROM "cities"."br_saopaulo_waze_alerts" '\
                                     'WHERE year=2019 AND month = '+str(mes)+' AND day='+str(diames)+ ' AND city=\'São Paulo\''

            qjams = ('SELECT "pub_utc_date", "polygon_slug", "uuid", "street", "length",'
                     ' "level", "delay", "type", "line_geojson" '
                             ' FROM "cities"."br_saopaulo_waze_jams" '
                             ' WHERE year=2019 AND month = '+str(mes)+' AND day='+str(diames)+ ' AND city=\'São Paulo\'')
            
            print(str(ddatas.date())+' loading and preprocessing data',time.ctime())
            
            alerts, jams = amazon_download(ddatas,qalerts,qjams)
            
            print(str(ddatas.date())+' download complete',time.ctime())
            
            accids, alerts_uuids = process_alerts(alerts)
            
            jams_ix = process_jams(jams)
            
            print(str(ddatas.date())+' seaching dataset correspondence',time.ctime())
            
            jam_acc = search_acc_jams(alerts_uuids,accids,jams_ix)

            print(str(ddatas.date())+' merging data',time.ctime())
            
            all_df = merge_acc_jams(accids,jams_ix,jam_acc)
            
            print(str(ddatas.date())+' preprocessing extra features',time.ctime())
            
            all_df,all_first = process_extra_features(all_df,jams_ix)
            
            print(str(ddatas.date())+' merging extra features',time.ctime())

            all_df = merge_extra_features(alerts_uuids,all_first,all_df)
            
            print(str(ddatas.date())+' Searching VS',time.ctime())

            teste_c,teste_e = search_vs(df_all_vs,all_df)
            
            print(str(ddatas.date())+' merging VS',time.ctime())
            
            
            all_df = merge_vs(teste_c,all_df)
            
            all_df['class']=all_df['class'].fillna(0)
            ac = all_df
            ac['roadtype']  =  ac.roadtype.fillna(0)
            ac['day'] = ac.pub_utc_dt.dt.day 
            
            # remove atributos espaço temporais
            ac = ac.drop(['pub_utc_dt','uuid_y','coord'],axis=1)
            ac['subtype'] = ac['subtype'].fillna('not reported')
            # ac['roadtype'] = ac.roadtype.apply(str)
            #reposiciona atributo classe na ultima coluna
            classes = ac['class']
            ac = ac.drop('class',axis=1)
            ac['class'] = classes
            ac.to_csv('../data/datasets/ajvs_'+str(ddatas.date())+'.csv')
            print(str(ddatas.date())+' finish',time.ctime())
            

    Data = pd.DataFrame()
    for mes in range(1, 13):
        ld = mr(year, mes)[1] + 1
        for diames in range(1, ld):
            
            ddatas = dateutil.parser.parse(str(year) + "-" + str(mes) + "-" + str(diames) + "T00:00:00.000")
            # print(ddatas)
            Dat = pd.read_csv('../data/datasets/ajvs_'+str(ddatas.date())+'.csv')
            Dat = Dat[Dat.repeat_c<5] 
            Data = pd.concat([Data,Dat])
    
    
    Data = Data.drop(['repeat_c'],axis=1)
    Data.to_pickle('../data/datasets/12mo_5min.pkl')