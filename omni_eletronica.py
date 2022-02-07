from cmath import exp
from numpy import append
import requests as req
import datetime
import time
from datetime import datetime, date, timedelta
import os
import json
import pymysql as PyMySQL
import pandas as pd

dbo_params = {
                'host': '',
                'user': '',
                'passwd': '',
                'db': '',
                'port': 3306
            }

params = { 
                'client': '',
                'building': '',
                'gw':
                    {
                        "GER-1":[],
                        "GER-2":[]
                    },
                'token': '',
                'path_local': './data/tempData/',
                'path_drive': ''
            }

amihub_api = 'http://unsafe.ami-hub.com/api/'
sleep_time = 900
dft_time = 

'''
DATABASE FUNCTIONS
'''

def connectDB():

    global dbo_params

    connection = PyMySQL.connect(host   = dbo_params['host'],
                                 user   = dbo_params['user'],
                                 passwd = dbo_params['passwd'],
                                 db     = dbo_params['db'])
    
    cursor = connection.cursor()

    dbo_params['connection'] = connection
    dbo_params['cursor'] = cursor
    print(dbo_params)

def searchNewerBD(building,gateway,meter):
    
    dft_time = "2021-09-01T00:00:00"
    
    df = pd.read_sql("SELECT db.measures.ts FROM db.measures " \
                     "WHERE db.measures.building = '%s' " \
                     "AND db.measures.gateway = '%s' "
                     "AND db.measures.meter = '%s' "
                     "ORDER BY db.measures.ts DESC " \
                     "LIMIT 3;"
                    % (building,gateway,meter), 
                    dbo_params['connection'])
        
    if df.empty:
        return dft_time
    else:
        ts = df['ts'].iloc[0]
        print(ts)
        return brToUtc(ts)

def saveDataDB(building, gateway, meter, ts, consumption):
        
    sql = "INSERT INTO db.measures (building,gateway,meter,ts,consumption) "\
          "VALUES ('%s','%s','%s','%s', %f)" \
          % (str(building), str(gateway), str(meter), str(ts).replace("T"," "), float(consumption))

    cursor_aux = dbo_params['cursor']
    cursor_aux = cursor_aux.execute(sql)
    connection_aux = dbo_params['connection']
    connection_aux.commit()

def queryDuplicatesFromDB():
    df = pd.read_sql("SELECT *, "\
                     "COUNT(*) AS CNT "\
                     "FROM db.measures "\
                     "GROUP BY gateway, "\
                               "meter, "\
                               "ts "\
                     "HAVING COUNT(*) > 1;")
    
    return df
    
def removeDuplicatesFromDB():
    
    try:
        sql = ("WITH ToDelete AS "\
            "( "\
                    "SELECT id, "\
                    "ROW_NUMBER() OVER (PARTITION BY gateway, meter, ts ORDER BY id) AS rn "\
                    "FROM measures"\
                ") " \
                "DELETE FROM measures USING measures JOIN ToDelete ON measures.ID = ToDelete.ID"\
                "WHERE ToDelete.rn > 1; ")

        cursor_aux = dbo_params['cursor']
        cursor_aux = cursor_aux.execute(sql)
        connection_aux = dbo_params['connection']
        connection_aux.commit()
    except:
        print("Fail removing duplicates")

'''
AUXILIAR FUNCTIONS
'''
def cleanMeter(meter):
    return str(meter).split("-")[0]

def convertToTimestamp(ts):
    return int(datetime.timestamp(datetime.strptime(ts,"%Y-%m-%d %H:%M:%S")))

def utcToBr(ts):
    ts = str(ts).replace(" ","T")
    ts = datetime.fromisoformat(ts) - timedelta(hours=3)
    return datetime.fromisoformat(str(ts)).isoformat()

def brToUtc(ts):
    ts = str(ts).replace(" ","T")
    ts = datetime.fromisoformat(ts) + timedelta(hours=3)
    return datetime.fromisoformat(str(ts)).isoformat()

'''
QUERY FUNCTIONS
'''

def getMeterList(params):
    try:
        for hw in params['gw']:
            print(hw)
            url = amihub_api + params['token'] + '/data/'+ hw
            print(url)
            tmp = req.get(url).json()
            for meters in tmp['payload'][hw]:
                if meters not in params['gw'][hw]:
                    params['gw'][hw].append(meters)
                print(str(meters).split('-')[0])
    except:
        print("Fail getting Meter List")

def getMeterData(params):
     
    for hw in params['gw']:
        for meter in params['gw'][hw]:

            after = searchNewerBD(params['building'], hw, cleanMeter(meter))
            before = (datetime.fromisoformat(after) + timedelta(days=4)).isoformat()
            
            url = amihub_api + params['token'] + '/data/'+ hw + "/" + meter +'/?count=1000&start=' + after + '&end=' + before
            #print(url)
            tmp = req.get(url).json()
            #print(json.dumps(tmp['payload'], indent =4))

            try:
                last_ts = after
                
                for data in tmp['payload'][hw][meter]:
                    
                    if (utcToBr(data['measure_at']) > utcToBr(after)) and \
                    (utcToBr(data['measure_at']) != last_ts):
                            print(meter,data['measure_at'],utcToBr(data['measure_at']),float(data['value']))
                            saveDataDB(params['building'],hw,cleanMeter(meter),utcToBr(data['measure_at']),float(data['value']))
                            last_ts = utcToBr(data['measure_at'])
                            
            except:
                print("Error in URL: " + url)
    

connectDB()

getMeterList(params)

while True:

    getMeterData(params)

    print("Going to Sleep")
    time.sleep(sleep_time)

