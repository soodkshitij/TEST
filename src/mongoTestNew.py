import pymongo
import sys
import logging
from pymongo import MongoClient
import time
import glob

con=None
MESONET_ROW_SIZE = 11
MESOWEST_MIN_ROW_SIZE = 10

def get_mongo_connection(host='localhost', port=27017):
    global con
    if con is None:
        print ("Establishing connection %s host and port %d" %(host,port))
        try:
            con = pymongo.MongoClient(host, port)
        except Exception as e:
            print (e)
            return None
    return con

def get_count_of_data(fromTS, toTS):
    return get_mongo_connection().mesowest.mesowest.find({ "timestamp_utc" : { "$gt" :  fromTS, "$lt" : toTS}}).count()

def get_dates():
    return get_mongo_connection().mesowest.mesowest.distinct("date_utc")

def get_data(fromTS, toTS, offset, limit):
    content= ""
    if offset or limit:
        data =  list(get_mongo_connection().mesowest.mesowest
                     .find({ "timestamp_utc" : { "$gt" : fromTS, "$lt" : toTS}}).skip(offset).limit(limit))
    else:
        data =  list(get_mongo_connection().mesowest.mesowest
                     .find({ "timestamp_utc" : { "$gt" :  fromTS, "$lt" : toTS}}))
    for d in data:
        content+=d['STN']+','+d['YYMMDD/HHMM']+','+d['MNET']+','+d['SLAT']+','+d['SLON']+','+d['SELV']+','\
                 +d['TMPF']+','+d['SKNT']+','+d['DRCT']+','+d['GUST']+','+d['PMSL']+','+d['ALTI']+','\
                 +d['DWPF']+','+d['RELH']+','+d['WTHR']+','+d['P24I']+'\n'
    return content

def put_data(request):
    splittedArray = []
    bulkInsertArray = []
    content = (request.data).decode('utf-8')
    # Timestamp UTC
    timestamp = request.timestamp_utc
    for line in content.split('\n'):
        line = line.strip()
        splittedArray =  line.split(',')
        if len(splittedArray) > MESOWEST_MIN_ROW_SIZE and splittedArray[0] != "STN":
            doc =  {'STN':splittedArray[0],'YYMMDD/HHMM':splittedArray[1],'MNET':splittedArray[2],'SLAT':splittedArray[3],
                    'SLON':splittedArray[4],'SELV':splittedArray[5],'TMPF':splittedArray[6],'SKNT':splittedArray[7],
                    'DRCT':splittedArray[8],'GUST':splittedArray[9],'PMSL':splittedArray[10],'ALTI':splittedArray[11],
                    'DWPF':splittedArray[12],'RELH':splittedArray[13],'WTHR':splittedArray[14],'P24I':splittedArray[14],
                    'date_utc':(splittedArray[1].split(' '))[0].replace('-',''),'timestamp_utc':
                        int(time.mktime(time.strptime(splittedArray[1], '%Y-%m-%d %H:%M:%S')))*1000}
            bulkInsertArray.append(doc)

        elif len(splittedArray) == MESONET_ROW_SIZE and splittedArray[0] != "#":
            """
               Sample data: 
               # id,name,mesonet,lat,lon,elevation,agl,cit,state,country,active
               UI UC,Urbana, IL,gpsmet,40.099998474121094,-88.22000122070312,264.3999938964844,-9999.0,null,null,null,true
            """
            # TODO need to fix the timestamp once clear about client stream data
            # Need to get the timestamp_utc from request
            doc =  {'STN': splittedArray[0],'YYMMDD/HHMM':timestamp ,'MNET': None,'SLAT':splittedArray[3],'SLON':splittedArray[4],
                    'SELV': splittedArray[5],'TMPF': None,'SKNT': None,'DRCT': None,'GUST': None,'PMSL': None,'ALTI': None,
                    'DWPF': None,'RELH': None,'WTHR': None,'P24I': None,'date_utc':timestamp,
                    'timestamp_utc':timestamp}
            bulkInsertArray.append(doc)

    get_mongo_connection().mesowest.mesowest.insert_many(bulkInsertArray)
    return True
    
if __name__ == '__main__':
    print(get_count_of_data(1483228800000, 2483228800000))
    print(get_data(1483228800000, 2483228800000,0,100))
    print(get_dates())