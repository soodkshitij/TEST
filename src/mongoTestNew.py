import pymongo
import sys
import logging
from pymongo import MongoClient
import time
import glob

con=None
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
        data =  list(get_mongo_connection().mesowest.mesowest.find({ "timestamp_utc" : { "$gt" :  fromTS, "$lt" : toTS}}).skip(offset).limit(limit))
    else:
        data =  list(get_mongo_connection().mesowest.mesowest.find({ "timestamp_utc" : { "$gt" :  fromTS, "$lt" : toTS}}))    
    for d in data:
        content+=d['STN']+','+d['YYMMDD/HHMM']+','+d['MNET']+','+d['SLAT']+','+d['SLON']+','+d['SELV']+','+d['TMPF']+','+d['SKNT']+','+d['DRCT']+','+d['GUST']+','+d['PMSL']+','+d['ALTI']+','+d['DWPF']+','+d['RELH']+','+d['WTHR']+','+d['P24I']+'\n'
    return content

def put_data(content):
    splittedArray = []
    bulkInsertArray = []
    for line in content.split('\n'):
        line = line.strip()
        splittedArray =  line.split(',')
        if len(splittedArray)>10 and splittedArray[0]!="STN":
            singleTuple =  {'STN':splittedArray[0],'YYMMDD/HHMM':splittedArray[1],'MNET':splittedArray[2],'SLAT':splittedArray[3],'SLON':splittedArray[4],'SELV':splittedArray[5],'TMPF':splittedArray[6],'SKNT':splittedArray[7],'DRCT':splittedArray[8],'GUST':splittedArray[9],'PMSL':splittedArray[10],'ALTI':splittedArray[11],'DWPF':splittedArray[12],'RELH':splittedArray[13],'WTHR':splittedArray[14],'P24I':splittedArray[14],'date_utc':(splittedArray[1].split(' '))[0].replace('-',''),'timestamp_utc':int(time.mktime(time.strptime(splittedArray[1], '%Y-%m-%d %H:%M:%S')))*1000}
            #singleTuple = {"STN":splittedArray[0],"timestamp_utc":int(time.mktime(time.strptime(splittedArray[1], '%Y%m%d/%H%M'))) * 1000, "date_utc": splittedArray[1].split('/')[0],"raw":line}
            bulkInsertArray.append(singleTuple)

        """if len(splittedArray)>10 and splittedArray[0]!="STID":
            singleTuple = {"station":splittedArray[0],"timestamp_utc":int(time.mktime(time.strptime(splittedArray[1], '%Y%m%d/%H%M'))) * 1000, "date_utc": splittedArray[1].split('/')[0],"raw":line}
            bulkInsertArray.append(singleTuple)"""

    get_mongo_connection().mesowest.mesowest.insert_many(bulkInsertArray)
    return True
    
if __name__ == '__main__':
    #list_of_files = glob.glob('./data/*.out') 
    #for file_name in list_of_files:
    #    file = open(file_name).readlines()
    #    put_data(file)

    print(get_count_of_data(1483228800000, 2483228800000))
    print(get_data(1483228800000, 2483228800000,0,100))
    #put_data("BULLF,2018-03-16 21:45:00,8.00,37.52,-110.73,1128.00,-9999.00,6.33,-9999.00,8.26,-9999.00,-9999.00,-9999.00,-9999.00,-9999.00,-9999.00\nBULLF,2012-02-01 18:15:00,8.00,37.52,-110.73,1128.00,41.00,1.62,168.80,2.86,-9999.00-9999.00,-90.26,0.07,-9999.00,-9999.00\nBULLF,2012-02-01 18:30:00,8.00,37.52,-110.73,1128.00,41.55,1.23,151.10,2.70,-9999.00,-9999.00,-89.99,0.07,-9999.00,-9999.00\nBULLF,2012-02-01 18:45:00,8.00,37.52,-110.73,1128.00,41.96,1.88,161.00,5.10,-9999.00,-9999.00,-89.79,0.07,-9999.00,-9999.00")
    print(get_dates())