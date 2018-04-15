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

    #fromTS = int(time.mktime(time.strptime(from_timestamp, '%Y-%m-%d %H:%M:%S'))) * 1000
    #toTS = int(time.mktime(time.strptime(to_timestamp, '%Y-%m-%d %H:%M:%S'))) * 1000
    return get_mongo_connection().mesowest.mesowest.find({ "timestamp_utc" : { "$gt" :  fromTS, "$lt" : toTS}}).count()

def get_data(fromTS, toTS, offset, limit):

    #fromTS = int(time.mktime(time.strptime(from_timestamp, '%Y-%m-%d %H:%M:%S'))) * 1000
    #toTS = int(time.mktime(time.strptime(to_timestamp, '%Y-%m-%d %H:%M:%S'))) * 1000
    if offset or limit:
        return list(get_mongo_connection().mesowest.mesowest.find({ "timestamp_utc" : { "$gt" :  fromTS, "$lt" : toTS}}).skip(offset).limit(limit))
    else:
        return list(get_mongo_connection().mesowest.mesowest.find({ "timestamp_utc" : { "$gt" :  fromTS, "$lt" : toTS}}))

def put_data(content):

    splittedArray = []
    bulkInsertArray = []

    for line in content:
        line = line.strip()
        splittedArray =  line.split()

        if len(splittedArray)==16:
            singleTuple = {"station":splittedArray[0],"timestamp_utc":int(time.mktime(time.strptime(splittedArray[1], '%Y%m%d/%H%M'))) * 1000,"raw":line}
            bulkInsertArray.append(singleTuple)

        """if len(splittedArray)>20 and splittedArray[0]!="STID":
            singleTuple = {"station":splittedArray[0],"timestamp_utc":int(time.mktime(time.strptime(splittedArray[1], '%Y%m%d/%H%M'))) * 1000,"raw":line}
            bulkInsertArray.append(singleTuple)"""

    get_mongo_connection().mesowest.mesowest.insert_many(bulkInsertArray)
    return True
    
if __name__ == '__main__':
    list_of_files = glob.glob('./data/*.out') 
    for file_name in list_of_files:
        file = open(file_name).readlines()
        put_data(file)
    #print(get_count_of_data(981079200000, 1486004400000))
    #print(get_data(981079200000, 1486004400000,0,100))
