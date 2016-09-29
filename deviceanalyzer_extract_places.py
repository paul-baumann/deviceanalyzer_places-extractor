#!/usr/bin/python
# -*- coding: utf-8 -*-

##
# This file extracts relevant places from Wi-Fi 
# and cellular data of the Device Analyzer data set.
# To merge the places use 'deviceanalyzer_merge_places.py' script.
#
# Copyright Paul Baumann
## 

import os
import sys
import numpy
import Util
from Database_Handler import Database_Handler
import warnings
import time
import datetime
from collections import Counter 
from scipy.stats import mode

SLOT_IN_MINUTES = 15
DATA_AVAILABLE = 0
IN_TRANSIT = 0

def run_algorithm():
        
    dbHandler = Util.Get_DB_Handler()
    query = ('SELECT distinct(user_id) from wifi')
    result = dbHandler.select(query)
    matrix = numpy.array(result)
      
    user_ids = matrix[:,0]
    
    if (len(sys.argv) > 2):
        first_user = int(sys.argv[1])
        last_user = int(sys.argv[2])
        user_ids = user_ids[first_user:last_user]
    
    
    for user_id in user_ids:
        start = time.time()
        
        extract_places(user_id, start)
        print 'EXTRACTION DONE USER: %s | time: %s' % (user_id, time.time() - start)


def extract_places(user_id, start):
    
    stored_wifi_ssids = []
    stored_cell_ids = []
    
    ## get connected
    dbHandler = Get_DB_Handler()
    query_connected = ('select (CONCAT(FROM_UNIXTIME(unixtime.c, \'%%Y%%m%%d\'), LPAD(FLOOR(FROM_UNIXTIME(unixtime.c, \'%%i\') / %s) + FLOOR(FROM_UNIXTIME(unixtime.c, \'%%H\') * (60 / %s)), 2, 0))) as unique_index, '
                       'data_value from (SELECT UNIX_TIMESTAMP(STR_TO_DATE(substring_index(wall_clock,\'.\',1), \'%%Y-%%m-%%dT%%H:%%i:%%s\')) as c, data_value from wifi '
                       'where data_key LIKE "wifi|connected|%%" and user_id = %s and wall_clock != \'(invalid date)\' order by wall_clock) as unixtime '
                       'group by unique_index, data_value;') % (SLOT_IN_MINUTES, SLOT_IN_MINUTES, user_id)
    result_connected = dbHandler.select(query_connected)
    matrix_connected = numpy.array(result_connected)
    
    if result_connected:
        wifi_connected_unique_index_array = numpy.array(matrix_connected[:,0]).astype(long)
        wifi_connected_ssid_array = numpy.array(matrix_connected[:,1])
        
        unique_wifi_connected_unique_index_array = numpy.unique(wifi_connected_unique_index_array)
        unique_wifi_connected_ssid_array = numpy.unique(wifi_connected_ssid_array)
        
        numeric_unique_place_ids = numpy.empty((len(wifi_connected_ssid_array),))
        for idx, unique_wifi_connected_ssid in enumerate(unique_wifi_connected_ssid_array):
            numeric_unique_place_ids[wifi_connected_ssid_array == unique_wifi_connected_ssid] = (idx + 1) 
             

        ## ASSIGN PLACE IDS TO TIMESLOTS BASED ON CONNECTED WIFI
        fields = ['user_id','unique_index','place_id']
                
        result = numpy.empty((len(wifi_connected_ssid_array), 3))
        result[:,0] = user_id
        result[:,1] = wifi_connected_unique_index_array
        result[:,2] = numeric_unique_place_ids
        
        dbHandler = Get_DB_Handler() 
        dbHandler.insertMany('_timeslot_to_placeid', fields, result)
        
        ## ASSIGN NETWORK IDS TO PLACE IDS
        fields = ['user_id','place_id', 'network_id', 'is_connected', 'is_wifi']
                
        result = numpy.empty((len(unique_wifi_connected_ssid_array), 5), dtype='|S48')
        result[:,0] = user_id
        result[:,1] = numpy.arange(1, len(unique_wifi_connected_ssid_array) + 1, 1)
        result[:,2] = unique_wifi_connected_ssid_array
        result[:,3] = 1
        result[:,4] = 1
        
        dbHandler = Get_DB_Handler() 
        dbHandler.insertMany('_network_to_placeid', fields, result)
        
        stored_wifi_ssids.extend(numpy.unique(unique_wifi_connected_ssid_array))
        
        print 'WIFI CONNECTED DONE USER: %s | time: %s' % (user_id, time.time() - start)

        ########### WI-FI ################
        ## SEARCH FOR ALL WI-FI ENTRIES FOR THE CURRENT TIMESLOT      
        dbHandler = Get_DB_Handler()
        query = ('select wifidata.sssid, wifidata.unique_index, _timeslot_to_placeid.place_id from ' 
            '(SELECT  unixtime.ssid as sssid, ' 
            '(CONCAT(FROM_UNIXTIME(unixtime.c, \'%%Y%%m%%d\'), LPAD(FLOOR(FROM_UNIXTIME(unixtime.c, \'%%i\') / %s) + FLOOR(FROM_UNIXTIME(unixtime.c, \'%%H\') * (60 / %s)), 2, 0))) as unique_index ' 
            'from (SELECT UNIX_TIMESTAMP(STR_TO_DATE(substring_index(wall_clock,\'.\',1), \'%%Y-%%m-%%dT%%H:%%i:%%s\')) as c, ' 
                  'data_value as ssid ' 
                  'from wifi where data_key LIKE \'wifi|scan|%%\' ' 
                  'and user_id = %s ' 
                  'and wall_clock != \'(invalid date)\' ' 
                  'and data_value not like \'%%[%%\' '
                  'and LENGTH(data_value) = 40 order by c) as unixtime) as wifidata '
                  'inner join _timeslot_to_placeid on wifidata.unique_index = _timeslot_to_placeid.unique_index '
                  'group by wifidata.unique_index, wifidata.sssid order by wifidata.unique_index;') % (SLOT_IN_MINUTES, SLOT_IN_MINUTES, user_id)
        result = dbHandler.select(query)
        matrix = numpy.array(result)
                   
        if result:
            mask = [matrix[:,0] == x for x in numpy.unique(numpy.array(stored_wifi_ssids))]
            mask = sum(mask) == 0
            
            wifi_scans_ssid_array = numpy.array(matrix[mask,0])
            wifi_scans_unique_index_array = numpy.array(matrix[mask,1]).astype(long)
            wifi_scans_place_db_ids = numpy.array(matrix[mask,2]).astype(long)
            
            unique_ssids = numpy.unique(wifi_scans_ssid_array)
            unique_ssids_place_id = [mode(wifi_scans_place_db_ids[wifi_scans_ssid_array == x])[0][0] for x in unique_ssids]     
            

            ## ASSIGN WIFI OBSERVED NETWORK IDS TO PLACE IDS
            fields = ['user_id','place_id', 'network_id', 'is_connected', 'is_wifi']
                    
            result = numpy.empty((len(unique_ssids), 5), dtype='|S48')
            result[:,0] = user_id
            result[:,1] = unique_ssids_place_id
            result[:,2] = unique_ssids
            result[:,3] = 0
            result[:,4] = 1
            
            dbHandler = Get_DB_Handler() 
            dbHandler.insertMany('_network_to_placeid', fields, result)
            
            stored_wifi_ssids.extend(numpy.unique(wifi_scans_ssid_array))
            
        print 'WIFI OBSERVED DONE USER: %s | time: %s' % (user_id, time.time() - start)
        
        ## GET ALL UNIQUE WI-FI APs  
        dbHandler = Get_DB_Handler()
        query = ('select distinct(data_value) from wifi where '
                 'data_key LIKE \'wifi|scan|%%\' ' 
                 'and user_id = %s ' 
                 'and wall_clock != \'(invalid date)\' ' 
                 'and data_value not like \'%%[%%\' '
                 'and LENGTH(data_value) = 40;') % (user_id)
        result = dbHandler.select(query)
        matrix = numpy.array(result)
                   
        if result:
            mask = [matrix[:,0] == x for x in numpy.unique(numpy.array(stored_wifi_ssids))]
            mask = sum(mask) == 0
            
            unobserved_wifi_ssids = numpy.array(matrix[mask,0])

            ## ASSIGN UNOBSERVED WIFI NETWORK IDS TO PLACE IDS
            fields = ['user_id','place_id', 'network_id', 'is_connected', 'is_wifi']
                    
            result = numpy.empty((len(unobserved_wifi_ssids), 5), dtype='|S48')
            result[:,0] = user_id
            result[:,1] = IN_TRANSIT
            result[:,2] = unobserved_wifi_ssids
            result[:,3] = 0
            result[:,4] = 1
            
            dbHandler = Get_DB_Handler() 
            dbHandler.insertMany('_network_to_placeid', fields, result)
        
        print 'WIFI TRANSITION DONE USER: %s | time: %s' % (user_id, time.time() - start)

        ########### CELLULAR ################                
        ## SEARCH FOR ALL CELL ENTRIES FOR THE CURRENT TIMESLOT    
        dbHandler = Get_DB_Handler()           
        query = ('select wifidata.sssid, wifidata.unique_index, _timeslot_to_placeid.place_id from ' 
        '(SELECT unixtime.data_key as ddata_key, unixtime.data_value as sssid, ' 
        '(CONCAT(FROM_UNIXTIME(unixtime.c, \'%%Y%%m%%d\'), LPAD(FLOOR(FROM_UNIXTIME(unixtime.c, \'%%i\') / %s) + FLOOR(FROM_UNIXTIME(unixtime.c, \'%%H\') * (60 / %s)), 2, 0))) as unique_index ' 
        'from (SELECT UNIX_TIMESTAMP(STR_TO_DATE(substring_index(wall_clock,\'.\',1), \'%%Y-%%m-%%dT%%H:%%i:%%s\')) as c, ' 
              'data_key, data_value ' 
              'from phone where (data_key = \'phone|celllocation|cid\' or data_key = \'phone|celllocation|basestation\') ' 
              'and user_id = %s ' 
              'and wall_clock != \'(invalid date)\' ' 
              'order by c) as unixtime) as wifidata '
              'inner join _timeslot_to_placeid on wifidata.unique_index = _timeslot_to_placeid.unique_index '
              'group by wifidata.unique_index, wifidata.sssid order by wifidata.unique_index;') % (SLOT_IN_MINUTES, SLOT_IN_MINUTES, user_id)
        result_connected = dbHandler.select(query)
        matrix_connected = numpy.array(result_connected)
        
        if result_connected:
            wifi_scans_ssid_array = numpy.array(matrix_connected[:,0])
            wifi_scans_unique_index_array = numpy.array(matrix_connected[:,1]).astype(long)
            wifi_scans_place_db_ids = numpy.array(matrix_connected[:,2]).astype(long)
            
            unique_ssids = numpy.unique(wifi_scans_ssid_array)
            unique_ssids_place_id = [mode(wifi_scans_place_db_ids[wifi_scans_ssid_array == x])[0][0] for x in unique_ssids]     

            ## ASSIGN CELL OBSERVED NETWORK IDS TO PLACE IDS
            fields = ['user_id','place_id', 'network_id', 'is_connected', 'is_wifi']
                    
            result = numpy.empty((len(unique_ssids), 5), dtype='|S48')
            result[:,0] = user_id
            result[:,1] = unique_ssids_place_id
            result[:,2] = unique_ssids
            result[:,3] = 0
            result[:,4] = 0
            
            dbHandler = Get_DB_Handler() 
            dbHandler.insertMany('_network_to_placeid', fields, result)  
            
            stored_cell_ids.extend(numpy.unique(wifi_scans_ssid_array))
            
        print 'CELL OBSERVED DONE USER: %s | time: %s' % (user_id, time.time() - start)
        
        ## GET ALL UNIQUE CELL IDs APs  
        dbHandler = Get_DB_Handler()
        query = ('select distinct(data_value) from phone where '
                 '(data_key = \'phone|celllocation|cid\' or data_key = \'phone|celllocation|basestation\') ' 
                 'and user_id = %s ' 
                 'and wall_clock != \'(invalid date)\' ' 
                 ';') % (user_id)
        result = dbHandler.select(query)
        matrix = numpy.array(result)
                   
        if result:
            mask = [matrix[:,0] == x for x in numpy.unique(numpy.array(stored_cell_ids))]
            mask = sum(mask) == 0
            
            unobserved_wifi_ssids = numpy.array(matrix[mask,0])

            ## ASSIGN UNOBSERVED CELLULAR NETWORK IDS TO PLACE IDS
            fields = ['user_id','place_id', 'network_id', 'is_connected', 'is_wifi']
                    
            result = numpy.empty((len(unobserved_wifi_ssids), 5), dtype='|S48')
            result[:,0] = user_id
            result[:,1] = IN_TRANSIT
            result[:,2] = unobserved_wifi_ssids
            result[:,3] = 0
            result[:,4] = 0
            
            dbHandler = Get_DB_Handler() 
            dbHandler.insertMany('_network_to_placeid', fields, result)
    
        
def Get_DB_Handler():
    
    return Database_Handler("localhost", 3306, "root", "root", "DATABASE_NAME")

if __name__ == "__main__":
    
    run_algorithm() 
    
    
    