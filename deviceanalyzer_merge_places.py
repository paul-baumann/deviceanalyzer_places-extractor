#!/usr/bin/python
# -*- coding: utf-8 -*-

##
# This file merges relevant places that were extracted 
# with the script 'deviceanalyzer_extract_places.py' 
# from Wi-Fi and cellular data of the Device Analyzer data set
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

def run_algorithm():
    
    dbHandler = Get_DB_Handler()
    query = ('SELECT distinct(user_id) from wifi')
    result = dbHandler.select(query)
    matrix = numpy.array(result)
      
    user_ids = matrix[:,0]
    
    first_user = int(sys.argv[1])
    last_user = int(sys.argv[2])
    
    user_ids = user_ids[first_user:last_user]
    
    for user_id in user_ids:
        start = time.time()
        
        min_values = []
        max_values = []

        ## GET MIN AND MAX CLOCK TIME FOR WI-FI         
        dbHandler = Get_DB_Handler()
        query = ('select min(DATE_FORMAT(STR_TO_DATE(substring_index(wall_clock,\'.\',1), \'%%Y-%%m-%%dT%%H:%%i:%%s\'), \'%%Y%%m%%d\')), '
                 'max(DATE_FORMAT(STR_TO_DATE(substring_index(wall_clock,\'.\',1), \'%%Y-%%m-%%dT%%H:%%i:%%s\'), \'%%Y%%m%%d\')) '
                 'from wifi where user_id = %s and wall_clock != \'(invalid date)\';') % (user_id)
        result = dbHandler.select(query)
        matrix = numpy.array(result)
        
        if result:
            if matrix[0,0] is not None:
                min_values.extend(matrix[:,0])
            if matrix[0,1] is not None:
                max_values.extend(matrix[:,1]) 

        ## GET MIN AND MAX CLOCK TIME FOR CELLULAR        
        dbHandler = Get_DB_Handler()
        query = ('select min(DATE_FORMAT(STR_TO_DATE(substring_index(wall_clock,\'.\',1), \'%%Y-%%m-%%dT%%H:%%i:%%s\'), \'%%Y%%m%%d\')), '
                 'max(DATE_FORMAT(STR_TO_DATE(substring_index(wall_clock,\'.\',1), \'%%Y-%%m-%%dT%%H:%%i:%%s\'), \'%%Y%%m%%d\')) '
                 'from phone where user_id = %s and wall_clock != \'(invalid date)\';') % (user_id)
        result = dbHandler.select(query)
        matrix = numpy.array(result)
        
        if result:
            a = matrix[0,0]
            if matrix[0,0] is not None:
                min_values.extend(matrix[:,0])
            if matrix[0,1] is not None:
                max_values.extend(matrix[:,1])    
            
        min_values = numpy.array(min_values).astype(long)
        max_values = numpy.array(max_values).astype(long)
        
        ## SELECT THE RANGE OF DAYS THAT CONTAIN BOTH CELLULAR AND WI-FI DATA
        first_day = datetime.datetime.strptime(str(min(min_values)), "%Y%m%d").date()
        last_day = datetime.datetime.strptime(str(max(max_values)), "%Y%m%d").date()
        unique_index_array = Util.get_unique_index_between_days(first_day, last_day, SLOT_IN_MINUTES).astype(long);
        
        places_for_all_unique_index_wifi_connected = numpy.zeros((len(unique_index_array),))
        places_for_all_unique_index_wifi = numpy.zeros((len(unique_index_array),))
        places_for_all_unique_index_cell = numpy.zeros((len(unique_index_array),))
        places_for_all_unique_index = numpy.zeros((len(unique_index_array),))

        ## DERIVE PLACES FOR TIMESLOTS BASED ON CELLULAR DATA        
        dbHandler = Get_DB_Handler()
        query = ('select inner_table.placeid, inner_table.unique_index from ('
                'select _network_to_placeid.place_id as placeid, ' 
                'count(_network_to_placeid.place_id) as freq_placeid, ' 
                '(CONCAT(SUBSTRING(wall_clock,1,4), SUBSTRING(wall_clock,6,2), SUBSTRING(wall_clock,9,2), '
                'LPAD((SUBSTRING(wall_clock,12,2) * (60 / %s)) + (FLOOR(SUBSTRING(wall_clock,15,2) / %s)), 2, 0))) as unique_index ' 
                'from phone inner join _network_to_placeid on phone.data_value = _network_to_placeid.network_id and phone.user_id = _network_to_placeid.user_id '
                'where phone.user_id = %s and _network_to_placeid.is_wifi = 0 and wall_clock != \'(invalid date)\' group by unique_index, placeid order by freq_placeid DESC '
                ') as inner_table group by inner_table.unique_index;') % (SLOT_IN_MINUTES, SLOT_IN_MINUTES, user_id)
                
        result = dbHandler.select(query)
        matrix = numpy.array(result)
        
        if result:
            cell_places = matrix[:,0].astype(long)
            cell_unique_index = matrix[:,1].astype(long)
            
            mask = Util.ismember(cell_unique_index, unique_index_array)
            not_nan_mask = ~numpy.isnan(mask)
            mask = numpy.array(mask)[not_nan_mask]
            
            places_for_all_unique_index[mask] = cell_places[not_nan_mask]
            places_for_all_unique_index_cell[mask] = cell_places[not_nan_mask]

        print 'CELL DATA -- DONE USER: %s | time: %s' % (user_id, time.time() - start)
        
        ## DERIVE PLACES FOR TIMESLOTS BASED ON WI-FI DATA        
        dbHandler = Get_DB_Handler()
        query = ('select inner_table.placeid, inner_table.unique_index from ('
                'select _network_to_placeid.place_id as placeid, ' 
                'count(_network_to_placeid.place_id) as freq_placeid, ' 
                '(CONCAT(SUBSTRING(wall_clock,1,4), SUBSTRING(wall_clock,6,2), SUBSTRING(wall_clock,9,2), '
                'LPAD((SUBSTRING(wall_clock,12,2) * (60 / %s)) + (FLOOR(SUBSTRING(wall_clock,15,2) / %s)), 2, 0))) as unique_index ' 
                'from wifi inner join _network_to_placeid on wifi.data_value = _network_to_placeid.network_id and wifi.user_id = _network_to_placeid.user_id '
                'where wifi.user_id = %s and _network_to_placeid.is_wifi = 1 and wall_clock != \'(invalid date)\' group by unique_index, placeid order by freq_placeid DESC '
                ') as inner_table group by inner_table.unique_index;') % (SLOT_IN_MINUTES, SLOT_IN_MINUTES, user_id)
    
        result = dbHandler.select(query)
        matrix = numpy.array(result)
        
        if result:
            wifi_places = matrix[:,0].astype(long)
            wifi_unique_index = matrix[:,1].astype(long)
            
            mask = Util.ismember(wifi_unique_index, unique_index_array)
            not_nan_mask = ~numpy.isnan(mask)
            mask = numpy.array(mask)[not_nan_mask]
            
            places_for_all_unique_index[mask] = wifi_places[not_nan_mask]
            places_for_all_unique_index_wifi[mask] = wifi_places[not_nan_mask]
        
        print 'WI-FI DATA -- DONE USER: %s | time: %s' % (user_id, time.time() - start)
        
        ## DERIVE PLACES FOR TIMESLOTS BASED ON WI-FI CONNECTED DATA        
        dbHandler = Get_DB_Handler()
        query = ('select place_id, unique_index from _timeslot_to_placeid where user_id = %s;') % (user_id)
    
        result = dbHandler.select(query)
        matrix = numpy.array(result)
        
        if result:
            wifi_connected_places = matrix[:,0].astype(long)
            wifi_connected_unique_index = matrix[:,1].astype(long)
            
            mask = Util.ismember(wifi_connected_unique_index, unique_index_array)
            not_nan_mask = ~numpy.isnan(mask)
            mask = numpy.array(mask)[not_nan_mask]
            
            places_for_all_unique_index[mask] = wifi_connected_places[not_nan_mask]
            places_for_all_unique_index_wifi_connected[mask] = wifi_connected_places[not_nan_mask]
        
        print 'WI-FI CONNECTED DATA -- DONE USER: %s | time: %s' % (user_id, time.time() - start)
        
        ## ASSIGN PLACE IDS TO TIMESLOTS BASED ON ALL DATA
        fields = ['user_id','unique_index','place_id_wifi_connected','place_id_wifi','place_id_cell','place_id_all_combined']
                
        result = numpy.empty((len(unique_index_array), 6))
        result[:,0] = user_id
        result[:,1] = unique_index_array
        result[:,2] = places_for_all_unique_index_wifi_connected
        result[:,3] = places_for_all_unique_index_wifi
        result[:,4] = places_for_all_unique_index_cell
        result[:,5] = places_for_all_unique_index
        
        dbHandler = Get_DB_Handler() 
        dbHandler.insertMany('_timeslot_to_placeid_all', fields, result)
        
        print 'DONE USER: %s | time: %s' % (user_id, time.time() - start)
        
        
def Get_DB_Handler():
    
    return Database_Handler("localhost", 3306, "root", "root", "DATABASE_NAME")


if __name__ == "__main__":
    
    run_algorithm() 
    
    
    