This file gives a brief description of how to extract places from the Device Analyzer data set.

After populating 'wifi' and 'phone' database tables, the two files 'deviceanalyzer_extract_places.py' and 'deviceanalyzer_merge_places.py' allow extracting and merging relevant places.

First, execute 'deviceanalyzer_extract_places.py' to extract places.
Then execute 'deviceanalyzer_merge_places.py' to merge them.

The corresponding results are stored in one of the tables included in 'device-analyzer_place-extraction.sql'. 
The final results are stored in the table '_timeslot_to_placeid_all'.