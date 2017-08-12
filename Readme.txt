"Project Submission.pdf" is the written report for this project.
"Link to Map.txt" contains the link and description for the OSM file I downloaded for this project.
"sample_brisbane_50.osm" is the 2 MB sample of my full OSM data.
"References.txt" contains links to the websites and forums I visited to help me with a few functions.
"Cleaning_OSM_Data_Project.py" is the python script I ran to step through the full .osm file, process the tags, and reformat them ready to be inserted into a JSON file
"Cleaned_OSM_Data_into_JSON_Project.py" is the python script I ran to turn my cleaned output from the full .osm file into a JSON file. In this file the variables OSMFILE_FOLDER and OSMFILE refer to the location of the file, and so for the sample, OSMFILE will need to be "sample_brisbane_50".
"All_Data_Queries_Project.py" contains the various functions I created and queries I ran on the dataset, include some diagnostic functions to check values, and other analysis functions to count unique users, for example. I tried to create generic function where I could to be easily re-useable.