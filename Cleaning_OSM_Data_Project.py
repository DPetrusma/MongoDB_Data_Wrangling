#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
import codecs
import json

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
#This means we will match any digits and spaces, starting from the first character, as many as we can, and until the final character, so nothing else will be matched.
#This is no longer used as it is too general
number_space = re.compile(r'^[0-9 ]+$')
#For the postcode, we look for exactly 4 digits, followed by some whitespace
postcode_regex = re.compile(r'^(\d{4})(\s+)$')

#This is our list of tags to put in the "created" bucket
CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

#This is our list of expected, okay street types
expected_street_type = 	[
						"Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
						"Trail", "Parkway", "Commons", "Crescent", "Parade", "Terrace", "Way", "Circuit", "Vista", "Close",
						"Corso", "Highway", "Motorway", "Arterial",
						#I might handle these ones differently somehow
						"North", "South", "East", "West"
						]

# This is how we turn bad ones into good ones
bad_street_mapping = { "St": "Street",
						"St.": "Street",
						"Ave": "Avenue",
						"Rd.": "Road",
						"road" : "Road",
						"terrace" : "Terrace"
						}
	
def update_name(name, mapping):
    """
    Args:
        name (str): The string value to potentially corect
        mapping (dict): The dictionary containing incorrect values and their corrected version
        
    Returns:
        str: If the input string was in the dictionary, returned the corrected value, else return the original value
    
    """
	# Use out mapping dictionary to output the fixed name of a street as soon as we find it
    for m in mapping:
		if m in name:
			name = name.replace(m, mapping[m])
			break

    return name
    
def multi_value_attribute(attr_list, attr_delimiter):
    """
    Args:
        attr_list (str): The string from an attribute of a MongoDB document that should be multi-valued
        attr_delimiter (str): The character to use as a delimiter to split the string into a list
        
    Returns:
        list: A list of the input string split by the input delimiter
    """
    return attr_list.split(attr_delimiter)


def shape_element(element):
    """
    Args:
        element (element): The XML element from the iterparse function to be processed
        
    Returns:
        dict: If the element is not tagged as a "way" or "node", return None. Otherwise, return a dictionary with the tags mapped to positions and values in the dictionary
    """
    node = {}

	#Check if it's a tag we care about
    if element.tag == "node" or element.tag == "way" :
		#Step through each attribute
        for a in element.attrib:
			#Check if that attribute need to go in the CREATED element of the dictionary
			if a in CREATED:
				#First, if the "created" element doesn't exist, create an empty dictionary there
				if "created" not in node:
					node["created"] = {}
                    
				node["created"][a] = element.attrib[a]
			#If we are at the latitude or longitude, stick them in a list (this will happen twice but it's okay)
			elif a in ["lat", "lon"]:
				node["pos"] = [float(element.attrib["lat"]), float(element.attrib["lon"])]
			else:
				node[a] = element.attrib[a]
		#Show the type as an element
        node["type"] = element.tag
		#Now that we've done each attribute, step through the next-level tags
        for t in element:
            if "k" in t.attrib:
                attr_name = t.attrib["k"]
                attr_value = t.attrib["v"]
            
                #If it has problem characters, ignore it
                if problemchars.search(attr_name) != None:
                    x = 1
                    pass
				#We want to ignore the extra "street:" tags
                elif attr_name[:12] == "addr:street:":
                    x = 2
                    pass
				#If it starts with "addr:", add the value to the address dictionary
                elif attr_name[:5] == "addr:":
                    x = 3
					#First, if the "address" element doesn't exist, create an empty dictionary there
                    if "address" not in node:
                        node["address"] = {}
                        
                    #For the street name, we want to audit and clean that up first
                    if attr_name == "addr:street" and attr_value not in expected_street_type:
                        x = 4
                        #node[attr_name] = update_name(attr_value, bad_street_mapping)
                        attr_value = update_name(attr_value, bad_street_mapping)
                        
                    node["address"][attr_name[5:]] = attr_value
				#For the other attributes, just add them to the dictionary
                else:
                    #At this point, we are ignoring all the colons that may mean sub-lists, and simply using an underscore, as it will be too easy to miss the different combinations
                    attr_name = attr_name.replace(":","_")
                    #This is just for the postcode
                    r = postcode_regex.search(attr_value)
                    
                    if attr_name == "type":
                        x = 5
                        #If the attribute is "type", we don't want it to override the "type" of "way", "node", etc, so we'll change it a little
                        node["type_seconday"] = attr_value
                    elif r != None:
                        x = 6
                        #For the postcode field, we only want to match the first four numbers if there is following whitespace. This will replace the second group, the whitespace, with nothing
                        node[attr_name] = re.sub(r.group(2),"",attr_value)
                        #node[attr_name] = attr_value.replace(" ", "")
                    elif ";" in attr_value:
                        x = 7
                        #If the value contains a semi-colon, I will assume that it should be multi-valued
                        node[attr_name] = multi_value_attribute(attr_value,";")
                    elif "," in attr_value:
                        x = 8
                        #If the value contains a comma, I will assume that it should be multi-valued
                        node[attr_name] = multi_value_attribute(attr_value,",")
                    else:
                        x = 9
                        node[attr_name] = attr_value
					
			#This is for the "ways"
            if "ref" in t.attrib:
                x = 10
				#Initialise the empty list if we don't already have that element
                if "node_refs" not in node:
                    node["node_refs"] = []
				#Append the ref to it
                node["node_refs"].append(t.attrib["ref"])
                
            #This is checking where the route_ref gets "stuck" and why it wasn't processing where I expected it to
            #if t.attrib["k"] == "route_ref":
            #    print x
        return node
    else:
        return None
