#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
import codecs
import json

import Cleaning_OSM_Data_Project as cl

#OSMFILE_FOLDER = "C:\Users\Dylan\Downloads\Large Files\\"
OSMFILE_FOLDER = ""
#OSMFILE = "brisbane_australia.osm"
OSMFILE = "sample_brisbane_50.osm"

def process_map(file_in, pretty = False):
    file_out = "{0}.json".format(file_in.split(".")[0])
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = cl.shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data

data = process_map(OSMFILE_FOLDER + OSMFILE, True)
pprint.pprint(data[-1])
