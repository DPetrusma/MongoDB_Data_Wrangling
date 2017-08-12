#!/usr/bin/env python
import pprint as pp
from pymongo import MongoClient as MC
import re

problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\.\t\r\n\|]')
non_digit = re.compile(r'\D')
post_code_regex = re.compile(r'^\d{4}$')
#post_code_regex = '/^\d{4}$/'

def get_db(db_name):
    """
    Args:
        db_name (str): The name of the MongoDB database to use
        
    Returns:
        pymongo.database.Database: A database object with the name of the input from the local server
    
    """
        
    client = MC('localhost:27017')
    db = client[db_name]
    return db

def generic_aggregate(attribute_to_group, findNone=True, limit=None, sort_order=1):
    """
    Args:
        attribute_to_group (str): The name of the attribute to group all of the docuements by
        findNone (bool): Whether or not we include those documents where the attribute does not exist
        limit (int): How many records to display after the aggregation
        sort_order (int): 1 or -1, determining whether we sort ascending or descening in out output
        
    Returns:
        list: A list of dictionaries that form our aggregation pipeline to group by the attribute in the input
    
    """
    if findNone == True:
        aggregate = [
                    { "$group" : { "_id" : "$" + attribute_to_group, "count" : { "$sum" : 1 } } },
                    { "$sort" : { "count" : sort_order } }
                    ]
    else:
        aggregate = [
                    { "$match" : { attribute_to_group : { "$exists" : True } } },
                    { "$group" : { "_id" : "$" + attribute_to_group, "count" : { "$sum" : 1 } } },
                    { "$sort" : { "count" : sort_order } }
                    ]
                    
    if limit != None:
        aggregate.append( { "$limit" : limit } )
    return aggregate
    
def  find_public_transport():
    """
    Returns:
        list: A list of dictionaries that form our aggregation pipeline to match documents with a "transport_zone" and group by that attribute
    
    """
    pt_condition = [
                    { "$match" : { "transport_zone" : { "$exists" : True } } },
                    { "$group" : { "_id" : "$transport_zone", "count" : { "$sum" : 1 } } },
                    { "$sort" : { "_id" : 1 } }
                    ]
                    
    return pt_condition
    
def find_attribute_unwind(attribute_to_group):
    """
    Args:
        attribute_to_group (str): The name of the attribute to group all of the documents by
        
    Returns:
        list: A list of dictionaries that form our aggregation pipeline to group by the attribute in the input after it has been unwound
    
    """
    c = [
        { "$match" : { attribute_to_group : { "$exists" : True } } },
        { "$unwind" : "$" + attribute_to_group },
        { "$group" : { "_id" : "$" + attribute_to_group, "list" : { "$sum" : 1 } } },
        { "$sort" : { "count" : 1 } }
        ]
        
    return c
    
def number_of_routes():
    """
    Returns:
        list: A list of dictionaries that form our aggregation pipeline to match documents with a list in the "route_ref" attribute and average the length of those lists
    
    """
    #Since we can't use the $type to directly check if a field is an array, we indirectly test it
    #by looking for the existance of the first element, i.e., route_ref.0
    c = [
        { "$match" : { "route_ref.0" : { "$exists" : True } } },
        { "$project" : { "_id" : "$_id", "route_ref" : "$route_ref", "number_routes" : { "$size" : "$route_ref" } } },
        { "$group" : { "_id" : 1, "average_number_of_routes" : { "$avg" : "$number_routes" } } }
        ]
    
    return c
    
def percentage_on_many_routes():
    """
    Returns:
        list: A list of dictionaries that form our aggregation pipeline to match documents with a "route_ref" attribute, count those with a list and those without a list in that attribute, and find the percentage of documents with a list in "route_ref"
    
    """
    #Source for my code below
    #http://stackoverflow.com/questions/22819303/mongodb-aggregation-divide-computed-fields
    #http://stackoverflow.com/questions/14102596/conditional-sum-in-mongodb
    c = [
        { "$match" : { "route_ref" : { "$exists" : True } } },
        { "$project" : { "_id" : "$_id", "route_ref_type" : { "$type" : "$route_ref" } } },
        { "$group" : {
                    "_id" : "route_ref_type",
                    "one_route" :
                    #All the routes that are of type string, meaning only one value in this case
                        { "$sum" : { "$cond" : [ {"$eq" : ["$route_ref_type", "string"]}, 1, 0] } },
                    "many_route" :
                    #All the routes that are not strings, i.e., they are arrays
                        { "$sum" : { "$cond" : [ {"$ne" : ["$route_ref_type", "string"]}, 1, 0] } },
                    "total_routes" :
                        { "$sum" : 1 }
                    }
        },
        { "$project" : { "_id" : 1, "ratio_with_many_routes" : { "$divide" : [ "$many_route", "$total_routes" ] } } }
        ]
    
    return c
        
    
def count_distinct_attribute(attribute_to_count):
    """
    Args:
        attribute_to_count (str): The name of the attribute to find distinct values for
        
    Returns:
        list: A list of dictionaries that form our aggregation pipeline to count the distinct values of the attribute from the input
    
    """
    #Source for my code
    #http://stackoverflow.com/questions/11782566/mongodb-select-countdistinct-x-on-an-indexed-column-count-unique-results-for
    c = [
        { "$match" : { attribute_to_count : { "$exists" : True } } },
        { "$group" : { "_id" : "$" + attribute_to_count } },
        { "$group" : { "_id" : 1, "count" : { "$sum" : 1 } } }
        ]
        
    return c
    
def find_problem_characters_in_value(field_name):
    """
    Args:
        field_name (str): The name of the attribute to look in for problem characters
        
    Returns:
        list: A list of dictionaries that form our aggregation pipeline to unwind the attribute from the input and match a regular expression to find problem characters
    
    """
    c = [
        { "$unwind" : "$" + field_name },
        { "$match" : { field_name : { "$regex" : problemchars } } },
        { "$group" : { "_id" : "$" + field_name } }
        ]
        
    return c
        
def print_aggregate(db, grouping):
    """
    Args:
        db (pymongo.database.Database): The MongoDB database object to query with our aggregation
        grouping (list): The list of dictionaries containing the desired aggregation pipeline
        
    Returns:
        list: A list of documents from the query with our aggregation pipeline in the "brisbane" collection on the given database
    
    """
    return [doc for doc in db.brisbane.aggregate(grouping)]
                    

if __name__ == '__main__':
    #First of all, we need to create the database object with the right MongoDB database
    db = get_db('project')
    
    #First, check if any postcode, which I know should be only 4 digits, has any bad values
    post_code_check = [
                        { "$match" : { "postcode" : { "$exists" : True } } },
                        { "$match" : { "postcode" : { "$not" : post_code_regex } } }
                        ]
    pp.pprint(print_aggregate(db, post_code_check))
    
    #These are some of the attributes I grouped by for some simple anlaysis
    grouping_1 = generic_aggregate("type", True)
    grouping_2 = generic_aggregate("created.user", True)
    grouping_3 = generic_aggregate("source", True)
    grouping_4 = generic_aggregate("created_by", True)
    grouping_5 = generic_aggregate("address.postcode", True)
    grouping_6 = generic_aggregate("maxspeed", False)
    grouping_7 = generic_aggregate("created.user", False, 1, -1)
    grouping_8 = generic_aggregate("amenity", False, 5, -1)
    grouping_9 = generic_aggregate("cuisine", False, 3, -1)
    grouping_10 = generic_aggregate("denomination", False)
    grouping_11 = generic_aggregate("religion", False)
    grouping_12 = generic_aggregate("source", False, 1, -1)
    
    #Only print one as an example
    pp.pprint(print_aggregate(db, grouping_1))
    
    #Printing documents with a "transport_zone" attribute
    pp.pprint(print_aggregate(db, find_public_transport()))
    
    #Some simple statistics - total number of documents, as well as number of "ways" and "nodes"
    print "Number of documents: " + str(db.brisbane.find().count())
    print "Number of ways: " + str(db.brisbane.find({ "type" : "way" }).count())
    print "Number of nodes: " + str(db.brisbane.find({ "type" : "node" }).count())
    
    #More simple statistics - the number of unique users, calculated two different ways
    print "Number of Unique Users (method 1): " + str(len(db.brisbane.distinct("created.user")))
    print "Number of Unique Users (method 2): " + str(print_aggregate(db, count_distinct_attribute("created.user")))
    
    #This was a part of my check of the "type" field that was overwritten
    pp.pprint(db.brisbane.find_one({"type" : "water" } ))
	
    #I was interested in the statistics on the bus routes, so here I calculate the average number of routes for multi-route nodes, as well as the percentage of nodes (that have a route) on multiple routes
    pp.pprint(print_aggregate(db, number_of_routes()))
    pp.pprint(print_aggregate(db, percentage_on_many_routes()))
    
    #The "amenity" attribute was the one I thought most likely to contain problem characters
    pp.pprint(print_aggregate(db, find_problem_characters_in_value("amenity")))