import glob
import json
import re
import os
import pprint
import copy
from collections import OrderedDict
import sys

import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from util import utils

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(ROOT_DIR, 'output_json')
OUTPUT_DIR = os.path.join(ROOT_DIR, 'avg_json')

def main(input_dir=INPUT_DIR, json_out_dir=OUTPUT_DIR, profile_name='avgprofile'):
    profile_filename = profile_name+".json"
    processed_dir = os.path.join(ROOT_DIR, 'processed_profiles')
    utils.make_sure_path_exists(input_dir)
    utils.make_sure_path_exists(processed_dir)
    avgprofile = {}
    profiles = []
    pp = pprint.PrettyPrinter(indent=4)
    print("Start averaging profile runs: \ninput_dir:\t{}\nprocessed_profiles:\t{}\noutput_dir:\t{}".format(input_dir,
                                                                                           processed_dir,
                                                                                           json_out_dir))
    # load profiles in 'profiles' list
    for file in glob.glob(os.path.join(INPUT_DIR, "*")):
        profiles.append(json.load(open(file), object_pairs_hook=OrderedDict))
    if len(profiles) > 1:
        # remove datagen profile
        profiles.pop(0)
    
    avgprofile = copy.deepcopy(profiles[0]) 
    #avgprofile = json.load(open(glob.glob("./output_json/*")[0]))
    #pp.pprint(avgprofile)  
    validprofiles = True
    for ak in avgprofile.keys():
        try:
            for pk in avgprofile[ak].keys():
                if pk != "RDDIds" and pk != "jobs" and pk != "name" and pk != "genstage" and pk != "skipped" and pk != "cachedRDDs" and pk != "parentsIds" and pk != "numtask":
                    avgprofile[ak][pk] = [] 
            for p in profiles:
                for pk in avgprofile[ak].keys():
                    #print("stage#="+ak+", key="+pk+", name="+p[ak]["name"])
                    if pk != "RDDIds" and pk != "jobs" and pk != "name" and pk != "genstage" and pk != "skipped" and pk != "numtask":
                        avgprofile[ak][pk].append(p[ak][pk]) if pk != "cachedRDDs" and pk != "parentsIds" else False
                    else: 
                        validprofiles = validprofiles and avgprofile[ak][pk] == p[ak][pk] if pk != "cachedRDDs" and pk != "parentsIds" else validprofiles
                        if not (validprofiles) : 
                            print("profile error, stage#="+ak+", key="+pk+", name="+p[ak]["name"])
                    if pk == "cachedRDDs" or pk == "parentsIds":
                        #print("validprofile: "+str(validprofiles))
                        #print("setcompare:"+str(set(p[ak][pk]) == set(avgprofile[ak][pk])))
                        validprofiles = validprofiles and (set(p[ak][pk]) == set(avgprofile[ak][pk]) or pk == "parentsIds")
                        if not (set(p[ak][pk]) == set(avgprofile[ak][pk])) : 
                            print("profile error, stage#="+ak+", name="+pk+", setavg= "+str(set(avgprofile[ak][pk]))+", setpro= "+str(set(p[ak][pk])))
        except KeyError:
            print("key error, stage#="+ak+", name="+pk+"\n")
    #print(avgprofile)
    #pp.pprint(avgprofile)
    collprofile = copy.deepcopy(avgprofile)
    for ak in avgprofile.keys():
        try:
            for pk in avgprofile[ak].keys():
                if pk != "RDDIds" and pk != "jobs" and pk != "name" and pk != "genstage" and pk != "skipped" and pk != "cachedRDDs" and pk != "parentsIds" and pk != "numtask":
                    avgprofile[ak][pk] = np.mean(collprofile[ak][pk])
        except KeyError:
            print("key error, stage#="+ak+", key="+pk+"\n")
    #print(avgprofile)
    #pp.pprint(avgprofile)
    if not (validprofiles) : 
        print("Profiles in set are not homogeneous: please ensure they all belong to the same profiling session")
        print("Profile with average values not created")
    else:
        print("Profiles in set are valid")
        # Create json files output
        filedir = OUTPUT_DIR
        filepath = os.path.join(filedir,profile_filename)
        if not os.path.exists(filedir):
            os.makedirs(filedir)
        with open(filepath, "w") as jsonoutput:
            json.dump(avgprofile, jsonoutput, indent=4, sort_keys=False)
        print("Profile with average values created as file: "+filepath)
        profile_filename = profile_name+"_collection.json"
        filepath = os.path.join(filedir, profile_filename)
        with open(filepath, "w") as jsonoutput:
            json.dump(collprofile, jsonoutput, indent=4, sort_keys=False)
        # move profiles to processed_dir directory    
        for profile in glob.glob(os.path.join(INPUT_DIR, "*")):
            os.rename(profile, os.path.join(processed_dir, profile.split(os.path.sep)[-1]))
            
if __name__ == "__main__":
    main()
