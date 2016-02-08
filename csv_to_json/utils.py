import csv
import json
from pymongo import MongoClient

def get_collection(db_host, db_port, db_name, db_collection):
    client = MongoClient('mongodb://{0}:{1}/'.format(db_host, db_port))
    db = client[db_name]

    return db[db_collection]

def convert_file(csv_file):
    """
    Clean up the csv data a bit and converts it to a dictionary. Returns a generator.
    """
    reader = csv.DictReader(csv_file)

    for record in reader:
        clean_record = {}
        for k, v in record.items():
            if k:
                clean_record[k.strip().replace(" ", "_").lower()] = v.strip()
        yield clean_record

def create_json_strings(json_records):
    return json.dumps([json_obj for json_obj in json_records],sort_keys=True, indent=4)