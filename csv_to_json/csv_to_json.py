"""Gov Contract converter/importer.

Usage: 
    gov_contracts import <csv_input_file> <db_host> [--db_port DB_PORT] [--db_name DB_NAME] [--db_collection COLL_NAME]
    gov_contracts convert <csv_input_file> [--json_out JSON_FILE ]

Commands:
    import                      Used to import CSV data to a Mongo database.
    convert                     Used to convert CSV data to JSON and outputs it to console or specified file.

Options:
    -h, --help                  Show this screen.
    --db_port DB_PORT           Database port to be used [default: 27017].
    --db_name DB_NAME           Name of the MongoDB database where the csv data is to me imported [default: csv_to_json].
    --db_collection COLL_NAME   Name of the MongoDB collection where the csv data is to me imported [default: csv_to_json_data].
    --json_out JSON_FILE        Send output to a JSON file.

"""

from datetime import datetime
from docopt import docopt
import utils


def run_import(csv_input_file, db_host, db_port, db_name, db_collection):
    """
    Imports to the CSV data to a MongoDB database.
    """

    print('Started CSV import - {0}'.format(datetime.now()))

    collection = utils.get_collection(db_host, db_port, db_name, db_collection)

    with open(csv_input_file, encoding='utf-8', errors='ignore') as csv_file:
        reader = csv.DictReader(csv_file)

        for json_obj in create_json(reader):
            collection.insert_one(json_obj)

    print('Finished: CSV import - {0}'.format(datetime.now()))

def run_convert(csv_input_file, export_file=None):
    """
    Converts the CSV data into a JSON format. Default behaviour is to print to command line. Outputs to file if specified.
    """

    print('Started: CSV convert - {0}'.format(datetime.now()))

    with open(csv_input_file, encoding='utf-8', errors='ignore') as csv_file:
        json_records = utils.convert_file(csv_file)
        
        if export_file:
            with open(export_file, mode='w') as json_file:
                # json_file.write(json.dumps([json_obj for json_obj in json_records],sort_keys=True, indent=4))
                json_file.write(utils.create_json_strings(json_records))
        else:
            print(utils.create_json_strings(json_records))

    print('Finished: CSV convert - {0}'.format(datetime.now()))

if __name__ == '__main__':
    arguments = docopt(__doc__)

    if arguments['convert']:
        if arguments['--json_out']:
            run_convert(arguments['<csv_input_file>'], export_file=arguments['--json_out'])
        else:
            run_convert(arguments['<csv_input_file>'])
    else:
        run_import(arguments['<csv_input_file>'], arguments['<db_host>'], arguments['--db_port'], 
            arguments['--db_name'], arguments['--db_collection'])
