
from sqlalchemy import create_engine
import pandas as pd
import os

from retriever.lib.tools import choose_engine, name_matches
from retriever import SCRIPT_LIST

script_list=SCRIPT_LIST()

Home_Dir=os.path.expanduser('~')  # users Home directory

def install(dataset_name,backend='csv',conn_file=None,db_file=None,debug=False,not_cached=False):

    if dataset_name is not None:
        scripts = name_matches(script_list, dataset_name)
    else:
        raise Exception("no dataset specified.")
    if not scripts:
        print("The dataset {} isn't currently available in the Retriever".format(
            dataset_name))
        print("run 'retriever.list()' to see a list of currently available datasets")
        return

    setting_dict=create_setting_dict(dataset_name,backend,conn_file,db_file,not_cached)
    setting_dict['command'] = 'install'

    engine=choose_engine(setting_dict)

    for dataset in scripts:           #installing instructions
        print("=> Installing", dataset.name)
        try:
            dataset.download(engine, debug=debug)
            dataset.engine.final_cleanup()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(e)
            if debug:
                raise
    print("Done!")

    db_name = engine.database_name()    # name of database where dataset is downloaded
    tb_name = engine.table_name()       # name of table where dataset is downloaded
    return db_name,tb_name

 # Generates a Dictionary from user provided information or connection.conn file, this object is then passed to choose_engine() function
def create_setting_dict(dataset_name,backend='csv',conn_file=None,db_file=None,not_cached=False):
    setting_dict = {}

    setting_dict['engine'] = backend
    setting_dict['dataset'] = dataset_name
    setting_dict['not_cached'] = not_cached

    if (backend == 'sqlite'):
        if (db_file):
            setting_dict['file'] = db_file
        else:
            setting_dict['file'] = './sqlite.db'
        setting_dict['table_name'] = '{db}_{table}'         #table name format for sqlite

    elif (backend == 'csv'):
        setting_dict['table_name'] = './{db}_{table}.csv'   #table name format for csv

    elif (backend == 'json'):
        setting_dict['table_name'] = './{db}_{table}.json'  #table name format for json

    elif (backend == 'mysql' or backend == 'postgres'):
        if (conn_file):
            with open(os.path.abspath(conn_file)) as f:
                for line in f:
                    (key, val) = line.split()
                    setting_dict[key.strip()] = val.strip()

        # default settings for mysql/postgres if not present
        if ('user' not in setting_dict):
            setting_dict['user'] = 'root'
        if ('password' not in setting_dict):
            setting_dict['password'] = 'password'
        if ('host' not in setting_dict):
            setting_dict['host'] = 'localhost'
        if ('port' not in setting_dict):
            setting_dict['port'] = '8888'
        if ('database_name' not in setting_dict):
            setting_dict['database_name'] = '{db}'
        if ('table_name' not in setting_dict):
            setting_dict['table_name'] = '{db}.{table}'

    return setting_dict


def fetch(dataset_name,backend='csv',conn_file=None,db_file=None,debug=False,not_cached=False):

    setting_dict = create_setting_dict(dataset_name, backend, conn_file, db_file, not_cached)

    db_name,tb_name= install(dataset_name,backend,conn_file,db_file,debug,not_cached)  # internally calls install function and  get database and table name

    if(backend=='csv'):
        file_name=tb_name.split("/")[-1]
        return pd.read_csv(file_name)   #

    if(backend=='json'):
        file_name = tb_name.split("/")[-1]
        return pd.read_json(file_name)

    if(backend=='mysql'):
        tb_name=tb_name.split(".")[-1]
        db_engine = create_engine(backend+"+pymysql://"+setting_dict['user']+":" + setting_dict['password'] + "@"+setting_dict['host']+"/"+db_name)

    if(backend=='sqlite'):
        db_engine = create_engine('sqlite:////'+os.path.abspath(setting_dict['file']))

    return pd.read_sql(tb_name, db_engine) # returns dataframe object


def list():  # list all scripts present
    return [script.shortname for script in script_list]


