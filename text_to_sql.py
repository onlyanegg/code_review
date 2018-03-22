#!/usr/bin/env python

'''
Insert data from `.txt` files in `./data` into a sqlite3 database following the
format in `.csv` files in `./spec`.
'''

# Built-in libs
import os
import re
import sqlite3

# 3rd party libs
#import yaml

def main():
  #config = parse_config()
  #conn = get_db_connection(config['sqlite3_db'])
  conn = get_db_connection('text_to_sql.sqlite3')
  for spec_file in get_spec_files():
    data_format = get_data_format(spec_file)
    table = get_table(conn, data_format)
    get_data_files = get_files_from_directory(
      './data',
      '{}.*\.txt$'.format(data_format['name'])
    )
    for data_file in get_data_files():
      insert_data(conn, data_format, data_file, table)

def get_files_from_directory(directory, pattern):
  '''
  Returns a function the gets files from the specified directory which match
  the specified pattern.
  '''
  def wrapper():
    files = []
    for file_name in os.listdir(directory):
      if re.search(pattern, file_name):
        files.append(file_name)
    return files
  return wrapper

get_spec_files = get_files_from_directory('./spec', '\.csv$')
#get_data_files = get_files_from_directory('./data', '\.txt$')

def get_data_format(spec_file):
  '''
  Returns a data_format object which defines the format of the data file and
  the associated table.

  {
    'name': 'table_name',
    'columns': [
      {
        'name': 'column_name',
        'width': column_width,
        'type': 'column_type'
      },
      ...
    ]
  }
  '''


  data_format = {}
  data_format['name'] = spec_file.replace('.csv', '')
  data_format['columns'] = []

  with open('./spec/{}'.format(spec_file), 'r') as f:
    lines = f.readlines()
  for line in lines[1:]:  # The first line is just titles
    name, width, _type = line.rstrip('\n').split(',')
    data_format['columns'].append({
      'name': name,
      'width': width,
      'type': _type
    })

  return data_format

def get_table(database_connection, data_format):
  '''
  Returns the table associated with the file format. Creates it if necessary.
  '''

  cursor = database_connection.cursor()
  table_names = [result[0] for result in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")]
  if data_format['name'] not in table_names:
    cursor.execute('CREATE table {} ({})'.format(
      data_format['name'],
      ', '.join(['{name} {type}({width})'.format(**column) for column in data_format['columns']])
    ))
    
  return data_format['name']

def insert_data(database_connection, data_format, data_file, table):
  '''
  Insert data into the correct table
  '''

  cursor = database_connection.cursor()  
  with open('./data/{}'.format(data_file), 'r') as f:
    rows= f.readlines()
  for row in rows:
    columns = split_row(data_format, row)
    command = "INSERT INTO '{}' VALUES ({})".format(data_format['name'], ', '.join(columns))
    cursor.execute(command)
    database_connection.commit()
    #  "INSERT INTO '{}' VALUES ({})".format(
    #    data_format['name'],
    #    ', '.join(columns)
    #  )
    #)

def split_row(data_format, row):
  '''
  Split line from data file into a list based on the data format
  '''

  row_list = []
  for column in data_format['columns']:
    width = int(column['width'])

    value = row[:width]
    row = row[width:]

    if column['type'] == 'TEXT':
      value = "'{}'".format(value)
    row_list.append(value)

  return row_list
  
def get_db_connection(db_path):
  '''
  Returns a connection object for the sqlite3 DB
  '''

  return sqlite3.connect(db_path)  

def _parse_config(config_file):
  with open(config_file, 'r') as f:
    config = yaml.load(f)
  return config

class DataFormat(object):
  '''
  A data format object contains a name and a list of columns generated from a
  spec file
  '''

  def __init__(self, spec_file):
    self.spec_file = spec_file
    self.name = spec_file.replace('.csv', '')

  @property
  def columns(self):
    pass

if __name__ == '__main__':
  main()
