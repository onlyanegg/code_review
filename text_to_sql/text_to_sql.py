#!/usr/bin/env python

'''
Insert data from `.txt` files in `./data` into a sqlite3 database following the
format in `.csv` files in `./spec`.
'''

# pylint: disable=bad-indentation, bad-continuation, missing-docstring
# pylint: disable=invalid-name, too-few-public-methods

# Built-in libs
import os
import re
import sqlite3
import argparse

def main():
  args = _parse_args()
  conn = sqlite3.connect(args.connection, factory=CustomConnection)
  cursor = conn.cursor()

  get_spec_files = _get_files_from_directory(args.spec_dir, r'\.csv$')
  for spec_file in get_spec_files():
    data_format = DataFormat(os.sep.join([args.spec_dir, spec_file]))
    get_data_files = _get_files_from_directory(
      args.data_dir, r'{}.*\.txt$'.format(data_format.name)
    )
    for data_file in get_data_files():
      data = Data(os.sep.join([args.data_dir, data_file]), data_format)
      cursor.insert(data)
      conn.commit()

def _get_files_from_directory(directory, pattern):
  '''
  Returns a function which return file names from the specified directory
  which match the specified pattern
  '''

  def wrapper():
    files = []
    for file_name in os.listdir(directory):
      if re.search(pattern, file_name):
        files.append(file_name)
    return files
  return wrapper


def _parse_args():
  parser = argparse.ArgumentParser(
    description='Insert data from text files into a SQLite3 DB'
  )
  parser.add_argument(
    '-c', '--connection', dest="connection",
    default='text_to_sql.sqlite3',
    help='Database connection string'
  )
  parser.add_argument(
    '-s', '--spec_dir', dest="spec_dir",
    default='../spec',
    help='Directory for DB specification CSV files'
  )
  parser.add_argument(
    '-d', '--data_dir', dest="data_dir",
    default='../data',
    help='Directory for data files'
  )

  return parser.parse_args()

class DataFormat(object):
  '''
  Encapsulates the data format

  'columns': [
    {
      'name': 'column_name',
      'width': column_width,
      'type': 'column_type'
    },
    ...
  ]
  '''

  def __init__(self, data_format_file):
    self.name = os.path.basename(data_format_file).replace('.csv', '')
    self.columns = []

    with open(data_format_file, 'r') as f:
      lines = f.readlines()
    for line in lines[1:]:  # The first line is just titles
      name, width, _type = line.rstrip('\n').split(',')
      self.columns.append({
        'name': name,
        'width': width,
        'type': _type
      })

class Data(object):
  '''
  Stores data from data files. Takes a path to a data file and a DataFormat
  object
  '''

  def __init__(self, data_file, data_format):
    self.data_file = data_file
    self.data_format = data_format

  def values(self):
    '''
    Generator which yields a list of values to be passed to the cursor's insert
    method
    '''

    with open(self.data_file, 'r') as f:
      line = f.readline()
      while line:
        yield self._split_row(line)
        line = f.readline()

  def _split_row(self, row):
    '''
    Split a line from the data file into a list following the data format
    '''

    row_list = []
    for column in self.data_format.columns:
      width = int(column['width'])

      value = row[:width].strip()
      row = row[width:]

      if column['type'] == 'TEXT':
        value = "'{}'".format(value)
      row_list.append(value)

    return row_list

class CustomConnection(sqlite3.Connection):
  '''
  Subclasses the sqlite3 connection class to instantiate a custom cursor
  '''

  def cursor(self):
    return super(CustomConnection, self).cursor(CustomCursor)

class CustomCursor(sqlite3.Cursor):
  '''
  Subclasses the sqlite3 cursor class and adds an opinionated insert method
  '''

  def insert(self, data):
    '''
    Inserts data from a Data object into the SQLite3 DB
    '''

    table_names = [
      result[0] for result in self.execute((
        "SELECT name FROM sqlite_master "
        "WHERE type='table'"
      ))
    ]
    if data.data_format.name not in table_names:
      columns = ', '.join([
        '{name} {type}({width})'.format(**column) for column
        in data.data_format.columns
      ])
      self.execute(
        'CREATE table {} ({})'.format(data.data_format.name, columns)
      )

    values = data.values()
    row = values.next()
    while row:
      self.execute((
          "INSERT INTO '{}'"
          "VALUES ({})"
        ).format(data.data_format.name, ', '.join(row))
      )
      try:
        row = values.next()
      except StopIteration:
        row = None

if __name__ == '__main__':
  main()
