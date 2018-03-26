#!/usr/bin/env python

'''
Tests for text_to_sql.py
'''

# pylint: disable=bad-indentation, bad-continuation, missing-docstring
# pylint: disable=invalid-name

# Built-in libs
import unittest
import subprocess
import sqlite3
import os
import sys

# pylint: disable=import-error, wrong-import-position
# Units under test
sys.path.append('..')
from text_to_sql.text_to_sql import DataFormat
from text_to_sql.text_to_sql import Data

def main():
  unittest.main()

class test_text_to_sql(unittest.TestCase):
  '''
  An integration test
  '''

  def setUp(self):
    self.db = 'test.sqlite3'

    subprocess.call([
      '../text_to_sql/text_to_sql.py',
      '--connection', self.db,
      '--spec_dir', './test_spec',
      '--data_dir', './test_data'
    ],
      cwd=os.getcwd()
    )

    self.conn = sqlite3.connect(self.db)

  def test_testformat1_data(self):
    cursor = self.conn.cursor()

    c = cursor.execute("SELECT name FROM 'testformat1'")
    results = [result[0] for result in c]
    self.assertEqual(results, ['Foonyor', 'Barzane', 'Quuxitude'])

  def tearDown(self):
    os.unlink(self.db)

class test_DataFormat(unittest.TestCase):
  '''
  Tests for the DataFormat class
  '''

  @classmethod
  def setUpClass(cls):
    cls.data_format = DataFormat('test_spec/testformat1.csv')

  def test_table_name(self):
    self.assertEqual(self.data_format.name, 'testformat1')

  def test_columns(self):
    columns = [
      {
        'name': 'name',
        'width': '10',
        'type': 'TEXT'
      },
      {
        'name': 'valid',
        'width': '1',
        'type': 'BOOLEAN'
      },
      {
        'name': 'count',
        'width': '3',
        'type': 'INTEGER'
      }
    ]

    self.assertEqual(self.data_format.columns, columns)

class test_Data(unittest.TestCase):
  '''
  Tests for the Data class
  '''

  @classmethod
  def setUpClass(cls):
    cls.data = Data(
      'test_data/testformat1_2015-06-28.txt',
      DataFormat('test_spec/testformat1.csv')
    )

  def test_data(self):
    data = [
      ["'Foonyor'", '1', '1'],
      ["'Barzane'", '0', '-12'],
      ["'Quuxitude'", '1', '103']
    ]

    values = self.data.values()
    self.assertEqual(data, [x for x in values])

if __name__ == '__main__':
  main()
