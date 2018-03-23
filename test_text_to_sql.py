#!/usr/bin/env python

'''
Tests for text_to_sql.py
'''

# Built-in libs
import unittest
import subprocess
import sqlite3
import os

def main():
  #unittest.TextTestRunner(verbosity=2).run()
  unittest.main()

class test_text_to_sql(unittest.TestCase):
  def setUp(self):
    self.db = 'test.sqlite3'

    subprocess.call([
      './text_to_sql.py',
      '--connection', self.db,
      '--spec_dir', './test_spec',
      '--data_dir', './test_data'
    ])

    self.conn = sqlite3.connect(self.db)

  def test_testformat1_data(self):
    cursor = self.conn.cursor()

    c = cursor.execute("SELECT name FROM 'testformat1'")
    results = [result[0] for result in c]
    self.assertEqual(results, ['Foonyor', 'Barzane', 'Quuxitude'])

  def tearDown(self):
    os.unlink(self.db)

if __name__ == '__main__':
  main()
