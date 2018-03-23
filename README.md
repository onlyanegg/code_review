Description:
----

Run text\_to\_sql.py to store data from the `./data` directory into a sqlite3
database.

Files:
----

- text\_to\_sql.py:
    - The main application. Retrieves data from text files in the `./data`
      directory and stores it in a sqlite3 DB in the format specified by the
      related CSV file in `./spec`
- test\_text\_to\_sql.py:
    - Runs a single integration test which checks that the names inserted into
      the database match their expected values.
