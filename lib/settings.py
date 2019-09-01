import os

# ['database']
DATABASE_USER = 'your database user'
DATABASE_PASSWORD = 'your passcode'
DATABASE = 'testdb'
# create or insert database table name you want to insert data to
TABLE_NAME = 'test_table'

# ['path']
ROOT_PATH = os.getcwd()
LOG_PATH = os.path.join(ROOT_PATH, 'logs')
SPLIT_PATH = os.path.join(ROOT_PATH, 'split')
