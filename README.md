# MoveToDatabase
MoveToDatabase aim to handle dirty database data that searched by internet or collected from Social Engineering
tools. Handling this dirty data and moving correctly data line to your own database like Mysql.

For example, a csv sensitive data with 20,000,000 lines searched by internet, you want to take it to you own
Mysql database. Exactly, this csv file has lots of dirty data line, so this script can help you log all of
dirty data lines that inserted into database failed and in order to correct and manual insert it into your
database.

The logs folder contain error.log and debug.log files, all of dirty data that inserted into database failed
in error.log and you can insert it into your database manual.

## Feature
1. Based on config file.

    you can modify lib/settings.py file to configure your own database and file path.

2. Support multi process.

    Based on multiprocessing module accelerate handle those file just type -f or --fast option simple.

3. Support log level

    including debug log and error log, and the log has 1-3 level, select high log level can print more
    details information about handling progress.

4. Progress bar

    support progress bar help you master the handing time.

## Installation
Support Python3 only.

### Python3
```
git clone https://github.com/Lunpopo/MoveToDatabase.git
cd MoveToDatabase
pip3 install -r requirement.txt
python3 move_to_database.py -h
```

## Usage and Argument options
```
usage: move_to_database.py [-h] [-i CSV-FILE-PATH] [-f] [--log-level INT]
                           [--skip-error] [--drop-table TABLE-NAME] [-s]
                           [--clean-cache]

helper arguments:
  -h, --help            show this help message and exit

Mandatory module:
  arguments that have to be passed for the program to run

  -i CSV-FILE-PATH, --csv-to-database CSV-FILE-PATH
                        insert data that extracted from csv file to MYSQL
                        database

Options module:
  set up parameter to control this program or control log or more details
  output

  -f, --fast            hit -f or --fast option to toggle fast mode with
                        multiprocessing to handle big csv data or others big
                        file, multiprocessing amount is that according to your
                        computer CPU core amount. for example intel i7 CPU is
                        4 core so multiprocessing amount is 4
  --log-level INT       set output log level, default level is 1. for high
                        level can record more details log information and the
                        biggest level is 3
  --skip-error          toggle skip error mode that skip unimportant error
                        information, for example insert error information. and
                        if you want to look through more details information
                        and more error logs, please go to logs/ folder and
                        check it out.

Database module:
  database operation option and whether turn on verbose mode to output more
  details info

  --drop-table TABLE-NAME
                        input table name you want to drop and this option is
                        very dangerous so more carefully to take this option
  -s, --show-config     show database config from lib/settings.py file and you
                        can modify this config for yourself database

Clean module:
  clean redundant cache

  --clean-cache         clean log files
```

## Example
temporary close

## Author
[Lunpopo](https://github.com/Lunpopo/MoveToDatabase)

You can modify and redistribute this script following GNU License and see alse or more details about GNU
License to look through LICENSE file in this repository.

If you have good suggestion or good idea to improve this script, welcome to contact me on Github, Thanks a lot.
