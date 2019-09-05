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

    you can modify lib/settings.py file to configure your own default database and file path.

2. Support multi process.

    Based on multiprocessing module accelerate handle those file just type -f or --fast option simple.

3. Support log level

    including debug log and error log, and the log has 1-3 level, select high log level can print more
    details information about handling progress.

4. Progress bar

    support progress bar help you master the handing time.

5. Support take CSV file and TXT file import to your own database

	support take CSV file and TXT file only. As for other type, take them up later.

6. Providing a mysql command line interface 

	you can use -m option to run mysql command, for example: python3 move_to_database.py -m "show databases", aim to provide a fast interface.

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
usage: move_to_database.py [-h] [-i CSV-FILE-PATH] [-t TXT-FOLDER-PATH] [-f]
                           [-F FS] [-c COLUMN-LIST [COLUMN-LIST ...]]
                           [--log-level INT] [--skip-error] [-D DATABASE]
                           [-T TABLE-NAME] [-m DATABASE-CMD]
                           [--drop-table TABLE-NAME] [-s] [--clean-cache]

helper arguments:
  -h, --help            show this help message and exit

Mandatory module:
  arguments that have to be passed for the program to run

  -i CSV-FILE-PATH, --csv-to-database CSV-FILE-PATH
                        insert data that extracted from csv file to MYSQL
                        database
  -t TXT-FOLDER-PATH, --txt-to-database TXT-FOLDER-PATH
                        insert data that extracted from txt file or contain
                        txt files folder to MYSQL database

Options module:
  set up parameter to control this program or control log or more details
  output

  -f, --fast            hit -f or --fast option to toggle fast mode with
                        multiprocessing to handle big csv data or others big
                        file, multiprocessing amount is that according to your
                        computer CPU core amount. for example intel i7 CPU is
                        4 core so multiprocessing amount is 4
  -F FS, --field-separator FS
                        bond for -t options, use fs for the input field
                        separator(the value of the FS predefined variable),
                        this option like awk -F option to separate a line with
                        the FS string you specified
  -c COLUMN-LIST [COLUMN-LIST ...], --column-list COLUMN-LIST [COLUMN-LIST ...]
                        bond for -t options, provide database table column
                        name list, this option will create table in database
                        and insert data into database according this column
                        list
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

  -D DATABASE, --database DATABASE
                        designated database name custom in mysql database to
                        storage data, default database name in lib/settings.py
                        file without this option
  -T TABLE-NAME, --table-name TABLE-NAME
                        designated table name in mysql database to storage
                        data, default table name in lib/settings.py file
                        without this option
  -m DATABASE-CMD, --mysql-command DATABASE-CMD
                        providing a database interface, it will show command
                        result when you type database command with -m option,
                        for example: -m "show databases"
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
* 查看帮助信息:

	python3 move_to_database.py -h

* 查看数据库的配置: 

	python3 move_to_database.py -s

* 运行sql命令: 
	
	python3 move_to_database.py -m 'show tables' -D your_database

* 将 CSV 大文件导入database，采用多进程加快速度，进程数量取决于你的CPU的核心和线程数: 

	python3 move_to_database.py -i csv_file --skip-error -f --log-level 3

* 将 TXT 大文件导入database，暂时只支持单个TXT文件导入，多进程有空再说

	python3 move_to_database.py -t txt_file -D your_database_name -T your_table_name -F='----' -c username password

	-D 选择要导入的数据库

	-T 选择要导入的表，就算没有表也行，会自动创建一个，创建的表的结构就是 -c 指定的表结构

	-F TXT文件数据分隔符，例如 ---- 分隔 username 和 password

	-c 列名，这个必须指定，例如表的结构就是 username 和 password

## Author
[Lunpopo](https://github.com/Lunpopo/MoveToDatabase)

You can modify and redistribute this script following GNU License and see alse or more details about GNU
License to look through LICENSE file in this repository.

If you have good suggestion or good idea to improve this script, welcome to contact me on Github, Thanks a lot.
