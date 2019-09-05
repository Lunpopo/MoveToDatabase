import pymysql
from lib.settings import DATABASE_USER, DATABASE_PASSWORD
from lib.print_formatter import ColorFormatter


class Database(object):
    """Operating database class, including insert, execute sql command, drop etc..."""

    def __init__(self, host, logger, database):
        self.host = host
        self.database=database
        self.logger = logger
        self.conn = pymysql.connect(host=self.host, user=DATABASE_USER, password=DATABASE_PASSWORD,
                                    database=self.database, charset='utf8')
        # logger
        self.debug_logger = self.logger.get_logger('debug_logger')
        self.error_logger = self.logger.get_logger('error_logger')
        # 初始化数据库, 返回 cursor 指针
        self.cursor = self.init_database()
        # calculate successful insert sql count
        self.insert_success_count = 0
        # calculate failed insert sql count
        self.insert_failed_count = 0
        # calculate all of insert sql count
        self.insert_total_count = 0

    def init_database(self):
        """Return cursor init database"""
        cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        return cursor

    def create_table(self, table_name, table_column_list):
        try:
            columns = [ "{} {} {} {}".format(col_name, 'varchar(300)', 'null', 'default NULL') for col_name in table_column_list ]
            # columns_str data like this ->
            # id varchar(200) null default NULL, name varchar(200) null default NULL, age varchar(200) null default NULL
            columns_str = ", ".join(columns)

            # write data in debug log
            self.debug_logger.debug('CREATE TABLE {} ({})'.format(table_name, columns_str))
            # 创建一个自增长的表
            # create_table_sql = 'create table {} (id int PRIMARY KEY AUTO_INCREMENT, {})'.format(table_name, columns_str)
            # 不使用自增长索引
            create_table_sql = 'CREATE TABLE {} ({}) CHARACTER SET utf8 COLLATE utf8_general_ci'.format(table_name, columns_str)
            self.cursor.execute(create_table_sql)
            # 执行事务
            commit_result = self.execute_commit()
            if commit_result:
                self.debug_logger.info('CREATE TABLE {} ({}) successful'.format(table_name, columns_str))
                return 'True'
            else:
                self.debug_logger.error('CREATE TABLE {} ({}) failed'.format(table_name, columns_str))
                self.error_logger.error('CREATE TABLE {} ({}) failed'.format(table_name, columns_str))
                return 'False'

        except pymysql.err.ProgrammingError as e:
            ColorFormatter.error('Create table ProgrammingError: {}'.format(e))
            self.debug_logger.error('Create table ProgrammingError: {}'.format(e))
            self.error_logger.error('Create table ProgrammingError: {}'.format(e))
            return 'False'

        except pymysql.err.InternalError as e:
            if 'already exists' in str(e):
                return 'True'
            else:
                ColorFormatter.error('Create table InternalError: {}'.format(e))
                self.debug_logger.error('Create table InternalError: {}'.format(e))
                self.error_logger.error('Create table InternalError: {}'.format(e))
                return str(e)

        except Exception as e:
            ColorFormatter.error('Create table Unknow error: {}'.format(e))
            ColorFormatter.fatal('Please contact author}')
            self.debug_logger.error('Create table Unknow error: {}'.format(e))
            self.error_logger.error('Create table Unknow error: {}'.format(e))
            return 'False'

    def insert_data(self, table_name, column_list, data_list, skip_error=False):
        """
        insert data into table 这里只执行 sql 插入语句, 但是不提交事务, 提交事务需要 调用 self.execute_commit()

        :param table_name: table name
        :param column_list: all of database table column
        :param data_list: data list
        :param skip_error: skip unimportant insert error information, default False
        :return:
        """

        insert_data_sql = 'INSERT INTO {} ({}) VALUES ({})'.format(table_name, column_list, data_list)
        try:
            self.cursor.execute(insert_data_sql)
            self.insert_success_count += 1

        except pymysql.err.ProgrammingError as e:
            if not skip_error:
                ColorFormatter.error('Insert data ProgrammingError: {}, insert data sql is: {}'.format(e, insert_data_sql))
            self.debug_logger.error('Insert data ProgrammingError: {}, insert data sql is: {}'.format(e, insert_data_sql))
            self.error_logger.error('Insert data ProgrammingError: {}, insert data sql is: {}'.format(e, insert_data_sql))
            self.insert_failed_count += 1

        except pymysql.err.InternalError as e:
            if not skip_error:
                # 数据插入 内部错误, 将写入 error_log file
                ColorFormatter.error('Insert data InternalError: {}, insert data sql is: {}'.format(e, insert_data_sql))
            self.debug_logger.error("Insert data InternalError: {}, insert data sql is: {}".format(e, insert_data_sql))
            self.error_logger.error("Insert data InternalError: {}, insert data sql is: {}".format(e, insert_data_sql))
            self.insert_failed_count += 1

        except Exception as e:
            ColorFormatter.error('Insert data Unknow error: {}, insert data sql is: {}'.format(e, insert_data_sql))
            ColorFormatter.fatal('Please contact author')
            # 写入日志文件
            self.debug_logger.error("Insert data Unknow error: {}, insert data sql is: {}".format(e, insert_data_sql))
            self.error_logger.error("Insert data Unknow error: {}, insert data sql is: {}".format(e, insert_data_sql))
            self.insert_failed_count += 1

        self.insert_total_count += 1

    def execute_commit(self):
        # 执行事务
        try:
            self.conn.commit()
            self.debug_logger.debug('execute sql commit')
            return True
        except:
            self.debug_logger.error('execute sql commit failed')
            self.error_logger.error('execute sql commit failed')
            return False

    def drop_table(self, table_name):
        """for damage database, more dangerous operation, be carefully!"""

        try:
            drop_table_sql = "DROP TABLE {}".format(table_name)
            self.cursor.execute(drop_table_sql)
            self.conn.commit()
            return True

        except pymysql.err.InternalError as e:
            ColorFormatter.error('InternalError: {}'.format(e))
            self.debug_logger.error('InternalError: {}'.format(e))
            self.error_logger.error('InternalError: {}'.format(e))
            return False

        except Exception as e:
            ColorFormatter.error('Drop table Unknow error: {}'.format(e))
            ColorFormatter.fatal('Please contact author')
            self.debug_logger.error('Drop table Unknow error: {}'.format(e))
            self.error_logger.error('Drop table Unknow error: {}'.format(e))
            return True

    def execute_command(self, cmd):
        """execute sql command, for example: show databases"""

        self.cursor.execute(cmd)
        dict_result = self.cursor.fetchall()
        return dict_result
