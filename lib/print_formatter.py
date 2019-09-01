from termcolor import colored
import time
import sys


class ColorFormatter(object):
    def __init__(self):
        pass

    @staticmethod
    def fatal(string):
        sys.stdout.write(colored(string, 'red') + '\r\n')

    @staticmethod
    def error(string):
        sys.stdout.write("[{}] [{}] {}\r\n".format(
            colored(time.strftime("%H:%M:%S"), 'cyan'),
            colored("ERROR", 'red'),
            string
        ))

    @staticmethod
    def success(string):
        sys.stdout.write("[{}] [{}] {}\r\n".format(
            colored(time.strftime("%H:%M:%S"), 'cyan'),
            colored("SUCCESS", 'green'),
            string
        ))

    @staticmethod
    def info(string):
        sys.stdout.write("[{}] [{}] {}\r\n".format(
            colored(time.strftime("%H:%M:%S"), 'cyan'),
            colored("INFO", 'white'),
            string
        ))

    @staticmethod
    def warning(string):
        sys.stdout.write("[{}] [{}] {}\r\n".format(
            colored(time.strftime("%H:%M:%S"), 'cyan'),
            colored("WARNING", 'yellow'),
            string
        ))

    @staticmethod
    def prompt(string, opts='y/n', default="n"):
        """
        获取用的输入,用于后面的交互

        :param string: 传入的字符串
        :param opts: 例如填入yN
        :param default: n
        :return: 返回用户的输入
        """
        opts = list(opts)
        choice = input("[{}] [{}] {}".format(
            colored(time.strftime("%H:%M:%S"), 'cyan'),
            colored('PROMPT', 'magenta'),
            string,
            "/".join(opts)
        ))
        if choice.lower() not in [o.lower() for o in opts]:
            choice = default
        return choice.lower()