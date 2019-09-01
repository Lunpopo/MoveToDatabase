# encoding: utf8
#!/usr/bin/env python3
import sys
from termcolor import colored


class ProgressBar(object):
    """
    Handle progress bar class, be used for generator or iterator object

    Usage example:
    '''
    count = 115

    progress_bar = ProgressBar(total_line=count, done_info='OK')

    for i in range(count):
        progress_bar.handle_progress()
        time.sleep(0.01)
    '''
    """
    # current progress
    __current_progress = 1
    # progress bar length
    __progress_len = 40

    # 初始化函数，需要知道总共的处理次数
    def __init__(self, total_line, done_info=None, description=None):
        self.total_line = total_line
        self.done_info = done_info
        self.description = description

    def handle_progress(self):
        # calculate number of progress bar string
        progress_char_count = int(self.__current_progress * self.__progress_len / self.total_line)
        # calculate number of not handle bar string
        not_progress_char_count = self.__progress_len - progress_char_count
        # work progress percent
        progress_percent = self.__current_progress * 100.0 / self.total_line

        # flush progress bar style every time
        if self.description:
            progress_bar_str = "{} [{}{}] {:.2f}% ({}/{})\r".format(
                self.description,
                '#'*progress_char_count,
                '-'*not_progress_char_count,
                progress_percent,
                self.__current_progress,
                self.total_line
            )
        else:
            progress_bar_str = "[{}{}] {:.2f}% ({}/{})\r".format(
                '#'*progress_char_count,
                '-'*not_progress_char_count,
                progress_percent,
                self.__current_progress,
                self.total_line
            )

        # output progress bar
        sys.stdout.write(colored(progress_bar_str, 'white'))
        sys.stdout.flush()

        if self.__current_progress == self.total_line:
            print('')
            if self.done_info:
                print(self.done_info)
            self.__current_progress = 1

        self.__current_progress += 1

    def handle_multiprocessing_progress(self, current_progress):
        # calculate number of progress bar string
        progress_char_count = int(current_progress * self.__progress_len / self.total_line)
        # calculate number of not handle bar string
        not_progress_char_count = self.__progress_len - progress_char_count
        # work progress percent
        progress_percent = current_progress * 100.0 / self.total_line

        # flush progress bar style every time
        if self.description:
            progress_bar_str = "{} [{}{}] {:.2f}% ({}/{})\r".format(
                self.description,
                '#'*progress_char_count,
                '-'*not_progress_char_count,
                progress_percent,
                current_progress,
                self.total_line
            )
        else:
            progress_bar_str = "[{}{}] {:.2f}% ({}/{})\r".format(
                '#'*progress_char_count,
                '-'*not_progress_char_count,
                progress_percent,
                current_progress,
                self.total_line
            )

        # output progress bar
        sys.stdout.write(colored(progress_bar_str, 'white'))
        sys.stdout.flush()

        if current_progress == self.total_line:
            print('')
            if self.done_info:
                print(self.done_info)
