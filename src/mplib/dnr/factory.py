# coding: utf-8
# __author__: u"John"
from __future__ import with_statement
from os import path
import re


# region 属性访问定义
class AttributeDict(dict):
    """
    能够把dict的key当作class的attribute
    """
    def __getattr__(self, attr):
        return self[attr]

    def __setattr__(self, attr, value):
        self[attr] = value
# endregion


# region 可复用基本类
class BaseReducer(object):
    """
    变量初始化设定是可以复用的代码
    """
    def __init__(self):
        self.code_message = AttributeDict(code=0, message=u"")
        self.dict_abspath_list = []  # 预留词库类文件批量处理控制变量
        self.current_dict_abspath = None  # 当前正在处理词库的文件
        self.data_abspath_list = []  # 预留数据类文件批量处理控制变量
        self.current_data_abspath = None  # 当前正在处理数据的文件
        self.file_extension = None  # 当前文件的扩展名
        self.export_name = None  # 导出文件的命名
        self.export_head = None  # 导出文件的列名
        self.id_column_index = None  # 指定需要处理的列索引号，起始号为0
        self.data_column_index = None  # 数据有效列标志位
        self.keywords_list = []  # 关键词词库
        self.current_keywords = None  # 正在使用的关键词或正则表达式
        self.result_list = []  # 匹配到关键词的水帖列表，用于关键词正确性检验
        self.current_result = None  # 当前正在使用的关键词
        self.error_list = []  # 用于异常处理的错误列表
        self.current_error = None  # 当前错误信息
        self.show_process = False  # 是否显示关键词匹配情况
        self.raw_list = []  # 处理前的数据
        self.current_string = None  # 正在处理的字符串
        self.trash_list = []  # 水军数据
        self.cleaned_list = []  # 去水之后的数据
        return

    @staticmethod
    def get_file_extension(file_path):
        return path.splitext(file_path)[1][1:]

    @staticmethod
    def get_file_name(file_path):
        return path.splitext(path.basename(file_path))[0]

    @staticmethod
    def one_column_indexer(data_list):
        """
        将单列的数据变成带序号的数据
        :param data_list:  list(unicode)
        :return:  list(tuple(index, unicode))
        """
        ret = []
        for i in xrange(len(data_list)):
            ret.append((i + 1, data_list[i]))
        return ret
# endregion


# region 关键词去水
class KeywordsReducer(BaseReducer):
    """
    利用关键词去水的工具，支持关键词是正则表达式的情况
    """
    def __init__(self):
        BaseReducer.__init__(self)
        self.one_hit_strategy = True

    def get_contents(self):
        """
        可以通过外部赋值设置值，也可以通过别的方法实现
        数据库的操作后将数据赋值给 self.raw_list
        :return:
        """
        if self.raw_list:
            return
        else:
            pass
        if self.current_data_abspath:
            self.file_extension = self.get_file_extension(self.current_data_abspath)
            self.export_name = self.get_file_name(self.current_data_abspath)
            if self.file_extension == u"txt":
                try:
                    f = open(self.current_data_abspath, u"rb")
                except IOError as e:
                    self.current_error = str(e)
                    self.code_message.code = 1
                    self.code_message.message = u"get_contents open(file) IOError"
                    self.error_list.append(self.current_error)
                else:
                    with f:
                        for line in f:
                            line = line.decode(u"utf-8").replace(u"\r", u"").replace(u"\n", u"").split(u"\t")
                            self.raw_list.append(tuple(line))
                        self.code_message.message = u"data file import successfully"
        else:
            self.raw_list = [(1, u"关键词"), (2, u"这是一个正"), (3, u"这是一个藏得很深的水贴，你检测不出来"),
                             (4, u"则表达式呢")]
        return

    def get_keywords(self):
        """
        数据库的操作后将数据赋值给 self.keywords_list
        :return:
        """
        if self.keywords_list:
            return
        else:
            pass
        if self.current_dict_abspath:
            self.export_name = u"{0}-{1}".format(self.export_name , self.get_file_name(self.current_dict_abspath))
            try:
                f = open(self.current_dict_abspath, u"rb")
            except IOError as e:
                self.current_error = str(e)
                self.code_message.code = 1
                self.code_message.message = u"get_keywords open(file) IOError"
                self.error_list.append(self.current_error)
            else:
                with f:
                    self.keywords_list += f.read().decode(u"utf-8").split(u"\r\n")
                    self.code_message.message = u"keywords dictionary import successfully"
        else:
            self.keywords_list = [u"关键词", u"(正|则|表|达|式)"]
        return

    def keywords_finder(self):
        """
        python的正则表达式可以直接支持中文
        :return:
        """
        try:
            pattern = re.compile(pattern=self.current_keywords, flags=0)
        except Exception as e:
            self.current_error = str(e)
            self.code_message.code = 2
            self.code_message.message = u"keywords_finder re.compile error"
            self.error_list.append(self.current_error)
            self.current_result = []
        else:
            self.current_result = re.findall(pattern, self.current_string)
        if self.show_process:
            if self.current_result:
                print u"找到关键词\n{0}".format(self.current_keywords)
                for r in self.current_result:
                    print r
            else:
                # print u"没有找到关键词\n{0}".format(keywords)
                pass
        else:
            pass
        return

    def main(self):
        """
        复杂应用才需要Override这部分
        :return:
        """
        self.get_contents()
        self.get_keywords()
        for string in self.raw_list:
            self.current_string = string[self.data_column_index]
            hit = False
            for keywords in self.keywords_list:
                self.current_keywords = keywords
                self.keywords_finder()
                if self.current_result:
                    hit_info = string + (keywords,)
                    self.trash_list.append(hit_info)
                    self.result_list.append(self.current_result)
                    hit = True
                    if self.one_hit_strategy:
                        break
                    else:
                        continue
                else:
                    continue
            if hit:
                continue
            else:
                self.cleaned_list.append(string)
        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


if __name__ == u"__main__":
    kr = KeywordsReducer()
    kr.data_column_index = 1
    # kr.show_process = True
    kr.one_hit_strategy = False
    # kr.current_dict_abspath = u"爱乐维-通用去水词.txt"
    try:
        kr.main()
    except Exception as exc:
        print str(exc)
    print u"{0}以下是水贴{0}".format(u"-" * 30)
    for row in kr.trash_list:
        print u"<ID>:{0}, <内容>:{1}, <关键词>:{2}".format(row[0], row[1], row[2])
    print u"{0}以下是保留贴{0}".format(u"-" * 30)
    for row in kr.cleaned_list:
        print u"<ID>:{0}, <内容>:{1}".format(row[0], row[1])
    _file_name = u"/root/home/你好牛逼...啊.txt"
    print u"{0}这是一条分割线{0}".format(u"-" * 30)
    print u"<{0}>的扩展名是<{1}>".format(_file_name, BaseReducer.get_file_extension(_file_name))
    print u"<{0}>的文件名是<{1}>".format(_file_name, BaseReducer.get_file_name(_file_name))




