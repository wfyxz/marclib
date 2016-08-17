 # coding: utf-8
# __author__: u"John"
from __future__ import with_statement
from os import path
import re


# region 属性访问定义——字典后接'.' + key名，即可得到字典中key对应的内容
class AttributeDict(dict):
    """
    能够把dict的key当作class的attribute
    """
    def __getattr__(self, attr):
        return self[attr]

    def __setattr__(self, attr, value):
        self[attr] = value
# endregion


# region 可复用基本类 可以得到文件名，文件拓展，还有一个加序号的方法
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
        # splitext分离拓展名

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
        self.one_hit_strategy = False
        self.usehottags = True
        self.hottags = []
        self.safewords_list = []
        self.current_safedict_abspath = ''

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
                    # 2进制模式打开
                except IOError as e:
                    self.current_error = str(e)
                    self.code_message.code = 1
                    self.code_message.message = u"get_contents open(file) IOError"
                    self.error_list.append(self.current_error)
                else:
                    with f:
                        for line in f:
                            # line = line.encode(u"gb2312").replace(u"\r", u"").replace(u"\n", u"").split(u"\t")
                            line = line.decode(u"utf-8").replace(u"\r", u"").replace(u"\n", u"").split(u"\t")
                            self.raw_list.append(tuple(line))
                        self.code_message.message = u"data file import successfully"
        else:
            self.raw_list = [(1, u"关键词"), (2, u"这是一个正"), (3, u"这是一个藏得很深的水贴，你检测不出来"),
                             (4, u"则表达式呢")]
        return

    def get_hottags(self):
        if self.hottags:
            return
        else:
            pass
        if self.raw_list:
            cont = [i[self.data_column_index] for i in self.raw_list]
            wordcut = r'\n'.join(cont)
            wordcuts = wordcut.split(r'\n')

            result = []
            for i in wordcuts:
                try:
                    seg_list = re.findall('#.*?#', i)
                    for j in seg_list:
                        if len(j) >= 3:
                            result.append(j.strip('#'))
                except:
                    print("some wrong")

            dic_result = {}
            for i in result:
                if i in dic_result:
                    dd = dic_result.get(i)
                    dic_result[i] = dd + 1
                else:
                    dic_result[i] = 1
            dic_result = sorted(dic_result.items(), key=lambda asd: asd[1], reverse=True)
            #dic_data = DataFrame(dic_result, columns=['keyword', 'frequency'])

            self.hottags = [i[0] for i in dic_result[:8]]
            # print [i[1] for i in dic_result[:20]]
            # for i in self.hottags:
            #     print i
            # print len(self.hottags)

    def get_safewords(self):
        """
            数据库的操作后将数据赋值给 self.safewords_list
            :return:
            """
        if self.safewords_list:
            return
        else:
            pass
        if self.current_safedict_abspath:
            self.export_name = u"{0}-{1}".format(self.export_name, self.get_file_name(self.current_dict_abspath))
            try:
                f = open(self.current_safedict_abspath, u"rb")
            except IOError as e:
                self.current_error = str(e)
                self.code_message.code = 1
                self.code_message.message = u"get_safewords open(file) IOError"
                self.error_list.append(self.current_error)
            else:
                with f:
                    # self.safewords_list += f.read().decode(u"gb2312").split(u"\r\n")
                    self.safewords_list += f.read().decode(u"utf-8").split(u"\r\n")
                    self.code_message.message = u"cleanwords dictionary import successfully"
        else:
            self.safewords_list = [u"非关键词", u"奥运"]

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
                    # self.keywords_list += f.read().decode(u"gb2312").split(u"\r\n")
                    self.keywords_list += f.read().decode(u"utf-8").split(u"\r\n")
                    self.code_message.message = u"keywords dictionary import successfully"
        else:
            self.keywords_list = [u"关键词", u"(正|则|表|达|式)"]

        return

    def keyword_merge(self):
        rawkeywords = self.hottags + self.keywords_list
        newkeywords = []
        if self.current_safedict_abspath:
            for i in rawkeywords:
                contain = False
                for j in self.safewords_list:
                    if j in i and j:
                        contain = True
                    else:
                        continue
                if contain:
                    pass
                else:
                    newkeywords.append(i)

            self.keywords_list = newkeywords
        else:
            self.keywords_list = rawkeywords

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
        if self.usehottags:
            self.get_hottags()
        self.get_safewords()
        self.get_keywords()
        self.keyword_merge()
        countlist = [0]*len(self.keywords_list)
        for string in self.raw_list:
            self.current_string = string[self.data_column_index]
            hit = False
            # for keywords in self.keywords_list:
            for i in range(0,len(self.keywords_list)):
                keywords = self.keywords_list[i]
                self.current_keywords = keywords
                self.keywords_finder()
                if self.current_result:
                    hit_info = string + (keywords,)
                    self.trash_list.append(hit_info)
                    self.result_list.append(self.current_result)
                    hit = True
                    countlist[i] +=1
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

        totallength = float(len(self.raw_list))
        tagscount = 0
        keywordcount = 0
        for i in countlist[:8]:
            tagscount += i
        for i in countlist[8:]:
            keywordcount += i
        print u'前八個tags標記微博數量為 ' + str(tagscount) + u' 占' + str(tagscount/totallength*100) + '%'
        print u'關鍵詞標記微博數量為 ' + str(keywordcount) + u' 占' + str(keywordcount / totallength * 100) + '%'
        print u"{0}以下是前八個tags標記的水贴{0}".format(u"-" * 30)
        for i in range(0, 8):
            # if countlist[i]/totallength*100 < 1:
            print u'tag "' + self.keywords_list[i] + u'" 匹配的微博数量为 ' \
                  + str(countlist[i]) + u'  占' + str(countlist[i] / totallength * 100) + '%'
        print u"{0}以下是關鍵詞標記的水贴{0}".format(u"-" * 30)
        for i in range(8,len(countlist)):
            # if countlist[i]/totallength*100 < 1:
            print u'关键词 "' + self.keywords_list[i] + u'" 匹配的微博数量为 ' \
                  + str(countlist[i]) + u'  占' + str(countlist[i]/totallength*100) + '%'
        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


# region 数字个数去水
class NumbersReducer(BaseReducer):
    """
    利用数字个数去水的工具，数字暂定为3
    """
    def __init__(self):
        BaseReducer.__init__(self)
        self.numbers = 3

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
                    # 2进制模式打开
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
            self.raw_list = [(1, u"关键词1"), (2, u"1这是一个正2"), (3, u"这2是一个藏得很深的2水贴，你检测不3出来"),
                             (4, u"1则4表达2式6呢")]
        return

    def number_finder(self):
        """
            python的正则表达式可以直接支持中文
            :return:
            """
        try:
            numbers = re.findall(r"\d+\.?\d*", self.current_string)
        except Exception as e:
            self.current_error = str(e)
            self.code_message.code = 3
            self.code_message.message = u"number_finder re.findll error"
            self.error_list.append(self.current_error)
            self.current_result = []
        else:
            self.current_result = len(numbers) == self.numbers
        if self.show_process:
            if self.current_result == self.numbers:
                print u"含有的数字个数\n{0}".format(len(numbers))
            else:
                # print u"没有匹配\n{0}".format(self.numbers)
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
        for string in self.raw_list:
            self.current_string = string[self.data_column_index]

            self.number_finder()
            if self.current_result:
                self.trash_list.append(self.current_string)

            else:
                self.cleaned_list.append(self.current_string)

        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


# region 非正常字符个数去水
class AbnormalReducer(BaseReducer):
    """
    利用数字个数去水的工具，数字暂定为3
    """
    def __init__(self):
        BaseReducer.__init__(self)
        self.abnormal = 3

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
                    # 2进制模式打开
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
            self.raw_list = [(1, u"关键%词1"), (2, u"1这是%一%个正2"), (3, u"这2%是一个藏得很深%的2水贴，你检测%不3出来"),
                             (4, u"1%则%4表达%2式6%呢")]
        return

    def number_finder(self):
        """
            python的正则表达式可以直接支持中文
            :return:
            """
        try:
            matchs = re.findall(ur"[^\dA-Za-z\u3007\u4E00-\u9FCB\uE815-\uE864]", self.current_string)
        except Exception as e:
            self.current_error = str(e)
            self.code_message.code = 3
            self.code_message.message = u"number_finder re.findll error"
            self.error_list.append(self.current_error)
            self.current_result = []
        else:
            self.current_result = len(matchs) >= self.abnormal
        if self.show_process:
            if self.current_result == self.abnormal:
                print u"含有的数字个数\n{0}".format(len(matchs))
            else:
                # print u"没有匹配\n{0}".format(self.abnormal)
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
        for string in self.raw_list:
            self.current_string = string[self.data_column_index]

            self.number_finder()
            if self.current_result:
                self.trash_list.append(self.current_string)

            else:
                self.cleaned_list.append(self.current_string)

        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


if __name__ == u"__main__":

    # kr = AbnormalReducer()
    # kr = NumbersReducer()
    kr = KeywordsReducer()
    kr.data_column_index = 2
    kr.show_process = False
    # kr.num_match_strategy = False
    # kr.current_data_abspath = ur"D:\364\weibo75.txt"
    # kr.current_data_abspath = ur"D:\weibotop20.txt"
    kr.current_data_abspath = ur"D:\WorkSpace\Data\weibotest.txt"
    kr.current_dict_abspath = ur"D:\WorkSpace\Data\keywords.txt"
    kr.current_safedict_abspath = ur"D:\WorkSpace\Data\safewords.txt"
    try:
        kr.main()
    except Exception as exc:
        print str(exc)

    print u'共有微博 ' +str(len(kr.raw_list))
    print u'水有 ' + str(len(kr.raw_list)-len(kr.cleaned_list)) + u'条'
    print u'重复水有 ' + str(len(kr.trash_list)-len(kr.raw_list)+len(kr.cleaned_list)) + u'条'
    print u'非水有 ' + str(len(kr.cleaned_list)) + u'条'
