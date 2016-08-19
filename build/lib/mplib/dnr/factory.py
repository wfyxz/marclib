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
    利用数字个数去水的工具，数字暂定为34
    """
    def __init__(self):
        BaseReducer.__init__(self)
        self.numbers = 34

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
            numbercounts = ''
            for i in numbers:
                numbercounts += i
            self.current_result = len(numbercounts) >= self.numbers
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
                self.trash_list.append(string)

            else:
                self.cleaned_list.append(string)

        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


# region 非正常字符个数去水
class AbnormalReducer(BaseReducer):
    """
    利用异常字符种数去水的工具，数字暂定为5
    """
    def __init__(self):
        BaseReducer.__init__(self)
        self.abnormal = 5

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
            matchs = re.findall(ur"[^ ,，.\[\]()（）\dA-Za-z\u3007\u4E00-\u9FCB\uE815-\uE864]", self.current_string)
        except Exception as e:
            self.current_error = str(e)
            self.code_message.code = 3
            self.code_message.message = u"number_finder re.findll error"
            self.error_list.append(self.current_error)
            self.current_result = []
        else:
            numbercounts = ''
            for i in matchs:
                numbercounts += i
            numbercounts = set(numbercounts)
            self.current_result = len(numbercounts) >= self.abnormal
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
                self.trash_list.append(string)

            else:
                self.cleaned_list.append(string)

        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


if __name__ == u"__main__":

    # kr = AbnormalReducer()
    kr = NumbersReducer()
    # kr = KeywordsReducer()
    kr.numbers = 34
    kr.data_column_index = 2
    kr.show_process = False
    # kr.num_match_strategy = False
    # kr.current_data_abspath = ur"D:\364\weibo75.txt"
    # kr.current_data_abspath = ur"D:\weibotop20.txt"
    kr.current_data_abspath = ur"D:\WorkSpace\Data\weibo1.txt"
    kr.current_dict_abspath = ur"D:\WorkSpace\Data\keywords.txt"
    kr.current_safedict_abspath = ur"D:\WorkSpace\Data\safewords.txt"
    try:
        kr.main()
    except Exception as exc:
        print str(exc)

    print u"{0}统计信息{0}".format(u"-" * 30)
    print u'共有微博 ' +str(len(kr.raw_list))
    print u'水有 ' + str(len(kr.raw_list)-len(kr.cleaned_list)) + u'条'
    print u'重复水有 ' + str(len(kr.trash_list)-len(kr.raw_list)+len(kr.cleaned_list)) + u'条'
    print u'非水有 ' + str(len(kr.cleaned_list)) + u'条'


    content = [
        u'\u8fd1UA\uff0c\u5ba2\u5385350\u5200/\u6708+\u7f51\u8d39\u5e73\u5206\uff0810.5\u5200\uff09\uff0c9\u6708\u62db\u957f\u79df\u5973\u5ba4\u53cb\uff0c\u5730\u5740\uff1a115 st & 76 ave\uff0c2\u5206\u949f\u5230mckernan station\uff0clrt\u57504\u5206\u949f\u5230university station,\u5ba4\u53cb\u5168\u5973\u751f\uff0c\u95e8\u53e3\u5404\u79cd\u516c\u4ea4\u8f66\u901aUA\u53ca\u8d85\u5e02\uff0c\u6709\u5174\u8da3\u7684\u8bf7\u79c1\u4fe1\u6216\u7535\u8bdd:7807102029\uff0c\u8c22\u8c22\u7231\u57ce\u4e3b\u9875\u541b@\u7231\u57ce\u5fae\u535a-\u5e2e\u5e2e\u5fd9 [\u7231\u4f60]',
        u'Day1 \u4e00\u4e00 5\uff1a30\u6765\u5230\u706f\u706b\u901a\u660e\u7684\u5723\u5730 1. 2.4\u516c\u91cc\u70ed\u8eab\uff08\u51d1\u5230240\uff0f500\uff1b2. 1\u8282Grit Cardio \u7b2c\u4e00\u6b21\u4f53\u9a8c\uff1b3.1\u8282CX \u63d0\u524d\u611f\u53d7\u83b1\u7f8efilm\u5185\u5bb9\uff1b4.50\u5206\u949f\u529b\u91cf\u8bad\u7ec3 \u5728\u4f17\u591a\u5f6a\u608d\u7684\u8001\u5916\u805a\u96c6\u5730\u64b8\u94c1 \u8001\u523a\u6fc0\u4e86\u2026\u2026\u4e0a\u5348\u7ed3\u675f[\u594b\u6597] \u671f\u5f85\u4e0b\u5348\u7684BC \u7edd\u5bf9\u4f1a\u7528\u5fc3\u611f\u53d7[\u9f13\u638c]',
        u'I just ran 10.05KM with @\u60a6\u8dd1\u5708, within:01:06:03.http://wap.thejoyrun.com/po_2077579_65248952.html',
        u'\u65b0\u57fa\u5730\u8981\u7ed9\u5b69\u5b50\u4eec\u62c9\u8dd1\u5708\uff0c\u8c01\u6709\u8d44\u6e90\u53ef\u4ee5\u5b9a\u505a\u8dd1\u5708\u56f4\u680f\uff0c\u4ef7\u683c\u4fbf\u5b9c\u4e9b\u7684\uff0c\u5c3a\u5bf8\u9700\u89811.8\u7c73*2\u7c73\uff0c\u5927\u6982\u9700\u8981120-130\u7247\u5de6\u53f3\uff0c\u6700\u597d\u5305\u5b89\u88c5\u3002\u6574\u6574\u6253\u4e86\u4e00\u5929\u7535\u8bdd\u627e\u4e86\u65e0\u6570\u5bb6\u5b9a\u505a\u56f4\u680f\u7684\uff0c\u603b\u4ef7\u90fd\u5728\u4e24\u4e07\u4ee5\u4e0a\uff0c\u5b9e\u5728\u592a\u8d35\uff0c\u81ea\u5df1\u505a\u5b9e\u5728\u662f\u5e72\u4e0d\u52a8\u4e86\u3002\u8bf7\u5927\u5bb6\u5e2e\u5fd9\u3002\u611f\u8c22\u3002\u5fae\u4fe1\uff1a413268154\u3002\u7535\u8bdd\uff1a13501105906',
        u'10701070107010701070107010701070\u6211\u4e70\u4e86[\u5fae\u7b11]\u4efb\u6027[\u5fae\u7b11]\u4e0d\u6015\u6b7b[\u5fae\u7b11]',
        u'\u6652\u4e2a\u7709\u6bdb\u65bd\u672f\u540e\u7acb\u5373\u56fe \u4e0b\u5348\u4e0d\u73a9\u624b\u673a\u4e86 \u9888\u690e\u4e0d\u8212\u670d \u5982\u679c\u7d27\u6025\u4e8b\u60c5\u6253\u6211\u7535\u8bdd13023961110/18060287080/17750695051']

    B = 0
    for i in kr.trash_list:
        if i[kr.data_column_index] in content:
            B += 1
            #print i[kr.data_column_index]

    A = 0
    for i in kr.cleaned_list:
        if i[kr.data_column_index] in content:
            A += 1

    D = len(kr.raw_list)-len(kr.cleaned_list) - B
    C = len(kr.cleaned_list) - A
    if A+B != 0:
        precise = A/float(A+B)*100
    else:
        precise = 0
    if A+C != 0:
        recall = A/float(A+C)*100
    else:
        recall = 0
    if precise + recall != 0:
        F = precise * recall / (precise + recall)
    else:
        F = 0
    print 'AB is:      ' + str(A) + ' ' + str(B)
    print 'CD is:      ' + str(C) + ' ' + str(D)
    print 'precise is: ' + str(precise) + '%'
    print 'recall is:  ' + str(recall) + '%'
    print '100*F is        ' + str(F)

