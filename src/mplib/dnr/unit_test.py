# coding: utf-8
# __author__: u"John"
from api import *
import os
import jieba
import numpy
import pandas as pd
from sklearn import metrics
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
import re
from pandas import DataFrame
import io
import csv
import datetime


def string_preprocess(string):
    raw_string = string
    http_info = re.compile('[a-zA-z]+://[^\s]*')
    string_without_http = http_info.sub(ur'链接', raw_string)
    at_info = re.compile(ur'@[^ @，,。.]*')
    string_without_http_and_at = at_info.sub(ur'@', string_without_http)
    number_eng_info = re.compile(ur'[0-9|a-zA-Z]')
    clean_string = number_eng_info.sub('', string_without_http_and_at)
    return clean_string


def fast_clean(data_path=ur'D:\workspace\Data\WeiboData',
               train_data_path=ur'D:\workspace\weibo\data\8000条测试数据.xlsx',
               stop_words_path=ur"D:\workspace\weibo\data\stop_words.txt",
               use_beyes=True):
    path = data_path
    names = os.listdir(path)
    times = [datetime.timedelta(0)] * 2
    length = float(len(names))

    # region 贝叶斯分类器
    # 读取训练数据
    filename = train_data_path
    threshold = 5000
    dataframe = pd.read_excel(filename, sheetname=2, index_col=None, header=None)
    rdata = dataframe[:threshold]
    train_words = [string_preprocess(string) for string in rdata[2]]
    train_tags = [tag for tag in rdata[1]]

    # 从文件导入停用词表
    with io.open(stop_words_path, 'r', encoding='utf-8') as f:
        stpwrd_content = f.read()
        stop_words = stpwrd_content.splitlines()

    # 文档向量化
    # v = HashingVectorizer(tokenizer=lambda x: jieba.cut(x, cut_all=True), n_features=30000, non_negative=True,
    #                       stop_words=stop_words)
    v = TfidfVectorizer(tokenizer=lambda x: jieba.cut(x), analyzer='word', stop_words=stop_words)
    train_data = v.fit_transform(train_words)

    # 训练模型
    clf = MultinomialNB(alpha=0.03)
    clf.fit(train_data, numpy.asarray(train_tags))
    # endregion

    for name in names:
        data = path + '\\' + name + ur'\weibo1.txt'
        savefile = path + '\\' + name
        print 'Processing: ', data

        # 普通方法
        starttime = datetime.datetime.now()
        r, h = weibo_cleaning(data_path=data, save_file_path=savefile,
                              data_index_name=u'text', sources_index_name=u'source',
                              has_header=True, output_data=not use_beyes,return_header=True)

        endtime = datetime.datetime.now()
        interval = endtime - starttime
        print ur'Cleaning data done! Time cost: ', interval
        times[0] += interval

        # 贝叶斯
        starttime = datetime.datetime.now()
        new_words = [string_preprocess(string[2]) for string in r]
        test_data = v.transform(new_words)
        prediction = clf.predict(test_data)
        # 筛选
        clean_data = []
        trash_data = []
        for index in range(len(prediction)):
            if prediction[index] == 1:
                clean_data.append(r[index])
            else:
                trash_data.append(r[index])
        # 保存
        clean_data = DataFrame(clean_data)
        trash_data = DataFrame(trash_data)
        clean_data.to_csv(path + '\\' + name + ur'\clean_data.txt', header=h, encoding=u'utf-8',
                          index=None, sep='\t', mode='w', quoting=csv.QUOTE_NONE)
        # trash_data.to_csv(path + '\\' + name + ur'\trash_data.txt', header=h, encoding=u'utf-8',
        #                   index=None, sep='\t', mode='w', quoting=csv.QUOTE_NONE)

        endtime = datetime.datetime.now()
        interval = endtime - starttime
        print ur'Cleaning data done! Time cost: ', interval
        times[1] += interval

    outcome = [time.seconds / length for time in times]
    print length
    print outcome

if __name__ == u"__main__":
    fast_clean()

