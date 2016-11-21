# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from factory import *
from helper import *
from os.path import join
import traceback


def string_preprocess(string):
    raw_string = string
    http_info = re.compile("[a-zA-z]+://[^\s]*")
    string_without_http = http_info.sub(r"链接", raw_string)
    at_info = re.compile(r"@[^ @，,。.]*")
    string_without_http_and_at = at_info.sub(ur"@", string_without_http)
    number_eng_info = re.compile(r"[0-9|a-zA-Z]")
    clean_string = number_eng_info.sub("", string_without_http_and_at)
    return clean_string


def keywords_splitter(data=list(), **parameter_diction):
    keywords_reducer = KeywordsReducer()
    keywords_reducer.raw_list = data
    keywords_reducer.data_column_name = parameter_diction.get("data_index_name")
    keywords_reducer.one_hit_strategy = False
    keywords_reducer.current_dict_abspath = parameter_diction.get("keyword_path")
    keywords_reducer.current_data_abspath = parameter_diction.get("data_path")
    keywords_reducer.has_header = parameter_diction.get("has_header")
    keywords_reducer.save_file_path = parameter_diction.get("save_file_path")
    try:
        keywords_reducer.main()
        if parameter_diction.get("save_file_path"):
            if keywords_reducer.trash_list:
                if parameter_diction.get("data_path"):
                    filename = join(keywords_reducer.save_file_path, "keywords_data_trash.txt")
                    export_to_txt(data_list=keywords_reducer.trash_list, file_name=filename,
                                  column_head=keywords_reducer.header)
            if keywords_reducer.cleaned_list:
                filename = join(keywords_reducer.save_file_path, "clean_data.txt")
                export_to_txt(data_list=keywords_reducer.cleaned_list,
                              file_name=filename,
                              column_head=keywords_reducer.header)
        else:
            return keywords_reducer.cleaned_list
    except:
        traceback.print_exc()
    return


def numbers_splitter(data=list(), **parameter_diction):
    numbers_reducer = NumbersReducer()
    numbers_reducer.raw_list = data
    numbers_reducer.data_column_name = parameter_diction.get("data_index_name")
    numbers_reducer.min_numbers = parameter_diction.get("min_char")
    numbers_reducer.max_numbers = parameter_diction.get("max_char")
    numbers_reducer.current_data_abspath = parameter_diction.get("data_path")
    numbers_reducer.has_header = parameter_diction.get("has_header")
    numbers_reducer.save_file_path = parameter_diction.get("save_file_path")
    try:
        numbers_reducer.main()
        if parameter_diction.get("save_file_path"):
            if numbers_reducer.trash_list:
                if parameter_diction.get("data_path"):
                    filename = join(numbers_reducer.save_file_path, "numbers_data_trash.txt")
                    export_to_txt(data_list=numbers_reducer.trash_list,
                                  file_name=filename,
                                  column_head=numbers_reducer.header)
            if numbers_reducer.cleaned_list:
                filename = join(numbers_reducer.save_file_path, "clean_data.txt")
                export_to_txt(data_list=numbers_reducer.cleaned_list,
                              file_name=filename,
                              column_head=numbers_reducer.header)
        else:
            return numbers_reducer.cleaned_list
    except:
        traceback.print_exc()
    return


def tags_splitter(data=list(), **parameter_diction):
    tags_reducer = TagsReducer()
    tags_reducer.raw_list = data
    tags_reducer.data_column_name = parameter_diction.get("data_index_name")
    tags_reducer.numbers = parameter_diction.get("min_char")
    tags_reducer.current_data_abspath = parameter_diction.get("data_path")
    tags_reducer.has_header = parameter_diction.get("has_header")
    tags_reducer.save_file_path = parameter_diction.get("save_file_path")
    tags_reducer.main()
    try:
        if parameter_diction.get("save_file_path"):
            if tags_reducer.trash_list:
                if parameter_diction.get("data_path"):
                    filename = join(tags_reducer.save_file_path, "tags_data_trash.txt")
                    export_to_txt(data_list=tags_reducer.trash_list,
                                  file_name=filename,
                                  column_head=tags_reducer.header)
            if tags_reducer.cleaned_list:
                filename = join(tags_reducer.save_file_path, "clean_data.txt")
                export_to_txt(data_list=tags_reducer.cleaned_list,
                              file_name=filename,
                              column_head=tags_reducer.header)
        else:
            return tags_reducer.cleaned_list
    except:
        traceback.print_exc()
    return


def abnormal_splitter(data=list(), **parameter_diction):
    abnormal_reducer = AbnormalReducer()
    abnormal_reducer.raw_list = data
    abnormal_reducer.data_column_name = parameter_diction.get("data_index_name")
    abnormal_reducer.abnormal = parameter_diction.get("max_symbol")
    abnormal_reducer.current_data_abspath = parameter_diction.get("data_path")
    abnormal_reducer.has_header = parameter_diction.get("has_header")
    abnormal_reducer.save_file_path = parameter_diction.get("save_file_path")
    try:
        abnormal_reducer.main()
        if parameter_diction.get("save_file_path"):
            if abnormal_reducer.trash_list:
                if parameter_diction.get("data_path"):
                    filename = join(abnormal_reducer.save_file_path, "abnormal_data_trash.txt")
                    export_to_txt(data_list=abnormal_reducer.trash_list,
                                  file_name=filename,
                                  column_head=abnormal_reducer.header)
            if abnormal_reducer.cleaned_list:
                filename = join(abnormal_reducer.save_file_path, "clean_data.txt")
                export_to_txt(data_list=abnormal_reducer.cleaned_list,
                              file_name=filename,
                              column_head=abnormal_reducer.header)
        else:
            return abnormal_reducer.cleaned_list
    except:
        traceback.print_exc()
    return


def series_splitter(data=list(), **parameter_diction):
    series_reducer = SeriesReducer()
    series_reducer.raw_list = data
    series_reducer.data_column_name = parameter_diction.get("data_index_name")
    series_reducer.current_data_abspath = parameter_diction.get("data_path")
    series_reducer.has_header = parameter_diction.get("has_header")
    series_reducer.save_file_path = parameter_diction.get("save_file_path")
    try:
        series_reducer.main()
        if parameter_diction.get("save_file_path"):
            if series_reducer.trash_list:
                if parameter_diction.get("data_path"):
                    filename = join(series_reducer.save_file_path, "series_data_trash.txt")
                    export_to_txt(data_list=series_reducer.trash_list,
                                  file_name=filename,
                                  column_head=series_reducer.header)
            if series_reducer.cleaned_list:
                filename = join(series_reducer.save_file_path, "clean_data.txt")
                export_to_txt(data_list=series_reducer.cleaned_list,
                              file_name=filename,
                              column_head=series_reducer.header)
        else:
            return series_reducer.cleaned_list
    except:
        traceback.print_exc()
    return


def tagging_splitter(data=list(), **parameter_diction):
    tagging_reducer = TaggingReducer()
    tagging_reducer.raw_list = data
    tagging_reducer.data_column_name = parameter_diction.get("data_index_name")
    tagging_reducer.current_data_abspath = parameter_diction.get("data_path")
    tagging_reducer.current_dict_abspath = parameter_diction.get("keyword_path")
    tagging_reducer.has_header = parameter_diction.get("has_header")
    tagging_reducer.save_file_path = parameter_diction.get("save_file_path")
    try:
        tagging_reducer.main()
        if parameter_diction.get("save_file_path"):
            if tagging_reducer.trash_list:
                if parameter_diction.get("data_path"):
                    filename = join(tagging_reducer.save_file_path, "tagging_data_trash.txt")
                    export_to_txt(data_list=tagging_reducer.trash_list,
                                  file_name=filename,
                                  column_head=tagging_reducer.header)
            if tagging_reducer.cleaned_list:
                filename = join(tagging_reducer.save_file_path, "clean_data.txt")
                export_to_txt(data_list=tagging_reducer.cleaned_list,
                              file_name=filename,
                              column_head=tagging_reducer.header)
        else:
            return tagging_reducer.cleaned_list
    except Exception as e:
        print str(e)
    return


def sources_splitter(data=list(), **parameter_diction):
    keywords_reducer = SourcesReducer()
    keywords_reducer.raw_list = data
    keywords_reducer.data_column_name = parameter_diction.get("sources_index_name")
    keywords_reducer.current_dict_abspath = parameter_diction.get("sources_path")
    keywords_reducer.current_data_abspath = parameter_diction.get("data_path")
    keywords_reducer.has_header = parameter_diction.get("has_header")
    keywords_reducer.save_file_path = parameter_diction.get("save_file_path")
    try:
        keywords_reducer.main()
        if parameter_diction.get("save_file_path"):
            if keywords_reducer.trash_list:
                if parameter_diction.get("data_path"):
                    filename = join(keywords_reducer.save_file_path, "sources_data_trash.txt")
                    export_to_txt(data_list=keywords_reducer.trash_list, file_name=filename,
                                  column_head=keywords_reducer.header)
            if keywords_reducer.cleaned_list:
                filename = join(keywords_reducer.save_file_path, "clean_data.txt")
                export_to_txt(data_list=keywords_reducer.cleaned_list, file_name=filename,
                              column_head=keywords_reducer.header)
        else:
            return keywords_reducer.cleaned_list
    except:
        traceback.print_exc()
    return


def find_trash_data(data_path, save_file_path=u"D:\WorkSpace\Data",
                    solutions=[ur"keywords", ur"tags", ur"sources", ur"series"],
                    data_index_name="text", sources_index_name="source",
                    has_header=True, keyword_path=ur"D:\WorkSpace\Data\keywords.txt",
                    sources_path=ur"D:\WorkSpace\Data\trash_sources.txt",
                    min_char=4, max_char=600, max_symbol=5, ):

    if isinstance(solutions, list):
        test_classifier = solutions
    else:
        test_classifier = [solutions]
    classifiers = {
        "keywords": keywords_splitter,
        "tags": tags_splitter,
        "sources": sources_splitter,
        "series": series_splitter,
        "numbers": numbers_splitter,
        "abnormal": abnormal_splitter,
        "tagging": tagging_splitter,
    }
    for classifier_index in range(len(test_classifier)):
        start_time = datetime.datetime.now()
        classifier = test_classifier[classifier_index]
        if classifier_index == 0:
            classifiers[classifier](data_path=data_path, save_file_path=save_file_path,
                                    data_index_name=data_index_name, sources_index_name=sources_index_name,
                                    has_header=has_header, keyword_path=keyword_path,
                                    sources_path=sources_path, min_char=min_char, max_char=max_char,
                                    max_symbol=max_symbol,)
        else:
            classifiers[classifier](data_path=join(save_file_path, "clean_data.txt"), save_file_path=save_file_path,
                                    data_index_name=data_index_name, sources_index_name=sources_index_name,
                                    has_header=has_header, keyword_path=keyword_path,
                                    sources_path=sources_path, min_char=min_char, max_char=max_char,
                                    max_symbol=max_symbol,)
        end_time = datetime.datetime.now()
        interval = end_time - start_time
        print classifier, ur"cleaning done! Time cost: ", interval


def rules_clean(raw_data, data_index_name=2, sources_index_name=3,
                keyword_path=ur"D:\WorkSpace\Data\keywords.txt",
                sources_path=ur"D:\WorkSpace\Data\trash_sources.txt", min_char=4):
    print "before dnr there's {0} rows in data".format(len(raw_data))
    raw_data = keywords_splitter(data=raw_data, data_path='', save_file_path='',
                                 data_index_name=data_index_name,
                                 has_header=False, keyword_path=keyword_path, )
    print "after keywords_splitter there's {0} rows in data".format(len(raw_data))
    raw_data = sources_splitter(data=raw_data, data_path='', save_file_path='',
                                sources_index_name=sources_index_name,
                                has_header=False, sources_path=sources_path, )
    print "after sources_splitter there's {0} rows in data".format(len(raw_data))
    raw_data = series_splitter(data=raw_data, data_path='', save_file_path='',
                               data_index_name=data_index_name,
                               has_header=False)
    print "after series_splitter there's {0} rows in data".format(len(raw_data))
    raw_data = tags_splitter(data=raw_data, data_path='', save_file_path='',
                             data_index_name=data_index_name, min_char=min_char,
                             has_header=False)
    print "after tags_splitter there's {0} rows in data".format(len(raw_data))
    return raw_data


def find_weibo_data(data_path=ur"D:\workspace\Data\WeiboData",
                    train_data_path=ur"D:\workspace\weibo\data\8000条测试数据.xlsx",
                    stop_words_path=ur"D:\workspace\weibo\data\stop_words.txt",
                    data_index_name=u"text", sources_index_name=u"source", has_header=True,
                    keyword_path=ur"D:\WorkSpace\Data\keywords.txt",
                    sources_path=ur"D:\WorkSpace\Data\trash_sources.txt",
                    min_char=4):

    folders = os.listdir(data_path)
    times = [datetime.timedelta(0)] * 2
    length = float(len(folders))

    # region 贝叶斯分类器准备
    # 读取训练数据
    filename = train_data_path
    threshold = 5000
    df = pd.read_excel(filename, sheetname=2, index_col=None, header=None)
    raw_data = df[:threshold]
    train_words = [string_preprocess(string) for string in raw_data[2]]
    train_tags = [tag for tag in raw_data[1]]

    # 从文件导入停用词表
    with io.open(stop_words_path, "r", encoding="utf-8") as f:
        stop_words_content = f.read()
        stop_words = stop_words_content.splitlines()

    # 文档向量化
    # v = HashingVectorizer(tokenizer=lambda x: jieba.cut(x, cut_all=True), n_features=30000, non_negative=True,
    #                       stop_words=stop_words)
    v = TfidfVectorizer(tokenizer=lambda x: jieba.cut(x), analyzer="word", stop_words=stop_words)
    train_data = v.fit_transform(train_words)

    # 训练模型
    clf = MultinomialNB(alpha=0.03)
    clf.fit(train_data, numpy.asarray(train_tags))
    # endregion

    # 本地文件分布在各个文件夹
    # 对某个文件夹中的数据
    for folder in folders:
        data = join(join(data_path, folder), "weibo1.txt")
        save_file = join(data_path, folder)
        print "Processing: ", data

        start_time = datetime.datetime.now()
        try:
            # 表头信息处理
            with io.open(data, "r", encoding="utf-8") as f:
                if has_header:
                    header = f.readline().rstrip("\n").rstrip(" ").rstrip("\t").split("\t")
                    data_index = header.index(data_index_name)
                    sources_index = header.index(sources_index_name)
                else:
                    header = None
                    data_index = data_index_name
                    sources_index = sources_index_name

                data = [line.rstrip("\n").rstrip(" ").rstrip("\t").split("\t") for line in f]
            # print header
            # print len(data)

            # 利用写好的规则进行降噪
            clean_data = rules_clean(data, data_index_name=data_index, sources_index_name=sources_index,
                                     keyword_path=keyword_path, sources_path=sources_path, min_char=min_char)
            end_time = datetime.datetime.now()
            interval = end_time - start_time
            print ur"Cleaning data done! Time cost: ", interval
            times[0] += interval

            # 利用贝叶斯进行降噪
            start_time = datetime.datetime.now()
            new_words = [string_preprocess(string[data_index]) for string in clean_data]
            test_data = v.transform(new_words)
            prediction = clf.predict(test_data)
            # 筛选
            clean_data2 = []
            # trash_data2 = []
            for index in range(len(prediction)):
                if prediction[index] == 1:
                    clean_data2.append(clean_data[index])
                # else:
                #     trash_data2.append(clean_data[index])

            # 保存数据
            clean_data2 = DataFrame(clean_data2)
            # trash_data2 = DataFrame(trash_data2)
            clean_data2.to_csv(join(save_file, "clean_data.txt"), header=header, encoding="utf-8",
                               index=None, sep="\t", mode="w", quoting=csv.QUOTE_NONE)
            end_time = datetime.datetime.now()
            interval = end_time - start_time
            print r"Cleaning data done! Time cost: ", interval
            times[1] += interval

        except:
            print data, r" failed."
            pass

    outcome = [time.seconds / length for time in times]
    print length
    print outcome


def other_platform_cleaning():
    pass


if __name__ == u"__main__":
    # find_trash_data(data_path=ur"D:\WorkSpace\Data\WeiboData\1\weibo1.txt", save_file_path=u"D:\WorkSpace\Data",
    #                 solutions=[ur"keywords", ur"tags", ur"sources", ur"series"],
    #                 data_index_name=2, sources_index_name=3, has_header=False)
    # find_trash_data(data_path=ur"D:\WorkSpace\Data\WeiboData\291\weibo1.txt", save_file_path=u"D:\WorkSpace\Data",
    #                 solutions=[ur"keywords", ur"tags", ur"sources", ur"series"],
    #                 data_index_name="text", sources_index_name="source", has_header=True)
    # find_trash_data(data_path=ur"D:\WorkSpace\Data\虎扑---帖1.txt",
    #                 keyword_path=ur"D:\workspace\Data\通用词库1",
    #                 save_file_path=u"D:\WorkSpace\Data",
    #                 solutions=[ur"tagging", ur"numbers"],
    #                 data_index_name="Content", has_header=True)
    # 2半个小时
    find_weibo_data()

    # numbers_splitter(data_path=ur"D:\WorkSpace\Data\weibo1.txt",
    #                  )
    # tags_splitter(data_path=ur"D:\WorkSpace\Data\sources_data_clean.txt",
    #                  )
    # abnormal_splitter(data_path=ur"D:\WorkSpace\Data\numbers_data_clean.txt",
    #                   )
    # keywords_splitter(data_path=ur"D:\WorkSpace\Data\data_sample.txt",
    #                   keywords_path=ur"D:\WorkSpace\Data\keywords.txt",
    #                   index=2, one_hit=False)
    # keywords_splitter(data_path=ur"D:\WorkSpace\Data\test_data.txt",
    #                   keywords_path=ur"D:\WorkSpace\Data\keywords.txt",
    #                   index=2, one_hit=False)
    # sources_splitter(data_path=ur"D:\WorkSpace\Data\clean_data.txt",
    #                  sources_path=ur"D:\WorkSpace\Data\trash_sources.txt",
    #                  index=3)


