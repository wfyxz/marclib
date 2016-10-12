# coding: utf-8
# __author__: u"John"
from factory import *
from helper import *
import datetime


def keywords_splitter(data=[], **parameter_diction):
    keywords_reducer = KeywordsReducer()
    keywords_reducer.raw_list = data
    keywords_reducer.data_column_name = parameter_diction['data_index_name']
    keywords_reducer.one_hit_strategy = False
    keywords_reducer.current_dict_abspath = parameter_diction['keyword_path']
    keywords_reducer.current_data_abspath = parameter_diction['data_path']
    keywords_reducer.has_header = parameter_diction['has_header']
    keywords_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        keywords_reducer.main()
        if parameter_diction['save_file_path']:
            if keywords_reducer.trash_list:
                if parameter_diction['data_path']:
                    filename = keywords_reducer.save_file_path + u'\\' + u'keywords_data_trash.txt'
                    export_to_txt(data_list=keywords_reducer.trash_list, file_name=filename,
                                  column_head=keywords_reducer.header)
            if keywords_reducer.cleaned_list:
                # export_to_excel(data_list=keywords_reducer.cleaned_list,
                #                 file_name=u"data_clean.xlsx",
                #                 column_head=keywords_reducer.header)
                filename = keywords_reducer.save_file_path + u'\\' + u'clean_data.txt'
                export_to_txt(data_list=keywords_reducer.cleaned_list,
                              file_name=filename,
                              column_head=keywords_reducer.header)
        else:
            return keywords_reducer.cleaned_list
    except Exception as e:
        print str(e)
    return


def numbers_splitter(data=[], **parameter_diction):
    numbers_reducer = NumbersReducer()
    numbers_reducer.raw_list = data
    numbers_reducer.data_column_name = parameter_diction['data_index_name']
    numbers_reducer.min_numbers = parameter_diction['min_char']
    numbers_reducer.max_numbers = parameter_diction['max_char']
    numbers_reducer.current_data_abspath = parameter_diction['data_path']
    numbers_reducer.has_header = parameter_diction['has_header']
    numbers_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        numbers_reducer.main()
        if parameter_diction['save_file_path']:
            if numbers_reducer.trash_list:
                if parameter_diction['data_path']:
                    filename = numbers_reducer.save_file_path + u'\\' + u'numbers_data_trash.txt'
                    export_to_txt(data_list=numbers_reducer.trash_list,
                                  file_name=filename,
                                  column_head=numbers_reducer.header)
            if numbers_reducer.cleaned_list:
                filename = numbers_reducer.save_file_path + u'\\' + u'clean_data.txt'
                export_to_txt(data_list=numbers_reducer.cleaned_list,
                              file_name=filename,
                              column_head=numbers_reducer.header)
        else:
            return numbers_reducer.cleaned_list
    except Exception as e:
        print str(e)
    return


def tags_splitter(data=[], **parameter_diction):
    tags_reducer = TagsReducer()
    tags_reducer.raw_list = data
    tags_reducer.data_column_name = parameter_diction['data_index_name']
    tags_reducer.numbers = parameter_diction['min_char']
    tags_reducer.current_data_abspath = parameter_diction['data_path']
    tags_reducer.has_header = parameter_diction['has_header']
    tags_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        tags_reducer.main()
        if parameter_diction['save_file_path']:
            if tags_reducer.trash_list:
                if parameter_diction['data_path']:
                    filename = tags_reducer.save_file_path + u'\\' + u'tags_data_trash.txt'
                    export_to_txt(data_list=tags_reducer.trash_list,
                                  file_name=filename,
                                  column_head=tags_reducer.header)
            if tags_reducer.cleaned_list:
                filename = tags_reducer.save_file_path + u'\\' + u'clean_data.txt'
                export_to_txt(data_list=tags_reducer.cleaned_list,
                              file_name=filename,
                              column_head=tags_reducer.header)
        else:
            return tags_reducer.cleaned_list
    except Exception as e:
        print str(e)
    return


def abnormal_splitter(data=[], **parameter_diction):
    abnormal_reducer = AbnormalReducer()
    abnormal_reducer.raw_list = data
    abnormal_reducer.data_column_name = parameter_diction['data_index_name']
    abnormal_reducer.abnormal = parameter_diction['max_symbol']
    abnormal_reducer.current_data_abspath = parameter_diction['data_path']
    abnormal_reducer.has_header = parameter_diction['has_header']
    abnormal_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        abnormal_reducer.main()
        if parameter_diction['save_file_path']:
            if abnormal_reducer.trash_list:
                if parameter_diction['data_path']:
                    filename = abnormal_reducer.save_file_path + u'\\' + u'abnormal_data_trash.txt'
                    export_to_txt(data_list=abnormal_reducer.trash_list,
                                  file_name=filename,
                                  column_head=abnormal_reducer.header)
            if abnormal_reducer.cleaned_list:
                filename = abnormal_reducer.save_file_path + u'\\' + u'clean_data.txt'
                export_to_txt(data_list=abnormal_reducer.cleaned_list,
                              file_name=filename,
                              column_head=abnormal_reducer.header)
        else:
            return abnormal_reducer.cleaned_list
    except Exception as e:
        print str(e)
    return


def series_splitter(data=[], **parameter_diction):
    series_reducer = SeriesReducer()
    series_reducer.raw_list = data
    series_reducer.data_column_name = parameter_diction['data_index_name']
    series_reducer.current_data_abspath = parameter_diction['data_path']
    series_reducer.has_header = parameter_diction['has_header']
    series_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        series_reducer.main()
        if parameter_diction['save_file_path']:
            if series_reducer.trash_list:
                if parameter_diction['data_path']:
                    filename = series_reducer.save_file_path + u'\\' + u'series_data_trash.txt'
                    export_to_txt(data_list=series_reducer.trash_list,
                                  file_name=filename,
                                  column_head=series_reducer.header)
            if series_reducer.cleaned_list:
                filename = series_reducer.save_file_path + u'\\' + u'clean_data.txt'
                export_to_txt(data_list=series_reducer.cleaned_list,
                              file_name=filename,
                              column_head=series_reducer.header)
        else:
            return series_reducer.cleaned_list
    except Exception as e:
        print str(e)
    return


def tagging_splitter(data=[], **parameter_diction):
    tagging_reducer = TaggingReducer()
    tagging_reducer.raw_list = data
    tagging_reducer.data_column_name = parameter_diction['data_index_name']
    tagging_reducer.current_data_abspath = parameter_diction['data_path']
    tagging_reducer.current_dict_abspath = parameter_diction['keyword_path']
    tagging_reducer.has_header = parameter_diction['has_header']
    tagging_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        tagging_reducer.main()
        if parameter_diction['save_file_path']:
            if tagging_reducer.trash_list:
                if parameter_diction['data_path']:
                    filename = tagging_reducer.save_file_path + u'\\' + u'tagging_data_trash.txt'
                    export_to_txt(data_list=tagging_reducer.trash_list,
                                  file_name=filename,
                                  column_head=tagging_reducer.header)
            if tagging_reducer.cleaned_list:
                filename = tagging_reducer.save_file_path + u'\\' + u'clean_data.txt'
                export_to_txt(data_list=tagging_reducer.cleaned_list,
                              file_name=filename,
                              column_head=tagging_reducer.header)
        else:
            return tagging_reducer.cleaned_list
    except Exception as e:
        print str(e)
    return


def sources_splitter(data=[], **parameter_diction):
    keywords_reducer = SourcesReducer()
    keywords_reducer.raw_list = data
    keywords_reducer.data_column_name = parameter_diction['sources_index_name']
    keywords_reducer.current_dict_abspath = parameter_diction['sources_path']
    keywords_reducer.current_data_abspath = parameter_diction['data_path']
    keywords_reducer.has_header = parameter_diction['has_header']
    keywords_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        keywords_reducer.main()
        if parameter_diction['save_file_path']:
            if keywords_reducer.trash_list:
                if parameter_diction['data_path']:
                    filename = keywords_reducer.save_file_path + u'\\' + u'sources_data_trash.txt'
                    export_to_txt(data_list=keywords_reducer.trash_list, file_name=filename,
                                  column_head=keywords_reducer.header)
            if keywords_reducer.cleaned_list:
                # export_to_excel(data_list=keywords_reducer.cleaned_list,
                #                 file_name=u"data_clean.xlsx",
                #                 column_head=None)
                filename = keywords_reducer.save_file_path + u'\\' + u'clean_data.txt'
                export_to_txt(data_list=keywords_reducer.cleaned_list, file_name=filename,
                              column_head=keywords_reducer.header)
        else:
            return keywords_reducer.cleaned_list
    except Exception as e:
        print str(e)
    return


def find_trash_data(data_path, save_file_path=u"D:\WorkSpace\Data",
                    solutions=[ur'keywords', ur'tags', ur'sources', ur'series'],
                    data_index_name='text', sources_index_name='source',
                    has_header=True, keyword_path=ur"D:\WorkSpace\Data\keywords.txt",
                    sources_path=ur"D:\WorkSpace\Data\trash_sources.txt",
                    min_char=4, max_char=600, max_symbol=5, ):

    if isinstance(solutions, list):
        test_classifier = solutions
    else:
        test_classifier = [solutions]
    classifiers = {
        'keywords': keywords_splitter,
        'tags': tags_splitter,
        'sources': sources_splitter,
        'series': series_splitter,
        'numbers': numbers_splitter,
        'abnormal': abnormal_splitter,
        'tagging': tagging_splitter,
    }
    for classifier_index in range(len(test_classifier)):
        starttime = datetime.datetime.now()
        classifier = test_classifier[classifier_index]
        if classifier_index == 0:
            classifiers[classifier](data_path=data_path, save_file_path=save_file_path,
                                    data_index_name=data_index_name, sources_index_name=sources_index_name,
                                    has_header=has_header, keyword_path=keyword_path,
                                    sources_path=sources_path, min_char=min_char, max_char=max_char,
                                    max_symbol=max_symbol,)
        else:
            classifiers[classifier](data_path=save_file_path + r'\clean_data.txt', save_file_path=save_file_path,
                                    data_index_name=data_index_name, sources_index_name=sources_index_name,
                                    has_header=has_header, keyword_path=keyword_path,
                                    sources_path=sources_path, min_char=min_char, max_char=max_char,
                                    max_symbol=max_symbol,)
        endtime = datetime.datetime.now()
        interval = endtime - starttime
        print classifier, ur'cleaning done! Time cost: ', interval


def div_list(ls, n):
    if not isinstance(ls, list) or not isinstance(n, int):
        return []
    ls_len = len(ls)
    if n <= 0 or 0 == ls_len:
        return []
    if n > ls_len:
        return []
    elif n == ls_len:
        return [[i] for i in ls]
    else:
        j = ls_len/n
        ls_return = []
        for i in xrange(0, (n-1)*j, j):
            ls_return.append(ls[i:i+j])
        ls_return.append(ls[(n-1)*j:])

        return ls_return


def apply_clean_ways(raw_data, data_index_name=2, sources_index_name=3,
                     keyword_path=ur"D:\WorkSpace\Data\keywords.txt",
                     sources_path=ur"D:\WorkSpace\Data\trash_sources.txt", min_char=4):
    raw_data = keywords_splitter(data=raw_data, data_path='', save_file_path='',
                                 data_index_name=data_index_name,
                                 has_header=False, keyword_path=keyword_path, )

    raw_data = sources_splitter(data=raw_data, data_path='', save_file_path='',
                                sources_index_name=sources_index_name,
                                has_header=False, sources_path=sources_path, )

    raw_data = series_splitter(data=raw_data, data_path='', save_file_path='',
                               data_index_name=data_index_name,
                               has_header=False)

    raw_data = tags_splitter(data=raw_data, data_path='', save_file_path='',
                             data_index_name=data_index_name, min_char=min_char,
                             has_header=False)
    return raw_data


def weibo_cleaning(data_path, save_file_path=u"D:\WorkSpace\Data",
                   data_index_name=u'text', sources_index_name=u'source',
                   has_header=True, keyword_path=ur"D:\WorkSpace\Data\keywords.txt",
                   sources_path=ur"D:\WorkSpace\Data\trash_sources.txt",
                   min_char=4):
    # 读入数据——可以改为read_csv
    starttime = datetime.datetime.now()
    filename = data_path

    with io.open(filename, "r", encoding='utf-8') as f:
        if has_header:
            header = f.readline().rstrip('\n').rstrip(' ').rstrip('\t').split("\t")
            data_index = header.index(data_index_name)
            sources_index = header.index(sources_index_name)
        else:
            header = None
            data_index = data_index_name
            sources_index = sources_index_name

        data = [line.rstrip('\n').rstrip(' ').rstrip('\t').split("\t") for line in f]
    # print header
    # print len(data)

    clean_data = apply_clean_ways(data, data_index_name=data_index, sources_index_name=sources_index,
                                  keyword_path=keyword_path, sources_path=sources_path, min_char=min_char)

    # 输出数据
    df = pd.DataFrame(clean_data, columns=header)
    df.to_csv(save_file_path + u'\\clean_data.txt', encoding=u'utf-8', index=None,
              sep='\t', mode='w', quoting=csv.QUOTE_NONE,)
    endtime = datetime.datetime.now()
    interval = endtime - starttime
    print ur'Cleaning data done! Time cost: ', interval


if __name__ == u"__main__":
    # find_trash_data(data_path=ur"D:\WorkSpace\Data\WeiboData\1\weibo1.txt", save_file_path=u"D:\WorkSpace\Data",
    #                 solutions=[ur'keywords', ur'tags', ur'sources', ur'series'],
    #                 data_index_name=2, sources_index_name=3, has_header=False)
    # find_trash_data(data_path=ur"D:\WorkSpace\Data\WeiboData\2\weibo1.txt", save_file_path=u"D:\WorkSpace\Data",
    #                 solutions=[ur'keywords', ur'tags', ur'sources', ur'series'],
    #                 data_index_name='text', sources_index_name='source', has_header=True)
    # find_trash_data(data_path=ur"D:\WorkSpace\Data\虎扑---帖1.txt",
    #                 keyword_path=ur"D:\workspace\Data\通用词库1",
    #                 save_file_path=u"D:\WorkSpace\Data",
    #                 solutions=[ur'tagging', ur'numbers'],
    #                 data_index_name='Content', has_header=True)

    weibo_cleaning(data_path=ur"D:\WorkSpace\Data\WeiboData\3\weibo1.txt", save_file_path=u"D:\WorkSpace\Data",)
    # weibo_cleaning(data_path=ur"D:\WorkSpace\Data\clean_data.txt", save_file_path=u"D:\WorkSpace\Data", )

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
