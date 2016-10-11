# coding: utf-8
# __author__: u"John"
from factory import *
from helper import *
import datetime


def keywords_splitter(**parameter_diction):
    keywords_reducer = KeywordsReducer()
    keywords_reducer.data_column_name = parameter_diction['data_index_name']
    keywords_reducer.one_hit_strategy = False
    keywords_reducer.current_dict_abspath = parameter_diction['keyword_path']
    keywords_reducer.current_data_abspath = parameter_diction['data_path']
    keywords_reducer.has_header = parameter_diction['has_header']
    keywords_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        keywords_reducer.main()
        if parameter_diction['save_file_path'] and keywords_reducer.trash_list:
            filename = keywords_reducer.save_file_path + u'\\' + u'keywords_data_trash.txt'
            export_to_txt(data_list=keywords_reducer.trash_list, file_name=filename,
                          column_head=keywords_reducer.header)
        if parameter_diction['save_file_path'] and keywords_reducer.cleaned_list:
            # export_to_excel(data_list=keywords_reducer.cleaned_list,
            #                 file_name=u"data_clean.xlsx",
            #                 column_head=keywords_reducer.header)
            filename = keywords_reducer.save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=keywords_reducer.cleaned_list,
                          file_name=filename,
                          column_head=keywords_reducer.header)
    except Exception as e:
        print str(e)
    return


def numbers_splitter(**parameter_diction):
    numbers_reducer = NumbersReducer()
    numbers_reducer.data_column_name = parameter_diction['data_index_name']
    numbers_reducer.min_numbers = parameter_diction['min_char']
    numbers_reducer.max_numbers = parameter_diction['max_char']
    numbers_reducer.current_data_abspath = parameter_diction['data_path']
    numbers_reducer.has_header = parameter_diction['has_header']
    numbers_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        numbers_reducer.main()
        if parameter_diction['save_file_path'] and numbers_reducer.trash_list:
            filename = numbers_reducer.save_file_path + u'\\' + u'numbers_data_trash.txt'
            export_to_txt(data_list=numbers_reducer.trash_list,
                          file_name=filename,
                          column_head=numbers_reducer.header)
        if parameter_diction['save_file_path'] and numbers_reducer.cleaned_list:
            filename = numbers_reducer.save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=numbers_reducer.cleaned_list,
                          file_name=filename,
                          column_head=numbers_reducer.header)
    except Exception as e:
        print str(e)
    return


def tags_splitter(**parameter_diction):
    tags_reducer = TagsReducer()
    tags_reducer.data_column_name = parameter_diction['data_index_name']
    tags_reducer.numbers = parameter_diction['min_char']
    tags_reducer.current_data_abspath = parameter_diction['data_path']
    tags_reducer.has_header = parameter_diction['has_header']
    tags_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        tags_reducer.main()
        if parameter_diction['save_file_path'] and tags_reducer.trash_list:
            filename = tags_reducer.save_file_path + u'\\' + u'tags_data_trash.txt'
            export_to_txt(data_list=tags_reducer.trash_list,
                          file_name=filename,
                          column_head=tags_reducer.header)
        if parameter_diction['save_file_path'] and tags_reducer.cleaned_list:
            filename = tags_reducer.save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=tags_reducer.cleaned_list,
                          file_name=filename,
                          column_head=tags_reducer.header)
    except Exception as e:
        print str(e)
    return


def abnormal_splitter(**parameter_diction):
    abnormal_reducer = AbnormalReducer()
    abnormal_reducer.data_column_name = parameter_diction['data_index_name']
    abnormal_reducer.abnormal = parameter_diction['max_symbol']
    abnormal_reducer.current_data_abspath = parameter_diction['data_path']
    abnormal_reducer.has_header = parameter_diction['has_header']
    abnormal_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        abnormal_reducer.main()
        if parameter_diction['save_file_path'] and abnormal_reducer.trash_list:
            filename = abnormal_reducer.save_file_path + u'\\' + u'abnormal_data_trash.txt'
            export_to_txt(data_list=abnormal_reducer.trash_list,
                          file_name=filename,
                          column_head=abnormal_reducer.header)
        if parameter_diction['save_file_path'] and abnormal_reducer.cleaned_list:
            filename = abnormal_reducer.save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=abnormal_reducer.cleaned_list,
                          file_name=filename,
                          column_head=abnormal_reducer.header)
    except Exception as e:
        print str(e)
    return


def series_splitter(**parameter_diction):
    series_reducer = SeriesReducer()
    series_reducer.data_column_name = parameter_diction['data_index_name']
    series_reducer.current_data_abspath = parameter_diction['data_path']
    series_reducer.has_header = parameter_diction['has_header']
    series_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        series_reducer.main()
        if parameter_diction['save_file_path'] and series_reducer.trash_list:
            filename = series_reducer.save_file_path + u'\\' + u'series_data_trash.txt'
            export_to_txt(data_list=series_reducer.trash_list,
                          file_name=filename,
                          column_head=series_reducer.header)
        if parameter_diction['save_file_path'] and series_reducer.cleaned_list:
            filename = series_reducer.save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=series_reducer.cleaned_list,
                          file_name=filename,
                          column_head=series_reducer.header)
    except Exception as e:
        print str(e)
    return


def dictionary_splitter(**parameter_diction):
    dictionary_reducer = DictionaryReducer()
    dictionary_reducer.data_column_name = parameter_diction['data_index_name']
    dictionary_reducer.current_data_abspath = parameter_diction['data_path']
    dictionary_reducer.current_dict_abspath = parameter_diction['keyword_path']
    dictionary_reducer.has_header = parameter_diction['has_header']
    dictionary_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        dictionary_reducer.main()
    except Exception as e:
        print str(e)
    return


def sources_splitter(**parameter_diction):
    keywords_reducer = SourcesReducer()
    keywords_reducer.data_column_name = parameter_diction['sources_index_name']
    keywords_reducer.current_dict_abspath = parameter_diction['sources_path']
    keywords_reducer.current_data_abspath = parameter_diction['data_path']
    keywords_reducer.has_header = parameter_diction['has_header']
    keywords_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        keywords_reducer.main()
        if parameter_diction['save_file_path'] and keywords_reducer.trash_list:
            filename = keywords_reducer.save_file_path + u'\\' + u'sources_data_trash.txt'
            export_to_txt(data_list=keywords_reducer.trash_list, file_name=filename,
                          column_head=keywords_reducer.header)
        if parameter_diction['save_file_path'] and keywords_reducer.cleaned_list:
            # export_to_excel(data_list=keywords_reducer.cleaned_list,
            #                 file_name=u"data_clean.xlsx",
            #                 column_head=None)
            filename = keywords_reducer.save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=keywords_reducer.cleaned_list, file_name=filename,
                          column_head=keywords_reducer.header)
    except Exception as e:
        print str(e)
    return


def find_clean_data(data_path, save_file_path=u"D:\WorkSpace\Data",
                    solutions=[ur'keywords', ur'tags', ur'sources', ur'series'],
                    data_index_name='text', sources_index_name='source',
                    has_header=True, keyword_path=ur"D:\WorkSpace\Data\keywords.txt",
                    sources_path=ur"D:\WorkSpace\Data\trash_sources.txt",
                    min_char=10, max_char=600, max_symbol=5, ):

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
        'abnomal': abnormal_splitter,
        'dictionary': dictionary_splitter,
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
        print ur'Cleaning data done! Time cost: ', interval

if __name__ == u"__main__":
    find_clean_data(data_path=ur"D:\WorkSpace\Data\WeiboData\1\weibo1.txt", save_file_path=u"D:\WorkSpace\Data",
                    solutions=[ur'keywords', ur'tags', ur'sources', ur'series'],
                    data_index_name='text', sources_index_name='source', has_header=True,)
    # find_clean_data(data_path=ur"D:\WorkSpace\Data\虎扑---帖1.txt", keyword_path=ur"D:\workspace\Data\通用词库",
    #                 save_file_path=u"D:\WorkSpace\Data",
    #                 solutions=[ur'numbers', ur'dictionary'],
    #                 data_data_index_name='Content', has_header=True)

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
