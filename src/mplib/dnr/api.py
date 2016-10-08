# coding: utf-8
# __author__: u"John"
from factory import *
from helper import *
import datetime


def keywords_splitter(data_path, keywords_path, index=2, one_hit=False, save_file_path=u"D:\WorkSpace\Data",
                      header=False):
    keywords_reducer = KeywordsReducer()
    keywords_reducer.data_column_index = index
    keywords_reducer.one_hit_strategy = one_hit
    keywords_reducer.current_dict_abspath = keywords_path
    keywords_reducer.current_data_abspath = data_path
    keywords_reducer.header = header
    try:
        keywords_reducer.main()
        if keywords_reducer.trash_list:
            filename = save_file_path + u'\\' + u'keywords_data_trash.txt'
            export_to_txt(data_list=keywords_reducer.trash_list, file_name=filename, column_head=None)
        if keywords_reducer.cleaned_list:
            # export_to_excel(data_list=keywords_reducer.cleaned_list,
            #                 file_name=u"data_clean.xlsx",
            #                 column_head=None)
            filename = save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=keywords_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return


def numbers_splitter(data_path, min_number=8, index=2, save_file_path=u"D:\WorkSpace\Data", header=False):
    numbers_reducer = NumbersReducer()
    numbers_reducer.data_column_index = index
    numbers_reducer.numbers = min_number
    numbers_reducer.current_data_abspath = data_path
    numbers_reducer.header = header
    try:
        numbers_reducer.main()
        # pickle_dump(file_name=u"trash_list", dump_object=keywords_reducer.trash_list)
        # pickle_dump(file_name=u"cleaned_list", dump_object=keywords_reducer.cleaned_list)
        if numbers_reducer.trash_list:
            filename = save_file_path + u'\\' + u'numbers_data_trash.txt'
            export_to_txt(data_list=numbers_reducer.trash_list,
                          file_name=filename,
                          column_head=None)
            # tll = len(numbers_reducer.trash_list[0])
            # if tll == 1:
            #     numbers_reducer.trash_list = [i for i in numbers_reducer.trash_list]
            # trash_head = [u"数据列" if i == index else u"原数据第{0}列".format(i) for i in xrange(tll)]
            # export_to_excel(data_list=numbers_reducer.trash_list,
            #                 file_name=u"{0}_trash.xlsx".format(numbers_reducer.export_name),
            #                 column_head=trash_head)
        if numbers_reducer.cleaned_list:
            filename = save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=numbers_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
            # cll = len(numbers_reducer.cleaned_list[0])
            # clean_head = [u"数据列" if i == index else u"原数据第{0}列".format(i) for i in xrange(cll)]
            # export_to_excel(data_list=numbers_reducer.cleaned_list,
            #                 file_name=u"{0}_cleaned.xlsx".format(numbers_reducer.export_name),
            #                 column_head=clean_head)
    except Exception as e:
        print str(e)
    return


def tags_splitter(data_path, min_number=8, index=2, save_file_path=u"D:\WorkSpace\Data", header=False):
    tags_reducer = TagsReducer()
    tags_reducer.data_column_index = index
    tags_reducer.numbers = min_number
    tags_reducer.current_data_abspath = data_path
    tags_reducer.header = header
    try:
        tags_reducer.main()
        # pickle_dump(file_name=u"trash_list", dump_object=keywords_reducer.trash_list)
        # pickle_dump(file_name=u"cleaned_list", dump_object=keywords_reducer.cleaned_list)
        if tags_reducer.trash_list:
            filename = save_file_path + u'\\' + u'tags_data_trash.txt'
            export_to_txt(data_list=tags_reducer.trash_list,
                          file_name=filename,
                          column_head=None)
            # export_to_excel(data_list=numbers_reducer.trash_list,
            #                 file_name=u"{0}_trash.xlsx".format(numbers_reducer.export_name),
            #                 column_head=trash_head)
        if tags_reducer.cleaned_list:
            filename = save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=tags_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return


def abnormal_splitter(data_path, abnormal_number=5, index=2, save_file_path=u"D:\WorkSpace\Data", header=False):
    abnormal_reducer = AbnormalReducer()
    abnormal_reducer.data_column_index = index
    abnormal_reducer.abnormal = abnormal_number
    abnormal_reducer.current_data_abspath = data_path
    abnormal_reducer.header = header
    try:
        abnormal_reducer.main()
        if abnormal_reducer.trash_list:
            filename = save_file_path + u'\\' + u'abnormal_data_trash.txt'
            export_to_txt(data_list=abnormal_reducer.trash_list,
                          file_name=filename,
                          column_head=None)
        if abnormal_reducer.cleaned_list:
            filename = save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=abnormal_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return


def series_splitter(data_path, index=2, save_file_path=u"D:\WorkSpace\Data", header=False):
    series_reducer = SeriesReducer()
    series_reducer.data_column_index = index
    series_reducer.current_data_abspath = data_path
    series_reducer.header = header
    try:
        series_reducer.main()
        if series_reducer.trash_list:
            filename = save_file_path + u'\\' + u'series_data_trash.txt'
            export_to_txt(data_list=series_reducer.trash_list,
                          file_name=filename,
                          column_head=None)
        if series_reducer.cleaned_list:
            filename = save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=series_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return


def sources_splitter(data_path, sources_path, index=3, save_file_path=u"D:\WorkSpace\Data", header=False):
    keywords_reducer = SourcesReducer()
    keywords_reducer.data_column_index = index
    keywords_reducer.current_dict_abspath = sources_path
    keywords_reducer.current_data_abspath = data_path
    keywords_reducer.header = header
    try:
        keywords_reducer.main()
        if keywords_reducer.trash_list:
            filename = save_file_path + u'\\' + u'sources_data_trash.txt'
            export_to_txt(data_list=keywords_reducer.trash_list, file_name=filename, column_head=None)
        if keywords_reducer.cleaned_list:
            # export_to_excel(data_list=keywords_reducer.cleaned_list,
            #                 file_name=u"data_clean.xlsx",
            #                 column_head=None)
            filename = save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=keywords_reducer.cleaned_list, file_name=filename, column_head=None)
    except Exception as e:
        print str(e)
    return


def find_clean_data(data_path, save_file_path=u"D:\WorkSpace\Data", solutions=ur'keywords',
                    content_index=2, sources_index=3,
                    header=True, keyword_path=ur"D:\WorkSpace\Data\keywords.txt",
                    sources_path=ur"D:\WorkSpace\Data\trash_sources.txt",
                    min_char=10, max_symbol=5,):
    if isinstance(solutions, str) or isinstance(solutions, unicode):
        cur_solution = solutions.decode('utf-8')
        if cur_solution == ur'keywords':
            keywords_splitter(data_path=data_path,
                              keywords_path=keyword_path,
                              save_file_path=u"D:\WorkSpace\Data",
                              index=content_index,
                              header=header,
                              )
        if cur_solution == ur'tags':
            tags_splitter(data_path=data_path,
                          min_number=min_char,
                          save_file_path=u"D:\WorkSpace\Data",
                          index=content_index,
                          header=header,
                          )
        if cur_solution == ur'abnormal':
            abnormal_splitter(data_path=data_path,
                              abnormal_number=max_symbol,
                              index=content_index,
                              save_file_path=save_file_path,
                              header=True,
                              )
        if cur_solution == ur'sources':
            sources_splitter(data_path=data_path,
                             sources_path=sources_path,
                             index=sources_index,
                             save_file_path=save_file_path,
                             header=header,
                             )
        if cur_solution == ur'numbers':
            numbers_splitter(data_path=data_path,
                             min_number=min_char,
                             index=content_index,
                             save_file_path=save_file_path,
                             header=header,
                             )
        if cur_solution == ur'series':
            series_splitter(data_path=data_path,
                            index=content_index,
                            save_file_path=save_file_path,
                            header=header,
                            )
    if isinstance(solutions, list):
        for solutions_index in range(len(solutions)):
            start_time = datetime.datetime.now()
            if solutions_index == 0:
                data_path = data_path
                name = solutions[solutions_index]
                cur_solution = name.decode('utf-8')
                if cur_solution == ur'keywords':
                    keywords_splitter(data_path=data_path,
                                      keywords_path=keyword_path,
                                      save_file_path=u"D:\WorkSpace\Data",
                                      index=content_index,
                                      header=header,
                                      )
                if cur_solution == ur'tags':
                    tags_splitter(data_path=data_path,
                                  min_number=min_char,
                                  save_file_path=u"D:\WorkSpace\Data",
                                  index=content_index,
                                  header=header,
                                  )
                if cur_solution == ur'abnormal':
                    abnormal_splitter(data_path=data_path,
                                      abnormal_number=max_symbol,
                                      index=content_index,
                                      save_file_path=save_file_path,
                                      header=header,
                                      )
                if cur_solution == ur'sources':
                    sources_splitter(data_path=data_path,
                                     sources_path=sources_path,
                                     index=sources_index,
                                     save_file_path=save_file_path,
                                     header=header,
                                     )
                if cur_solution == ur'numbers':
                    numbers_splitter(data_path=data_path,
                                     min_number=min_char,
                                     index=content_index,
                                     save_file_path=save_file_path,
                                     header=header,
                                     )
                if cur_solution == ur'series':
                    series_splitter(data_path=data_path,
                                    index=content_index,
                                    save_file_path=save_file_path,
                                    header=header,
                                    )
            else:
                data_path = save_file_path+r'\clean_data.txt'
                name = solutions[solutions_index]
                cur_solution = name.decode('utf-8')
                # 没有表头
                if cur_solution == ur'keywords':
                    keywords_splitter(data_path=data_path,
                                      keywords_path=keyword_path,
                                      save_file_path=save_file_path,
                                      index=content_index,
                                      header=False,
                                      )
                if cur_solution == ur'tags':
                    tags_splitter(data_path=data_path,
                                  min_number=min_char,
                                  save_file_path=save_file_path,
                                  index=content_index,
                                  header=False,
                                  )
                if cur_solution == ur'abnormal':
                    abnormal_splitter(data_path=data_path,
                                      abnormal_number=max_symbol,
                                      index=content_index,
                                      save_file_path=save_file_path,
                                      header=False,
                                      )
                if cur_solution == ur'sources':
                    sources_splitter(data_path=data_path,
                                     sources_path=sources_path,
                                     index=sources_index,
                                     save_file_path=save_file_path,
                                     header=False,
                                     )
                if cur_solution == ur'numbers':
                    numbers_splitter(data_path=data_path,
                                     min_number=min_char,
                                     index=content_index,
                                     save_file_path=save_file_path,
                                     header=False,
                                     )
                if cur_solution == ur'series':
                    series_splitter(data_path=data_path,
                                    index=content_index,
                                    save_file_path=save_file_path,
                                    header=False,
                                    )
            end_time = datetime.datetime.now()
            interval = (end_time - start_time).seconds
            print '' + solutions[solutions_index] + ur' Done!'
            print 'Time consuming: ' + str(interval) + 's'
    print ur'Cleaning data done!'


def keywords_splitter2(**parameter_diction):
    keywords_reducer = KeywordsReducer()
    keywords_reducer.data_column_index = parameter_diction['content_index']
    keywords_reducer.one_hit_strategy = False
    keywords_reducer.current_dict_abspath = parameter_diction['keyword_path']
    keywords_reducer.current_data_abspath = parameter_diction['data_path']
    keywords_reducer.header = parameter_diction['header']
    keywords_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        keywords_reducer.main()
        if parameter_diction['save_file_path'] and keywords_reducer.trash_list:
            filename = keywords_reducer.save_file_path + u'\\' + u'keywords_data_trash.txt'
            export_to_txt(data_list=keywords_reducer.trash_list, file_name=filename, column_head=None)
        if parameter_diction['save_file_path'] and keywords_reducer.cleaned_list:
            # export_to_excel(data_list=keywords_reducer.cleaned_list,
            #                 file_name=u"data_clean.xlsx",
            #                 column_head=None)
            filename = keywords_reducer.save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=keywords_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return


def numbers_splitter2(**parameter_diction):
    numbers_reducer = NumbersReducer()
    numbers_reducer.data_column_index = parameter_diction['content_index']
    numbers_reducer.numbers = parameter_diction['min_char']
    numbers_reducer.current_data_abspath = parameter_diction['data_path']
    numbers_reducer.header = parameter_diction['header']
    numbers_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        numbers_reducer.main()
        if parameter_diction['save_file_path'] and numbers_reducer.trash_list:
            filename = numbers_reducer.save_file_path + u'\\' + u'numbers_data_trash.txt'
            export_to_txt(data_list=numbers_reducer.trash_list,
                          file_name=filename,
                          column_head=None)
        if parameter_diction['save_file_path'] and numbers_reducer.cleaned_list:
            filename = numbers_reducer.save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=numbers_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return


def tags_splitter2(**parameter_diction):
    tags_reducer = TagsReducer()
    tags_reducer.data_column_index = parameter_diction['content_index']
    tags_reducer.numbers = parameter_diction['min_char']
    tags_reducer.current_data_abspath = parameter_diction['data_path']
    tags_reducer.header = parameter_diction['header']
    tags_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        tags_reducer.main()
        if parameter_diction['save_file_path'] and tags_reducer.trash_list:
            filename = tags_reducer.save_file_path + u'\\' + u'tags_data_trash.txt'
            export_to_txt(data_list=tags_reducer.trash_list,
                          file_name=filename,
                          column_head=None)
        if parameter_diction['save_file_path'] and tags_reducer.cleaned_list:
            filename = tags_reducer.save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=tags_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return


def abnormal_splitter2(**parameter_diction):
    abnormal_reducer = AbnormalReducer()
    abnormal_reducer.data_column_index = parameter_diction['content_index']
    abnormal_reducer.abnormal = parameter_diction['max_symbol']
    abnormal_reducer.current_data_abspath = parameter_diction['data_path']
    abnormal_reducer.header = parameter_diction['header']
    abnormal_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        abnormal_reducer.main()
        if parameter_diction['save_file_path'] and abnormal_reducer.trash_list:
            filename = abnormal_reducer.save_file_path + u'\\' + u'abnormal_data_trash.txt'
            export_to_txt(data_list=abnormal_reducer.trash_list,
                          file_name=filename,
                          column_head=None)
        if parameter_diction['save_file_path'] and abnormal_reducer.cleaned_list:
            filename = abnormal_reducer.save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=abnormal_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return


def series_splitter2(**parameter_diction):
    series_reducer = SeriesReducer()
    series_reducer.data_column_index = parameter_diction['content_index']
    series_reducer.current_data_abspath = parameter_diction['data_path']
    series_reducer.header = parameter_diction['header']
    series_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        series_reducer.main()
        if parameter_diction['save_file_path'] and series_reducer.trash_list:
            filename = series_reducer.save_file_path + u'\\' + u'series_data_trash.txt'
            export_to_txt(data_list=series_reducer.trash_list,
                          file_name=filename,
                          column_head=None)
        if parameter_diction['save_file_path'] and series_reducer.cleaned_list:
            filename = series_reducer.save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=series_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return


def sources_splitter2(**parameter_diction):
    keywords_reducer = SourcesReducer()
    keywords_reducer.data_column_index = parameter_diction['sources_index']
    keywords_reducer.current_dict_abspath = parameter_diction['sources_path']
    keywords_reducer.current_data_abspath = parameter_diction['data_path']
    keywords_reducer.header = parameter_diction['header']
    keywords_reducer.save_file_path = parameter_diction['save_file_path']
    try:
        keywords_reducer.main()
        if parameter_diction['save_file_path'] and keywords_reducer.trash_list:
            filename = keywords_reducer.save_file_path + u'\\' + u'sources_data_trash.txt'
            export_to_txt(data_list=keywords_reducer.trash_list, file_name=filename, column_head=None)
        if parameter_diction['save_file_path'] and keywords_reducer.cleaned_list:
            # export_to_excel(data_list=keywords_reducer.cleaned_list,
            #                 file_name=u"data_clean.xlsx",
            #                 column_head=None)
            filename = keywords_reducer.save_file_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=keywords_reducer.cleaned_list, file_name=filename, column_head=None)
    except Exception as e:
        print str(e)
    return


def find_clean_data2(data_path, save_file_path=u"D:\WorkSpace\Data",
                     solutions=ur'keywords',
                     content_index=2, sources_index=3,
                     header=True, keyword_path=ur"D:\WorkSpace\Data\keywords.txt",
                     sources_path=ur"D:\WorkSpace\Data\trash_sources.txt",
                     min_char=10, max_symbol=5, ):
    if isinstance(solutions, list):
        test_classifier = solutions
    else:
        test_classifier = [solutions]
    classifiers = {
        'keywords': keywords_splitter2,
        'tags': tags_splitter2,
        'sources': sources_splitter2,
        'series': series_splitter2,
        'numbers': numbers_splitter2,
        'abnomal': abnormal_splitter2,
    }
    parameters_dic1 = {
        'data_path': data_path,
        'save_file_path': save_file_path,
        'content_index': content_index,
        'sources_index': sources_index,
        'header': header,
        'keyword_path': keyword_path,
        'sources_path': sources_path,
        'min_char': min_char,
        'max_symbol': max_symbol,
    }
    parameters_dic2 = {
        'data_path': save_file_path + r'\clean_data.txt',
        'save_file_path': save_file_path,
        'content_index': content_index,
        'sources_index': sources_index,
        'header': False,
        'keyword_path': keyword_path,
        'sources_path': sources_path,
        'min_char': min_char,
        'max_symbol': max_symbol,
    }
    for classifier_index in range(len(test_classifier)):
        classifier = test_classifier[classifier_index]
        if classifier_index == 0:
            classifiers[classifier](**parameters_dic1)
        else:
            classifiers[classifier](**parameters_dic2)

        print ur'Cleaning data done!'


def find_clean_data3(data_path, save_file_path=u"D:\WorkSpace\Data",
                     solutions=ur'keywords',
                     content_index=2, sources_index=3,
                     header=True, keyword_path=ur"D:\WorkSpace\Data\keywords.txt",
                     sources_path=ur"D:\WorkSpace\Data\trash_sources.txt",
                     min_char=10, max_symbol=5, ):

    if isinstance(solutions, list):
        test_classifier = solutions
    else:
        test_classifier = [solutions]
    classifiers = {
        'keywords': keywords_splitter2,
        'tags': tags_splitter2,
        'sources': sources_splitter2,
        'series': series_splitter2,
        'numbers': numbers_splitter2,
        'abnomal': abnormal_splitter2,
    }

    for classifier_index in range(len(test_classifier)):
        starttime = datetime.datetime.now()
        classifier = test_classifier[classifier_index]
        if classifier_index == 0:
            classifiers[classifier](data_path=data_path, save_file_path=save_file_path,
                                    content_index=content_index, sources_index=sources_index,
                                    header=header, keyword_path=keyword_path,
                                    sources_path=sources_path, min_char=min_char, max_symbol=max_symbol,)
        else:
            classifiers[classifier](data_path=save_file_path + r'\clean_data.txt', save_file_path=save_file_path,
                                    content_index=content_index, sources_index=sources_index,
                                    header=False, keyword_path=keyword_path,
                                    sources_path=sources_path, min_char=min_char, max_symbol=max_symbol,)
        endtime = datetime.datetime.now()
        interval = endtime - starttime
        print ur'Cleaning data done! Time cost: ', interval

if __name__ == u"__main__":
    find_clean_data3(data_path=ur"D:\WorkSpace\Data\data_sample.txt", save_file_path=u"D:\WorkSpace\Data",
                     solutions=[ur'keywords', ur'tags', ur'sources', ur'series'],
                     content_index=2, sources_index=3, header=False,)
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
