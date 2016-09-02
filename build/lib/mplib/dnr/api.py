# coding: utf-8
# __author__: u"John"
from factory import *
from helper import *
import datetime


def keywords_splitter(data_path, keywords_path, index=2, one_hit=False,savefile_path=u"D:\WorkSpace\Data",header=False):
    keywords_reducer = KeywordsReducer()
    keywords_reducer.data_column_index = index
    keywords_reducer.one_hit_strategy = one_hit
    keywords_reducer.current_dict_abspath = keywords_path
    keywords_reducer.current_data_abspath = data_path
    keywords_reducer.header = header
    try:
        keywords_reducer.main()
        if keywords_reducer.trash_list:
            filename = savefile_path + u'\\' + u'keywords_data_trash.txt'
            export_to_txt(data_list=keywords_reducer.trash_list,
                            file_name=filename,
                            column_head=None)
        if keywords_reducer.cleaned_list:
            # export_to_excel(data_list=keywords_reducer.cleaned_list,
            #                 file_name=u"data_clean.xlsx",
            #                 column_head=None)
            filename = savefile_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=keywords_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return

def numbers_splitter(data_path, min_number=8, index=2, savefile_path=u"D:\WorkSpace\Data",header=False):
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
            filename = savefile_path + u'\\' + u'numbers_data_trash.txt'
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
            filename = savefile_path + u'\\' + u'clean_data.txt'
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

def tags_splitter(data_path, min_number=8, index=2, savefile_path=u"D:\WorkSpace\Data",header=False):
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
            filename = savefile_path + u'\\' + u'tags_data_trash.txt'
            export_to_txt(data_list=tags_reducer.trash_list,
                          file_name=filename,
                          column_head=None)
            # export_to_excel(data_list=numbers_reducer.trash_list,
            #                 file_name=u"{0}_trash.xlsx".format(numbers_reducer.export_name),
            #                 column_head=trash_head)
        if tags_reducer.cleaned_list:
            filename = savefile_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=tags_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return

def abnormal_splitter(data_path, abnormal_number=5, index=2, savefile_path=u"D:\WorkSpace\Data",header=False):
    abnormal_reducer = AbnormalReducer()
    abnormal_reducer.data_column_index = index
    abnormal_reducer.abnormal = abnormal_number
    abnormal_reducer.current_data_abspath = data_path
    abnormal_reducer.header = header
    try:
        abnormal_reducer.main()
        if abnormal_reducer.trash_list:
            filename = savefile_path + u'\\' + u'abnormals_data_trash.txt'
            export_to_txt(data_list=abnormal_reducer.trash_list,
                          file_name=filename,
                          column_head=None)
        if abnormal_reducer.cleaned_list:
            filename = savefile_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=abnormal_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return

def sources_splitter(data_path, sources_path, index=3, savefile_path=u"D:\WorkSpace\Data",header=False):
    keywords_reducer = SourcesReducer()
    keywords_reducer.data_column_index = index
    keywords_reducer.current_dict_abspath = sources_path
    keywords_reducer.current_data_abspath = data_path
    keywords_reducer.header = header
    try:
        keywords_reducer.main()
        if keywords_reducer.trash_list:
            filename = savefile_path + u'\\' + u'sources_data_trash.txt'
            export_to_txt(data_list=keywords_reducer.trash_list,
                            file_name=filename,
                            column_head=None)
        if keywords_reducer.cleaned_list:
            # export_to_excel(data_list=keywords_reducer.cleaned_list,
            #                 file_name=u"data_clean.xlsx",
            #                 column_head=None)
            filename = savefile_path + u'\\' + u'clean_data.txt'
            export_to_txt(data_list=keywords_reducer.cleaned_list,
                          file_name=filename,
                          column_head=None)
    except Exception as e:
        print str(e)
    return

def name_correct(str):
    name_dic = {
        'keywords': ['keywords', 'keyword', 'key', 'k'],
        'tags': ['tags', 'tag', 't'],
        'abnormal': ['abnormal', 'abnormals', 'ab', 'a'],
        'sources': ['sources', 'source', 's']
    }

def find_clean_data(data_path,
                    savefile_path=u"D:\WorkSpace\Data",
                    solutions=[ur'keywords', ur'sources', ur'tags'],
                    content_index=2,
                    sources_index=3,
                    header=True,
                    keyword_path=ur"D:\WorkSpace\Data\keywords.txt",
                    sources_path=ur"D:\WorkSpace\Data\trash_sources.txt",
                    min_char=10,
                    max_symbol=5,
                    ):
    operator = {'keywords': keywords_splitter,
                'tags': tags_splitter,
                'abnormal': abnormal_splitter,
                'sources': sources_splitter}
    if isinstance(solutions, str) or isinstance(solutions, unicode):
        cur_solution = solutions.decode('utf=8')
        if cur_solution == ur'keywords':
            keywords_splitter(data_path=data_path,
                              keywords_path=keyword_path,
                              savefile_path=u"D:\WorkSpace\Data",
                              index=content_index,
                              header=True,
                              )
        if cur_solution == ur'tags':
            tags_splitter(data_path=data_path,
                          min_number=min_char,
                          savefile_path=u"D:\WorkSpace\Data",
                          index=content_index,
                          header=True,
                          )
        if cur_solution == ur'abnormal':
            abnormal_splitter(data_path=data_path,
                              abnormal_number=max_symbol,
                              index=content_index,
                              savefile_path=savefile_path,
                              header=True,
                              )
        if cur_solution == ur'sources':
            sources_splitter(data_path=data_path,
                             sources_path=sources_path,
                             index=sources_index,
                             savefile_path=savefile_path,
                             header=True,
                             )
        if cur_solution == ur'numbers':
            numbers_splitter(data_path=data_path,
                             aim_number=min_char,
                             index=content_index,
                             savefile_path=savefile_path,
                             header=True,
                             )
    if isinstance(solutions, list):
        for i in range(len(solutions)):
            starttime = datetime.datetime.now()
            if i == 0:
                data_path = data_path
                name = solutions[i]
                cur_solution = name.decode('utf=8')
                if cur_solution == ur'keywords':
                    keywords_splitter(data_path=data_path,
                                      keywords_path=keyword_path,
                                      savefile_path=u"D:\WorkSpace\Data",
                                      index=content_index,
                                      header=True,
                                      )
                if cur_solution == ur'tags':
                    tags_splitter(data_path=data_path,
                                  min_number=min_char,
                                  savefile_path=u"D:\WorkSpace\Data",
                                  index=content_index,
                                  header=True,
                                  )
                if cur_solution == ur'abnormal':
                    abnormal_splitter(data_path=data_path,
                                      abnormal_number=max_symbol,
                                      index=content_index,
                                      savefile_path=savefile_path,
                                      header=True,
                                      )
                if cur_solution == ur'sources':
                    sources_splitter(data_path=data_path,
                                     sources_path=sources_path,
                                     index=sources_index,
                                     savefile_path=savefile_path,
                                     header=True,
                                     )
                if cur_solution == ur'numbers':
                    numbers_splitter(data_path=data_path,
                                     aim_number=min_char,
                                     index=content_index,
                                     savefile_path=savefile_path,
                                     header=True,
                                     )
            else:
                data_path = savefile_path+r'\clean_data.txt'
                name = solutions[i]
                cur_solution = name.decode('utf=8')
                if cur_solution == ur'keywords':
                    keywords_splitter(data_path=data_path,
                                      keywords_path=keyword_path,
                                      savefile_path=u"D:\WorkSpace\Data",
                                      index=content_index,
                                      header=True,
                                      )
                if cur_solution == ur'tags':
                    tags_splitter(data_path=data_path,
                                  min_number=min_char,
                                  savefile_path=u"D:\WorkSpace\Data",
                                  index=content_index,
                                  header=False,
                                  )
                if cur_solution == ur'abnormal':
                    abnormal_splitter(data_path=data_path,
                                      abnormal_number=max_symbol,
                                      index=content_index,
                                      savefile_path=savefile_path,
                                      header=False,
                                      )
                if cur_solution == ur'sources':
                    sources_splitter(data_path=data_path,
                                     sources_path=sources_path,
                                     index=sources_index,
                                     savefile_path=savefile_path,
                                     header=False,
                                     )
                if cur_solution == ur'numbers':
                    numbers_splitter(data_path=data_path,
                                     aim_number=min_char,
                                     index=content_index,
                                     savefile_path=savefile_path,
                                     header=False,
                                     )
            endtime = datetime.datetime.now()
            interval = (endtime - starttime).seconds
            print '' + solutions[i] + ur' Done!'
            print 'Time consuming: ' + str(interval) + 's'
    print ur'Cleaning data done!'

if __name__ == u"__main__":
    find_clean_data(data_path=ur"D:\WorkSpace\Data\data_sample.txt",
                    savefile_path=u"D:\WorkSpace\Data",
                    solutions=[ur'keywords', ur'tags', ur'sources'],
                    content_index=2,
                    sources_index=3,
                    header=False,
                    )
    # numbers_splitter(data_path=ur"D:\WorkSpace\Data\weibo1.txt",
    #                  )
    # tags_splitter(data_path=ur"D:\WorkSpace\Data\sources_data_clean.txt",
    #                  )
    # abnormal_splitter(data_path=ur"D:\WorkSpace\Data\numbers_data_clean.txt",
    #                   )
    # keywords_splitter(data_path=ur"D:\WorkSpace\Data\abnormals_data_clean.txt",
    #                   keywords_path=ur"D:\WorkSpace\Data\keywords.txt",
    #                   index=2, one_hit=False)

    # keywords_splitter(data_path=ur"D:\WorkSpace\Data\weibo1.txt",
    #                   keywords_path=ur"D:\WorkSpace\Data\keywords.txt",
    #                   index=2, one_hit=False)
    # sources_splitter(data_path=ur"D:\WorkSpace\Data\keywords_data_clean.txt",
    #                   sources_path=ur"D:\WorkSpace\Data\trash_sources.txt",
    #                   index=3)

