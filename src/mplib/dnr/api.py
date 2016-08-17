# coding: utf-8
# __author__: u"John"
from factory import *
from helper import *


def keywords_splitter(data_path, keywords_path, safewords_path='', index=2, one_hit=False):
    keywords_reducer = KeywordsReducer()
    keywords_reducer.data_column_index = index
    keywords_reducer.one_hit_strategy = one_hit
    keywords_reducer.current_dict_abspath = keywords_path
    keywords_reducer.current_data_abspath = data_path
    keywords_reducer.current_safedict_abspath = safewords_path
    try:
        keywords_reducer.main()
        # pickle_dump(file_name=u"trash_list", dump_object=keywords_reducer.trash_list)
        # pickle_dump(file_name=u"cleaned_list", dump_object=keywords_reducer.cleaned_list)
        if keywords_reducer.trash_list:
            export_to_excel(data_list=keywords_reducer.trash_list,
                             file_name=u"data_trash.xlsx",
                             column_head=None)
            # tll = len(keywords_reducer.trash_list[0])
            # if tll == 1:
            #     keywords_reducer.trash_list = [i for i in keywords_reducer.trash_list]
            # trash_head = [u"数据列" if i == index else u"原数据第{0}列".format(i) for i in xrange(tll)]
            # export_to_excel(data_list=keywords_reducer.trash_list,
            #                 file_name=u"{0}_trash.xlsx".format(keywords_reducer.export_name),
            #                 column_head=trash_head)
        if keywords_reducer.cleaned_list:
            export_to_excel(data_list=keywords_reducer.cleaned_list,
                            file_name=u"data_clean.xlsx",
                            column_head=None)
            # cll = len(keywords_reducer.cleaned_list[0])
            # clean_head = [u"数据列" if i == index else u"原数据第{0}列".format(i) for i in xrange(cll)]
            # export_to_excel(data_list=keywords_reducer.cleaned_list,
            #                 file_name=u"{0}_cleaned.xlsx".format(keywords_reducer.export_name),
            #                 column_head=clean_head)
    except Exception as e:
        print str(e)
    return

def numbers_splitter(data_path=False, aim_number=3, index=1):
    numbers_reducer = NumbersReducer()
    numbers_reducer.data_column_index = index
    numbers_reducer.numbers = aim_number
    numbers_reducer.current_data_abspath = data_path
    try:
        numbers_reducer.main()
        # pickle_dump(file_name=u"trash_list", dump_object=keywords_reducer.trash_list)
        # pickle_dump(file_name=u"cleaned_list", dump_object=keywords_reducer.cleaned_list)
        if numbers_reducer.trash_list:
            print 'there ara trashes.'
            for row in numbers_reducer.trash_list:
                print row
            # tll = len(numbers_reducer.trash_list[0])
            # if tll == 1:
            #     numbers_reducer.trash_list = [i for i in numbers_reducer.trash_list]
            # trash_head = [u"数据列" if i == index else u"原数据第{0}列".format(i) for i in xrange(tll)]
            # export_to_excel(data_list=numbers_reducer.trash_list,
            #                 file_name=u"{0}_trash.xlsx".format(numbers_reducer.export_name),
            #                 column_head=trash_head)
        if numbers_reducer.cleaned_list:
            print 'there ara cleans.'
            for row in numbers_reducer.cleaned_list:
                print row
            # cll = len(numbers_reducer.cleaned_list[0])
            # clean_head = [u"数据列" if i == index else u"原数据第{0}列".format(i) for i in xrange(cll)]
            # export_to_excel(data_list=numbers_reducer.cleaned_list,
            #                 file_name=u"{0}_cleaned.xlsx".format(numbers_reducer.export_name),
            #                 column_head=clean_head)
    except Exception as e:
        print str(e)
    return

def abnormal_splitter(data_path=False, abnormal_number=30, index=1):
    abnormal_reducer = AbnormalReducer()
    abnormal_reducer.data_column_index = index
    abnormal_reducer.numbers = abnormal_number
    abnormal_reducer.current_data_abspath = data_path
    try:
        abnormal_reducer.main()
        # pickle_dump(file_name=u"trash_list", dump_object=keywords_reducer.trash_list)
        # pickle_dump(file_name=u"cleaned_list", dump_object=keywords_reducer.cleaned_list)
        if abnormal_reducer.trash_list:
            print 'there ara trashes.'
            for row in abnormal_reducer.trash_list:
                print row
            # tll = len(numbers_reducer.trash_list[0])
            # if tll == 1:
            #     numbers_reducer.trash_list = [i for i in numbers_reducer.trash_list]
            # trash_head = [u"数据列" if i == index else u"原数据第{0}列".format(i) for i in xrange(tll)]
            # export_to_excel(data_list=numbers_reducer.trash_list,
            #                 file_name=u"{0}_trash.xlsx".format(numbers_reducer.export_name),
            #                 column_head=trash_head)
        if abnormal_reducer.cleaned_list:
            print 'there ara cleans.'
            for row in abnormal_reducer.cleaned_list:
                print row
            # cll = len(numbers_reducer.cleaned_list[0])
            # clean_head = [u"数据列" if i == index else u"原数据第{0}列".format(i) for i in xrange(cll)]
            # export_to_excel(data_list=numbers_reducer.cleaned_list,
            #                 file_name=u"{0}_cleaned.xlsx".format(numbers_reducer.export_name),
            #                 column_head=clean_head)
    except Exception as e:
        print str(e)
    return


if __name__ == u"__main__":
    keywords_splitter(data_path=ur"D:\364\weibo32.txt",
                      keywords_path=ur"D:\WorkSpace\Data\keywords.txt",
                      safewords_path=ur'D:\WorkSpace\Data\safewords.txt',
                      index=2, one_hit=False)
    # abnormal_splitter()
    # cl = pickle_load(u"cleaned_list.pickle")
    # tl = pickle_load(u"trash_list.pickle")
    # for i in cl[0:5]:
    #     print i
    # for i in tl[0:5]:
    #     print i
    # print len(cl[0])
    # print len(tl[0])
    # head = [u"原数据第{0}列".format(i) if i != 1 else u"被处理对象列" for i in xrange(2)]
    # for i in head:
    #     print i
