# coding: utf-8
# __author__: u"John"
from factory import *
from helper import *


def keywords_splitter(data_path, keywords_path, index=1, one_hit=False):
    keywords_reducer = KeywordsReducer()
    keywords_reducer.data_column_index = index
    keywords_reducer.one_hit_strategy = one_hit
    keywords_reducer.current_dict_abspath = keywords_path
    keywords_reducer.current_data_abspath = data_path
    try:
        keywords_reducer.main()
        # pickle_dump(file_name=u"trash_list", dump_object=keywords_reducer.trash_list)
        # pickle_dump(file_name=u"cleaned_list", dump_object=keywords_reducer.cleaned_list)
        if keywords_reducer.trash_list:
            tll = len(keywords_reducer.trash_list[0])
            if tll == 1:
                keywords_reducer.trash_list = [i for i in keywords_reducer.trash_list]
            trash_head = [u"数据列" if i == index else u"原数据第{0}列".format(i) for i in xrange(tll)]
            export_to_excel(data_list=keywords_reducer.trash_list,
                            file_name=u"{0}_trash.xlsx".format(keywords_reducer.export_name),
                            column_head=trash_head)
        if keywords_reducer.cleaned_list:
            cll = len(keywords_reducer.cleaned_list[0])
            clean_head = [u"数据列" if i == index else u"原数据第{0}列".format(i) for i in xrange(cll)]
            export_to_excel(data_list=keywords_reducer.cleaned_list,
                            file_name=u"{0}_cleaned.xlsx".format(keywords_reducer.export_name),
                            column_head=clean_head)
    except Exception as e:
        print str(e)
    return


if __name__ == u"__main__":
    keywords_splitter(data_path=ur"D:\WorkSpace\MarcPoint\mppreprocess\marcdnr\src\marcdnr\随机数据.txt",
                      keywords_path=ur"D:\WorkSpace\MarcPoint\mppreprocess\marcdnr\src\marcdnr\词库.txt",
                      index=0,
                      one_hit=False)
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
