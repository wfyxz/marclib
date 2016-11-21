# coding: utf-8
# __author__: u"John"
from factory import Parser


def parse_tag(sql, index=0):
    p = Parser()
    p.main(sql=sql, index=index)
    return p.parsed_list


if __name__ == u"__main__":
    sql = u"SELECT TaggedItemAttr FROM mp_women_clothing.TaggedItemAttr LIMIT 10;"
    print parse_tag(sql)
