#!/usr/bin/python
# -*- coding: UTF-8 -*-
import re

test_case = [
    u"中心",
    u"中心后缀",
    u"前面是是中心的后缀",
    u"前缀是是是中心不不后缀",
    u"前缀是是是中心不后缀",
    u"前缀是是中心不不后缀"
]

for i in test_case:
    if re.search(u"(^(?!.*(后缀|后面)))中心",i) is not None:
        print i