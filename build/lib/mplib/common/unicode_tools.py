# coding: utf-8
# __author__: u"John"


def decode(string, charset="utf8"):
    """
    处理中文编码，根据要求返回unicode
    :param string:
    :param charset:
    :return:
    """
    try:
        return string.decode(charset)
    except UnicodeDecodeError:
        return string


def encode(string, charset="utf8"):
    """
    处理Unicode编码，根据需求返回str
    :param string:
    :param charset:
    :return:
    """
    try:
        return string.encode(charset)
    except UnicodeEncodeError:
        return string


def change_charset(string, from_charset="utf8", to_charset="utf8"):
    if isinstance(string, unicode):
        return string.encode(to_charset)

    elif isinstance(string, str):
        try:
            return string.decode(from_charset).encode(to_charset)
        except UnicodeDecodeError:
            return string
        except UnicodeEncodeError:
            return string

    else:
        return string


def to_unicode(string, from_charset="utf8"):
    if isinstance(string, unicode):
        return string

    elif isinstance(string, str):
        return string.decode(from_charset)

    elif isinstance(string, int):
        return unicode(string)

    else:
        return string
