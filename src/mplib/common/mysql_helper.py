# coding: utf-8
# __author__: u"John"
import MySQLdb


def connect_db(host, user, passwd, db, port=3306, charset=u"utf8"):
    connection = MySQLdb.Connect(host=host, user=user, passwd=passwd, db=db, port=port, charset=charset)
    return connection


def db_cursor(host, user, passwd, db, port=3306, charset=u"utf8"):
    cursor = MySQLdb.Connect(host=host, user=user, passwd=passwd, db=db, port=port, charset=charset).cursor()
    return cursor


class MySQLdb(object):

    def __init__(self, host, user, passwd, db, port=3306, charset=u"utf8"):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port
        self.charset = charset

    def query(self, sql, dict_cursor=False, fetchone=False):
        conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, port=self.port,
                               charset=self.charset)
        if dict_cursor:
            cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        else:
            cursor = conn.cursor()
        cursor.execute(sql)
        try:
            if fetchone:
                ret = cursor.fetchone()
            else:
                ret = cursor.fetchall()
        except Exception as e:
            print u"error message:{0}".format(e)
            return False
        else:
            return ret
        finally:
            cursor.close()
            conn.close()

    def execute(self, sql):
        conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, port=self.port,
                               charset=self.charset)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            print u"error message:{0}".format(e)
            return False
        else:
            return True
        finally:
            cursor.close()
            conn.close()

    def execute_many(self, sql, args):
        conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, port=self.port,
                               charset=self.charset)
        cursor = conn.cursor()
        try:
            cursor.executemany(sql, args)
            conn.commit()
        except Exception as e:
            print u"error message:{0}".format(e)
            return False
        else:
            return True
        finally:
            cursor.close()
            conn.close()


