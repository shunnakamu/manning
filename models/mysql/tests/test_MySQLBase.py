#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import sys
import os

from manning.models.mysql import MySQLBase


class TestMySQLBase(unittest.TestCase):
    def setUp(self):
        self.test_dir_path = os.path.dirname(os.path.abspath(__file__))
        self.maxDiff = None
        # linux
        if os.name == 'posix':
            self.data_path = self.test_dir_path + "/datas/"
            self.yaml_file_path = self.test_dir_path + "/../config/config.yaml"
        # windows
        elif os.name == 'nt':
            self.data_path = self.test_dir_path + "\\datas\\"
            self.yaml_file_path = self.test_dir_path + "\\..\\config\\config.yaml"
        else:
            print "Unsupported OS"

    def test_create_table(self):
        table_name = "test"
        mysql_base = MySQLBase.MySQLBase(yaml_file_path=self.yaml_file_path, table_name=table_name)
        mysql_base.create_table()
        result_list = mysql_base.execute_query(query="DESC %s" % table_name)
        expected_list = [
            {'Default': None, 'Extra': u'auto_increment', 'Field': u'id', 'Key': u'PRI', 'Null': u'NO', 'Type': u'int(11)'},
            {'Default': None, 'Extra': u'', 'Field': u'test_str', 'Key': u'', 'Null': u'YES', 'Type': u'text'},
            {'Default': None, 'Extra': u'', 'Field': u'test_score', 'Key': u'', 'Null': u'YES', 'Type': u'float'},
            {'Default': u'CURRENT_TIMESTAMP', 'Extra': u'', 'Field': u'created_at', 'Key': u'', 'Null': u'YES', 'Type': u'datetime'},
            {'Default': u'CURRENT_TIMESTAMP', 'Extra': u'on update CURRENT_TIMESTAMP', 'Field': u'updated_at', 'Key': u'', 'Null': u'YES', 'Type': u'datetime'}
        ]
        self.assertEqual(result_list, expected_list)

    def test_initialize_db(self):
        mysql_base = MySQLBase.MySQLBase(yaml_file_path=self.yaml_file_path, table_name="test")
        mysql_base.initialize_db()
        result_list = mysql_base.execute_query(query="SHOW TABLES")
        expected_list = [
            {'Tables_in_test': u'test'}, {'Tables_in_test': u'test2'}
        ]
        self.assertEqual(result_list, expected_list)

    def test_import_text_file(self):
        table_name = "test"
        text_file_path = self.data_path + "mysqlbase_test.tsv"
        mysql_base = MySQLBase.MySQLBase(yaml_file_path=self.yaml_file_path, table_name=table_name)
        mysql_base.import_text_file(text_file_path=text_file_path, delimiter="\t")
        result_list = mysql_base.find_all()

        self.assertTrue(all([
            result_list[0]["test_str"] == u"餃子",
            result_list[0]["test_score"] == 0.8,
            result_list[1]["test_str"] == u"ラーメン",
            result_list[1]["test_score"] == 1.234
        ]))


if __name__ == "__main__":
    unittest.main()
