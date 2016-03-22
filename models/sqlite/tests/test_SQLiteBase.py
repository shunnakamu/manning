#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import sys
import os

from manning.models.sqlite import SQLiteBase


class TestSQLiteBase(unittest.TestCase):
    def setUp(self):
        self.test_dir_path = os.path.dirname(os.path.abspath(__file__))
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

    def test_get_existing_table_column(self):
        # prepare data
        db_file = self.test_dir_path + "test.db"
        sqlite_base = SQLiteBase.SQLiteBase(database_file=db_file, yaml_file_path=self.yaml_file_path, table_name="test")
        sqlite_base.create_table()

        result_columns = sqlite_base.get_existing_table_column()
        expected_columns = sqlite_base.get_column_name_list()
        os.remove(db_file)
        self.assertEqual(result_columns, expected_columns)

    def test_check_schema_not_changed(self):
        db_file = self.test_dir_path + "test.db"
        sqlite_base = SQLiteBase.SQLiteBase(database_file=db_file, yaml_file_path=self.yaml_file_path, table_name="test")
        sqlite_base.create_table()

        result_list = []
        result_list.append(sqlite_base.check_schema_not_changed())
        sqlite_base.execute_query("ALTER TABLE %s ADD COLUMN test INTEGER" % sqlite_base.table_name)
        result_list.append(sqlite_base.check_schema_not_changed())

        expected_list = [True, False]

        os.remove(db_file)
        self.assertEqual(result_list, expected_list)

if __name__ == "__main__":
    unittest.main()
