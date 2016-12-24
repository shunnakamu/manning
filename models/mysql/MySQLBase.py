#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb
import traceback
import copy
import yaml
import os

fixed_column_list = [
    "created_at DATETIME DEFAULT CURRENT_TIMESTAMP",
    "updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
]


def load_config(yaml_file_path):
    config = yaml.load(open(yaml_file_path, 'r'))
    return config


def get_table_columns(yaml_file_path, table_name):
    config = load_config(yaml_file_path)
    try:
        table_columns = config["tables"][table_name]["columns"]
    except KeyError:
        raise KeyError("table columns for %s not found in config" % table_name)
    return table_columns


class MySQLBase(object):
    def __init__(self, yaml_file_path, table_name):
        self.yaml_file_path = yaml_file_path
        self.config = load_config(self.yaml_file_path)
        try:
            self.table_name = self.config["tables"][table_name]["name"]
            self.columns = self.config["tables"][table_name]["columns"]
        except KeyError:
            raise "table %s doesn't exist in config" % table_name

        try:
            self.additional_config = self.config["tables"][table_name]["additional_config"]
        except KeyError:
            self.additional_config = None

        self.raw_columns = []
        for column_item in self.columns:
            self.raw_columns.append(column_item.split(" ")[0])

        self.all_columns = copy.deepcopy(self.columns)
        self.all_raw_columns = copy.deepcopy(self.raw_columns)
        self.all_columns.extend(
            fixed_column_list
        )
        for column_item in fixed_column_list:
            self.all_raw_columns.append(column_item.split(" ")[0])

        self.conn = self.get_connection()

    def get_connection(self):
        try:
            conn = MySQLdb.connect(
                host=self.config["connection"]["host"],
                port=self.config["connection"]["port"],
                db=self.config["connection"]["db"],
                user=self.config["connection"]["user"],
                passwd=self.config["connection"]["pw"],
                charset="utf8"
            )
        except MySQLdb.OperationalError:
            print traceback.format_exc()
        return conn

    def get_cursor(self, dict_cursor=True):
        if dict_cursor:
            return self.conn.cursor(MySQLdb.cursors.DictCursor)
        else:
            return self.conn.cursor()

    def execute_query(self, query, dict_flg=True):
        cursor = self.get_cursor(dict_cursor=dict_flg)
        try:
            cursor.execute(query)
        except MySQLdb.OperationalError:
            print query
            print traceback.format_exc()
            raise MySQLdb.OperationalError
        result_list = list(cursor.fetchall())
        self.conn.commit()
        cursor.close()
        return result_list

    def create_table(self):
        cursor = self.get_cursor()
        cursor.execute("DROP TABLE IF EXISTS %s" % self.table_name)
        columns_str = ", ".join(self.all_columns)
        if self.additional_config:
            query = "CREATE TABLE %s (%s) %s" % (self.table_name, columns_str, self.additional_config)
        else:
            query = "CREATE TABLE %s (%s)" % (self.table_name, columns_str)
        try:
            cursor.execute(query)
        except MySQLdb.ProgrammingError:
            print query
            raise MySQLdb.ProgrammingError
        cursor.close()
        return True

    def initialize_db(self):
        table_dict = self.config["tables"]
        for table_config_name, config_dict in table_dict.items():
            table_instance = MySQLBase(self.yaml_file_path, config_dict["name"])
            table_instance.create_table()
        return True

    def import_text_file(self, text_file_path, with_id=False, with_date_time=False, delimiter="\t"):
        column_list = copy.deepcopy(self.all_raw_columns)
        if not with_id:
            column_list.remove("id")

        if not with_date_time:
            for fixed_column_item in fixed_column_list:
                column_list.remove(fixed_column_item.split(" ")[0])

        if os.name == 'nt':
            text_file_path = text_file_path.replace("\\", "\\\\")

        query = """
       LOAD DATA LOCAL INFILE
         \"%s\"
       INTO TABLE %s
       FIELDS TERMINATED BY \"%s\"
         (%s)
       """ % (
            text_file_path, self.table_name, delimiter, ",".join(column_list)
        )
        self.execute_query(query=query)
        return True

    def find_all(self):
        query = "SELECT * FROM %s" % self.table_name
        return self.execute_query(query=query)

    def truncate_table(self):
        query = "TRUNCATE TABLE %s" % self.table_name
        return self.execute_query(query=query)

    def get_column_name_list(self):
        columns = get_table_columns(self.yaml_file_path, self.table_name)
        column_list = []
        for column in columns:
            column_str = column.split(" ")[0]
            column_list.append(column_str)
        return column_list

    def bulk_insert(self, value_list):
        cursor = self.get_cursor(dict_cursor=False)
        column_name_list = self.get_column_name_list()
        columns_str = ", ".join(column_name_list)
        prepared_list = []
        for i in range(len(self.columns)):
            prepared_list.append("%%s")
        prepared_str = ", ".join(prepared_list)
        query = "INSERT INTO %s (%s) VALUES (%s)" % (self.table_name, columns_str, prepared_str)
        try:
            cursor.executemany(query, value_list)
        except TypeError as e:
            print query
            # print value_list
            for value in value_list:
                if type(value) == list:
                    for value_value in value:
                        print value_value
                else:
                    value
            print traceback.format_exc()
            raise e
        cursor.commit()
        return cursor.close()

    def bulk_insert_with_query(self, query, value_list):
        cursor = self.get_cursor(dict_cursor=False)
        try:
            cursor.executemany(query, value_list)
        except (TypeError, _mysql_exceptions.OperationalError) as e:
            print query
            # print value_list
            for value in value_list:
                if type(value) == list:
                    for value_value in value:
                        print value_value
                else:
                    value
            print traceback.format_exc()
            raise e
        return True
