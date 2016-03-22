#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sqlite3
import tempfile
import traceback

import pandas.io.sql as psql
import yaml


def load_config(yaml_file_path):
    config = yaml.load(open(yaml_file_path, 'r'))
    return config


def get_table_name(yaml_file_path, table_name):
    config = load_config(yaml_file_path)
    try:
        new_table_name = config["tables"][table_name]["name"]
    except KeyError:
        raise KeyError("table name %s not found in config" % table_name)
    return new_table_name


def get_table_columns(yaml_file_path, table_name):
    config = load_config(yaml_file_path)
    try:
        table_columns = config["tables"][table_name]["columns"]
    except KeyError:
        raise KeyError("table columns for %s not found in config" % table_name)
    return table_columns


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SQLiteBase:
    def __init__(self, database_file, yaml_file_path, table_name):
        self.yaml_file_path = yaml_file_path
        self.database_file = database_file
        self.table_name = get_table_name(yaml_file_path, table_name)
        self.columns = get_table_columns(yaml_file_path, table_name)

    def get_connection(self):
        conn = sqlite3.connect(self.database_file)
        return conn

    def get_dict_factory_connection(self):
        conn = sqlite3.connect(self.database_file)
        conn.row_factory = dict_factory
        return conn

    def create_table(self):
        conn = self.get_connection()
        conn.execute("DROP TABLE IF EXISTS %s" % self.table_name)
        columns_str = ", ".join(self.columns)
        conn.execute("CREATE TABLE %s (%s)" % (self.table_name, columns_str))

    def import_file_to_sqlite(self, file_path, tmp_file_mode=True, nt_mode=False):
        if tmp_file_mode:
            temp = tempfile.NamedTemporaryFile(mode='w', prefix="sqlite_", delete=False)
            temp.write(".mode tabs\n")
            if nt_mode:
                file_path = file_path.replace("\\", "\\\\")
            temp.write(".import \"%s\" %s" % (file_path, self.table_name))
            temp.close()
            command = "sqlite3 %s < %s" % (self.database_file, temp.name)
            os.system(command)
            os.remove(temp.name)
        else:
            command = "sqlite3 -separator $'\t' %s \".import %s %s\"" % (
                self.database_file, file_path, self.table_name
            )
            os.system(command)

    def get_column_name_list(self):
        columns = get_table_columns(self.yaml_file_path, self.table_name)
        column_list = []
        for column in columns:
            column_str = column.split(" ")[0]
            column_list.append(column_str)
        return column_list

    def execute_query(self, query, dict_flg=False, str_flg=False):
        if dict_flg:
            conn = self.get_dict_factory_connection()
        else:
            conn = self.get_connection()
        if str_flg:
            conn.text_factory = str
        cur = conn.cursor()
        try:
            cur.execute(query)
        except sqlite3.OperationalError:
            print query
            print traceback.format_exc()
            raise sqlite3.OperationalError
        result = cur.fetchall()
        cur.close()
        return result

    def execute_query_with_pandas(self, query, str_flg=False):
        conn = self.get_connection()
        if str_flg:
            conn.text_factory = str
        try:
            result = psql.read_sql(query, conn)
        except psql.DatabaseError:
            raise psql.DatabaseError("query : %s" % query)
        return result

    def execute_query_single_column(self, query):
        result_raw = self.execute_query(query)
        result_list = []
        for result_item in result_raw:
            result_list.append(result_item[0])
        return result_list

    def get_dict(self, query):
        conn = self.get_dict_factory_connection()
        cur = conn.cursor()
        return cur.execute(query, ()).fetchall()

    def create_index(self, column):
        query = "CREATE INDEX %s_ON_%s ON %s (%s)" % (column, self.table_name, self.table_name, column)
        return self.execute_query(query)

    def bulk_insert(self, value_list):
        conn = self.get_connection()
        column_name_list = self.get_column_name_list()
        columns_str = ", ".join(column_name_list)
        prepared_list = []
        for i in range(len(self.columns)):
            prepared_list.append("?")
        prepared_str = ", ".join(prepared_list)
        query = "INSERT INTO %s (%s) VALUES (%s)" % (self.table_name, columns_str, prepared_str)
        try:
            conn.executemany(query, value_list)
        except sqlite3.ProgrammingError:
            print query
            print value_list
            print traceback.format_exc()
            raise sqlite3.ProgrammingError
        conn.commit()
        return conn.close()

    def bulk_insert_with_query(self, query, data_list):
        conn = self.get_connection()
        try:
            conn.executemany(query, data_list)
        except sqlite3.OperationalError:
            print query
            print data_list
            print traceback.format_exc()
            raise sqlite3.ProgrammingError
        conn.commit()
        conn.close()

    def check_existance(self):
        query = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' and name= \"%s\"" % self.table_name
        result = self.execute_query(query)[0][0]
        if result == 1:
            return True
        return False

    def find_all(self, dict_flg=True):
        query = "SELECT * FROM %s" % self.table_name
        return self.execute_query(query, dict_flg=dict_flg)

    def get_find_all_cur(self, dict_flg=False):
        if dict_flg:
            conn = self.get_dict_factory_connection()
        else:
            conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM %s" % self.table_name)
        return cur

    def dump(self, dump_file):
        temp = tempfile.NamedTemporaryFile(mode='w', prefix="sqlite_", delete=False)
        temp.write(".output %s\n" % dump_file)
        temp.write(".dump %s" % self.table_name)
        temp.close()
        command = "sqlite3 %s < %s" % (self.database_file, temp.name)
        os.system(command)
        os.remove(temp.name)

    def get_existing_table_column(self):
        conn = self.get_connection()
        query = "SELECT * FROM %s LIMIT 1" % self.table_name
        cursor = conn.execute(query)
        return list(map(lambda x: x[0], cursor.description))

    def check_schema_not_changed(self):
        result_columns = self.get_existing_table_column()
        expected_columns = self.get_column_name_list()
        return result_columns == expected_columns