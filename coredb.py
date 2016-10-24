#!/usr/bin/env python
# -*- coding: utf-8 -*-

import importlib
import base64
import StringIO
from messytables import CSVTableSet, type_guess, \
  types_processor, headers_guess, headers_processor, \
  offset_processor, any_tableset

engines = []
modules = {
    'psycopg2': {'port': 5432, 'name': 'Postgres SQL'},
    'mysql': {'port': 3306, 'name': 'MySQL/MariaDB'},
    'pymssql': {'port': 49426, 'name': 'SQL Server'},
    'pymongo': {'port': 27017, 'name': 'MongoDB'},
    'cx_Oracle': {'port': 1521, 'name': 'Oracle'},
    'csv': {'port': None, 'name': 'CSV'},
    'sqlite3': {'port': None, 'name': 'SQLite'},
    'openpyxl': {'port': None, 'name': 'Excel (.xlsx)'},
    'xlrd': {'port': None, 'name': 'Excel (.xls)'}
}

for k, v in modules.items():
    try:
        globals()[k] = importlib.import_module(k)
        engines.append((k, v['name']))
    except:
        pass
# sort it!
engines = sorted(engines, key=lambda x: (x[1]))

class DB():
    def __init__(self, engine, data=None):
        self.engine = engine
        self.conn = None
        self.data = base64.decodestring(data)
        # TODO: do connection here?

    def connect(self, host=None, port=None, database=None, username=None, password=None, file=None):
        # TODO: mysql, pymssql, csv, sqlite3, pymongo, cx_Oracle
        self.database = database
        conn_string = ''
        if self.engine == 'psycopg2':
            if database:
                conn_string += "dbname='%s' " % database
            if username:
                conn_string += "user='%s' " % username
            if host:
                conn_string += "host='%s' " % host
            if port:
                conn_string += "port='%s' " % port
            if password:
                conn_string += "password='%s' " % password
            self.conn = psycopg2.connect(conn_string)

        elif self.engine == 'pymssql':
            self.conn = pymssql.connect(host, username, password, database, port=port, as_dict=True, charset='LATIN1')

        elif self.engine == 'csv':
            # https://messytables.readthedocs.io/en/latest/
            fh = StringIO.StringIO(self.data)
            #dialect = csv.Sniffer().sniff(f.read(1024))
            #f.seek(0)
            #self.conn = csv.DictReader(f, dialect=dialect)
            #fh = open('messy.csv', 'rb')

            # Load a file object:
            table_set = CSVTableSet(fh)
            row_set = table_set.tables[0]
            offset, headers = headers_guess(row_set.sample)
            row_set.register_processor(headers_processor(headers))
            row_set.register_processor(offset_processor(offset + 1))
            types = type_guess(row_set.sample, strict=True)
            row_set.register_processor(types_processor(types))

            self.conn = row_set

        return self.conn

    def get_rows(self, table=None):
        """get data by rows"""
        if self.engine == 'csv':
            return self.conn


    def show_tables(self):
        res = {}

        # TODO: more clear dictionary creation
        if self.engine == 'csv':
            c = 1
            # user would modify default table
            res['default'] = {
                'pk': [],
                'fk': [],
                'fields': []
            }
            for row in self.conn:
                if c == 1:
                    for column in row:
                        res['default']['fields'].append([column.column, column.type])
                c += 1
            res['default']['count'] = c
        elif self.engine == 'psycopg2':
            cur = self.conn.cursor()
            sql = "SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)'"
            cur.execute(sql)
            tables = cur.fetchall()
            for i in tables:
                # get rows count
                sql = "select count(*) from %s" % i[0]
                cur.execute(sql)
                res[i[0]] = {
                    'count': cur.fetchone()[0]
                }
                sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='%s'" % i[0]
                cur.execute(sql)
                res[i[0]]['fields'] = cur.fetchall()

                # get primary keys
                # SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
                sql = """
                    SELECT a.attname
                    FROM   pg_index i
                    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                         AND a.attnum = ANY(i.indkey)
                    WHERE  i.indrelid = '%s'::regclass
                    AND    i.indisprimary
                """ % i[0]
                cur.execute(sql)
                res[i[0]]['pk'] = [j[0] for j in cur.fetchall()]

                # get foreign keys
                sql = """
                    SELECT c.constraint_name, tc.table_name, kcu.column_name,
                      ccu.table_name AS foreign_table_name,
                      ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                    WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='%s';
                """ % i[0]
                cur.execute(sql)
                res[i[0]]['fk'] = [{
                    'name': j[0],
                    'table': j[1], 'column': j[2],
                    'f_table': j[3], 'f_column': j[4]} for j in cur.fetchall()]

        elif self.engine == 'pymssql':
            cur = self.conn.cursor()
            # more info (table): https://www.mssqltips.com/sqlservertutorial/196/informationschematables/
            # more info (columns): https://www.mssqltips.com/sqlservertutorial/183/informationschemacolumns/
            # get tables
            sql = "SELECT TABLE_NAME FROM information_schema.tables"
            cur.execute(sql)
            tables = [j['TABLE_NAME'] for j in cur.fetchall()]

            # get foreign keys
            sql = """SELECT * FROM sys.objects"""
            cur.execute(sql)
            #print '>>>', sql
            from pprint import pprint
            for i in cur.fetchall():
                print '+++', i['principal_id'], i['type'], i['type_desc'], i['name']
            return {}

            for i in tables:
                # get rows count
                t = i.encode('latin-1') # TODO: this is a connection parameter
                print t
                # get primary keys
                sql = """
                    SELECT Col.Column_Name FROM
                        INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab,
                        INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col
                    WHERE
                        Col.Constraint_Name = Tab.Constraint_Name
                        AND Col.Table_Name = Tab.Table_Name
                        AND Constraint_Type = 'PRIMARY KEY'
                        AND Col.Table_Name = '%s'""" % t
                cur.execute(sql)
                res[i] = {
                    'pk': [j['Column_Name'] for j in cur.fetchall()]
                }

                #res[i] = {
                #    'fk': [j['Column_Name'] for j in cur.fetchall()]
                #}

                # get number of rows and fields
                try:
                    sql = "SELECT count(*) AS count FROM %s" % t
                    cur.execute(sql)
                    res[i]['count'] = cur.fetchone()['count']
                    sql = "SELECT COLUMN_NAME, DATA_TYPE from [%s].INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s'" % (self.database, t)
                    cur.execute(sql)
                    res[i]['fields'] = [(j['COLUMN_NAME'], j['DATA_TYPE']) for j in cur.fetchall()]
                except:
                    # TODO: fix unicode error with table names
                    # TODO: write error to etileno.log
                    res.pop(i) # error, remove table
        return res


    def get_data(self, table, fields, limit=500):
        """Return rows for 'table' with 'fields'"""
        res = {}
        cur = self.conn.cursor()

        if self.engine == 'pymssql':
            sql = "SELECT TOP %i %s from %s" % (limit, ','.join(fields), table)
            cur.execute(sql)
            res = cur.fetchall()

        return res
