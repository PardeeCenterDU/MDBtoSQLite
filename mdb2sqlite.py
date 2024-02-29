import pyodbc
import sqlite3
from collections import namedtuple
import re
import sys
import os
import shutil
import time
# https://www.sqlite.org/datatype3.html
# https://docs.python.org/3/library/sqlite3.html
# https://stackoverflow.com/questions/1381264/password-protect-a-sqlite-db-is-it-possible
# https://github.com/mkleehammer/pyodbc/issues/328
# https://github.com/mkleehammer/pyodbc/issues/328
def cur_columns(cur, table_name):
    for line in cur.columns(table_name):
        line = list(line)
        line[11], null_terminator, garbage = line[11].partition('\x00')
        yield tuple(line)

def decode_sketchy_utf16(raw_bytes):
    s = raw_bytes.decode("utf-16le", "ignore")
    try:
        n = s.index('\u0000')
        s = s[:n]  # respect null terminator
    except ValueError:
        pass
    return s
# coverting function
def mdb_sqlite(filename_in, filename_out):
    filename_in = os.path.abspath(filename_in)
    filename_out = os.path.abspath(filename_out)
    # boolean dict 
    bool_map = {"true":1,"yes":1,"false":0,"no":0}
    # connecting to MDB file
    cnxn = pyodbc.connect('Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};Dbq={};PWD=data4ifs12;'.format(filename_in))
    cursor = cnxn.cursor()
    # connecting to sqlite file
    conn = sqlite3.connect(filename_out)
    c = conn.cursor()
    # get a list of tables
    Table = namedtuple('Table', ['cat', 'schem', 'name', 'type'])
    tables = []
    for row in cursor.tables():
        if row.table_type in {"SYNONYM", 'TABLE'}:
            t = Table(row.table_cat, row.table_schem, row.table_name, row.table_type)
            tables.append(t)
    # coverting
    tables_finished = []
    for t in tables:
        # SQLite tables must begin with a character or _
        t_name = t.name
        if not re.match('[a-zA-Z]', t.name):
            t_name = '_' + t_name
        # get table definition
        columns = []
        prev_converter = cnxn.get_output_converter(pyodbc.SQL_WVARCHAR)
        cnxn.add_output_converter(pyodbc.SQL_WVARCHAR, decode_sketchy_utf16)
        col_info = cursor.columns(table=t.name).fetchall() 
        cnxn.add_output_converter(pyodbc.SQL_WVARCHAR, prev_converter)
        col_info = [c_info for c_info in col_info if c_info.table_name==t.name]
        col_bit_set = set()
        for row in col_info:
            # change bit type to integer
            if row.type_name == "BIT":
                row.type_name = "INTEGER"
                col_bit_set.add(row.column_name)
            # change Number field to RegionId
            if row.column_name == "Number":
                row.column_name = "RegionId"
            columns.append('"{}" {}({})'.format(row.column_name, row.type_name, row.column_size))
        cols = ', '.join(columns)
        # create the table in SQLite
        c.execute('DROP TABLE IF EXISTS "{}"'.format(t_name))
        c.execute('CREATE TABLE "{}" ({})'.format(t_name, cols))
        # copy the values from MDB
        cursor.execute('SELECT * FROM "{}"'.format(t.name))
        for row in cursor:
            values = []
            for i in range(len(row)):
                value = row[i]
                if value is None:
                    values.append(None)
                else:
                    if isinstance(value, bytearray):
                        print(value)
                        value = sqlite3.Binary(value)
                    else:
                        value = u'{}'.format(value)
                    # change boolean text to 0 & 1 if the column was BIT type
                    if (value.lower() in bool_map) and (col_info[i].column_name in col_bit_set):
                        value = bool_map[value.lower()]
                    values.append(value)
            v = ', '.join(['?']*len(values))
            sql = 'INSERT INTO "{}" VALUES(' + v + ')'
            c.execute(sql.format(t_name), values)
        tables_finished.append(f"finished {t.name}")
    conn.commit()
    conn.close()
    cnxn.close()
# recursively convert files in IFs folder
def path_convert(ifs_path):
    ifs_path = os.path.abspath(ifs_path)
    new_path = f"{ifs_path}SQLite"
    if os.path.exists(new_path) and os.path.isdir(new_path):
        shutil.rmtree(new_path)
    for root, dirs, files in os.walk(ifs_path):
        structure = root.replace(ifs_path, new_path) #os.path.join(datacopy_path, dirs[len(ifs_path):])
        #print(root, dirs, files, structure)
        files_mdb = [f for f in files if f.endswith(".mdb")]
        if (not os.path.isdir(structure)) and files_mdb:
            os.mkdir(structure)
            for f in files_mdb:
                mdb_path = os.path.abspath(os.path.join(root,f))
                sqlite_path = os.path.abspath(os.path.join(structure,f))
                sqlite_path = sqlite_path.replace(".mdb", ".db")
                try:
                    mdb_sqlite(mdb_path, sqlite_path)
                except:
                    print(f"Skipped: {mdb_path}")   
#
print('Ctrl + C to terminate at any time')
print('Enter quit to exit')
Path = False
while Path == False: 
    ifs_path_default = "C:\\Users\\Public\\IFs"
    file_path = input(f"Enter a folder path if different than {ifs_path_default} :")
    file_path = file_path.strip()
    # exit
    if file_path.lower() == "quit":
        break
    # correct folder
    if file_path == "":
        file_path = ifs_path_default
    file_path = os.path.abspath(file_path)
    if os.path.exists(file_path) and os.path.isdir(file_path):
        convert_begin = input(f"Begin converting MDB files under \n{file_path} ? \n [y/n/quit] :")
        convert_begin = convert_begin.strip()
        if convert_begin.lower() == "quit":
            break
        elif convert_begin.lower() == "y":
            print("In progress")
            start = time.time()
            path_convert(file_path)
            end = time.time()
            spent = round((end - start)/60,2)
            print(f"Done, {spent} mins used")
            Path = True
    else:
        print(f"Folder {file_path} does not exist")
