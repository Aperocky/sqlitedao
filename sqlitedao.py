import sqlite3

class SqliteDao:

    # One connection per database of sqlite
    INSTANCE_MAP = {}

    @staticmethod
    def get_instance(db_path):
        if db_path not in SqliteDao.INSTANCE_MAP:
            SqliteDao.INSTANCE_MAP[db_path] = SqliteDao(db_path)
        return SqliteDao.INSTANCE_MAP[db_path]

    @staticmethod
    def terminate_instance(db_path):
        if db_path in SqliteDao.INSTANCE_MAP:
            SqliteDao.INSTANCE_MAP[db_path].close()
            SqliteDao.INSTANCE_MAP.pop(db_path)

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        self.conn.close()

    def is_table_exist(self, table_name):
        query = "SELECT name from sqlite_master WHERE type='table' AND name=?"
        cursor = self.conn.execute(query, (table_name,))
        table = cursor.fetchone()
        cursor.close()
        return not (table == None)

    # Print all tables in the database, name only
    def print_tables(self):
        query = "SELECT name from sqlite_master WHERE type='table'"
        cursor = self.conn.execute(query)
        print("\n".join([str(dict(t)) for t in cursor.fetchall()]))
        cursor.close()

    def create_table(self, table_name, column_dict, index_dict=None):
        extended_feature = isinstance(column_dict, ColumnDict):
        query = "CREATE TABLE IF NOT EXISTS {} ( ".format(table_name)
        columns = []
        for k, v in column_dict.items():
            if extended_feature:
                columns.append(ColumnDict.to_query(k, v))
            else:
                columns.append("{} {}".format(k, v))
        query += ", ".join(columns) + " )"
        print("Execute CREATE TABLE query: {}".format(query))
        cursor = self.conn.cursor()
        cursor.execute(query)
        if index_dict:
            for k, v in index_dict.items():
                if not isinstance(v, list):
                    raise ValueError("Index need a list of columns")
                index_string = ",".join(v)
                index_query = "CREATE INDEX IF NOT EXISTS {} ON {} ({})".format(k, table_name, index_string)
                print("Execute CREATE INDEX query: {}".format(index_query))
                cursor.execute(index_query)
        self.conn.commit()
        cursor.close()

    # fetch rows where search_dict is satisfied
    def search_table(self, table_name, search_dict):
        extended_feature = isinstance(search_dict, SearchDict):
        cursor = self.conn.cursor()
        query = "SELECT * from {}".format(table_name)
        if not search_dict:
            cursor.execute(query)
            return cursor
        query += " WHERE "
        key_strings = []
        value_strings = []
        for k, v in search_dict.items():
            if extended_feature:
                key_strings.append("{} {} ?".format(v["value"], v["operator"])
            else:
                key_strings.append("{} = ?".format(k))
            value_strings.append(v)
        query += " AND ".join(key_strings)
        print("Running READ query: {}".format(query))
        cursor.execute(query, value_strings)
        return cursor

    def insert_partial_row(self, table_name, row_values, row_col):
        if len(row_col) != len(row_values):
            raise ValueError("Columns need to be same with values")
        query = "INSERT INTO {} ".format(table_name)
        query += "( " + ",".join(row_col) + " )"
        query += " VALUES (" + ",".join(["?"] * len(row_col)) + ")"
        print("Running INSERT query: {}".format(query))
        cursor = self.conn.cursor()
        cursor.execute(query, row_values)
        self.conn.commit()
        cursor.close()

    def insert_full_row(self, table_name, row_values):
        query = "INSERT INTO {} VALUES ".format(table_name)
        query += "(" + ",".join(["?"] * len(row_values)) + ")"
        print("Running INSERT query: {}".format(query))
        cursor = self.conn.cursor()
        cursor.execute(query, row_values)
        self.conn.commit()
        cursor.close()

    def update_row(self, table_name, update_dict, search_dict):
        if not update_dict:
            return
        if not search_dict:
            raise ValueError("must apply search conditions")
        query = "UPDATE {} SET ".format(table_name)
        set_strings = []
        search_strings = []
        value_strings = []
        for k, v in update_dict.items():
            set_strings.append("{}=?".format(k))
            value_strings.append(v)
        for k, v in search_dict.items():
            search_strings.append("{}=?".format(k))
            value_strings.append(v)
        query += ", ".join(set_strings) + " WHERE "
        query += " AND ".join(search_strings) + " LIMIT 1"
        print("Running UPDATE query: {}".format(query))
        cursor = self.conn.cursor()
        cursor.execute(query, row_values)
        self.conn.commit()
        cursor.close()

    def update_rows(self, table_name, update_dict, search_dict):
        extended_feature = isinstance(search_dict, SearchDict):
        if not update_dict:
            return
        query = "UPDATE {} SET ".format(table_name)
        set_strings = []
        search_strings = []
        value_strings = []
        for k, v in update_dict.items():
            set_strings.append("{}=?".format(k))
            value_strings.append(v)
        for k, v in search_dict.items():
            if extended_feature:
                key_strings.append("{} {} ?".format(v["value"], v["operator"])
            else:
                key_strings.append("{} = ?".format(k))
            value_strings.append(v)
        query += ", ".join(set_strings)
        if search_strings:
            query += " WHERE "
            query += " AND ".join(search_strings)
        print("Running UPDATE query: {}".format(query))
        cursor = self.conn.cursor()
        cursor.execute(query, row_values)
        self.conn.commit()
        cursor.close()

    def delete_rows(self, table_name, search_dict):
        extended_feature = isinstance(search_dict, SearchDict):
        cursor = self.conn.cursor()
        query = "DELETE FROM {}".format(table_name)
        if not search_dict:
            cursor.execute(query)
        query += " WHERE "
        key_strings = []
        value_strings = []
        for k, v in search_dict.items():
            if extended_feature:
                key_strings.append("{} {} ?".format(v["value"], v["operator"])
            else:
                key_strings.append("{} = ?".format(k))
            value_strings.append(v)
        query += " AND ".join(key_strings)
        print("Running DELETE query: {}".format(query))
        cursor.execute(query, value_strings)


class SearchDict(dict):

    def add_filter(column_name, value, operator="="):
        self[column_name] = {"value": value, "operator", operator}
        return self


class ColumnDict(dict):

    @staticmethod
    def to_query(key, value):
        query = "{} {}".format(key, value["data_type"])
        if value["primary"]:
            query += " PRIMARY KEY"
        if value[not_null]:
            query += " NOT NULL"
        return query

    def add_column(column_name, data_type, primary_key=False, not_null=False):
        self[column_name] = {"type": data_type, "primary": primary_key, "notnull": not_null}
        return self

