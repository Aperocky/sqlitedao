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
        extended_feature = isinstance(column_dict, ColumnDict)
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
        extended_feature = isinstance(search_dict, SearchDict)
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
                key_strings.append("{} {} ?".format(v["value"], v["operator"]))
            else:
                key_strings.append("{} = ?".format(k))
            value_strings.append(v)
        query += " AND ".join(key_strings)
        print("Running READ query: {}".format(query))
        cursor.execute(query, value_strings)
        return cursor

    def insert_row(self, table_name, row_tuple):
        # Row values are a dictionary representing the row.
        if not isinstance(row_tuple, dict):
            raise ValueError("row_tuple should be a dictionary")
        query = "INSERT INTO {} ".format(table_name)
        keys = row_tuple.keys()
        values = row_tuple.values()
        query += "(" + ",".join(keys) + ")"
        query += " VALUES "
        query += "(" + ",".join(["?"] * len(row_tuple)) + ")"
        cursor = self.conn.cursor()
        cursor.execute(query, values)
        self.conn.commit()
        cursor.close()

    def insert_rows(self, table_name, row_tuples):
        # Assert each row tuples have the same length and keys
        keys = row_tuples[0].keys()
        multiple_values = []
        for row_tuple in row_tuples:
            if row_tuple.keys() != keys:
                raise ValueError("batch should have same keys")
            multiple_values.append(row_tuple.values())
        query = "INSERT INTO {} ".format(table_name)
        query += "(" + ",".join(keys) + ")"
        query += " VALUES "
        query += "(" + ",".join(["?"] * len(keys)) + ")"
        cursor = self.conn.cursor()
        cursor.executemany(query, multiple_values)
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

    def update_many(self, table_name, update_dicts, search_dicts):
        # Search dict will always be basic dictionary this time.
        # Sanitize inputs! Assumed to be only used for table items.
        set_columns = update_dicts[0].keys()
        search_columns = search_dicts[0].keys()
        values = [ list(update.values()) + list(search.values()) for update, search in zip(update_dicts, search_dicts) ]
        set_strings = ["{}=?".format(e) for e in set_columns]
        search_strings = ["{}=?".format(e) for e in search_columns]
        query = "UPDATE {} SET ".format(table_name)
        query += ", ".join(set_strings) + " WHERE "
        query += " AND ".join(search_strings)
        cursor = self.conn.cursor()
        cursor.executemany(query, values)
        self.conn.commit()
        cursor.close()

    def update_rows(self, table_name, update_dict, search_dict):
        extended_feature = isinstance(search_dict, SearchDict)
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
                key_strings.append("{} {} ?".format(v["value"], v["operator"]))
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

    def delete_rows(self, table_name, search_dict, limit=None):
        extended_feature = isinstance(search_dict, SearchDict)
        cursor = self.conn.cursor()
        query = "DELETE FROM {}".format(table_name)
        if not search_dict:
            cursor.execute(query)
        query += " WHERE "
        key_strings = []
        value_strings = []
        for k, v in search_dict.items():
            if extended_feature:
                key_strings.append("{} {} ?".format(v["value"], v["operator"]))
            else:
                key_strings.append("{} = ?".format(k))
            value_strings.append(v)
        query += " AND ".join(key_strings)
        if limit is not None:
            if not isinstance(limit, int):
                raise ValueError("Limit need to be a number")
            query += " LIMIT {}".format(limit)
        print("Running DELETE query: {}".format(query))
        cursor.execute(query, value_strings)

    # ======================================== #
    # ACCOMODATE TABLE ITEMS                   #
    # ======================================== #

    def insert_item(self, table_item):
        self.insert_row(table_item.get_table(), table_item.get_row_tuple())

    def insert_items(self, table_items):
        self.insert_rows(table_item.get_table(), [item.get_row_tuple() for item in table_items])

    # Find item based on a index only table_item, returns the full item if found
    def find_item(self, table_item):
        cursor = self.search_table(table_item.get_table(), table_item.get_index_dict())
        row = cursor.fetchone()
        if row:
            row_dict = dict(row)
            return type(table_item)(row_dict)
        return None

    def delete_item(self, table_item):
        self.delete_rows(table_item.get_table(), table_item.get_index_dict(), 1)

    def update_item(self, table_item):
        self.update_row(table_item.get_table(), table_item.get_row_tuple(), table_item.get_index_dict())

    def update_items(self, table_items):
        self.update_row(table_items[0].get_table(), [item.get_row_tuple() for item in table_items],
            [item.get_index_dict() for item in table_items])


class SearchDict(dict):

    def add_filter(column_name, value, operator="="):
        self[column_name] = {"value": value, "operator": operator}
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


class TableItem:

    TABLE_NAME = "table_this_item_belong_to"
    INDEX_KEYS = ["dummy_index_1", "dummy_index_2"]

    # Row_tuple builds the correspondence to table
    def __init__(self, row_tuple):
        self.row_tuple = row_tuple

    @classmethod
    def get_table(cls):
        return cls.TABLE_NAME

    def get_row_tuple(self):
        return self.row_tuple

    def get_index_dict(self):
        try:
            return {k: self.row_tuple[k] for k in type(self).INDEX_KEYS}
        except KeyError as e:
            print("Row tuple does not contain index: {}".format(e))

