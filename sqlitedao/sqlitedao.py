import sqlite3

class SqliteDao:

    # One connection per database of sqlite
    INSTANCE_MAP = {}

    @staticmethod
    def get_instance(db_path, single_threaded=False):
        if db_path not in SqliteDao.INSTANCE_MAP:
            SqliteDao.INSTANCE_MAP[db_path] = SqliteDao(db_path, single_threaded)
        return SqliteDao.INSTANCE_MAP[db_path]

    @staticmethod
    def terminate_instance(db_path):
        if db_path in SqliteDao.INSTANCE_MAP:
            SqliteDao.INSTANCE_MAP[db_path].close()
            SqliteDao.INSTANCE_MAP.pop(db_path)

    @staticmethod
    def terminate_all_instances():
        SqliteDao.INSTANCE_MAP = {}

    def __init__(self, db_path, single_threaded=False):
        self.conn = sqlite3.connect(db_path, check_same_thread=single_threaded)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        self.conn.close()

    def is_table_exist(self, table_name):
        query = "SELECT name from sqlite_master WHERE type='table' AND name=?"
        cursor = self.conn.execute(query, (table_name,))
        table = cursor.fetchone()
        cursor.close()
        return not (table == None)

    def get_row_count(self, table_name):
        query = "SELECT count(*) from {}".format(table_name)
        cursor = self.conn.execute(query)
        num_count = cursor.fetchone()[0]
        cursor.close()
        return num_count

    def get_schema(self, info="name", type="table"):
        query = "SELECT {} from sqlite_master WHERE type='{}'".format(info, type)
        cursor = self.conn.execute(query)
        return [dict(t) for t in cursor.fetchall()]

    def drop_table(self, table_name):
        query = "DROP TABLE {}".format(table_name)
        self.conn.execute(query)

    def drop_index(self, index_name):
        query = "DROP TABLE {}".format(index_name)
        self.conn.execute(query)

    def create_table(self, table_name, column_dict, index_dict=None):
        extended_feature = isinstance(column_dict, ColumnDict)
        query = "CREATE TABLE IF NOT EXISTS {} ( ".format(table_name)
        columns = []
        primary_keys = []
        for k, v in column_dict.items():
            if extended_feature:
                columns.append(ColumnDict.to_query(k, v))
                if v["primary_key"]:
                    primary_keys.append(k)
            else:
                columns.append("{} {}".format(k, v))
        if primary_keys:
            primary_key_str = "PRIMARY KEY({})".format(", ".join(primary_keys))
            columns.append(primary_key_str)
        query += ", ".join(columns) + " )"
        cursor = self.conn.cursor()
        cursor.execute(query)
        if index_dict:
            for k, v in index_dict.items():
                if not isinstance(v, list):
                    raise ValueError("Index need a list of columns")
                index_string = ",".join(v)
                index_name = "idx_{}_".format(table_name) + k
                index_query = "CREATE INDEX IF NOT EXISTS {} ON {} ({})".format(index_name, table_name, index_string)
                cursor.execute(index_query)
        self.conn.commit()
        cursor.close()

    # fetch rows where search_dict is satisfied
    def search_table(self, table_name, search_dict, order_by=None, group_by=None, limit=None, offset=None, desc=True, debug=False):
        def group_by_ops():
            select_clause = query.split("from")[0]
            substitute_select_clause = "SELECT count(*) AS count,{} ".format(",".join(group_by))
            groupby_query = query.replace(select_clause, substitute_select_clause)
            groupby_query += " GROUP BY {}".format(",".join(group_by)) + " ORDER BY count DESC"
            return groupby_query

        cursor = self.conn.cursor()
        query = "SELECT * from {}".format(table_name)
        if not search_dict:
            if group_by is not None:
                query = group_by_ops()
            elif order_by is not None:
                direction = "DESC" if desc else "ASC"
                query += " ORDER BY {} {}".format(",".join(order_by), direction)
            if limit is not None:
                query += " LIMIT {}".format(limit)
                if offset is not None:
                    query += " OFFSET {}".format(offset)
            cursor.execute(query)
            result = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            return result

        # Query that contains where clause
        extended_feature = isinstance(search_dict, SearchDict)
        query += " WHERE "
        key_strings = []
        value_strings = []
        for k, v in search_dict.items():
            self.populate_search_dict(key_strings, value_strings, k, v, extended_feature)
        query += " AND ".join(key_strings)
        if group_by is not None:
            query = group_by_ops()
        elif order_by is not None:
            direction = "DESC" if desc else "ASC"
            query += " ORDER BY {} {}".format(",".join(order_by), direction)
        if limit is not None:
            query += " LIMIT {}".format(limit)
            if offset is not None:
                query += " OFFSET {}".format(offset)
        if debug:
            print(query)
        cursor.execute(query, value_strings)
        result = [dict(row) for row in cursor.fetchall()]
        cursor.close()
        return result

    def insert_row(self, table_name, row_tuple):
        # Row values are a dictionary representing the row.
        if not isinstance(row_tuple, dict):
            raise ValueError("row_tuple should be a dictionary")
        query = "INSERT INTO {} ".format(table_name)
        keys = row_tuple.keys()
        values = list(row_tuple.values())
        query += "(" + ",".join(keys) + ")"
        query += " VALUES "
        query += "(" + ",".join(["?"] * len(row_tuple)) + ")"
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, values)
            self.conn.commit()
        except sqlite3.IntegrityError as e:
            raise DuplicateError("Insertion violates uniqueness constraint: {}".format(e))
        cursor.close()

    def insert_rows(self, table_name, row_tuples):
        # Assert each row tuples have the same length and keys
        keys = row_tuples[0].keys()
        multiple_values = []
        for row_tuple in row_tuples:
            if row_tuple.keys() != keys:
                raise ValueError("batch should have same keys")
            multiple_values.append(list(row_tuple.values()))
        query = "INSERT OR IGNORE INTO {} ".format(table_name)
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
        query += " AND ".join(search_strings)
        cursor = self.conn.cursor()
        cursor.execute(query, value_strings)
        self.conn.commit()
        cursor.close()

    # Fills multiple rows one at the time.
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

    # For backfilling purpose, fills multiple matching rows at the same time.
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
            self.populate_search_dict(search_strings, value_strings, k, v, extended_feature)
        query += ", ".join(set_strings)
        if search_strings:
            query += " WHERE "
            query += " AND ".join(search_strings)
        cursor = self.conn.cursor()
        cursor.execute(query, value_strings)
        self.conn.commit()
        cursor.close()

    def delete_rows(self, table_name, search_dict):
        extended_feature = isinstance(search_dict, SearchDict)
        cursor = self.conn.cursor()
        query = "DELETE FROM {}".format(table_name)
        key_strings = []
        value_strings = []
        if len(search_dict) > 0:
            query += " WHERE "
            for k, v in search_dict.items():
                self.populate_search_dict(key_strings, value_strings, k, v, extended_feature)
            query += " AND ".join(key_strings)
        cursor.execute(query, value_strings)

    def populate_search_dict(self, key_strings, value_strings, k, v, extended_feature):
        if extended_feature:
            if SearchDict.is_comp(v):
                key_strings.append("{} {} ?".format(k, v["operator"]))
                value_strings.append(v["value"])
            else:
                key_strings.append("{} BETWEEN ? AND ?".format(k))
                value_strings.extend([v["value_low"], v["value_high"]])
        else:
            key_strings.append("{} = ?".format(k))
            value_strings.append(v)

    # ======================================== #
    # ACCOMODATE TABLE ITEMS                   #
    # ======================================== #

    def insert_item(self, table_item, update_if_duplicate=False):
        try:
            self.insert_row(table_item.get_table(), table_item.get_row_tuple())
        except DuplicateError:
            if update_if_duplicate:
                self.update_item(table_item)
            else:
                raise

    def insert_items(self, table_items):
        if len(set([e.TABLE_NAME for e in table_items])) > 1:
            raise ValueError("Items updated should be of the same type")
        self.insert_rows(table_items[0].get_table(), [item.get_row_tuple() for item in table_items])

    # Find item based on a index only table_item, returns the full item if found
    def find_item(self, table_item):
        if not table_item.INDEX_KEYS:
            raise NoIndexError("This table does not have index keys specified, use get_items instead")
        result = self.search_table(table_item.get_table(), table_item.get_index_dict())
        if result:
            return type(table_item)(result[0])
        return None

    def get_items(self, class_type, search_dict, order_by=None, limit=None, offset=None, desc=True):
        rows = self.search_table(class_type.TABLE_NAME, search_dict, order_by=order_by, limit=limit, offset=offset, desc=desc)
        return [class_type(row) for row in rows]

    def delete_item(self, table_item):
        if not table_item.INDEX_KEYS:
            raise NoIndexError("This table does not have index keys, and cannot delete individual items, use delete_rows instead")
        self.delete_rows(table_item.get_table(), table_item.get_index_dict())

    def update_item(self, table_item):
        if not table_item.INDEX_KEYS:
            raise NoIndexError("This table does not have index keys, and cannot update individual items")
        self.update_row(table_item.get_table(), table_item.get_row_tuple(), table_item.get_index_dict())

    def update_items(self, table_items):
        # Enforce index key and same table
        if not table_items[0].INDEX_KEYS:
            raise NoIndexError("This table does not have index keys, and cannot update batched individual items")
        if len(set([e.TABLE_NAME for e in table_items])) > 1:
            raise ValueError("Items updated should be of the same type")
        self.update_many(table_items[0].get_table(), [item.get_row_tuple() for item in table_items], [item.get_index_dict() for item in table_items])


class SearchDict(dict):

    def add_filter(self, column_name, value, operator="="):
        self[column_name] = {
            "statement_type": "comparison",
            "value": value,
            "operator": operator
        }
        return self

    def add_between(self, column_name, value_low, value_high):
        self[column_name] = {
            "statement_type": "between",
            "value_low": value_low,
            "value_high": value_high
        }

    @staticmethod
    def is_comp(val):
        return val["statement_type"] == "comparison"


class ColumnDict(dict):

    @staticmethod
    def to_query(key, value):
        query = "{} {} ".format(key, value["type"])
        if value["additional_attributes"]:
            query += value["additional_attributes"]
        return query

    def add_column(self, column_name, data_type, additional_attributes=None, primary_key=False):
        self[column_name] = {
            "type": data_type,
            "additional_attributes": additional_attributes,
            "primary_key": primary_key
        }
        return self


class TableItem:

    TABLE_NAME = "table_this_item_belong_to"
    INDEX_KEYS = ["dummy_index_1"]
    ALL_COLUMNS = {} # column name to type map

    # Row_tuple builds the correspondence to table
    def __init__(self, row_tuple=None, **kwargs):
        if row_tuple:
            self.row_tuple = row_tuple
        elif len(kwargs) and type(self).ALL_COLUMNS:
            ALL_COLUMNS = type(self).ALL_COLUMNS
            row_tuple = {col: None for col in ALL_COLUMNS.keys()}
            for key, val in kwargs.items():
                if key in ALL_COLUMNS:
                    row_tuple[key] = ALL_COLUMNS[key](val)
            for index in type(self).INDEX_KEYS:
                if index not in row_tuple:
                    raise "ALL_COLUMNS must contain index fields"
                if row_tuple[index] is None:
                    raise "index field must be populated"
            self.row_tuple = row_tuple
        else:
            raise "Must pass in either a tuple object or field arguments"

    @classmethod
    def get_table(cls):
        return cls.TABLE_NAME

    def __eq__(self, other):
        return self.get_row_tuple() == other.get_row_tuple()

    def get_row_tuple(self):
        return self.row_tuple

    def get_index_dict(self):
        try:
            return {k: self.row_tuple[k] for k in type(self).INDEX_KEYS}
        except KeyError as e:
            print("Row tuple does not contain index: {}".format(e))


class NoIndexError(Exception):
    pass


class DuplicateError(Exception):
    pass
