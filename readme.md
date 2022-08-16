## SqliteDao

![PyPI version](http://img.shields.io/pypi/v/sqlitedao.svg) &nbsp; ![Python 3.x](http://img.shields.io/badge/Python-3.x-green.svg) &nbsp; ![PyPI license](https://img.shields.io/github/license/mashape/apistatus.svg) &nbsp; ![Downloads](https://pepy.tech/badge/sqlitedao) &nbsp; ![Unit Test](https://github.com/Aperocky/sqlitedao/workflows/Unit%20Test/badge.svg)

A simplified DAO for SQL abstraction for personal projects.

    pip install sqlitedao

Demo project: [sqlite_img_app](https://github.com/Aperocky/sqlite_img_demo)

### Examples

Create Table easily:

    from sqlitedao import SqliteDao

    dao = SqliteDao.get_instance(DB_PATH)

    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text"
    }
    dao.create_table(TEST_TABLE_NAME, create_table_columns)

Or with a bit more control:

    from sqlitedao import ColumnDict

    columns = ColumnDict()
    columns\
        .add_column("name", "text", "PRIMARY KEY")\
        .add_column("position", "text")\
        .add_column("age", "integer")\
        .add_column("height", "text")
    create_table_indexes = {
        "name_index": ["position"]
    }
    dao.create_table(TEST_TABLE_NAME, columns, create_table_indexes)

Retrieve items as a list of python dictionaries:

    from sqlitedao import SearchDict

    search = SearchDict().add_filter("age", 50, operator=">")
    rows = xdao.search_table(TEST_TABLE_NAME, search)
    # [{"name": "Michael Jordan", "position": "SG", "age": 56, "height": "6-6"}]

Create DAO classes by inheriting `TableItem` easily and deal with less code:

    from sqlitedao import TableItem

    class Player(TableItem):

        TABLE_NAME = TEST_TABLE_NAME
        INDEX_KEYS = ["name"]
        ALL_COLUMNS = {
            "name": str,
            "position": str,
            "age": int,
            "height": str
        }

        def __init__(self, row_tuple=None, **kwargs):
            super().__init__(row_tuple, **kwargs)
            self.load_tuple()

        def load_tuple(self):
            self.name = self.row_tuple["name"]
            self.position = self.row_tuple["position"]
            self.age = self.row_tuple["age"]
            self.height = self.row_tuple["height"]

        def grow(self):
            self.age += 1
            self.row_tuple['age'] += 1


    # Perform DAO action with above structured class
    dao.insert_item(item)
    dao.insert_items(items)
    dao.update_item(changed_item)
    dao.update_items(changed_items)
    dao.find_item(item_with_only_index_populated)

see test files for more examples. This can greatly simplify and ease the creation cost for pet projects based on sqlite.
