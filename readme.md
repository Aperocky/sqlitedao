## SqliteDao

![PyPI version](http://img.shields.io/pypi/v/sqlitedao.svg) &nbsp; ![Python 3.x](http://img.shields.io/badge/Python-3.x-green.svg) &nbsp; ![PyPI license](https://img.shields.io/github/license/mashape/apistatus.svg) &nbsp; [![Downloads](https://pepy.tech/badge/sqlitedao)](https://pepy.tech/project/sqlitedao)

A simplified DAO for SQL abstraction for personal projects. All in one file.

    pip install sqlitedao

### Examples

Create Table easily:

    dao = SqliteDao.get_instance(DB_PATH)

    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text"
    }
    dao.create_table(TEST_TABLE_NAME, create_table_columns)

Or with a bit more control:

    columns = ColumnDict()
    columns\
        .add_column("name", "text", "PRIMARY KEY")\
        .add_column("position", "text")\
        .add_column("age", "integer")\
        .add_column("height", "text")
    create_table_indexes = {
        "name_index": ["name"]
    }
    dao.create_table(TEST_TABLE_NAME, columns, create_table_indexes)

INSERT:

    lebron = {"name": "LeBron James", "position": "SF", "age": 35, "height": "6-8.5"}
    kobe = {"name": "Kobe Bryant", "position":  "SG", "age": 41, "height": "6-6"}
    jordan = {"name": "Michael Jordan", "position": "SG", "age": 56, "height": "6-6"}
    dao.insert_rows(TEST_TABLE_NAME, [lebron, kobe, jordan])

READ:

    result = dao.search_table(TEST_TABLE_NAME, {"position": "SG"})
    [{'name': 'Kobe Bryant', 'position': 'SG', 'age': 41, 'height': '6-6'},
     {'name': 'Michael Jordan', 'position': 'SG', 'age': 56, 'height': '6-6'}]

Or with more search operations:

    result = dao.search_table(TEST_TABLE_NAME, SearchDict().add_filter("age", 40, operator="<"))
    result = dao.search_table(TEST_TABLE_NAME, SearchDict().add_filter("age", 40, operator="<"), group_by=["positions"])
    result = dao.search_table(TEST_TABLE_NAME, {}, order_by=["age"])

UPDATE:

    dao.update_many(TEST_TABLE_NAME, [{"age": 30}, {"age": 40}], [{"name": "A"}, {"name": "B"}])

`update_row`, `update_rows` is also available with slightly different functionality.

DELETE:

    dao.delete_rows(TEST_TABLE_NAME, {"position": "SG"})

Same for update and deletion.

Create classes inheriting `TableItem` easily and deal with even less code,

    dao.insert_item(item)
    dao.insert_items(items)
    dao.update_item(changed_item)
    dao.update_items(changed_items)
    dao.find_item(item_with_only_index_populated)
    ...

see `test/item_test.py` for example.

## Helpers

1. SearchDict: allow more comparators than "="

2. ColumnDict: allow 'primary key && not null' to columns.

* 1, 2 not needed for full functions

3. TableItem: baseclass for python items <-> db row connection.

