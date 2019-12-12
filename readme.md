## SqliteDao

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

Insert records easily:

    lebron = {"name": "LeBron James", "position": "SF", "age": 35, "height": "6-8.5"}
    kobe = {"name": "Kobe Bryant", "position":  "SG", "age": 41, "height": "6-6"}
    jordan = {"name": "Michael Jordan", "position": "SG", "age": 56, "height": "6-6"}
    dao.insert_rows(TEST_TABLE_NAME, [lebron, kobe, jordan])

Query easily:

    result = dao.search_table(TEST_TABLE_NAME, {"position": "SG"})
    [{'name': 'Kobe Bryant', 'position': 'SG', 'age': 41, 'height': '6-6'},
     {'name': 'Michael Jordan', 'position': 'SG', 'age': 56, 'height': '6-6'}]

Or with more operators:

    result = dao.search_table(TEST_TABLE_NAME, SearchDict().add_filter("age", 40, operator="<"))

Create objects off `TableItem` easily and deal with even less code,

    dao.insert_item(my_item)

see `test/item_test.py` for example.

## Helpers

1. SearchDict: allow more comparators than "="

2. ColumnDict: allow 'primary key && not null' to columns.

* 1, 2 not needed for full functions

3. TableItem: baseclass for python items <-> db row connection.

## TODO

Add support for pagination (offsets)
