## SqliteDao

![PyPI version](http://img.shields.io/pypi/v/sqlitedao.svg) &nbsp; ![Python 3.x](http://img.shields.io/badge/Python-3.x-green.svg) &nbsp; ![PyPI license](https://img.shields.io/github/license/mashape/apistatus.svg) &nbsp; [![Downloads](https://pepy.tech/badge/sqlitedao)](https://pepy.tech/project/sqlitedao)

A simplified DAO for SQL abstraction for personal projects. All in one file.

    pip install sqlitedao
    
Pet project: [crawfish](https://github.com/Aperocky/crawfish)

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

Create ORM classes by inheriting `TableItem` easily and deal with even less code,

    dao.insert_item(item)
    dao.insert_items(items)
    dao.update_item(changed_item)
    dao.update_items(changed_items)
    dao.find_item(item_with_only_index_populated)
    ...

see test files for example. This can greatly simplify and ease the creation cost for pet projects based on sqlite.
