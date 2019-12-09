## SqliteDao

A simplified DAO for SQL abstraction for personal projects. All in one file.

Opinionated: 

1. No one should be writing more SQL, abstracting them away is conforming to unix philosophy.

2. Table name or column names should not have spaces (or other weird characters) in them.

3. Single connection to a single sqlite db.

Functionalities:

1. Create tables, list tables etc with python dicts.

2. CRUD operations by dictionary inputs or items.

3. DAO abstraction base item.

## Helpers

1. SearchDict: allow more comparators than "="

2. ColumnDict: allow 'primary key && not null' to columns.

* 1, 2 not needed for full functions

3. TableItem: baseclass for python items <-> db row connection.
