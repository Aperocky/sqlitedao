"""

Test create table, index, list tables functionalities

"""

from sqlitedao import SqliteDao, ColumnDict
import os
import pytest

@pytest.fixture(autouse=True)
def dao():
    # Before test
    if os.path.exists("test.db"):
        os.remove("test.db")
    # Pass dao instance to test
    yield SqliteDao.get_instance("test.db")
    # Deconstruct
    SqliteDao.terminate_instance("test.db")

def test_basic_table_creation(dao):
    tables = dao.get_schema() # Should be empty
    assert len(dao.get_schema()) == 0
    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text"
    }
    dao.create_table("players", create_table_columns)
    tables = dao.get_schema()
    assert tables[0]["name"] == "players"
    assert dao.is_table_exist("players")

def test_basic_index_creation(dao):
    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text"
    }
    create_table_indexes = {
        "name_index": ["name"]
    }
    dao.create_table("players", create_table_columns, create_table_indexes)
    tables = dao.get_schema()
    assert tables[0]["name"] == "players"
    indexes = dao.get_schema(info="*", type="index")
    assert indexes[0]["name"] == "idx_players_name_index"
    assert indexes[0]["tbl_name"] == "players"
    dao.drop_table("players")
    assert len(dao.get_schema()) == 0

def test_extended_column_creation(dao):
    columns = ColumnDict()
    columns\
        .add_column("name", "text", "PRIMARY KEY")\
        .add_column("position", "text")\
        .add_column("age", "integer")\
        .add_column("height", "text")
    create_table_indexes = {
        "name_index": ["name"]
    }
    dao.create_table("players", columns, create_table_indexes)
    tables = dao.get_schema(info="*")
    assert tables[0]["name"] == "players"
    assert "name text PRIMARY KEY" in tables[0]["sql"]

def test_unique_instance(dao):
    duplicate_dao = SqliteDao.get_instance("test.db")
    assert dao is duplicate_dao
    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text"
    }
    dao.create_table("players", create_table_columns)
    assert duplicate_dao.get_schema()[0]["name"] == "players"

def test_regular_insert(dao):
    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text"
    }
    create_table_indexes = {
        "name_index": ["name"]
    }
    dao.create_table("players", create_table_columns, create_table_indexes)
