"""

Test create table, index, list tables functionalities

"""

from sqlitedao import SqliteDao, ColumnDict, SearchDict
import os
import pytest

# Some mock data
lebron = {"name": "LeBron James", "position": "SF", "age": 35, "height": "6-8.5"}
kobe = {"name": "Kobe Bryant", "position":  "SG", "age": 41, "height": "6-6"}
jordan = {"name": "Michael Jordan", "position": "SG", "age": 56, "height": "6-6"}

@pytest.fixture
def dao():
    # Before test
    if os.path.exists("test.db"):
        os.remove("test.db")
    # Pass dao instance to test
    yield SqliteDao.get_instance("test.db")
    # Deconstruct
    SqliteDao.terminate_instance("test.db")

@pytest.fixture(name="xdao")
def prepopulated_dao():
    # Before test
    if os.path.exists("test.db"):
        os.remove("test.db")
    # Pass dao instance to test
    dao = SqliteDao.get_instance("test.db")
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
    dao.insert_rows("players", [lebron, kobe, jordan])
    yield dao
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

def test_regular_insert_and_search(dao):
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
    dao.insert_row("players", lebron)
    dao.insert_row("Players", kobe)
    dao.insert_row("Players", jordan)
    result = dao.search_table("players", {"name": "LeBron James"})
    assert lebron == result[0]
    result = dao.search_table("players", {"position": "SG"})
    assert len(result) == 2

def test_batch_insert(dao):
    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text"
    }
    dao.create_table("players", create_table_columns)
    dao.insert_rows("players", [lebron, kobe, jordan])
    result = dao.search_table("players", {})
    assert len(result) == 3
    
def test_advanced_search_query(xdao):
    search = SearchDict().add_filter("age", 40, operator="<")
    assert lebron == xdao.search_table("players", search)[0]
    
