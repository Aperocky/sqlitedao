"""

Test create table, index, list tables functionalities

"""

from sqlitedao import SqliteDao, ColumnDict, SearchDict
import os
import pytest
import sqlite3

# Some mock data
TEST_DB_NAME = "test.db"
TEST_TABLE_NAME = "players"
lebron = {"name": "LeBron James", "position": "SF", "age": 35, "height": "6-8.5"}
kobe = {"name": "Kobe Bryant", "position": "SG", "age": 41, "height": "6-6"}
jordan = {"name": "Michael Jordan", "position": "SG", "age": 56, "height": "6-6"}


@pytest.fixture
def dao():
    # Before test
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)
    # Pass dao instance to test
    yield SqliteDao.get_instance(TEST_DB_NAME)
    # Deconstruct
    SqliteDao.terminate_instance(TEST_DB_NAME)
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)


@pytest.fixture(name="xdao")
def prepopulated_dao():
    # Before test
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)
    # Pass dao instance to test
    dao = SqliteDao.get_instance(TEST_DB_NAME)
    columns = (
        ColumnDict()
        .add_column("name", "text", primary_key=True)
        .add_column("position", "text")
        .add_column("age", "integer")
        .add_column("height", "text")
    )
    create_table_indexes = {"name_index": ["name"]}
    dao.create_table(TEST_TABLE_NAME, columns, create_table_indexes)
    dao.insert_rows(TEST_TABLE_NAME, [lebron, kobe, jordan])
    yield dao
    # Deconstruct
    SqliteDao.terminate_instance(TEST_DB_NAME)
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)


def test_singleton_connection():
    dao_one = SqliteDao.get_instance(TEST_DB_NAME)
    dao_two = SqliteDao.get_instance(TEST_DB_NAME)
    assert dao_one == dao_two
    assert len(SqliteDao.INSTANCE_MAP) == 1
    SqliteDao.terminate_all_instances()
    assert len(SqliteDao.INSTANCE_MAP) == 0


def test_basic_table_creation(dao):
    tables = dao.get_schema()  # Should be empty
    assert len(dao.get_schema()) == 0
    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text",
    }
    dao.create_table(TEST_TABLE_NAME, create_table_columns)
    tables = dao.get_schema()
    assert tables[0]["name"] == TEST_TABLE_NAME
    assert dao.is_table_exist(TEST_TABLE_NAME)
    dao.drop_table(TEST_TABLE_NAME)
    assert not dao.is_table_exist(TEST_TABLE_NAME)


def test_table_creation_with_bad_names(dao):
    tables = dao.get_schema()  # Should be empty
    assert len(dao.get_schema()) == 0
    create_table_columns = {
        "pkey": "text",
        "rval": "text",
    }
    with pytest.raises(ValueError):
        dao.create_table("", create_table_columns)
    with pytest.raises(ValueError):
        dao.create_table("only microsoft like spaces in db names", create_table_columns)
    with pytest.raises(ValueError):
        dao.create_table("C:/totally/sane/file/system", create_table_columns)
    with pytest.raises(ValueError):
        dao.create_table(
            "random (pkey); DROP TABLE sqlite_master; ", create_table_columns
        )


def test_basic_index_creation(dao):
    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text",
    }
    create_table_indexes = {"name_index": ["name"]}
    dao.create_table(TEST_TABLE_NAME, create_table_columns, create_table_indexes)
    tables = dao.get_schema()
    assert tables[0]["name"] == TEST_TABLE_NAME
    indexes = dao.get_schema(info="*", type="index")
    assert indexes[0]["name"] == "idx_players_name_index"
    assert indexes[0]["tbl_name"] == TEST_TABLE_NAME
    dao.drop_table(TEST_TABLE_NAME)
    assert len(dao.get_schema()) == 0


def test_multiple_index_creation(dao):
    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text",
    }
    create_table_indexes = {
        "name_index": ["name"],
        "age_and_height_index": ["age", "height"],
    }
    dao.create_table(TEST_TABLE_NAME, create_table_columns, create_table_indexes)
    indexes = dao.get_schema(info="*", type="index")
    assert len(indexes) == 2


def test_index_deletion(dao):
    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text",
    }
    create_table_indexes = {
        "name_index": ["name"],
        "age_and_height_index": ["age", "height"],
    }
    dao.create_table(TEST_TABLE_NAME, create_table_columns, create_table_indexes)
    indexes = dao.get_schema(info="*", type="index")
    assert len(indexes) == 2
    dao.drop_index(TEST_TABLE_NAME, "name_index")
    indexes = dao.get_schema(info="*", type="index")
    assert len(indexes) == 1
    assert indexes[0]["name"] == "idx_players_age_and_height_index"


def test_extended_column_creation(dao):
    columns = ColumnDict()
    columns.add_column("name", "text", "PRIMARY KEY").add_column(
        "position", "text"
    ).add_column("age", "integer").add_column("height", "text")
    create_table_indexes = {"name_index": ["name"]}
    dao.create_table(TEST_TABLE_NAME, columns, create_table_indexes)
    tables = dao.get_schema(info="*")
    assert tables[0]["name"] == TEST_TABLE_NAME
    assert "name text PRIMARY KEY" in tables[0]["sql"]


def test_unique_instance(dao):
    duplicate_dao = SqliteDao.get_instance(TEST_DB_NAME)
    assert dao is duplicate_dao
    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text",
    }
    dao.create_table(TEST_TABLE_NAME, create_table_columns)
    assert duplicate_dao.get_schema()[0]["name"] == TEST_TABLE_NAME


def test_regular_insert_and_search(dao):
    columns = ColumnDict()
    columns.add_column("name", "text", "PRIMARY KEY").add_column(
        "position", "text"
    ).add_column("age", "integer").add_column("height", "text")
    dao.create_table(TEST_TABLE_NAME, columns)
    dao.insert_row(TEST_TABLE_NAME, lebron)
    dao.insert_row("Players", kobe)
    dao.insert_row("Players", jordan)
    result = dao.search_table(TEST_TABLE_NAME, {"name": "LeBron James"})
    assert lebron == result[0]
    result = dao.search_table(TEST_TABLE_NAME, {"position": "SG"})
    assert len(result) == 2


def test_create_table_index(dao):
    columns = ColumnDict()
    columns.add_column("name", "text", "PRIMARY KEY")
    columns.add_column("position", "text")
    columns.add_column("age", "integer")
    columns.add_column("height", "text")
    create_table_indexes = {
        "name_index": ["name"],
        "name_and_age_index": ["name", "age"],
    }
    dao.create_table(TEST_TABLE_NAME, columns, create_table_indexes)
    dao.drop_index(TEST_TABLE_NAME, "name_index")
    dao.drop_index(TEST_TABLE_NAME, "name_and_age_index")
    with pytest.raises(sqlite3.OperationalError) as e:
        dao.drop_index(TEST_TABLE_NAME, "none_existent_index")


def test_batch_insert(dao):
    create_table_columns = {
        "name": "text",
        "position": "text",
        "age": "integer",
        "height": "text",
    }
    dao.create_table(TEST_TABLE_NAME, create_table_columns)
    dao.insert_rows(TEST_TABLE_NAME, [lebron, kobe, jordan])
    result = dao.search_table(TEST_TABLE_NAME, {})
    assert len(result) == 3


def test_advanced_search_query(xdao):
    search = SearchDict().add_filter("age", 40, operator="<")
    assert lebron == xdao.search_table(TEST_TABLE_NAME, search)[0]
    search = SearchDict().add_filter("age", 40, operator=">")
    assert len(xdao.search_table(TEST_TABLE_NAME, search, limit=1)) == 1


def test_update(xdao):
    # LeBron has aged
    search = SearchDict().add_filter("age", 40, operator="<")
    assert xdao.search_table(TEST_TABLE_NAME, search)[0] == lebron
    xdao.update_row(TEST_TABLE_NAME, {"age": 45}, {"name": "LeBron James"})
    assert len(xdao.search_table(TEST_TABLE_NAME, search)) == 0


def test_batch_update(xdao):
    players = xdao.search_table(TEST_TABLE_NAME, {})
    update_age = [{"age": p["age"] + 10} for p in players]
    indexes = [{"name": p["name"]} for p in players]
    xdao.update_many(TEST_TABLE_NAME, update_age, indexes)
    aged_players = xdao.search_table(TEST_TABLE_NAME, {})


def test_backfill_update(xdao):
    update = {"position": "PLAYER"}
    xdao.update_rows(TEST_TABLE_NAME, update, {})
    players = xdao.search_table(TEST_TABLE_NAME, {})
    assert all(e["position"] == "PLAYER" for e in players)


def test_backfill_update_with_search_dict(xdao):
    update = {"position": "PLAYER"}
    search = SearchDict().add_filter("age", 40, operator="<")
    xdao.update_rows(TEST_TABLE_NAME, update, search)
    players = xdao.search_table(TEST_TABLE_NAME, {})
    assert not all(e["position"] == "PLAYER" for e in players)
    assert sum(e["position"] == "PLAYER" for e in players) == 1


def test_backfill_update_with_search(xdao):
    update = {"position": "PLAYER"}
    xdao.update_rows(TEST_TABLE_NAME, update, {"position": "SG"})
    players = xdao.search_table(TEST_TABLE_NAME, {"position": "PLAYER"})
    assert len(players) == 2


def test_delete_rows(xdao):
    xdao.delete_rows(TEST_TABLE_NAME, {"name": "Michael Jordan"})
    assert len(xdao.search_table(TEST_TABLE_NAME, {})) == 2
    xdao.insert_row(TEST_TABLE_NAME, jordan)
    xdao.delete_rows(TEST_TABLE_NAME, {})
    assert len(xdao.search_table(TEST_TABLE_NAME, {})) == 0


def test_delete_rows_by_extended_search(xdao):
    xdao.delete_rows(TEST_TABLE_NAME, SearchDict().add_filter("age", 40, ">"))
    assert len(xdao.search_table(TEST_TABLE_NAME, {})) == 1
    assert xdao.search_table(TEST_TABLE_NAME, {})[0] == lebron


def test_get_rowcount(xdao):
    row_count = xdao.get_row_count(TEST_TABLE_NAME)
    assert row_count == 3


def test_groupby(xdao):
    groupby_positions = xdao.search_table(TEST_TABLE_NAME, {}, group_by=["position"])
    assert groupby_positions[0]["position"] == "SG"
    assert groupby_positions[1]["count"] == 1


def test_groupby_with_search(xdao):
    search = SearchDict().add_filter("age", 40, operator=">")
    groupby_positions = xdao.search_table(
        TEST_TABLE_NAME, search, group_by=["position"]
    )
    assert len(groupby_positions) == 1
    assert groupby_positions[0]["count"] == 2


def test_groupby_with_limit(xdao):
    search = SearchDict().add_filter("age", 20, operator=">")
    groupby_positions = xdao.search_table(
        TEST_TABLE_NAME, search, group_by=["position"], limit=1
    )
    assert len(groupby_positions) == 1
    assert groupby_positions[0]["position"] == "SG"


def test_orderby(xdao):
    rows = xdao.search_table(TEST_TABLE_NAME, {}, order_by=["age"])
    assert rows[0]["name"] == "Michael Jordan"
    assert rows[1]["name"] == "Kobe Bryant"


def test_orderby_with_limit(xdao):
    rows = xdao.search_table(TEST_TABLE_NAME, {}, order_by=["age"], limit=2)
    assert len(rows) == 2
    assert rows[1]["name"] == "Kobe Bryant"


def test_orderby_groupby_conflict(xdao):
    groupby_positions = xdao.search_table(
        TEST_TABLE_NAME, {}, group_by=["position"], order_by=["age"]
    )
    assert groupby_positions[0]["position"] == "SG"
    assert groupby_positions[1]["count"] == 1


def test_orderby_with_search(xdao):
    search = SearchDict().add_filter("age", 45, operator="<")
    rows = xdao.search_table(TEST_TABLE_NAME, search, order_by=["age"])
    assert len(rows) == 2
    assert rows[0]["name"] == "Kobe Bryant"
    rows = xdao.search_table(TEST_TABLE_NAME, search, order_by=["age"], desc=False)
    assert rows[0]["name"] == "LeBron James"


def test_offset(xdao):
    rows = xdao.search_table(TEST_TABLE_NAME, {}, order_by=["age"], limit=2, offset=2)
    assert len(rows) == 1
    assert rows[0]["name"] == "LeBron James"


def test_between_with_search(xdao):
    search = SearchDict()
    search.add_between("age", 35, 50)
    rows = xdao.search_table(TEST_TABLE_NAME, search)
    assert len(rows) == 2
    search.clear()
    search.add_between("age", 38, 50)
    rows = xdao.search_table(TEST_TABLE_NAME, search)
    assert len(rows) == 1
    assert rows[0]["name"] == "Kobe Bryant"


def test_between_with_update(xdao):
    search = SearchDict()
    search.add_between("age", 37, 50)
    update = {"age": 60}
    xdao.update_rows(TEST_TABLE_NAME, update, search)
    rows = xdao.search_table(TEST_TABLE_NAME, {}, order_by=["age"])
    assert rows[0]["name"] == "Kobe Bryant"
    assert rows[0]["age"] == 60


def test_between_with_delete(xdao):
    search = SearchDict()
    search.add_between("age", 37, 50)
    xdao.delete_rows(TEST_TABLE_NAME, search)
    rows = xdao.search_table(TEST_TABLE_NAME, {})
    assert len(rows) == 2
    # RIP kobe
    assert not any([e["name"] == "Kobe Bryant" for e in rows])
    # Go lakers
    assert any([e["name"] == "LeBron James" for e in rows])
