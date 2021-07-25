from sqlitedao import SqliteDao, ColumnDict, SearchDict
from .dao_test import prepopulated_dao, dao
from .dao_test import TEST_TABLE_NAME
from .item_test import Player
import os
import pytest

MIGRATION_DB_NAME = "migrate.db"

class MigPlayer(Player):

    def migrate(self, dao):
        self.migrated = True
        dao.insert_item(self)


@pytest.fixture(name="mig")
def migration_dao():
    # Before test
    if os.path.exists(MIGRATION_DB_NAME):
        os.remove(MIGRATION_DB_NAME)
    # Pass dao instance to test
    dao = SqliteDao.get_instance(MIGRATION_DB_NAME)
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
    yield SqliteDao.get_instance(MIGRATION_DB_NAME)
    # Deconstruct
    SqliteDao.terminate_instance(MIGRATION_DB_NAME)
    if os.path.exists(MIGRATION_DB_NAME):
        os.remove(MIGRATION_DB_NAME)

def test_setup(xdao, mig):
    players = xdao.get_items(Player, {})
    mig_players = mig.get_items(Player, {})
    assert len(players) == 3
    assert len(mig_players) == 0

def test_migrate_all(xdao, mig):
    players = xdao.get_items(Player, {})
    mig_players = mig.get_items(Player, {})
    assert len(mig_players) == 0
    mig.insert_items(players)
    mig_players = mig.get_items(Player, {})
    assert len(mig_players) == 3
    lebron = mig.find_item(Player(name="LeBron James"))
    assert lebron.height == "6-8.5"

def test_migration_class(xdao, mig):
    players = xdao.get_items(MigPlayer, {})
    mig_players = mig.get_items(MigPlayer, {})
    assert len(mig_players) == 0
    for player in players:
        player.migrate(mig)
        assert player.migrated
    mig_players = mig.get_items(MigPlayer, {})
    assert len(mig_players) == 3

