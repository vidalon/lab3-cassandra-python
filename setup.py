from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table

from config import config

connection.setup(config['DB_HOSTS'], config['KEYSPACE'], protocol_version=3)
connection.session.execute(f"CREATE KEYSPACE IF NOT EXISTS {config['KEYSPACE']} WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}")

# Run model imports just to make sure the schema is setup
from model.accounts_by_user import AccountsByUser
from model.positions_by_account import PositionsByAccount
from model.trades_by_a_d import TradesByAccountAndDate
from model.trades_by_a_sd import TradesByAccountSymbolAndDate
from model.trades_by_a_std import TradesByAccountSymbolTypeAndDate
from model.trades_by_a_td import TradesByAccountTypeAndDate

sync_table(AccountsByUser)
sync_table(PositionsByAccount)
sync_table(TradesByAccountAndDate)
sync_table(TradesByAccountTypeAndDate)
sync_table(TradesByAccountSymbolTypeAndDate)
sync_table(TradesByAccountSymbolAndDate)

