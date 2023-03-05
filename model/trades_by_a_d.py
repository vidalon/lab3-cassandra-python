from cassandra.cqlengine import columns, connection
from cassandra.cqlengine.models import Model

from config import config


class TradesByAccountAndDate(Model):
    account         = columns.Text(primary_key=True)
    trade_id        = columns.DateTime(primary_key=True, clustering_order="DESC")
    type            = columns.Text()
    symbol          = columns.Text()
    shares          = columns.BigInt()
    price           = columns.Double()
    amount          = columns.Double()
