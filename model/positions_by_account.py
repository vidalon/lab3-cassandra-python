from cassandra.cqlengine import columns, connection
from cassandra.cqlengine.models import Model

from config import config


class PositionsByAccount(Model):
    account         = columns.Text(primary_key=True)
    symbol          = columns.Text(primary_key=True, clustering_order="DESC")
    quantity        = columns.BigInt(default=1)


