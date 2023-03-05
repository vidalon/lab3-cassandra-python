from cassandra.cqlengine import columns, connection
from cassandra.cqlengine.models import Model

from config import config


class AccountsByUser(Model):
    username        = columns.Text(primary_key=True)
    account_number  = columns.Text(primary_key=True, clustering_order="DESC")
    cash_balance    = columns.Double(default=0)
    name            = columns.Text(required=False)


