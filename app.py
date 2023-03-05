import random
import uuid
from datetime import datetime, timezone

from cassandra.cqlengine import connection
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import LWTException
from cassandra.query import SimpleStatement
from dateutil.relativedelta import relativedelta
from faker import Faker
from faker_marketdata import MarketDataProvider
from tabulate import tabulate

import setup
from config import config
from model.accounts_by_user import AccountsByUser
from model.positions_by_account import PositionsByAccount
from model.trades_by_a_d import TradesByAccountAndDate
from model.trades_by_a_sd import TradesByAccountSymbolAndDate
from model.trades_by_a_std import TradesByAccountSymbolTypeAndDate
from model.trades_by_a_td import TradesByAccountTypeAndDate

menu = ["Exit", 
        "Generate random data", 
        "Get database inventory", 
        "Get random user's accounts (Q1)", 
        "Get all positions for random account (Q2)", 
        "Get all trades for an account (Q3)"]
transaction_types = ['BUY', 'SELL']
max_number_of_shares = 10000000
max_share_price = 3000
query_page_size = 10

def get_full_table_name(model: Model):
    return f"{model._get_keyspace()}.{model._raw_column_family_name()}"

def generate_random_data(user_count, accounts_per_user_max_count, transaction_max_count):
    print(f"Generating {user_count} users, with up to {accounts_per_user_max_count} accounts and up to {transaction_max_count} transactions")
    fake = Faker()
    Faker.seed(random.randint(0,20000))
    accounts_per_user_count = 0
    transaction_count = 0
    for i in range(user_count):
        # Create username 
        username = fake.user_name()
        accounts_per_user = random.randint(1, accounts_per_user_max_count)
        for j in range(accounts_per_user):
            account_id = str(uuid.uuid4())
            cash = random.uniform(0.0,10000000.00)
            account_name = fake.ripe_id()

            AccountsByUser.create(username=username, account_number=account_id,cash_balance=cash, name=account_name)

            accounts_per_user_count = accounts_per_user_count + 1
            transactions_per_user_count = 0
            transactions_to_generate = random.randint(1, transaction_max_count)
            for k in range(transactions_to_generate):
                generate_random_trade(account_id)
                transactions_per_user_count = transactions_per_user_count + 1
                #print(f"Generated transaction {transactions_per_user_count} out of {transactions_to_generate}")
                transaction_count = transaction_count + 1
                print(f"Transaction count: {transaction_count}", end='\r')
    print(f"Generated {accounts_per_user} accounts per user and a total of {transaction_count} transactions")
            
def generate_random_trade(account_id):
    fake = Faker()
    Faker.seed(random.randint(0,10000))
    fake.add_provider(MarketDataProvider)
    
    # 1. Generate random symbol, at random price with a random number of shares
    trade_id = datetime.timestamp(datetime.now())
    symbol = fake.ticker()
    type = random.choice(transaction_types)
    shares = random.randint(1, max_number_of_shares)
    price = random.uniform(0.1, max_share_price)
    amount = shares * price

    # 2. Insert in all trades tables
    TradesByAccountAndDate.create(account=account_id, trade_id=trade_id, type=type, symbol=symbol, shares=shares, price=price, amount=amount)
    TradesByAccountSymbolAndDate.create(account=account_id, trade_id=trade_id, type=type, symbol=symbol, shares=shares, price=price, amount=amount)
    TradesByAccountSymbolTypeAndDate.create(account=account_id, trade_id=trade_id, type=type, symbol=symbol, shares=shares, price=price, amount=amount)
    TradesByAccountTypeAndDate.create(account=account_id, trade_id=trade_id, type=type, symbol=symbol, shares=shares, price=price, amount=amount)
    
    # 3. Update or create positions by account
    try:
        position = PositionsByAccount.get(account=account_id,symbol=symbol)
        position.quantity = position.quantity + 1
        position.save()
    except:
        PositionsByAccount.create(account=account_id,symbol=symbol)
    
def get_database_inventory():
    accounts_by_user_total = len(AccountsByUser.all())
    positions_by_account_total = len(PositionsByAccount.all())
    tbad_total = len(TradesByAccountAndDate.all())
    tbasd_total = len(TradesByAccountSymbolAndDate.all())
    tbastd_total = len(TradesByAccountSymbolTypeAndDate.all())
    tbatd_total = len(TradesByAccountTypeAndDate.all())

    print(f"Accounts: {accounts_by_user_total}")
    print(f"Positions: {positions_by_account_total}")
    print(f"Trades by Account and Date: {tbad_total}")
    print(f"Trades by Account, Symbol and Date: {tbasd_total}")
    print(f"Trades by Account, Symbol, Type and Date: {tbastd_total}")
    print(f"Trades by Account, Type and Date: {tbatd_total}")

def get_random_row(query, model: Model):
    user_index = random.randint(0, len(model.all()))
    
    statement = SimpleStatement(query, fetch_size=query_page_size)
    
    results = connection.session.execute(statement)
    iterations = int((user_index / query_page_size) - 1)
    if user_index > query_page_size:    
        for i in range(iterations):
            results = connection.session.execute(statement, paging_state=results.paging_state)
    adjusted_index = user_index - iterations * query_page_size

    return results[adjusted_index]

def print_query_results(query):
    option_selected = -1
    statement = SimpleStatement(query, fetch_size=query_page_size)
    results = connection.session.execute(statement)
    while option_selected != "n":
        items = results.current_rows
        print(f"\n\nShowing {len(items)} results", end='\n\n')
        
        data = []
        for item in items:
            data.append(item.values())

        if not items:
            return
        
        print(tabulate(data, headers=items[0].keys()))
        # Check if we are in the last page and exit
        if len(items) < query_page_size:
            print("\n\nNo more items to show...")
            break
        
        option_selected = input("Show more? (y/n): ").lower()
        if option_selected == "y":
            results = connection.session.execute(statement, paging_state=results.paging_state)

    print("\n\nGoing back to main menu...")

def print_model_items(target_model: Model, where_clause):
    table_name = get_full_table_name(target_model)
    query = f"SELECT * FROM {table_name} WHERE {where_clause}"
    print_query_results(query)

def get_random_username():
    table_name = get_full_table_name(AccountsByUser)
    query = f'SELECT {AccountsByUser.username.column.column_name} FROM {table_name}'
    print(query)
    result = get_random_row(query, AccountsByUser)
    username = result['username']
    
    print(f"Getting accounts for username: {username}", end='\n\n')

    print_model_items(AccountsByUser, f"username = '{username}'")

def get_all_positions_in_account():
    table_name = get_full_table_name(PositionsByAccount)
    query = f"SELECT {PositionsByAccount.account.column.column_name} FROM {table_name}"
    result = get_random_row(query, PositionsByAccount)
    account = result['account']

    print(f"Getting positions for account: {account}", end="\n\n")

    print_model_items(PositionsByAccount, f"account = '{account}'")

def get_all_trades_for_account():
    from_date_input = None
    to_date_input = None
    from_date_default = datetime.timestamp(datetime.now() - relativedelta(months=1))
    to_date_default = datetime.timestamp(datetime.now())
    transaction_type_input = None
    transaction_type_default = "BUY"
    symbol_input = None
    symbol_default = None
    
    def print_submenu():
        print("Which type of trade would you like to get?")
        print("\t0. Cancel.")
        print("\t1. All trades, order by tarde date. (Q3.1)")
        print("\t2. All trades, by date range. (Q3.2)")
        print("\t3. All trades, by date range and transaction type.(Q3.3)")
        print("\t4. All trades, by date range, transaction type and instrument simbol.(Q3.4)")
        print("\t5. All trades, by date range and instrument symbol. (Q3.5)")

        return input("\tType your menu option: ")
    
    def get_date_range_input():
        formated_from_date_default = datetime.fromtimestamp(from_date_default)
        to_from_date_default = datetime.fromtimestamp(to_date_default)
        from_date_input = input(f"\tChoose 'from' date format (YYYY-MM-DD) or hit Enter for default ({formated_from_date_default}): ")
        if not from_date_input:
            from_date_input = from_date_default
        to_date_input = input(f"\tChoose 'to' date format (YYYY-MM-DD) or hit Enter for default ({to_from_date_default}): ")
        if not to_date_input:
            to_date_input = to_date_default
        return datetime.fromtimestamp(from_date_input, tz=timezone.utc), datetime.fromtimestamp(to_date_input, tz=timezone.utc)
    
    def get_transaction_type_input():
        transaction_type_input = input(f"\tChoose transaction type [BUY | SELL] or hit Enter for default ({transaction_type_default}): ")
        if not transaction_type_input:
            transaction_type_input = transaction_type_default
        return transaction_type_input

    def get_symbol_input():
        table_name = get_full_table_name(PositionsByAccount)
        query = f"SELECT {PositionsByAccount.symbol.column.column_name} FROM {table_name}"
        symbol_default = get_random_row(query, PositionsByAccount)
        symbol_default = symbol_default["symbol"]
        symbol_input = input(f"\tChoose symbol or hit Enter for default ({symbol_default}): ")
        if not symbol_input:
            symbol_input = symbol_default
        
        return symbol_input
    
    def convert_string_to_date(date_in_string):
        return datetime.strptime(date_in_string, "%y-%m-%d")

    selected_model = None

    option_selected = "-1"
    option_selected = print_submenu()
    while option_selected != "0":
        if option_selected == "1":
            selected_model = TradesByAccountAndDate    
        elif option_selected == "2":
            selected_model = TradesByAccountAndDate
            [from_date_input, to_date_input] = get_date_range_input()
        elif option_selected == "3":
            selected_model = TradesByAccountTypeAndDate
            [from_date_input, to_date_input] = get_date_range_input()
            transaction_type_input = get_transaction_type_input()
        elif option_selected == "4":
            selected_model = TradesByAccountSymbolTypeAndDate
            [from_date_input, to_date_input] = get_date_range_input()
            transaction_type_input = get_transaction_type_input()
            symbol_input = get_symbol_input()
        elif option_selected == "5":
            selected_model = TradesByAccountSymbolAndDate
            [from_date_input, to_date_input] = get_date_range_input()
            symbol_input = get_symbol_input()
        
        table_name = get_full_table_name(selected_model)
        query = f"SELECT * FROM {table_name}"
        if from_date_input and to_date_input:
            query = query + f" WHERE trade_id > '{from_date_input}' and trade_id < '{to_date_input}'"
        if transaction_type_input:
            query = query + f" AND type = '{transaction_type_input}'"
        if symbol_input:
            query = query + f" AND symbol = '{symbol_input}'"
        
        if "WHERE" in query:
            query = query + " ALLOW FILTERING"

        print_query_results(query)

        option_selected = print_submenu()

    print("Going back to main menu...")
    
def print_menu():
    index = 0
    for item in menu: 
        print(f"{index}. {item}")
        index = index + 1

def main():
    option_selected = -1
    while option_selected != "0":
        if option_selected == "1":
            user_count = int(input("Enter number of users to generate: "))
            accounts_per_user_max_count = int(input("Enter the max number of accounts per user to generate: "))
            transaction_max_count = int(input("Enter max number of transactions per user to generate: "))

            generate_random_data(user_count , accounts_per_user_max_count, transaction_max_count)
        elif option_selected == "2":
            get_database_inventory()
        elif option_selected == "3":
            get_random_username()
        elif option_selected == "4":
            get_all_positions_in_account()
        elif option_selected == "5":
            get_all_trades_for_account()
        print_menu()
        option_selected = input("Type your menu option: ")
    print("Goodbye!")

if __name__ ==  "__main__":
    main()
