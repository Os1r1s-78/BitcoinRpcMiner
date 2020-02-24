import time
import datetime
import logging
import config as cfg
from db_access import *
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

logging.basicConfig(
    filename='log/app.log',
    filemode='w',
    format="%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)

rpc = None
cursor = None


def setup_db():
    # Set up database connection
    logging.info('Trying to connect to the database')
    try:
        db = pyodbc.connect(
            driver='{ODBC Driver 17 for SQL Server}',
            server=cfg.db['server'],
            database=cfg.db['database'],
            trusted_connection=cfg.db['trusted'],
            uid=cfg.db['username'] if cfg.db['trusted'] != 'yes' else None,
            pwd=cfg.db['password'] if cfg.db['trusted'] != 'yes' else None,
            timeout=15
        )
        global cursor
        cursor = db.cursor()
        logging.info('Successfully connected to the database')
        return True
    except (KeyboardInterrupt, SystemExit) as e:
        logging.critical('Application interrupted, exiting: ' + repr(e))
        raise
    except Exception as e:
        print('Error while trying to connect to the database: ' + repr(e))
        logging.critical(repr(e))
        return False


def setup_rpc():
    # Set up connection to the Bitcoin RPC client
    logging.info('Trying to connect to the Bitcoin RPC client')
    try:
        global rpc
        rpc = AuthServiceProxy("http://%s:%s@%s" % (cfg.rpc['rpcuser'], cfg.rpc['rpcpassword'], cfg.rpc['endpoint']))
        logging.info('Successfully connected to the Bitcoin RPC client')
        return True
    except (KeyboardInterrupt, SystemExit) as e:
        logging.critical('Application interrupted, exiting: ' + repr(e))
        raise
    except Exception as e:
        print('Error while trying to connect to the Bitcoin RPC client: ' + repr(e))
        logging.critical(repr(e))
        return False


def execute():
    first_iteration = True
    current_day = None
    active_block_hash = ""

    current_outputs = []
    current_freq_analysis = []
    current_size_analysis = []
    current_prot_analysis = []

    while True:
        try:
            if first_iteration:
                last_active_day = get_latest_active_day()
                current_day = last_active_day
                if last_active_day is not None:
                    delete_data_after_date(last_active_day, 'FrequencyAnalysis')
                    delete_data_after_date(last_active_day, 'SizeAnalysis')
                    delete_data_after_date(last_active_day, 'ProtocolAnalysis')
                first_iteration = False
            else:
                if active_block_hash == "" or None:
                    # Choose the next available block
                    latest_tx_output = get_latest_tx_output()
                    if latest_tx_output is not None:  # Choose the next block after the last tx entry
                        active_block_hash = rpc.getblock(latest_tx_output.Blockhash)['nextblockhash']
                    else:  # Set the next block to the genesis block
                        active_block_hash = rpc.getblockhash(0)
                        block_timestamp = rpc.getblock(active_block_hash)['time']
                        current_day = datetime.date.fromtimestamp(block_timestamp)

                block = rpc.getblock(active_block_hash)
                for tx_hash in block['tx']:
                    # The genesis' blocks transaction cannot be retrieved with the Bitcoin RPC, simply skip it
                    if tx_hash == '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b':
                        continue

                    # Process transaction
                    tx = rpc.getrawtransaction(tx_hash, 1)
                    for tx_out in tx['vout']:
                        print()
                        # todo: implement analysis

                next_block = rpc.getblock(block['nextblockhash'])
                rounded = time.mktime((current_day + datetime.timedelta(days=1)).timetuple())
                if next_block['time'] < time.mktime((current_day + datetime.timedelta(days=1)).timetuple()):
                    active_block_hash = next_block['hash']
                else:
                    print()
                    # todo: implement db insert and var resets
        except JSONRPCException as e:
            print(repr(e))
            logging.critical(repr(e))
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            print(repr(e))
            logging.critical(repr(e))


def main():
    rpc_loaded = False
    db_loaded = False

    while not rpc_loaded:
        rpc_loaded = setup_rpc()
        if not rpc_loaded:
            time.sleep(10)

    while not db_loaded:
        db_loaded = setup_db()
        if not db_loaded:
            time.sleep(10)

    execute()


if __name__ == '__main__':
    main()
