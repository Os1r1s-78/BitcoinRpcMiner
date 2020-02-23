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

# Set up connection to the Bitcoin RPC client
rpc_connection = AuthServiceProxy("http://%s:%s@%s" % (cfg.rpc['rpcuser'], cfg.rpc['rpcpassword'], cfg.rpc['endpoint']))


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
                if last_active_day is not None:
                    delete_data_after_date(last_active_day, 'FrequencyAnalysis')
                    delete_data_after_date(last_active_day, 'SizeAnalysis')
                    delete_data_after_date(last_active_day, 'ProtocolAnalysis')
                first_iteration = False
            else:
                # todo: implement
                print()
        except JSONRPCException as e:
            print(repr(e))
            logging.critical(repr(e))
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            print(repr(e))
            logging.critical(repr(e))


def main():
    execute()


if __name__ == '__main__':
    main()

# start_time = time.time()
# best_block_hash = rpc_connection.getbestblockhash()
# print("--- %s seconds ---" % (time.time() - start_time))
