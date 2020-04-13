import os
import time
import statistics
import calendar
import datetime
import logging
import atexit
import config as cfg
from db_access import *
from dbmodels import *
from protocols import *
from fileheaders import *
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

logging.basicConfig(
    filename='log/app.log',
    filemode='w',
    format="%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)
logging.getLogger().addHandler(logging.StreamHandler())

rpc = None


def setup_rpc():
    # Set up connection to the Bitcoin RPC client
    logging.info('Trying to connect to the Bitcoin RPC client')
    try:
        global rpc
        rpc = AuthServiceProxy("http://%s:%s@%s" % (cfg.rpc['rpcuser'], cfg.rpc['rpcpassword'], cfg.rpc['endpoint']), timeout=120)
        logging.info('Successfully connected to the Bitcoin RPC client')
        return True
    except (KeyboardInterrupt, SystemExit) as e:
        logging.critical('Application interrupted, exiting: ' + repr(e))
        raise
    except Exception as e:
        logging.critical('Error while trying to connect to the Bitcoin RPC client: ' + repr(e))
        return False


def execute():
    first_iteration = True
    current_day = None
    active_block_hash = ""

    current_outputs = []
    current_freq_analysis = FrequencyAnalysis()
    current_size_analysis = SizeAnalysis()
    current_file_analysis = FileAnalysis()
    current_prot_analysis = ProtocolAnalysis()

    last_active_day = None

    last_block_execution_times = []

    while True:
        try:
            if first_iteration:
                logging.info('Application first started, retrieving last active day of analysis')
                last_active_day = get_latest_active_day()
                if last_active_day is not None:
                    current_day = last_active_day
                    logging.info('The last active day of analysis was ' + last_active_day.strftime('%Y-%m-%d'))
                    logging.info('Deleting data beyond the last day of analysis')
                    delete_data_after_date(last_active_day, 'FrequencyAnalysis')
                    delete_data_after_date(last_active_day, 'SizeAnalysis')
                    delete_data_after_date(last_active_day, 'FileAnalysis')
                    delete_data_after_date(last_active_day, 'ProtocolAnalysis')
                    delete_tx_outputs_after_date(last_active_day)
                else:
                    logging.info('Couldn\'t find any analysis data, deleting all rows from all tables and starting over')
                    delete_all_data()
                first_iteration = False
            else:
                start_block_time = time.time()

                if active_block_hash == "" or None:
                    # Find the block to begin with, based on the last active day of analysis (using binary search)
                    if last_active_day is not None:
                        last_active_day_timestamp = calendar.timegm(last_active_day.timetuple())
                        active_block_hash = binary_search_for_next_block(last_active_day_timestamp)
                        active_block = rpc.getblock(active_block_hash)
                        current_day = datetime.datetime.utcfromtimestamp(active_block['time']).date()
                        logging.info('Set the next block to first block after where the program last stopped: ' + current_day.strftime('%Y-%m-%d') + ', Hash: ' + active_block_hash)
                    else:
                        active_block_hash = rpc.getblockhash(0)
                        block_timestamp = rpc.getblock(active_block_hash)['time']
                        current_day = datetime.datetime.utcfromtimestamp(block_timestamp).date()
                        logging.info('Set the next block to the genesis block, as there is no previous transaction outputs available')

                block = rpc.getblock(active_block_hash)
                logging.info('Processing transactions of block ' + active_block_hash)
                for tx_hash in block['tx']:
                    # The genesis' blocks transaction cannot be retrieved with the Bitcoin RPC, simply skip it
                    if tx_hash == '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b':
                        logging.info('Skipping the first transaction of the Bitcoin blockchain, as that is not retrievable with the RPC')
                        current_freq_analysis.p2pk += 1
                        continue

                    # Process transaction
                    tx = None
                    try:
                        tx = rpc.getrawtransaction(tx_hash, 1)
                    except JSONRPCException as e:
                        log_error("Error getting transaction " + tx_hash + " in block " + active_block_hash, "JSONRPCException")
                        logging.error('Error getting transaction ' + tx_hash + ' in block ' + active_block_hash)
                        continue

                    # logging.info('Processing outputs of transaction ' + tx['txid'] + ' in block ' + str(block['height']))
                    for tx_out in tx['vout']:
                        script_type = tx_out['scriptPubKey']['type']
                        script_asm = tx_out['scriptPubKey']['asm'].split(' ')
                        script_hex = tx_out['scriptPubKey']['hex']

                        protocol = determine_protocol(script_hex)
                        file_header = determine_file(script_hex)

                        if script_type == 'nulldata' or script_asm[0] == 'OP_RETURN':
                            # Weird way to access attributes by their string names
                            if protocol is not None and protocol:
                                setattr(current_prot_analysis, protocol, getattr(current_prot_analysis, protocol) + 1)
                            if file_header is not None and file_header:
                                setattr(current_file_analysis, file_header,
                                        getattr(current_file_analysis, file_header) + 1)

                            current_outputs.append(
                                TransactionOutput(tx['txid'], tx['blocktime'], tx['blockhash'], tx_out['value'],
                                                  tx_out['scriptPubKey']['type'], tx_out['scriptPubKey']['asm'],
                                                  tx_out['scriptPubKey']['hex'], protocol, file_header))

                            current_freq_analysis.nulldata += 1
                            current_size_analysis.avgsize += len(tx_out['scriptPubKey']['hex']) / 2  # Each byte is 2 characters long in ASCII
                            current_size_analysis.outputs += 1
                        else:
                            if script_type == 'pubkey' or (len(script_asm) == 2 and script_asm[1] == 'OP_CHECKSIG'):
                                current_freq_analysis.p2pk += 1
                            elif script_type == 'pubkeyhash' or (len(script_asm) == 5 and script_asm[0] == 'OP_DUP' and script_asm[1] == 'OP_HASH160' and script_asm[3] == 'OP_EQUALVERIFY' and script_asm[4] == 'OP_CHECKSIG'):
                                current_freq_analysis.p2pkh += 1
                            elif script_type == 'multisig' or (script_asm[len(script_asm) - 1] == 'OP_CHECKMULTISIG'):
                                current_freq_analysis.p2ms += 1
                            elif script_type == 'scripthash' or (len(script_asm) == 3 and script_asm[0] == 'OP_HASH160' and script_asm[2] == 'OP_EQUAL'):
                                current_freq_analysis.p2sh += 1
                            else:
                                current_freq_analysis.unknowntype += 1

                # todo: set breakpoint for timer

                logging.info('Writing outputs of block ' + active_block_hash + ' to database, then continuing with the next block')
                insert_tx_outputs(current_outputs)
                current_outputs.clear()

                logging.info('Reconnect to the rpc as the connection was likely killed during the insert')
                setup_rpc()

                # Measure the execution time for this block and estimate the time left to catch up
                last_block_execution_times.append(time.time() - start_block_time)
                if len(last_block_execution_times) > 100:
                    last_block_execution_times.pop(0)
                avg_block_execution_time = statistics.mean(last_block_execution_times)
                total_block_height = rpc.getblockcount()
                current_block_height = block['height']
                logging.info('Estimated time remaining: ' + str(datetime.timedelta(seconds=avg_block_execution_time * (total_block_height - current_block_height))) + " (Average block processing time is " + str(avg_block_execution_time) + " seconds)")

                # Check whether there is a next block
                while "nextblockhash" not in block:
                    logging.info('There is currently no next block available, waiting 1 second before checking again')
                    time.sleep(1)
                    block = rpc.getblock(active_block_hash)

                # Check whether the next block is still in the same day
                next_block = rpc.getblock(block['nextblockhash'])
                if next_block['time'] < calendar.timegm((current_day + datetime.timedelta(days=1)).timetuple()):
                    active_block_hash = next_block['hash']
                    logging.info('The next block is still in the same day. Continuing with block ' + str(next_block['height']) + ' with ' + str(len(next_block['tx'])) + ' transactions')
                else:
                    logging.info('Writing outputs and analysis to the database of day ' + current_day.strftime('%Y-%m-%d'))

                    # Adjust the data day variables
                    current_freq_analysis.dataday = current_day
                    current_size_analysis.dataday = current_day
                    current_file_analysis.dataday = current_day
                    current_prot_analysis.dataday = current_day

                    # Next block is the next day
                    active_block_hash = next_block['hash']
                    current_day = current_day + datetime.timedelta(days=1)

                    # Commit to database
                    # insert_tx_outputs(current_outputs)
                    insert_freq_analysis(current_freq_analysis)
                    keep_rpc_alive()  # to avoid broken pipe errors
                    insert_size_analysis(current_size_analysis)
                    keep_rpc_alive()
                    insert_file_analysis(current_file_analysis)
                    keep_rpc_alive()
                    insert_prot_analysis(current_prot_analysis)

                    # current_outputs.clear()
                    current_freq_analysis.reset()
                    current_size_analysis.reset()
                    current_file_analysis.reset()
                    current_prot_analysis.reset()
        except JSONRPCException as e:
            logging.critical('JSONRPCException: ' + repr(e))
            log_error(e, 'JSONRPCException')
            main()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            logging.critical('Exception: ' + repr(e))
            log_error(e, 'Exception')
            main()


def keep_rpc_alive():  # waiting too long between requests will break the connection to the Bitcoin RPC
    rpc.getblockcount()


def log_error(e, e_type):
    with open('log/errors.log', 'a') as error_file:
        error_file.write(str(datetime.datetime.now()) + ' - ' + e_type + ' - ' + repr(e) + '\n')


# Find the first block after the specified timestamp using binary search
def binary_search_for_next_block(last_active_day):
    left = 0
    right = rpc.getblockcount()

    while (right - left) > 2:
        middle = (left + right) // 2
        if last_active_day < rpc.getblock(rpc.getblockhash(middle))['time']:
            right = middle
        else:
            left = middle

    return rpc.getblockhash(right)


def main():
    atexit.register(exit_handler)

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


def exit_handler():
    if db is not None:
        db.close()


if __name__ == '__main__':
    main()
