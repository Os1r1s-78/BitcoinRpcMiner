import time
import datetime
import logging
import atexit
import config as cfg
from db_access import *
from dbmodels import *
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
    current_freq_analysis = FrequencyAnalysis()
    current_size_analysis = SizeAnalysis()
    current_prot_analysis = ProtocolAnalysis()

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
                    delete_data_after_date(last_active_day, 'ProtocolAnalysis')
                    delete_tx_outputs_after_date(last_active_day)
                else:
                    logging.info('Couldn\'t find any analysis data, deleting all rows from all tables and starting over')
                    delete_all_data()
                first_iteration = False
            else:
                if active_block_hash == "" or None:
                    # Choose the next available block
                    latest_tx_output = get_latest_tx_output()
                    if latest_tx_output is not None:  # Choose the next block after the last tx entry
                        active_block_hash = rpc.getblock(latest_tx_output[3])['nextblockhash']
                        current_block_time = rpc.getblock(active_block_hash)['time']
                        current_day = datetime.datetime.utcfromtimestamp(current_block_time).date()
                        logging.info('Set the next block to first block after where the program last stopped: ' + active_block_hash)
                    else:  # Set the next block to the genesis block
                        active_block_hash = rpc.getblockhash(0)
                        block_timestamp = rpc.getblock(active_block_hash)['time']
                        current_day = datetime.datetime.utcfromtimestamp(block_timestamp).date()
                        logging.info('Set the next block to the genesis block, as there is no previous transaction outputs available')

                block = rpc.getblock(active_block_hash)
                for tx_hash in block['tx']:
                    # The genesis' blocks transaction cannot be retrieved with the Bitcoin RPC, simply skip it
                    if tx_hash == '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b':
                        logging.info('Skipping the first transaction of the Bitcoin blockchain, as that is not retrievable with the RPC')
                        current_freq_analysis.p2pk += 1
                        continue

                    # Process transaction
                    tx = rpc.getrawtransaction(tx_hash, 1)
                    logging.info('Processing outputs of transaction ' + tx['txid'] + ' in block ' + str(block['height']))
                    for tx_out in tx['vout']:
                        script_type = tx_out['scriptPubKey']['type']
                        script = tx_out['scriptPubKey']['asm'].split(' ')

                        if script_type == 'nulldata' or script[0] == 'OP_RETURN':
                            current_outputs.append(
                                TransactionOutput(tx['txid'], tx['blocktime'], tx['blockhash'], tx_out['value'],
                                                  tx_out['scriptPubKey']['type'], tx_out['scriptPubKey']['asm'],
                                                  tx_out['scriptPubKey']['hex'], None, None))  # todo: implement protocol and file header

                            current_freq_analysis.nulldata += 1
                            current_size_analysis.avgsize += len(tx_out['scriptPubKey']['hex']) / 2
                            current_size_analysis.outputs += 1
                        else:
                            if script_type == 'pubkey' or (len(script) == 2 and script[1] == 'OP_CHECKSIG'):
                                current_freq_analysis.p2pk += 1
                            elif script_type == 'pubkeyhash' or (len(script) == 5 and script[0] == 'OP_DUP' and script[1] == 'OP_HASH160' and script[3] == 'OP_EQUALVERIFY' and script[4] == 'OP_CHECKSIG'):
                                current_freq_analysis.p2pkh += 1
                            elif script_type == 'multisig' or (script[len(script) - 1] == 'OP_CHECKMULTISIG'):
                                current_freq_analysis.p2ms += 1
                            elif script_type == 'scripthash' or (len(script) == 3 and script[0] == 'OP_HASH160' and script[2] == 'OP_EQUAL'):
                                current_freq_analysis.p2sh += 1
                            else:
                                current_freq_analysis.unknowntype += 1

                # Check whether the next block is still in the same day
                next_block = rpc.getblock(block['nextblockhash'])
                if next_block['time'] < time.mktime((current_day + datetime.timedelta(days=1)).timetuple()):
                    active_block_hash = next_block['hash']
                    logging.info('The next block is still in the same day. Continuing with block ' + str(next_block['height']) + ' with ' + str(len(next_block['tx'])) + ' transactions')
                else:
                    logging.info('Writing outputs and analysis to the database of day ' + current_day.strftime('%Y-%m-%d'))

                    # Adjust the data day variables
                    current_freq_analysis.dataday = current_day
                    current_size_analysis.dataday = current_day
                    current_prot_analysis.dataday = current_day

                    # Next block is the next day
                    active_block_hash = next_block['hash']
                    current_day = current_day + datetime.timedelta(days=1)

                    # Commit to database
                    insert_tx_outputs(current_outputs)
                    insert_freq_analysis(current_freq_analysis)
                    insert_size_analysis(current_size_analysis)
                    insert_prot_analysis(current_prot_analysis)

                    current_outputs.clear()
                    current_freq_analysis = FrequencyAnalysis()
                    current_size_analysis = SizeAnalysis()
                    current_prot_analysis = ProtocolAnalysis()
        except JSONRPCException as e:
            print(repr(e))
            logging.critical(repr(e))
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            print(repr(e))
            logging.critical(repr(e))


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
