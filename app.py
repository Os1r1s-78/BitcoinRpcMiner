import time
import datetime
import config as cfg
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

# Set up connection to the Bitcoin RPC client
rpc_connection = AuthServiceProxy("http://%s:%s@%s" % (cfg.rpc['rpcuser'], cfg.rpc['rpcpassword'], cfg.rpc['endpoint']))

first_iteration = True
current_day = None
active_block_hash = ""

current_outputs = []
current_freq_analysis = []
current_size_analysis = []
current_prot_analysis = []

while True:
    print()

# start_time = time.time()
# best_block_hash = rpc_connection.getbestblockhash()
# print("--- %s seconds ---" % (time.time() - start_time))
