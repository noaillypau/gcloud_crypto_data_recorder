import asyncio
import os, sys, time
import argparse, datetime, os, json
from log import log
from exchange_async import Exchange
import importlib
import ccxt as ccxt_not_async


root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

# arg parser
parser = argparse.ArgumentParser()
# exchange
parser.add_argument("exchange", help='name of the exchange')
# batch
parser.add_argument("batch", help='id of the batch')
# debug
parser.add_argument('-d','--debug', help='debuging mod, show errors, and rate of record', action="store_true")
# rebate config
parser.add_argument('-r','--rebate', help='Use rebate configs or not', action="store_true")
# show 
parser.add_argument('-ls','--list_symbol', help='Show list of exchanges', action="store_true")

args = parser.parse_args()
name_exchange = args.exchange
batch_id = str(args.batch)



if args.debug:
    print('Entering debug mod')
    USE_DEBUG = True
else:
    print('Entering Live mod')
    USE_DEBUG = False

if args.rebate:
    print('Using rebate configs')
    config_filename = 'exchanges_rebate.json'
else:
    print('Using normal config')
    config_filename = 'exchanges.json'



# check if we have a config for the given parser, if we do, check if we have the batch, if we do we instance list_symbol and launch reccord
if config_filename in os.listdir('configs/'):
    with open('configs/'+config_filename,'r') as f:
        dic_exchange = json.load(f)
        f.close()
    if name_exchange in dic_exchange.keys():
        if batch_id in dic_exchange[name_exchange]['sorted_symbols_batch']:
            list_symbol = dic_exchange[name_exchange]['sorted_symbols_batch'][batch_id]
        else:
            log('batch {} not found, available list is {}'.format(batch_id,str(list(dic_exchange[name_exchange]['sorted_symbols_batch'].keys()))))
            quit()
    else:
        log("{} is not in dic config please select one among {}".fomat(name_exchange,str(list(dic_exchange.keys()))))
        quit()
else:
    log('couldnot find json file {} in configs folder'.format(config_filename))
    quit()

if 'config.json' in os.listdir('configs/'):
    with open('configs/config.json','r') as f:
        config = json.load(f)
        f.close()
else:
    log('couldnot find json file config.json in configs folder')
    quit()

if args.list_symbol:
    print('Showing list of exchanges')
    for key in dic_exchange.keys():
        print('  ', key)
    quit()



async def main(name_exchange, config, list_symbol, USE_DEBUG):
    log('\nRuning data recording successfuly for exchange {} and list symbol: {}\n'.format(name_exchange,str(list_symbol)))
    exchange = Exchange(name_exchange, config, USE_DEBUG)    
    t = time.time()
    while True:
        input_coroutines = [exchange.record_data(symbol) for symbol in list_symbol]
        successes = await asyncio.gather(*input_coroutines, return_exceptions=True)
        if USE_DEBUG:
            print('+{}sec'.format(round(time.time()-t,1)))
        time.sleep((exchange.exchange.rateLimit * (len(list_symbol)+1))/1000)
        t = time.time()
    exchange.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(main(name_exchange, config, list_symbol, USE_DEBUG))