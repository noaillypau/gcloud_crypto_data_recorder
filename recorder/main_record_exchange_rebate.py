import asyncio
import os, sys, time

print(os.listdir())


import argparse, datetime, os, json
from log import log
from exchange_async import Exchange
import importlib
import ccxt as ccxt_not_async


root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

print(os.listdir())

# arg parser
parser = argparse.ArgumentParser()
# exchange
parser.add_argument("exchange")
# batch
parser.add_argument("batch")

args = parser.parse_args()
name_exchange = args.exchange
batch_id = str(args.batch)

# check if we have a config for the given parser, if we do, check if we have the batch, if we do we instance list_symbol and launch reccord
if 'exchanges_rebate.json' in os.listdir('configs/'):
    with open('configs/exchanges_rebate.json','r') as f:
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
    log('couldnot find json file exchanges_rebate.json in configs folder')
    print(os.listdir('configs/'))
    quit()

if 'config.json' in os.listdir('configs/'):
    with open('configs/config.json','r') as f:
        config = json.load(f)
        f.close()
else:
    log('couldnot find json file config.json in configs folder')
    print(os.listdir('configs/'))
    quit()



async def main(name_exchange, config, list_symbol):
    log('\nRuning data recording successfuly for exchange {} and list symbol: {}\n'.format(name_exchange,str(list_symbol)))
    exchange = Exchange(name_exchange, config)    
    while True:
        input_coroutines = [exchange.record_data(symbol) for symbol in list_symbol]
        successes = await asyncio.gather(*input_coroutines, return_exceptions=True)
    exchange.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(main(name_exchange, config, list_symbol))