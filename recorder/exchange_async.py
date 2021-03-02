import ccxt.async_support as ccxt # async ccxt
import aiofiles as aiof # async write in file
import numpy as np, time, datetime, os
import traceback



class Exchange():
    def __init__(self, name_exchange, config):
        # reprocess entry
        self.config = config
        self.limit_lob = min(config['LOB_LIMIT'],20)
        self.limit_trades = min(config['TRADES_LIMIT'],500)
        # create ccxt exchange
        self.exchange = getattr(ccxt, name_exchange) () 
        self.name_exchange = name_exchange

        self.columns = ['timestamp_ticker', 'bid', 'ask', 'vwap', 'bidVolume', 'askVolume', 'base24hVolume', 'quote24hVolume', 'trades_std_price', 'trades_std_buyPrice', 'trades_std_sellPrice', 'trades_std_volume', 'trades_std_buyVolume', 'trades_std_sellVolume', 'trades_volume', 'trades_buyVolume', 'trades_sellVolume', 'trades_total_time', 'trades_nbs_trades', 'bidVolume_0', 'bidPrice_0', 'bidVolume_1', 'bidPrice_1', 'bidVolume_2', 'bidPrice_2', 'bidVolume_3', 'bidPrice_3', 'bidVolume_4', 'bidPrice_4', 'bidVolume_5', 'bidPrice_5', 'bidVolume_6', 'bidPrice_6', 'bidVolume_7', 'bidPrice_7', 'bidVolume_8', 'bidPrice_8', 'bidVolume_9', 'bidPrice_9', 'bidVolume_10', 'bidPrice_10', 'bidVolume_11', 'bidPrice_11', 'bidVolume_12', 'bidPrice_12', 'bidVolume_13', 'bidPrice_13', 'bidVolume_14', 'bidPrice_14', 'bidVolume_15', 'bidPrice_15', 'bidVolume_16', 'bidPrice_16', 'bidVolume_17', 'bidPrice_17', 'bidVolume_18', 'bidPrice_18', 'bidVolume_19', 'bidPrice_19', 'askVolume_0', 'askPrice_0', 'askVolume_1', 'askPrice_1', 'askVolume_2', 'askPrice_2', 'askVolume_3', 'askPrice_3', 'askVolume_4', 'askPrice_4', 'askVolume_5', 'askPrice_5', 'askVolume_6', 'askPrice_6', 'askVolume_7', 'askPrice_7', 'askVolume_8', 'askPrice_8', 'askVolume_9', 'askPrice_9', 'askVolume_10', 'askPrice_10', 'askVolume_11', 'askPrice_11', 'askVolume_12', 'askPrice_12', 'askVolume_13', 'askPrice_13', 'askVolume_14', 'askPrice_14', 'askVolume_15', 'askPrice_15', 'askVolume_16', 'askPrice_16', 'askVolume_17', 'askPrice_17', 'askVolume_18', 'askPrice_18', 'askVolume_19', 'askPrice_19', 'timestamp_lob']


        if 'data' not in os.listdir():
            os.mkdir('data')



    # main functions

    async def record_data(self, symbol):
        try:
            data_to_record = await self.get_data(symbol)
            await self.upload_data(symbol, data_to_record)
            time.sleep(self.config['SLEEP'])
        except:
            print('\n[{}] - ERROR: cannot reccord data of {} because: '.format(datetime.datetime.now(), symbol))
            print(traceback.format_exc())
            print('')

    async def get_data(self, symbol):
        lob = await self.exchange.fetch_order_book(symbol, limit=self.limit_lob)
        trades = await self.exchange.fetchTrades(symbol, limit=self.limit_trades)
        ticker = await self.exchange.fetchTicker(symbol)
        return await self.process_data(lob, trades, ticker)

    async def upload_data(self, symbol, data_to_record):
        date_ticker = datetime.datetime.fromtimestamp(data_to_record['timestamp_ticker']/1000)
        date_str = date_ticker.strftime('%Y%m%d')
        filename = '{}_{}_{}.csv'.format(self.name_exchange,symbol.replace('/',''),date_str)
        async with aiof.open('data/'+filename, mode='a') as f_out:
            #await f_out.write(','.join(self.columns)+'\n')
            line = []
            for c in self.columns:
                line.append(str(data_to_record[c]) if c in data_to_record else '')
            line = ','.join(line) + '\n'        
            await f_out.write(line)


    # sub familly data

    async def process_ticker_data(self, ticker):
        dict_ticker = {}
        dict_ticker['timestamp_ticker'] = ticker['timestamp']
        dict_ticker['bid'] = ticker['bid']
        dict_ticker['ask'] = ticker['ask']
        dict_ticker['vwap'] = ticker['vwap']
        dict_ticker['bidVolume'] = ticker['bidVolume']
        dict_ticker['askVolume'] = ticker['askVolume']
        dict_ticker['base24hVolume'] = ticker['baseVolume']
        dict_ticker['quote24hVolume'] = ticker['quoteVolume']
        return dict_ticker

    async def process_trades_data(self, trades):
        dict_trades = {}
        # extract arrays
        list_price = np.array([trade['price'] for trade in trades])
        list_buyPrice = np.array([trade['price'] for trade in trades if trade['side']=='buy'])
        list_sellPrice = np.array([trade['price'] for trade in trades if trade['side']=='sell'])
        list_volume = np.array([trade['amount'] for trade in trades])
        list_buyVolume = np.array([trade['amount'] for trade in trades if trade['side']=='buy'])
        list_sellVolume = np.array([trade['amount'] for trade in trades if trade['side']=='sell'])
        # std price
        dict_trades['trades_std_price'] = list_price.std()
        dict_trades['trades_std_buyPrice'] = list_buyPrice.std()
        dict_trades['trades_std_sellPrice'] = list_sellPrice.std()
        # std volume
        dict_trades['trades_std_volume'] =list_volume.std()
        dict_trades['trades_std_buyVolume'] = list_buyVolume.std()
        dict_trades['trades_std_sellVolume'] = list_sellVolume.std()
        # sum volume
        dict_trades['trades_volume'] = list_volume.sum()
        dict_trades['trades_buyVolume'] = list_buyVolume.sum()
        dict_trades['trades_sellVolume'] = list_sellVolume.sum()
        # timing
        dict_trades['trades_total_time'] =  (trades[-1]['timestamp'] - trades[0]['timestamp'])
        dict_trades['trades_nbs_trades'] = len(trades)
        return dict_trades

    async def process_lob_data(self, lob):
        dict_lob = {}
        i = 0
        for price, volume in lob['bids']:
            dict_lob['bidVolume_{}'.format(i)] = volume
            dict_lob['bidPrice_{}'.format(i)] = price
            i += 1
        i = 0
        for price, volume in lob['asks']:
            dict_lob['askVolume_{}'.format(i)] = volume
            dict_lob['askPrice_{}'.format(i)] = price
            i += 1
        dict_lob['timestamp_lob'] = lob['timestamp']
        return dict_lob

    async def process_data(self, lob, trades, ticker):
        return {**await self.process_ticker_data(ticker), **await self.process_trades_data(trades), **await self.process_lob_data(lob)}


    async def debug(self, symbol):
        dict_ticker = await self.get_ticker_data(symbol)
        dict_trades = await self.get_trades_data(symbol)
        dict_lob = await self.get_lob_data(symbol)
        data_to_reccord = {**dict_ticker, **dict_trades, **dict_lob}
        for key, item in data_to_reccord.items():
            print('{}: {}'.format(key,item))
        print('\n')
