import ccxt.async_support as ccxt # async ccxt
import aiofiles as aiof # async write in file
import numpy as np, time, datetime, os
import traceback



class Exchange():
    def __init__(self, name_exchange, config, use_debug):
        # reprocess entry
        self.config = config
        self.use_debug = use_debug
        self.limit_lob = min(config['LOB_LIMIT'],20)
        self.limit_trades = min(config['TRADES_LIMIT'],500)
        # create ccxt exchange
        self.exchange = getattr(ccxt, name_exchange) () 
        self.name_exchange = name_exchange

        self.columns = ['timestamp_ticker', 'bid', 'ask', 'vwap', 'bidVolume', 'askVolume', 'base24hVolume', 'quote24hVolume', 'trades_std_price', 'trades_std_buyPrice', 'trades_std_sellPrice', 
        'trades_std_volume', 'trades_std_buyVolume', 'trades_std_sellVolume', 'trades_volume', 'trades_buyVolume', 'trades_sellVolume', 'trades_total_time', 'trades_nbs_trades']
        for i in range(self.limit_lob):
            self.columns.append('bidVolume_{}'.format(i))
            self.columns.append('bidPrice_{}'.format(i))
        for i in range(self.limit_lob):
            self.columns.append('askVolume_{}'.format(i))
            self.columns.append('askPrice_{}'.format(i))
        self.columns.append('timestamp_lob')


        if 'data' not in os.listdir():
            os.mkdir('data')



    # main functions

    async def record_data(self, symbol):
        try:
            data_to_record = await self.get_data(symbol)
            await self.upload_data(symbol, data_to_record)
        except:
            if self.use_debug:
                print('\n[{}] - ERROR: cannot reccord data of {} because: '.format(datetime.datetime.now(), symbol))
                print(traceback.format_exc())
                print('')
            time.sleep(1)

    async def get_data(self, symbol):
        lob = await self.exchange.fetch_order_book(symbol)
        trades = await self.exchange.fetchTrades(symbol)
        ticker = await self.exchange.fetchTicker(symbol)
        return await self.process_data(lob, trades, ticker)

    async def upload_data(self, symbol, data_to_record):
        date_ticker = datetime.datetime.fromtimestamp(data_to_record['timestamp_ticker']/1000)
        date_str = date_ticker.strftime('%Y%m%d')
        filename = '{}__{}__{}.csv'.format(self.name_exchange,symbol.replace('/',''),date_str)
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
