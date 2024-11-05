import multiprocessing as mp
import pandas as pd
import time
import requests
import difflib

class Symbol_finder:
    '''
    This class contains methods to retrieve cryptocurrency exchange information and generate TradingView URLs based on specified base and quote currencies.

    Methods:
        - extract_crypto_exchange(input_file, output_parquet): Extracts exchange information from livecoinwatch.com using trading pairs provided in a CSV file.
        - extract_tv_url(tv_symbols_file, BASE, QUOTE): Returns a list of dictionaries with exchange data and TradingView URLs based on trading pair data from tradingview.com.
        - get_market_data_with_proxy(coin, quote, offset, proxy): Retrieves market data from LiveCoinWatch with optional proxy and retry capabilities.
        - fetch_data(params): Helper method for parallel processing of market data retrieval.
        - parallel_process(data_list, proxy): Utilizes multiprocessing to speed up data extraction from livecoinwatch.com for all given trading pairs.
        - create_url(trading_pair, exchange): Builds a TradingView URL for the specified trading pair and exchange.
        - generate_trading_urls(base_currency, quote_currency, df_filtered): Constructs TradingView URLs for matched base and quote currencies in the provided DataFrame.
        - determine_exchange(df_cleaned, BASE, QUOTE): Determines the exchange for a given base and quote currency. If no quote is specified, it defaults to USD; if an exact match for the quote isn’t found, it returns the most similar available quote.

    Example:
        # Initialize class
        symbol_finder = Symbol_finder()
        
        # Extract TradingView URLs
        extracted_tv_url = symbol_finder.extract_tv_url(tv_symbols_file='path/to/tv_symbols.csv', BASE='TORN')
        print(extracted_tv_url)
        
        # Extract exchange data and save to a parquet file
        symbol_finder.extract_crypto_exchange(input_file='path/to/unique_symbols.csv')
    '''

    def extract_cryto_exchange(self, input_file, output_parquet='output.parquet'):
        
        data_list = pd.read_csv(input_file)
        result = self.parallel_process(data_list)
        result.to_parquet(output_parquet, index=False)
    
    def extract_tv_url(self,tv_symbols_file,BASE='BTC', QUOTE=None):
        
        df = pd.read_csv(tv_symbols_file)
        df_cleaned = df.dropna(subset=['currency_code'])
        df_cleaned['Base0'] = df_cleaned['symbol'].apply(lambda x: x.rstrip('.P') if x.endswith('.P') else x)
        df_cleaned['Base'] = df_cleaned.apply(lambda x: x['Base0'].replace(x['currency_code'], ''), axis=1)
        return self.determine_exchange(df_cleaned,BASE,QUOTE)

    def get_market_data_with_proxy(self,coin, quote, offset, proxy, retries=3):
        retry_count = 0
        while retry_count < retries:
            proxies = {
                'http': proxy,  # Proxy for HTTP requests
                'https': proxy  # Proxy for HTTPS requests
            }
            
            try:
                response = requests.get(
                    'https://http-api.livecoinwatch.com/markets',
                    params={'currency': quote, 'limit': 30, 'offset': offset, 'sort': 'depth', 'order': 'descending', 'coin': coin},
                    proxies=proxies,  # Pass proxies here
                    timeout=10  # Timeout to avoid long waiting periods
                )
                
                # Check if the response status code is OK (200)
                if response.status_code == 200:
                    return response.json().get('data', [])
                elif response.status_code == 503:
                    retry_count += 1
                    wait_time = 2 ** retry_count  # Exponential backoff (2, 4, 8 seconds)
                    print(f"503 Error: Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Request failed with status code {response.status_code}")
                    return None
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                retry_count += 1
                time.sleep(2 ** retry_count)

        print(f"Failed after {retries} retries")
        return None

    def fetch_data(self,params):
        coin, quote, n, proxy = params
        result = pd.DataFrame()
        coin1 = '_' * n + coin
        offset = 0
        while True:
            data = self.get_market_data_with_proxy(coin1, quote, offset, proxy)
            if not data:
                break
            df = pd.DataFrame(data)
            for index, row in df.iterrows():
                if row['base'] == coin and row['quote'] == quote:
                    row_df = pd.DataFrame([row], columns=df.columns)
                    result = pd.concat([result, row_df], ignore_index=True)
            offset += 30
        return result

    def parallel_process(self,data_list, proxy='http://speyinarxb:81ZxuK_Rgj4Fc2tidi@gate.smartproxy.com:7000'):
        tasks = []
        for index0, row0 in data_list.iterrows():
            coin, quote = row0['0'].split('/')
            for n in range(20):
                tasks.append((coin, quote, n, proxy))
        
        # Create a multiprocessing pool and run the tasks in parallel
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(self.fetch_data, tasks)

        # Combine the results from all processes into a single DataFrame
        result = pd.concat(results, ignore_index=True)
        return result


    def create_url(self,trading_pair, exchange):
            s = f"symbol={exchange.upper()}:{trading_pair.upper()}"
            url = f"https://s.tradingview.com/widgetembed/?frameElementId=tradingview_abc&{s}&interval=60&theme=dark"
            return url
    
    def generate_trading_urls(self,base_currency, quote_currency, df_filtered):
        
        
        df_filtered2 = df_filtered[df_filtered['currency_code'] == quote_currency]
        
        # Build the result list
        result = []
        for idx, row in df_filtered2.iterrows():
            result.append({
                'base_currency': base_currency,
                'quote_currency': quote_currency,
                'exchange': row['exchange'],
                'url': self.create_url(row['symbol'], row['source_id'])
            })
        
        return result


    def determine_exchange(self,df_cleaned,BASE='BTC',QUOTE=None):

        BASE = BASE.upper()
        if BASE not in df_cleaned['Base'].values:
            return 'There is no matches for the base currency!'
        df_filtered = df_cleaned[df_cleaned['Base']==BASE]
        
        if QUOTE is None:
            return self.determine_exchange(df_cleaned,BASE, QUOTE='USD')
        
        if QUOTE in df_filtered['currency_code'].values:
            return self.generate_trading_urls(BASE,QUOTE, df_filtered)
        else:
            most_similar_QUOTE = max(df_filtered['currency_code'].values, key=lambda x: difflib.SequenceMatcher(None, QUOTE, x).ratio())
            return self.determine_exchange(df_cleaned,BASE,most_similar_QUOTE)


if __name__=='__main__':
    symbol_finder = Symbol_finder()
    extracted_tv_url = symbol_finder.extract_tv_url(tv_symbols_file='extract_url/tv_symbols.csv',BASE='TORN')
    print(extracted_tv_url)
    symbol_finder.extract_cryto_exchange(input_file='unique_symbols.csv')