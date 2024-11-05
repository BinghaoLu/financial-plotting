import json
import finplot as fplt
import pandas as pd
from datetime import datetime, timedelta
from tvDatafeed import TvDatafeed, Interval

class Plot:
    def __init__(self, symbol, exchange, start_date=None, end_date=None, timeframe="hour"):
        self.timeframe = timeframe
        self.symbol = symbol
        self.exchange = exchange

        # Convert string dates to datetime
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Set default dates if None
        if start_date is None:
            start_date = datetime.now() - timedelta(days=7)
        if end_date is None:
            end_date = datetime.now()
        
        self.start_date_str = start_date.strftime('%Y-%m-%d')
        self.end_date_str = end_date.strftime('%Y-%m-%d')
        
        current_date = datetime.now()
        total_seconds_from_start = (current_date - start_date).total_seconds()
        is_current_end_date = (current_date - end_date).total_seconds() < 1
        
        tv = TvDatafeed()
        
        # Load data based on timeframe
        data_start, data_end = self._fetch_data(tv, total_seconds_from_start, end_date, is_current_end_date)
        data_start.to_csv('start1.csv')
        # Merge data
        if data_end is None:
            self.data = data_start
        else:
            data_start = data_start[~data_start.index.duplicated()]
            data_end = data_end[~data_end.index.duplicated()]

            self.data = data_start[~data_start.isin(data_end).all(axis=1)]
        self.data.reset_index(inplace=True)

    
    def _fetch_data(self, tv, total_seconds_from_start, end_date, is_current_end_date):
        if self.timeframe == "day":
            bars_start = int(total_seconds_from_start / (24 * 3600))
            data_start = tv.get_hist(symbol=self.symbol, exchange=self.exchange, interval=Interval.in_daily, n_bars=bars_start)
            data_end = None if is_current_end_date else self._get_end_data(tv, end_date, 24 * 3600, Interval.in_daily)
        elif self.timeframe == "hour":
            bars_start = int(total_seconds_from_start / 3600)
            data_start = tv.get_hist(symbol=self.symbol, exchange=self.exchange, interval=Interval.in_1_hour, n_bars=bars_start)
            data_end = None if is_current_end_date else self._get_end_data(tv, end_date, 3600, Interval.in_1_hour)
        elif self.timeframe == "minute":
            bars_start = int(total_seconds_from_start / 60)
            data_start = tv.get_hist(symbol=self.symbol, exchange=self.exchange, interval=Interval.in_1_minute, n_bars=bars_start)
            data_end = None if is_current_end_date else self._get_end_data(tv, end_date, 60, Interval.in_1_minute)
        else:
            raise ValueError("Invalid timeframe. Choose from 'day', 'hour', or 'minute'.")
        
        return data_start, data_end
    
    def _get_end_data(self, tv, end_date, seconds_per_unit, interval):
        total_seconds_from_end = (datetime.now() - end_date).total_seconds()
        bars_end = int(total_seconds_from_end / seconds_per_unit)
        return tv.get_hist(symbol=self.symbol, exchange=self.exchange, interval=interval, n_bars=bars_end)
    
    def save(self, with_article=False):
        fplt.screenshot(open(f'{self.symbol}_{self.exchange}_{self.start_date_str}_to_{self.end_date_str}_{self.timeframe}_with_article_{with_article}.png', 'wb'), fmt='png')
        fplt.close()
    
    def round_time(self, dt, timeframe='day'):
        adjustments = {
            'day': {'hours': dt.hour, 'minutes': dt.minute, 'seconds': dt.second, 'microseconds': dt.microsecond},
            'hour': {'minutes': dt.minute, 'seconds': dt.second, 'microseconds': dt.microsecond},
            'minute': {'seconds': dt.second, 'microseconds': dt.microsecond}
        }
        if timeframe not in adjustments:
            raise ValueError("Invalid timeframe. Choose from 'day', 'hour', or 'minute'.")
        return dt - timedelta(**adjustments[timeframe])
    
    def make_chart_without_news(self):
        ax, ax2 = fplt.create_plot('Finance Plot', rows=2)
        fplt.candlestick_ochl(self.data[['datetime', 'open', 'close', 'high', 'low']], ax=ax)
        fplt.volume_ocv(self.data[['open', 'close', 'volume']], ax=ax2)
        fplt.timer_callback(lambda: self.save(False), 0.5, single_shot=True)
        fplt.show()
    
    def make_chart_with_news(self, news_file='news_trader.signals_v2.json'):
        articles_df = self._load_news_data(news_file)
        if articles_df.empty:
            print("No news data available.")
            return
        articles_df['rounded_time'] = articles_df['proccessing_start_time'].apply(lambda x: self.round_time(x, timeframe=self.timeframe))
        articles_df['rounded_time'] = articles_df['rounded_time'].dt.tz_localize(None)

        articles_df_grouped = articles_df.groupby('rounded_time').size().reset_index(name='count')

        self.data['rounded_time'] = self.data['datetime'].apply(lambda x: self.round_time(x, timeframe=self.timeframe))
        merged_df = pd.merge(self.data, articles_df_grouped, on='rounded_time', how='outer')
    
        ax, ax2 = fplt.create_plot('Finance Plot', rows=2)
        fplt.candlestick_ochl(self.data[['datetime', 'open', 'close', 'high', 'low']], ax=ax)
        fplt.volume_ocv(self.data[['open', 'close', 'volume']], ax=ax2)
        
        for _, row in merged_df.iterrows():
            if not pd.isna(row['count']):
                fplt.add_line(
                    (row['rounded_time'], row['low'] * 0.5), 
                    (row['rounded_time'], row['high'] * 1.5), 
                    color='#FF0000', 
                    style='--', 
                    width=1.5,
                    ax=ax,
                )        
                fplt.plot(
                    [row['rounded_time']], [row['high']], 
                    ax=ax, 
                    color='#00FF00', 
                    style='^', 
                    legend=f'there are {row["count"]} news!'
                )
                
        fplt.timer_callback(lambda: self.save(True), 0.5, single_shot=True)
        fplt.show()

    def _load_news_data(self, news_file):
        try:
            with open(news_file, 'r') as file:
                articles = json.load(file)
            articles_df = pd.DataFrame(articles)
            articles_df['proccessing_start_time'] = articles_df['proccessing_start_time'].apply(lambda x: x['$date'])
            articles_df['proccessing_start_time'] = pd.to_datetime(articles_df['proccessing_start_time'])
            return articles_df
        except (FileNotFoundError, KeyError, ValueError) as e:
            print(f"Error loading news data: {e}")
            return pd.DataFrame()

if __name__ == '__main__':
    plot = Plot(symbol='TORNUSDT', exchange='BINANCE',start_date='2024-10-01',end_date='2024-11-01')
    # plot = Plot(symbol='SOLUSD', exchange='COINBASE')

    plot.make_chart_without_news()
    # plot.make_chart_with_news()
