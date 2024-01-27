from flask import Flask, jsonify
import pandas as pd
from flask_cors import CORS
import requests
from nsepython import equity_history
import datetime

class NSEIndia:
    # All the market segments and indices
    pre_market_keys = ['NIFTY', 'BANKNIFTY', 'SME', 'FO', 'OTHERS', 'ALL']

    live_market_keys = ['NIFTY 50', 'NIFTY NEXT 50', 'NIFTY MIDCAP 50', 'NIFTY MIDCAP 100', 'NIFTY MIDCAP 150', 
                        'NIFTY SMALLCAP 50', 'NIFTY SMALLCAP 100', 'NIFTY SMALLCAP 250', 'NIFTY MIDSMALLCAP 400', 
                        'NIFTY 100', 'NIFTY 200', 'NIFTY500 MULTICAP 50:25:25', 'NIFTY LARGEMIDCAP 250', 'NIFTY AUTO', 
                        'NIFTY BANK', 'NIFTY ENERGY', 'NIFTY FINANCIAL SERVICES', 'NIFTY FINANCIAL SERVICES 25/50', 
                        'NIFTY FMCG', 'NIFTY IT', 'NIFTY MEDIA', 'NIFTY METAL', 'NIFTY PHARMA', 'NIFTY PSU BANK', 'NIFTY REALTY', 
                        'NIFTY PRIVATE BANK', 'NIFTY HEALTHCARE INDEX', 'NIFTY CONSUMER DURABLES', 'NIFTY OIL & GAS', 
                        'NIFTY COMMODITIES', 'NIFTY INDIA CONSUMPTION', 'NIFTY CPSE', 'NIFTY INFRASTRUCTURE', 'NIFTY MNC', 
                        'NIFTY GROWTH SECTORS 15', 'NIFTY PSE', 'NIFTY SERVICES SECTOR', 'NIFTY100 LIQUID 15', 'NIFTY MIDCAP LIQUID 15', 
                        'NIFTY DIVIDEND OPPORTUNITIES 50', 'NIFTY50 VALUE 20', 'NIFTY100 QUALITY 30', 'NIFTY50 EQUAL WEIGHT', 
                        'NIFTY100 EQUAL WEIGHT', 'NIFTY100 LOW VOLATILITY 30', 'NIFTY ALPHA 50', 'NIFTY200 QUALITY 30', 
                        'NIFTY ALPHA LOW-VOLATILITY 30', 'NIFTY200 MOMENTUM 30', 'Securities in F&O', 'Permitted to Trade']
    
    holiday_keys = ['clearing', 'trading']


    def __init__(self):
        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'}
        self.session = requests.Session()
        self.session.get('http://nseindia.com', headers=self.headers)

    # NSE Pre-market data API section
    def NsePreMarketData(self, key):
        try:
            data = self.session.get(f"https://www.nseindia.com/api/market-data-pre-open?key={key}", 
                    headers=self.headers).json()['data']
        except:
            pass
        new_data = []
        for i in data:
            new_data.append(i['metadata'])
        df = pd.DataFrame(new_data)
        return df 

    # NSE Live-market data API section
    def NseLiveMarketData(self, key, symbol_list):
        try:
            data = self.session.get(f"https://www.nseindia.com/api/equity-stockIndices?index={key.upper().replace(' ','%20').replace('&','%26')}",
                    headers=self.headers).json()['data'] 
                    # Use of "replace(' ','%20').replace('&','%26')" -> In live market space is replaced by %20, & is replaced by %26
       
            df = pd.DataFrame(data)
            df = df.drop(['meta'], axis=1)
            if symbol_list:
                return list(df['symbol'])
            else:
                return df
        except requests.exceptions.RequestException as e:
            print(f"RequestException: {str(e)}")
            # Re-establish the session
            self.session = requests.Session()
            self.session.get('http://nseindia.com', headers=self.headers)
            # Retry the request
            return self.NseLiveMarketData(key, symbol_list)
        except Exception as e:
            print(f"Exception: {str(e)}")
            return None

    # NSE market holiday API section
    def NseHoliday(self, key):
        try:
            data = self.session.get(f'https://www.nseindia.com/api/holiday-master?type={key}', headers = self.headers).json()
        except:
            pass
        df = pd.DataFrame(list(data.values())[0])
        return df

class NSEIndia2:
    def __init__(self):
        try:
            self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'}
            self.session = requests.Session()
            self.session.get('http://nseindia.com', headers=self.headers)
        except:
            pass

    # NSE Option-chain data API section
    def GetOptionChainData(self, symbol, indices=False):
        try:
            if not indices:
                url = 'https://www.nseindia.com/api/option-chain-equities?symbol=' + symbol
            else:
                url = 'https://www.nseindia.com/api/option-chain-indices?symbol=' + symbol
        except requests.exceptions.RequestException as e:
            print(f"RequestException: {str(e)}")
            # Re-establish the session
            self.session = requests.Session()
            self.session.get('http://nseindia.com', headers=self.headers)
            # Retry the request
            return self.GetOptionChainData(symbol, indices)
        except Exception as e:
            print(f"Exception: {str(e)}")
            return None
        data = self.session.get(url, headers=self.headers).json()["records"]["data"]
            
        df = []
        for i in data: 
            for keys, values in i.items():
                if keys == 'CE' or keys == 'PE':
                    info = values
                    info['instrumentType'] = keys
                    df.append(info)
        df1 = pd.DataFrame(df)
        return pd.DataFrame(df1)



app = Flask(__name__)
# CORS(app, resources={r"*": {"origins": "http://localhost:5173"}})
CORS(app)

nse = NSEIndia()
nse2 = NSEIndia2()

@app.route('/nse/pre_market_data/<key>', methods=['GET'])
def get_pre_market_data(key):
    try:
        data = nse.NsePreMarketData(key)
        return jsonify(data.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/nse/live_market_data/<key>/<symbol_list>', methods=['GET'])
def get_live_market_data(key, symbol_list):
    try:
        data = nse.NseLiveMarketData(key, symbol_list.lower() == 'true')
        if symbol_list.lower() == 'true':
            return jsonify({'symbols': data})
        else:
            return jsonify(data.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/nse/holiday_data/<key>', methods=['GET'])
def get_holiday_data(key):
    try:
        data = nse.NseHoliday(key)
        return jsonify(data.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/nse/option_chain_data/<symbol>/<indices>', methods=['GET'])
def get_option_chain_data(symbol, indices):
    try:
        data = nse2.GetOptionChainData(symbol, indices.lower() == 'true')
        return jsonify(data.to_dict(orient='records'))
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/nse/live_market_data/nifty_50', methods=['GET'])
def get_nifty_50_live_market_data():
    try:
        data = nse.NseLiveMarketData('NIFTY 50', symbol_list=False)
        if data is not None:
            return jsonify(data.to_dict(orient='records'))
        else:
            return jsonify({'error': 'Data is None'})
    except Exception as e:
        print(f"Exception: {str(e)}")
        return jsonify({'error': str(e)})


@app.route('/get_equity_data/<symbol>/<series>/<date>', methods=['GET'])
def get_equity_data(symbol, series, date):

    def get_equity_data_as_dict(symbol, series, date,iterations=0):
        start_date = (date - datetime.timedelta(days=1)).strftime("%d-%m-%Y")

        # Assuming equity_history is a function that fetches equity data
        # Replace it with the actual implementation
        end_date_str = date.strftime("%d-%m-%Y")
        df = equity_history(str(symbol).upper(), str(series).upper(), start_date, end_date_str)

        # Convert DataFrame to JSON
        json_data = df.to_dict(orient='records')

        # check if json_data is an empty list
        if not json_data and iterations < 5:
            print('No data available')
            json_data = get_equity_data_as_dict(symbol, series, date - datetime.timedelta(days=1), iterations + 1)

        return json_data
    
    given_date = datetime.datetime.strptime(date, "%d-%m-%Y")
    json_data = get_equity_data_as_dict(symbol, series, given_date)

    return jsonify({"previous_day": json_data})

if __name__ == '__main__':
    app.run(debug=True)
