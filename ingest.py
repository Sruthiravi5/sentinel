import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from fredapi import Fred

load_dotenv()
fred = Fred(api_key=os.environ['FRED_API_KEY'])
run_ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')

series = {
    'delinquency': 'DRCCLACBS',
    'consumer_credit': 'TOTALSL',
    'unemployment': 'UNRATE',
}

os.makedirs(f'raw/{run_ts}', exist_ok=True)
for name, code in series.items():
    df = fred.get_series(code).reset_index()
    df.columns = ['date', 'value']
    path = f'raw/{run_ts}/{name}.csv'
    df.to_csv(path, index=False)
    print(f"{name}: {len(df)} rows -> {path}")