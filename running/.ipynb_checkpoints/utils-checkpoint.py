from datetime import time

def day_part(t):
    if t < time(12, 0):
        return 'Morning'
    elif t > time(18, 0):
        return 'Evening'
    else:
        return 'Afternoon'

def get_period(df):
    tmp_run = df.copy()
    tmp_run = tmp_run[tmp_run['time'] == tmp_run['time'].min()]
    tmp_run['period'] = tmp_run['timestamp_brazil'].apply(day_part)
    
    return tmp_run[['id', 'period']]

def get_total_km(df):
    tmp_run = df.copy()
    tmp_run = tmp_run[tmp_run['km'] == tmp_run['km'].max()]
    tmp_run.rename(columns={'km':'total_km'}, inplace=True)
    
    return tmp_run[['id', 'total_km']].drop_duplicates()