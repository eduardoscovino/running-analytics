import json
import pandas as pd
import numpy as np
import os
import pytz
from running.utils import day_part, get_period, get_total_km

class Run:
    
    def get_data_raw(self, file_name):
        
        root_dir = os.path.dirname(os.path.dirname(__file__))
        csv_path = os.path.join(root_dir, "data", "activities")
        
        with open(os.path.join(csv_path, file_name), 'r') as file:
            data = json.load(file)
        
        return data

    def get_speed(self, data_dict):
        for i in data_dict['metrics']:
            if i['type'] == 'speed':
                speed = pd.DataFrame(i['values'])
                speed['start_epoch_ms'] = pd.to_datetime(speed['start_epoch_ms'], unit='ms').dt.to_period('T')
                speed['start_epoch_ms'] = speed['start_epoch_ms'].dt.to_timestamp()
                speed = pd.DataFrame(speed.groupby('start_epoch_ms')['value'].mean()).reset_index()
                speed.rename(columns={'value':'speed'}, inplace=True)

        return speed[['start_epoch_ms', 'speed']]

    def get_pace(self, data_dict):
        for i in data_dict['metrics']:
            if i['type'] == 'pace':
                pace = pd.DataFrame(i['values'])
                pace['start_epoch_ms'] = pd.to_datetime(pace['start_epoch_ms'], unit='ms').dt.to_period('T')
                pace['start_epoch_ms'] = pace['start_epoch_ms'].dt.to_timestamp()
                pace = pd.DataFrame(pace.groupby('start_epoch_ms')['value'].mean()).reset_index()
                pace.rename(columns={'value':'pace'}, inplace=True)
        
        return pace[['start_epoch_ms', 'pace']]
    
    def get_distance(self, data_dict):
        for i in data_dict['metrics']:
            if i['type'] == 'distance':
                distance = pd.DataFrame(i['values'])
                distance['start_epoch_ms'] = pd.to_datetime(distance['start_epoch_ms'], unit='ms').dt.to_period('T')
                distance = pd.DataFrame(distance.groupby(['start_epoch_ms'])['value'].sum()).reset_index()
                distance.rename(columns={'value':'distance'}, inplace=True)
                distance['start_epoch_ms'] = distance['start_epoch_ms'].dt.to_timestamp()
                distance['cum_distance'] = distance['distance'].cumsum()
        
        return distance[['start_epoch_ms', 'distance', 'cum_distance']]
    
    def get_run(self, data_dict):
        df_run = self.get_speed(data_dict).merge(
            self.get_pace(data_dict), on='start_epoch_ms'
        ).merge(
            self.get_distance(data_dict), on='start_epoch_ms'
        )
        df_run['km'] = np.floor(df_run['cum_distance'])
        df_run['time'] = df_run['start_epoch_ms'] - df_run['start_epoch_ms'].min()
        df_run['time'] = df_run['time'].apply(lambda x: f"{x.components.minutes:02}:{x.components.seconds:02}")
        df_run['date'] = pd.to_datetime(df_run['start_epoch_ms'].dt.date)

        br_timezone = pytz.timezone('America/Sao_Paulo')
        df_run['timestamp_utc'] = df_run['start_epoch_ms'].dt.tz_localize('UTC')
        df_run['timestamp_brazil'] = df_run['timestamp_utc'].dt.tz_convert(br_timezone).dt.tz_localize(None).dt.time
        df_run['id'] = data_dict['id']

        tmp_run = get_period(df_run)
        df_run = df_run.merge(tmp_run, on='id')

        tmp_run = get_total_km(df_run)
        df_run = df_run.merge(tmp_run, on='id')

        df_run.drop(columns=['start_epoch_ms', 'timestamp_utc', 'timestamp_brazil'], inplace=True)

        df_run['note'] = data_dict.get('tags', {}).get('note', None)
        
        return df_run[[
            'id', 'date', 'time', 'speed', 'pace', 'distance', 'cum_distance', 'km', 'total_km', 'note', 'period'
        ]]

    def get_total_runs(self):
        csv_path = os.path.join('data', 'activities')

        file_names = [f for f in os.listdir(csv_path)]
        
        df_runs = pd.DataFrame()

        for file_name in file_names:
            df_runs = pd.concat([df_runs, self.get_run(self.get_data_raw(file_name))], axis=0)

        return df_runs[df_runs['date'].dt.year > 2023]

    def get_agg_runs(self):
        df_runs = self.get_total_runs()

        df_agg = df_runs.groupby(['date', 'period'], as_index=False).agg(
            {
                'time':'max',
                'speed':'mean',
                'pace':'mean',
                'km':'max'
            }
        ).sort_values('date')

        df_agg['cum_km'] = df_agg['km'].cumsum()

        return df_agg
    