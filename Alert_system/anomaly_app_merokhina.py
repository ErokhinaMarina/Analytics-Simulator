import pandahouse as ph
import pandas as pd
import numpy as np

import seaborn as sns
import telegram
import matplotlib.pyplot as plt
import io

from datetime import datetime, timedelta
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#подключение к БД, из которой будем загружать данные 
connection = {}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'm-erokhina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 9),
}

my_token = '' # токен бота
bot = telegram.Bot(token=my_token) # получаем доступ

# Отправка отчета:
chat_id = 


# Интервал запуска DAG
schedule_interval =  '*/15 * * * *'

def check_anomaly(df, metric,  a=4, n=5):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75']-df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1]<df['low'].iloc[-1] or df[metric].iloc[-1]> df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    return is_alert, df

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)

def dag_anomaly_erohina():
    
    @task()
    def extract_data():
        
        q = '''SELECT * FROM (
                   SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , toDate(time) as date
                        , formatDateTime(ts, '%R') as hm
                        , COUNT(DISTINCT user_id) as DAU_feed
                        , sum(action='view') as views
                        , sum(action='like') as likes
                        , round(likes/views, 3) AS CTR
                    FROM {db}.feed_actions
                    WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm
                    ORDER BY ts) AS f
                    JOIN (
                SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , toDate(time) as date
                        , formatDateTime(ts, '%R') as hm
                        , COUNT(DISTINCT user_id) as DAU_message
                        , COUNT(user_id) AS messages
                FROM {db}.message_actions
                WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
                GROUP BY  ts, date, hm
                ORDER BY ts) m on m.ts = f.ts and m.date = f.date and m.hm = f.hm'''
        
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task
    def run_alerts(data):

        metric_list=['DAU_feed', 'views', 'likes', 'CTR', 'DAU_message', 'messages']
        for metric in metric_list:
            df=data[['ts','date','hm',metric]].copy()
            is_alert,df=check_anomaly(df, metric)

            if is_alert == 1:
                current_val=df[metric].iloc[-1]
                last_val_diff=abs(1-(df[metric].iloc[-1]/df[metric].iloc[-2]))
                msg = f'Метрика {metric}:\n текущее значение {current_val:.2f}\n отклонение от предыдущего значения {last_val_diff:.2f}%'
            
                sns.set(rc={'figure.figsize':(16,10)})
                plt.tight_layout()
                
                ax=sns.lineplot(x = df['ts'], y = df[metric], label = 'metric')
                ax=sns.lineplot(x = df['ts'], y = df['up'], label = 'up')
                ax=sns.lineplot(x = df['ts'], y = df['low'], label = 'low')
                
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind%2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                
                ax.set(xlabel='time')
                ax.set(ylabel=metric)
                
                ax.set_title(metric)
                ax.set(ylim=(0,None))
                
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name='image.png'.format(metric)
                plt.close()
                
                bot.send_message(chat_id=chat_id,text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    df = extract_data()
    run_alerts(df)
    
dag_anomaly_erohina = dag_anomaly_erohina()