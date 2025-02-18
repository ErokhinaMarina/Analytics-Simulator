import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph

from datetime import datetime, timedelta
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
    'start_date': datetime(2024, 11, 25),
}


my_token = '' # токен бота
bot = telegram.Bot(token=my_token) # получаем доступ

# Отправка отчета:
chat_id = 


# Интервал запуска DAG. Отчет будет приходить каждый день в 11 утра
schedule_interval = '0 11 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def full_report_erohina():

    @task()
    def extract_report_feed():
        query_1 = """
                SELECT
                    toDate(time) as date,
                    count(DISTINCT user_id) as DAU_feed, 
                    sum(action = 'like') as likes,
                    sum(action = 'view') as views, 
                    round(likes/views, 3) as CTR,
                    views + likes as events,
                    count(DISTINCT post_id) as posts 
                FROM
                    {db}.feed_actions
                WHERE 
                    toDate(time) > today()-8 AND toDate(time) < today()
                GROUP BY
                    date
                ORDER BY
                    date
                    """
        df_feed = ph.read_clickhouse(query_1, connection=connection)
        return df_feed

    @task()
    def extract_report_message():
        query_2 = """
                SELECT
                    toDate(time) as date,
                    count(DISTINCT user_id) as DAU_message,
                    count(user_id) as messages
                FROM
                    {db}.message_actions
                WHERE 
                    toDate(time) > today()-8 AND toDate(time) < today()
                GROUP BY
                    date
                ORDER BY
                    date
                    """
        df_message = ph.read_clickhouse(query_2, connection=connection)
        return df_message

    @task()
    def extract_report_dau():
        query_3 = """
                SELECT 
                     date,
                     COUNT(DISTINCT user_id) as DAU,
                     COUNT(DISTINCT CASE WHEN os = 'iOS' THEN user_id END) as DAU_ios,
                     COUNT(DISTINCT CASE WHEN os = 'Android' THEN user_id END) as DAU_android
                FROM (
                        SELECT 
                             toDate(time) AS date,
                             user_id,
                             os
                        FROM 
                             {db}.feed_actions
                        WHERE 
                             toDate(time) > today()-8 AND toDate(time) < today()

                        UNION ALL
                        SELECT 
                            toDate(time) AS date,
                            user_id,
                            os
                        FROM 
                           {db}.message_actions
                        WHERE 
                            toDate(time) > today()-8 AND toDate(time) < today()
                    ) AS combined
                GROUP BY 
                    date
                ORDER BY 
                    date
                        """
        df_dau = ph.read_clickhouse(query_3, connection=connection)
        return df_dau

    @task()
    def extract_report_new_users():
        query_4 = """
                SELECT
                     first_seen_date AS date,
                     COUNT(DISTINCT user_id) AS new_users,
                     COUNT(DISTINCT CASE WHEN source = 'ads' THEN user_id END) AS new_users_ads,
                     COUNT(DISTINCT CASE WHEN source = 'organic' THEN user_id END) AS new_users_organic
                FROM (
                        SELECT
                             user_id,
                             source,
                             MIN(start_date) AS first_seen_date
                        FROM (
                               SELECT
                                    user_id,
                                    MIN(toDate(time)) AS start_date,
                                    source
                               FROM
                                   {db}.feed_actions
                               GROUP BY 
                                    user_id, source
                               UNION ALL
                               SELECT
                                    user_id,
                                    MIN(toDate(time)) AS start_date,
                                    source
                               FROM
                                    {db}.message_actions
                               GROUP BY 
                                    user_id, source
                             ) AS combined
                      GROUP BY
                              user_id, source
                    ) AS users
              WHERE
                   first_seen_date > today() - 7  
              GROUP BY 
                   first_seen_date
              ORDER BY 
                   first_seen_date
                            """
        df_new_users = ph.read_clickhouse(query_4, connection=connection)
        return df_new_users

    @task()
    def message_report(df_feed, df_message, df_dau, df_new_users):
        yesterday                   = df_feed['date'].iloc[-1].strftime('%Y-%m-%d')
        events                      = df_feed['events'].iloc[-1]
        
        DAU_feed                    = df_feed['DAU_feed'].iloc[-1]
        likes                       = df_feed['likes'].iloc[-1]
        views                       = df_feed['views'].iloc[-1]
        CTR                         = df_feed['CTR'].iloc[-1]
        posts                       = df_feed['posts'].iloc[-1]
        
        DAU_messages                = df_message['DAU_message'].iloc[-1]
        messages                    = df_message['messages'].iloc[-1]
        
        DAU_full                    = df_dau['DAU'].iloc[-1]
        DAU_ios                     = df_dau['DAU_ios'].iloc[-1]
        DAU_android                 = df_dau['DAU_android'].iloc[-1]
        
        new_users                   = df_new_users['new_users'].iloc[-1]
        new_users_ads               = df_new_users['new_users_ads'].iloc[-1]
        new_users_organic           = df_new_users['new_users_organic'].iloc[-1]
        

        message = f"""
        Отчет по приложению за {yesterday}:
        
        
        Всего событий: {events}
               
        Количество активных пользователей за день (DAU): {DAU_full}   
        
        DAU по платформе:
            ios: {DAU_ios}            
            android: {DAU_android}    
                    
        Новые пользователи: {new_users}   
        Новые пользователи по источнику:
            ads: {new_users_ads}          
            organic: {new_users_organic}  
            
                    
        События по ленте новостей:
        
        DAU: {DAU_feed}             
        
        Количество лайков: {likes}              
        
        Количество просмотров: {views}          
        
        CTR: {CTR}                  
        
        Количество постов: {posts}  
                
        
        События по месседжеру:
        
        DAU: {DAU_messages}                 
         
        Количество сообщений: {messages}    
         
        """

        bot.sendMessage(chat_id=chat_id, text=message)
        
        
    @task
    def chart_last_week(df_feed, df_message):
        fig, axes = plt.subplots(2, 3, figsize=(30,14))

        fig.suptitle('Отчет по приложению за прошедшую неделю', fontsize=30)

        sns.lineplot(ax = axes[0, 0], data = df_feed, x = 'date', y = 'DAU_feed')
        axes[0, 0].set_title('DAU ленты новостей')
        axes[0, 0].set_xlabel('Дата')
        axes[0, 0].set_ylabel('Количество пользователей')

        sns.lineplot(ax = axes[0, 1], data = df_feed, x = 'date', y = 'CTR')
        axes[0, 1].set_title('CTR')
        axes[0, 1].set_xlabel('Дата')
        axes[0, 1].set_ylabel('CTR')
        
        sns.lineplot(ax = axes[1, 0], data = df_feed, x = 'date', y = 'likes')
        axes[1, 0].set_title('Лайки')
        axes[1, 0].set_xlabel('Дата')
        axes[1, 0].set_ylabel('Лайки')

        sns.lineplot(ax = axes[1, 1], data = df_feed, x = 'date', y = 'views')
        axes[1, 1].set_title('Просмотры')
        axes[1, 1].set_xlabel('Дата')
        axes[1, 1].set_ylabel('Просмотры')

        sns.lineplot(ax = axes[0, 2], data = df_message, x = 'date', y = 'DAU_message')
        axes[0, 2].set_title('DAU сообщений')
        axes[0, 2].set_xlabel('Дата')
        axes[0, 2].set_ylabel('Количество пользователей')
        
        sns.lineplot(ax = axes[1, 2], data = df_message, x = 'date', y = 'messages')
        axes[1, 2].set_title('Сообщения')
        axes[1, 2].set_xlabel('Дата')
        axes[1, 2].set_ylabel('Количество сообщений')

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = f'Отчет ключевых метрик.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    df_feed = extract_report_feed()
    df_message = extract_report_message()
    df_dau = extract_report_dau()
    df_new_users = extract_report_new_users()
    
    message_report(df_feed, df_message, df_dau, df_new_users)
    chart_last_week(df_feed, df_message)
    
full_report_erohina = full_report_erohina()