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

#подключение к БД 
connection = {}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'm-erokhina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 11, 25),
}

my_token = '' # токен моего бота
bot = telegram.Bot(token=my_token) # получаем доступ


# Отправка отчета
chat_id = 


# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def report_feed_erohina():


    @task
    # Выгружаем данные за прошлый день
    def report_previous_day():
        query_1 = """
        SELECT 
            yesterday() as date, 
            count(DISTINCT user_id) as DAU, 
            sum(action = 'like') as likes,
            sum(action = 'view') as views, 
            round(likes/views, 3) as CTR
        FROM 
            {db}.feed_actions
        WHERE 
            toDate(time) = yesterday()
        """

        previous_day = ph.read_clickhouse(query_1, connection=connection)
        return previous_day

    @task
    # Выгружаем данные за прошлую неделею
    def report_previous_week():
        query_2 = """
        SELECT 
            toDate(time) as date, 
            count(DISTINCT user_id) as DAU, 
            sum(action = 'like') as likes,
            sum(action = 'view') as views, 
            round(likes/views, 3) as CTR
        FROM 
            {db}.feed_actions
        WHERE 
            toDate(time) > today()-8 AND toDate(time) < today()
        GROUP BY 
            date
        """

        previous_week = ph.read_clickhouse(query_2, connection=connection)
        return previous_week
    
    # Создаем и отправляем текстовую часть отчета
    @task()
    def message_report_previous_day(previous_day):
        yesterday = previous_day['date'].iloc[0].strftime('%Y-%m-%d')
        DAU = previous_day['DAU'].iloc[0]
        likes = previous_day['likes'].iloc[0]
        views = previous_day['views'].iloc[0]
        CTR = previous_day['CTR'].iloc[0]

        msg = f"""
        Отчет ключевых метрик ленты новостей за {yesterday}:
        
        DAU: {DAU}
        
        Количество просмотров: {views}
        
        Количество лайков: {likes}
        
        CTR: {CTR}
        
        """
        # Отправим сообщение в чат
        bot.sendMessage(chat_id=chat_id, text=msg)

    # Сформируем и отправим графики метрик за последнюю неделю
    @task
    def chart_report_previous_week(previous_week):
        fig, axes = plt.subplots(nrows = 2, ncols = 2, figsize=(18, 12))

        fig.suptitle('Графики ключевых метрик ленты новостей за прошедшую неделю', fontsize=30)

        sns.lineplot(ax = axes[0, 0], data = previous_week, x = 'date', y = 'DAU')
        axes[0, 0].set_title('DAU')
        axes[0, 0].set_xlabel('Дата')
        axes[0, 0].set_ylabel('Количество пользователей')
        
        sns.lineplot(ax = axes[0, 1], data = previous_week, x = 'date', y = 'CTR')
        axes[0, 1].set_title('CTR')
        axes[0, 1].set_xlabel('Дата')
        axes[0, 1].set_ylabel('CTR')
        
        sns.lineplot(ax = axes[1, 0], data = previous_week, x = 'date', y = 'views')
        axes[1, 0].set_title('Просмотры')
        axes[1, 0].set_xlabel('Дата')
        axes[1, 0].set_ylabel('Просмотры')
        
        sns.lineplot(ax = axes[1, 1], data = previous_week, x = 'date', y = 'likes')
        axes[1, 1].set_title('Лайки')
        axes[1, 1].set_xlabel('Дата')
        axes[1, 1].set_ylabel('Лайки')

        # Передадим полученный график в телеграм
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = f'Отчет ключевых метрик.png'
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    previous_day = report_previous_day()
    previous_week = report_previous_week()
    message_report_previous_day(previous_day)
    chart_report_previous_week(previous_week)


report_feed_erohina = report_feed_erohina()