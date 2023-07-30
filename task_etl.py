Построение ETL-пайплайна.
Ожидается, что на выходе будет DAG в airflow, который будет считаться каждый день за вчера. 

1. Параллельно будем обрабатывать две таблицы. В feed_actions для каждого юзера посчитаем число просмотров и лайков контента. В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка должна быть в отдельном таске.

2. Далее объединяем две таблицы в одну.

3. Для этой таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез.

4. И финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse.

5. Каждый день таблица должна дополняться новыми данными. 

Структура финальной таблицы должна быть такая:

Дата - event_date

Название среза - dimension

Значение среза - dimension_value

Число просмотров - views

Числой лайков - likes

Число полученных сообщений - messages_received

Число отправленных сообщений - messages_sent

От скольких пользователей получили сообщения - users_received

Скольким пользователям отправили сообщение - users_sent

Срез - это os, gender и age


from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
import numpy as np

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 't-pozharskaja',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 12),
}

# Интервал запуска DAG
schedule_interval = '0 12 * * *'

connection_simulator = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230520',
                      'user':'student',
                    'password':'dpo_python_2020'
                     }

def ch_get_df(query, connection):
    df = ph.read_clickhouse(query, connection=connection)
    return df

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_tpozharskaya():
    @task() # функция extract
    # В feed_actions для каждого юзера посчитаем число просмотров и лайков контента
    def extract_feed(): 
        query = """
            select user_id as user,
            toDate(time) as event_date,
            countIf(action='view') as views,
            countIf(action='like') as likews,
            if(gender=1, 'male', 'female') as gender,
            age,
            os
            from simulator_20230520.feed_actions
            where toDate(time) = yesterday()
            group by user_id,
            toDate(time),
            gender,
            age,
            os 
            """
        df_feed = ch_get_df(query, connection_simulator)
        return df_feed
    @task()
    # В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему
    def extract_messages():
        query = """
            select user,
            event_date,
            messages_received,
            messages_sent,
            users_received,
            users_sent
            from
              (select reciever_id as user,
                      toDate(time) as event_date,
                      count(reciever_id) as messages_received,   
                      uniq(user_id) as users_sent              
                      from simulator_20230520.message_actions
                      where toDate(time) = yesterday()
                      group by reciever_id,
                               toDate(time)) t
            join 
              (select user_id as user,
                      toDate(time) as event_date,
                      uniq(reciever_id) as users_received,             
                      count(user_id) as messages_sent            
              from simulator_20230520.message_actions
              where toDate(time) = yesterday()
              group by user_id,
                       toDate(time)) t1 on t.user = t1.user and t.event_date = t1.event_date
              """
          
        df_messages = ch_get_df(query, connection_simulator)
        return df_messages
    @task
    def df_merge(df_feed, df_messages):
        df = pd.merge(feed, messages, how = 'left', on = ['user', 'event_date'])
        df = df.fillna(0, inplace=True)
        return df 

dag_tpozharskaya = dag_tpozharskaya()      