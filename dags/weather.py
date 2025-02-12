try:
    from airflow import DAG
    from airflow.utils.dates import days_ago
    from airflow.operators.python import PythonOperator
    from airflow.operators.dummy import DummyOperator
    from airflow.utils.task_group import TaskGroup
    from airflow.providers.http.sensors.http import HttpSensor
    from airflow.providers.http.operators.http import SimpleHttpOperator
    from airflow.models import Variable
    from datetime import datetime
    import requests
    import json
    import pandas as pd
    import os
    from sklearn.model_selection import cross_val_score
    from sklearn.linear_model import LinearRegression
    from sklearn.tree import DecisionTreeRegressor
    from sklearn.ensemble import RandomForestRegressor
    from joblib import dump
except Exception as e:
    print(f'Exception : {e}')


# cities = ['paris','london','washington']
# cities = Variable.get("cities", deserialize_json=True)
# raw_files_folder = Variable.get("raw_files_folder", deserialize_json=True)
# clean_data_folder = Variable.get("clean_data_folder", deserialize_json=True)



def extract_data():
    cities = Variable.get("cities", deserialize_json=True)
    raw_files_folder = Variable.get("raw_files_folder", deserialize_json=True)
    cities_data = []
    for city in cities:
        data_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid=92d83785320da490c55fd4fcdc96f921'
        r = requests.get(url)
        city_data = r.json()
        cities_data.append(city_data)
    # save city data
    data_file_name = f'{data_time}.json'
    with open(os.path.join(raw_files_folder,data_file_name), 'w') as f:
        json.dump(cities_data, f)
    
        
        
        
def transform_data_into_csv(n_files=None, filename='data.csv'):
    parent_folder = Variable.get("raw_files_folder", deserialize_json=True)
    clean_data_folder = Variable.get("clean_data_folder", deserialize_json=True)
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
            files = files[:n_files]

    dfs = []

    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
                data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
            {
            'temperature': data_city['main']['temp'],
            'city': data_city['name'],
            'pression': data_city['main']['pressure'],
            'date': f.split('.')[0]
            }
        )

    df = pd.DataFrame(dfs)

    # print('\n', df.head(10))

    df.to_csv(os.path.join(clean_data_folder, filename), index=False)
    
    if not n_files:
        df = df.sort_values(['city', 'date'], ascending=True)

        dfs = []

        for c in df['city'].unique():
            df_temp = df[df['city'] == c]

            # creating target
            df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

            # creating features
            for i in range(1, 10):
                df_temp.loc[:, 'temp_m-{}'.format(i)
                            ] = df_temp['temperature'].shift(-i)

            # deleting null values
            df_temp = df_temp.dropna()

            dfs.append(df_temp)

        # concatenating datasets
        df_final = pd.concat(
            dfs,
            axis=0,
            ignore_index=False
            )
        
        # deleting date variable
        df_final = df_final.drop(['date'], axis=1)

        # creating dummies for city variable
        df_final = pd.get_dummies(df_final)

        df_final.to_csv(f'{clean_data_folder}/{"df_final.csv"}')





def compute_model_score(model):
    clean_data_folder = Variable.get("clean_data_folder", deserialize_json=True)
    df_path = f'{clean_data_folder}/df_final.csv'
    df = pd.read_csv(df_path)
    X = df.drop(['target'], axis=1)
    y = df['target']
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    return model_score


def train_and_save_model(model, model_folder='./app', model_file_name = 'model.pckl'):
    path_to_model = f'{model_folder}/{model_file_name}'
    clean_data_folder = Variable.get("clean_data_folder", deserialize_json=True)
    df_path = f'{clean_data_folder}/df_final.csv'
    df = pd.read_csv(df_path)
    X = df.drop(['target'], axis=1)
    y = df['target']
    # training the model
    model.fit(X, y)
    # saving model
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)
    




def eval_model():
    pass
      
def select_data():
    pass        

 
    
with DAG(
    dag_id='weather_dag',
    description='Evaluation dag for DataScientest Airflow module',
    tags=['evaluation', 'datascientest'],
    schedule_interval= '* * * * *',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute = 1),
        },
    catchup = False
    ) as weather_dag:

    extract = PythonOperator(
        task_id='Extract',
        python_callable=extract_data,
    )    
    
    with TaskGroup("Load") as load:
        load_2 =  PythonOperator(
            task_id='Load_2',
            python_callable=transform_data_into_csv,
            op_kwargs = {
                'n_files': 20
                }
            )
        load_3 = PythonOperator(
            task_id = "Load_3",
            python_callable=transform_data_into_csv,
            op_kwargs = {
                'filename': 'fulldata.csv'
                }
            )
    

        
    with TaskGroup("Train") as train:
        train_4a = PythonOperator(
            task_id = "Train_4a",
            python_callable=eval_model,
            )
        train_4b = PythonOperator(
            task_id = "Train_4b",
            python_callable=eval_model,
            ) 
        train_4c = PythonOperator(
            task_id = "Train_4c",
            python_callable=eval_model,
            )
        


    select = PythonOperator(
        task_id='Select',
        python_callable=select_data,
        )


extract >> load
load >> train
train >> select





# transform
# transform >> 



# def prepare_data(path_to_data='/app/clean_data', file_name ='fulldata.csv'):
#     # reading data
#     df = pd.read_csv(f'{path_to_data}/{file_name}')
#     # ordering data according to city and date
#     df = df.sort_values(['city', 'date'], ascending=True)

#     dfs = []

#     for c in df['city'].unique():
#         df_temp = df[df['city'] == c]

#         # creating target
#         df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

#         # creating features
#         for i in range(1, 10):
#             df_temp.loc[:, 'temp_m-{}'.format(i)
#                         ] = df_temp['temperature'].shift(-i)

#         # deleting null values
#         df_temp = df_temp.dropna()

#         dfs.append(df_temp)

#     # concatenating datasets
#     df_final = pd.concat(
#         dfs,
#         axis=0,
#         ignore_index=False
#         )
    
#     # deleting date variable
#     df_final = df_final.drop(['date'], axis=1)

#     # creating dummies for city variable
#     df_final = pd.get_dummies(df_final)

#     features = df_final.drop(['target'], axis=1)
#     target = df_final['target']

#     df_final.to_csv(f'{path_to_data}/{"df_final.csv"}')


    # transform = PythonOperator(
    #     task_id = "transform",
    #     python_callable = prepare_data,
    #     )