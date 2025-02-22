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
    from sklearn.model_selection import train_test_split 
    # from joblib import dump
    import joblib
    import pickle
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
    
        
        
        
def transform_data(n_files=None, filename='data.csv'):
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

        df_final.to_csv(f'{clean_data_folder}/df_final.csv')


        X = df_final.drop(['target'], axis=1)
        X.to_csv(f'{clean_data_folder}/X.csv')
        
        y = df_final['target']
        y.to_pickle(f'{clean_data_folder}/y.pkl')
        



def compute_model_score(task_instance, model):
    clean_data_folder = Variable.get("clean_data_folder", deserialize_json=True)
    X = pd.read_csv(f'{clean_data_folder}/X.csv')
    y = pd.read_pickle(f'{clean_data_folder}/y.pkl')
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()
    model_name = str(model)[:-2]
    task_instance.xcom_push(
        key = model_name,
        value = model_score)


def train_and_save_model(model):
    
    
    clean_data_folder = Variable.get("clean_data_folder", deserialize_json=True)
    X = pd.read_csv(f'{clean_data_folder}/X.csv')
    y = pd.read_pickle(f'{clean_data_folder}/y.pkl')
    # training the model
    model.fit(X, y)
    # saving model
    model_name = model.__class__.__name__

    file_name = f'{model_name}.pckl'
    with open(os.path.join(clean_data_folder,file_name), 'wb') as f:
        joblib.dump(model, f)
    

def evaluate_models_and_select(task_instance):
    models = Variable.get('models', deserialize_json = True)
    best_score = -10000000000
    for model_name in models:
        score = task_instance.xcom_pull(
            key=f'{model_name}',
            task_ids = [f'Train.{model_name}',]
            )[0]
        if score > best_score:
            best_score = score
            best_model_name = model_name
    best_model_class = globals()[best_model_name]
    best_model  = best_model_class()
    train_and_save_model(best_model)

 
    
with DAG(
    dag_id='weather_dag_old',
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
            python_callable=transform_data,
            op_kwargs = {
                'n_files': 20
                }
            )
        load_3 = PythonOperator(
            task_id = "Load_3",
            python_callable=transform_data,
            op_kwargs = {
                'filename': 'fulldata.csv'
                }
            )
    

        
    with TaskGroup("Train") as score:
        train_4a = PythonOperator(
            task_id = "LinearRegression",
            python_callable=compute_model_score,
            op_kwargs = {'model': LinearRegression()},
            provide_context=True
            )
        train_4b = PythonOperator(
            task_id = "DecisionTreeRegressor",
            python_callable=compute_model_score,
            op_kwargs = {'model': DecisionTreeRegressor()},
            provide_context=True
            ) 
        train_4c = PythonOperator(
            task_id = "RandomForestRegressor",
            python_callable=compute_model_score,
            op_kwargs = {'model': RandomForestRegressor()},
            provide_context=True
            )
        


    select = PythonOperator(
        task_id='Select',
        python_callable=evaluate_models_and_select,
        provide_context=True
        )


extract >> load
load >> score
score >> select





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
    
    