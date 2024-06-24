from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import json


def get_data(**kwargs):
    url = 'https://raw.githubusercontent.com/mrdbourke/zero-to-mastery-ml/master/data/car-sales-extended.csv'
     # Make,Colour,Odometer (KM),Doors,Price
    response = requests.get(url)

    if response.status_code == 200:
        df = pd.read_csv(url, header=None, names = ['Make','Color','Odometer (KM)','Doors','Price'])

        # convert dataframe to json from xcom
        json_data = df.to_json(orient='records')

        kwargs['ti'].xcom_push(key='data', value=json_data)
    else:
        raise Exception(f'Failed to get data from {url}, HTTP status code: {response.status_code}')


def preview_data(**kwargs):
    output_data = kwargs['ti'].xcom_pull(key='data', task_ids='get_data')
    print(output_data)
    if output_data:
        output_data = json.loads(output_data) # load the dict data to be json data
    else:
        raise ValueError(f'No Data recevied from XCom')

    # Create Dataframe from the JSON data
    df = pd.DataFrame(output_data)

    # Compute and Aggreate based on Make
    df_uni_make_color = df.groupby(['Make','Color'],as_index=False,dropna=True).aggregate({'Odometer (KM)':'avg', 'Price':'avg'})

    df_uni_make_color = df_uni_make_color.sort_values('Price', ascending=False)

    print(df_uni_make_color[['Odometer (KM)','Price']].head(20))



default_args = {
    'owner': 'ismailde.om',
    'start_date': datetime(2024, 6, 23),
    'catchup': False
}

dag = DAG('fetch_and_review_car_data', default_args=default_args, schedule_interval = timedelta(days=1))

data_data_py_operator = PythonOperator(task_id='get_data', python_callable=get_data, dag=dag)
preview_data_from_url = PythonOperator(task_id='preview_data', python_callable=preview_data, dag = dag)


data_data_py_operator >> preview_data_from_url