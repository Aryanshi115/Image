from fastapi import FastAPI, Query, HTTPException
import pandas as pd
import numpy as np
import boto3
import io
from elasticsearch import Elasticsearch
from datetime import datetime
import json

# from app.utils import Config
from utils import Config

import warnings
warnings.filterwarnings("ignore")

app = FastAPI()

config = Config()

s3_config = config.get_s3_config()
es_config = config.get_elasticsearch_config()

_service_name = s3_config.get('service_name')
_region_name = s3_config.get('region_name')
_aws_access_key_id = s3_config.get('aws_access_key_id')
_aws_secret_access_key = s3_config.get('aws_secret_access_key')

s3_client = boto3.client(service_name=_service_name, region_name=_region_name,
                         aws_access_key_id=_aws_access_key_id,
                         aws_secret_access_key=_aws_secret_access_key)

bucket_name = s3_config.get('bucket_name')
folder_name = s3_config.get('folder_prefix')

es_conn = es_config.get('url')
# es_conn = "http://localhost:9200" 

es_user = es_config.get('username')
es_pass = es_config.get('password')
es_index_1 = es_config.get('index_master')
es_index_2 = es_config.get('index_trends')

es = Elasticsearch(
    hosts=[es_conn],
    http_auth=(es_user, es_pass),
)

def retrieve_data_from_es(es_conn, es_user, es_pass, index_name):
    es = Elasticsearch(
    hosts=[es_conn],
    http_auth=(es_user, es_pass),
    )
    result = es.search(index=index_name, query={"match_all": {}}, size=10000)

    hits = result['hits']['hits']
    data = [hit['_source'] for hit in hits]

    df = pd.DataFrame(data)

    return df

# Function to delete all documents from an index
def delete_all_documents(index_name):
    try:    
        if es.indices.exists(index=index_name):
            es.delete_by_query(index=index_name, query= {"match_all": {}})
            print("All documents deleted from index '{}'.".format(index_name))
        else:
            print("Index '{}' does not exist.".format(index_name))
    except Exception as ex:
        return {"message": "Index deletion failed due to: {}".format(ex)}

@app.get("/load_data/")
async def load_data(value1: str, value2: str, value3: str, value4: str):
    try:
        dataset1 = f"{folder_name}/{value1}.csv"
        dataset2 = f"{folder_name}/{value2}.csv"
        dataset3 = f"{folder_name}/{value3}.csv"
        report_run_date = value4

        obj1 = s3_client.get_object(Bucket=bucket_name, Key=dataset1)
        open_poam = pd.read_csv(io.BytesIO(obj1['Body'].read()))
        obj2 = s3_client.get_object(Bucket=bucket_name, Key=dataset2)
        closed_poam = pd.read_csv(io.BytesIO(obj2['Body'].read()))
        obj3 = s3_client.get_object(Bucket=bucket_name, Key=dataset3)
        trends = pd.read_csv(io.BytesIO(obj3['Body'].read()))

        report_run_date = '11-01-2023'
        report_run_date = pd.to_datetime(report_run_date, format='%m-%d-%Y')

        open_poam = open_poam.dropna(how='all').dropna(axis=1, how='all')
        closed_poam = closed_poam.dropna(how='all').dropna(axis=1, how='all')

        open_poam.columns = open_poam.columns.str.lower().str.replace(' ', '_')
        closed_poam.columns = closed_poam.columns.str.lower().str.replace(' ', '_')
        open_poam.columns = open_poam.columns.str.lower().str.replace('-', '_')
        closed_poam.columns = closed_poam.columns.str.lower().str.replace('-', '_')
    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block1 - Not found due to {}".format(str(e))
        )
    
    try:
        # Get sets of column names for each dataframe
        open_poam_columns = set(open_poam.columns)
        closed_poam_columns = set(closed_poam.columns)

        # Find columns that are not common between the two dataframes
        columns_not_in_open_poam = closed_poam_columns - open_poam_columns
        columns_not_in_closed_poam = open_poam_columns - closed_poam_columns

        # Print the columns that are not common
        print("Columns in closed_poam but not in open_poam:", columns_not_in_open_poam)
        print("Columns in open_poam but not in closed_poam:", columns_not_in_closed_poam)

        # Function to determine the appropriate sample value based on datatype
        def get_sample_value(df, col):
            if df[col].dtype == 'object':
                return 'NA'
            elif df[col].dtype in ['int64', 'float64']:
                return 3999
            else:
                return 'NA'  # Default fallback

        # Add missing columns to open_poam with appropriate sample values
        for col in columns_not_in_open_poam:
            sample_value = get_sample_value(closed_poam, col)
            open_poam[col] = sample_value

        # Add missing columns to closed_poam with appropriate sample values
        for col in columns_not_in_closed_poam:
            sample_value = get_sample_value(open_poam, col)
            closed_poam[col] = sample_value
                
        open_poam['status_flag'] = 'open'
        closed_poam['status_flag'] = 'closed'
    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block2 - Not found due to {}".format(str(e))
        )
    
    try:
        final_poam = pd.concat([open_poam, closed_poam], ignore_index=True)
    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block3 - Not found due to {}".format(str(e))
        )
    
    try:
        # Iterate over each column
        for col in final_poam.columns:
            # Check the datatype of values in the column
            if final_poam[col].dtype == 'object':
                # Replace NaN values with 'NA'
                final_poam[col].fillna('NA', inplace=True)
            elif final_poam[col].dtype in ['int64', 'float64']:
                # Replace NaN values with 3999
                final_poam[col].fillna(3999, inplace=True)

        # List of columns to check and replace 'NA' with the replacement_date
        date_columns = [
            'original_detection_date', 
            'scheduled_completion_date', 
            'status_date', 
            'last_vendor_check_in_date'
        ]

        # Ensure all date columns are in datetime format
        final_poam['scheduled_completion_date'] = pd.to_datetime(final_poam['scheduled_completion_date'], format='%d-%m-%Y', errors='coerce')
        final_poam['status_date'] = pd.to_datetime(final_poam['status_date'], format='%d-%m-%Y', errors='coerce')
        final_poam['original_detection_date'] = pd.to_datetime(final_poam['original_detection_date'], format='%d-%m-%Y', errors='coerce')
        final_poam['last_vendor_check_in_date'] = pd.to_datetime(final_poam['last_vendor_check_in_date'], format='%d-%m-%Y', errors='coerce')

        # Convert date columns to 'MM-dd-YYYY' format strings and then back to datetime to ensure they are recognized as date datatype
        date_columns = ['scheduled_completion_date', 'status_date', 'original_detection_date', 'last_vendor_check_in_date']

        for col in date_columns:
            final_poam[col] = pd.to_datetime(final_poam[col].dt.strftime('%m-%d-%Y'))

    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block4 - Not found due to {}".format(str(e))
        )
    
    try:
        for col in date_columns:
            final_poam[col] = final_poam[col].fillna(pd.Timestamp('2100-12-31'))

        # Create a new column 'report_date' with value as 'report_run_date' for all rows
        final_poam['report_date'] = report_run_date

        final_poam['days_difference'] = (final_poam['report_date'] - final_poam['status_date']).dt.days
        # Create the new columns based on the conditions
        final_poam['last_30'] = final_poam.apply(lambda row: 'Yes' if 0 <= row['days_difference'] <= 30 else 'No', axis=1)
        final_poam['last_60'] = final_poam.apply(lambda row: 'Yes' if 0 <= row['days_difference'] <= 60 else 'No', axis=1)
        final_poam['last_90'] = final_poam.apply(lambda row: 'Yes' if 0 <= row['days_difference'] <= 90 else 'No', axis=1)
    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block5 - Not found due to {}".format(str(e))
        )
    
    try:
        # Add 'importance_flag' column based on the specified conditions
        final_poam['importance_flag'] = (
            (final_poam['vendor_dependency'] == 'No') & 
            (final_poam['false_positive'] == 'No') & 
            (final_poam['operational_requirement'] == 'No')
        ).apply(lambda x: 'Yes' if x else 'No')

        bin_edges = np.arange(0, 1081, 30).tolist() + [np.inf]

        # Define the bin labels
        bin_labels = [f'[{bin_edges[i]}, {bin_edges[i+1]})' for i in range(len(bin_edges)-2)] + ['[1080.0, inf)']

        # Create the 'days_to_complete_bins' column
        final_poam['days_to_complete_bins'] = pd.cut(final_poam['days_to_complete'], bins=bin_edges, labels=bin_labels, right=False)

        # Verify the creation of the new column
        print(final_poam[['days_to_complete', 'days_to_complete_bins']].head())

        date_columns = [
            'scheduled_completion_date',
            'status_date',
            'original_detection_date',
            'last_vendor_check_in_date',
            'report_date'
        ]
    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block6 - Not found due to {}".format(str(e))
        )
    
    try:
        final_trends = trends.transpose()
        final_trends.reset_index(inplace=True)

        # Set the first row as column names
        final_trends.columns = final_trends.iloc[0]
        final_trends = final_trends[1:]
        final_trends.columns.values[0] = 'month'

        final_trends = final_trends.dropna(how='all').dropna(axis=1, how='all')
        final_trends.columns = final_trends.columns.str.lower().str.replace(' ', '_')
        final_trends.columns = final_trends.columns.str.lower().str.replace('-', '_')
        # Strip the string '#_of_' from column names
        final_trends.columns = final_trends.columns.str.replace('#_of_', '')

        final_trends['month'] = pd.to_datetime(final_trends['month'], format='%b-%y')

        # Extract year and month as integers
        final_trends['yyyymm'] = final_trends['month'].dt.year * 100 + final_trends['month'].dt.month
        final_trends['month'] = final_trends['month'].dt.strftime('%b-%y')
    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block7 - Not found due to {}".format(str(e))
        )  
    
    try:
        # List of columns to convert to int64
        columns_to_convert = [
            'open_poa&ms',
            'open_poa&ms_with_vendor_dependencies',
            'open_poa&ms_with_operational_requirements',
            'open_poa&ms_with_false_positives',
            'open_poa&ms_of_importance',
            'open_poa&ms_past_due',
            'closed_poa&ms',
            'closed_poa&ms_in_30_days',
            'closed_poa&ms_in_60_days',
            'closed_poa&ms_in_90_days',
            'closed_poa&ms_(late)',
            'closed_poa&ms_of_importance_late',
            'average_days_to_close_late_poa&ms',
            'average_#_days_to_close_late_poa&ms_of_importance'
        ]

        # Convert specified columns to int64
        final_trends[columns_to_convert] = final_trends[columns_to_convert].astype('int64')
    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block8 - Not found due to {}".format(str(e))
        )  
    
    try:
        def prepare_data_for_es(df):
            # Convert dates to strings in 'yyyy-MM-dd' format
            for col in date_columns:
                df[col] = df[col].astype(str)
            return df
        
        final_poam = prepare_data_for_es(final_poam)
    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block9a - Not found due to {}".format(str(e))
        )  


    try:
        es_mapping_1 = {
            "mappings": {
                "properties": {}
            }
        }

        # Iterate through columns to define mappings
        for col in final_poam.columns:
            if col in date_columns:
                es_mapping_1["mappings"]["properties"][col] = {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                }
            elif final_poam[col].dtype == 'object':
                es_mapping_1["mappings"]["properties"][col] = {
                    "type": "keyword"
                }
            elif final_poam[col].dtype == 'int64':
                es_mapping_1["mappings"]["properties"][col] = {
                    "type": "integer"
                }
            elif final_poam[col].dtype == 'float64':
                es_mapping_1["mappings"]["properties"][col] = {
                    "type": "float"
                }
        
        # Trends Mapping
        es_mapping_2 = {
            "mappings": {
                "properties": {}
            }
        }

        # Iterate over columns and set the type based on the data type
        for col in final_trends.columns:
            dtype = final_trends[col].dtype
            if dtype == 'object':
                es_mapping_2["mappings"]["properties"][col] = {
                    "type": "keyword"
                }
            elif dtype == 'int64':
                es_mapping_2["mappings"]["properties"][col] = {
                    "type": "integer"
                }
            elif dtype == 'float64':
                es_mapping_2["mappings"]["properties"][col] = {
                    "type": "float"
                }
            elif dtype == 'datetime64[ns]':
                es_mapping_2["mappings"]["properties"][col] = {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                }
    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block7 - Not found due to {}".format(str(e))
        )

    try:
        delete_all_documents(es_index_1)
        delete_all_documents(es_index_2)
        try:
            es.indices.create(index=es_index_1, mappings=es_mapping_1)
            print("Index '{}' created successfully with mapping for dataset_1.".format(es_index_1))
        except Exception as ex:
            print("Index '{}' wasn't created because: {}".format(es_index_1, ex))
        
        try:
            es.indices.create(index=es_index_2, mappings=es_mapping_2)
            print("Index '{}' created successfully with mapping for final_trends.".format(es_index_2))
        except Exception as ex:
            print("Index '{}' wasn't created because: {}".format(es_index_2, ex))
        final_poam['json'] = final_poam.apply(lambda x: x.to_json(), axis=1)
        final_trends['json'] = final_trends.apply(lambda x: x.to_json(), axis=1)
    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block8 - Not found due to {}".format(str(e))
        )
    
    try:
        for idx, item in enumerate(final_poam['json']):
            try:
                document_id = str(idx+1)
                resp = es.index(index=es_index_1, id=document_id, document=json.loads(item))
                print(resp['result'], 'into ES Successfully => {}'.format(item))
                print()
            except Exception as ex:
                print('Failed due to => ', ex)
    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block6 - Not found due to {}".format(str(e))
        )
    
    try:
        for idx, item in enumerate(final_trends['json']):
            try:
                document_id = str(idx+1)
                resp = es.index(index=es_index_2, id=document_id, document=json.loads(item))
                print(resp['result'], 'into ES Successfully => {}'.format(item))
                print()
            except Exception as ex:
                print('Failed due to => ', ex)
    except Exception as e:
        print(str(e))        
        raise HTTPException(
            status_code=404, detail="Block6 - Not found due to {}".format(str(e))
        )
    
    return{
        "message": "Data Loaded Successfully."
    }

@app.get("/test_api/")
async def test_api(message1: str):
    return{
        "message": "Message - " + message1
    }

@app.get("/static_test_api/")
async def static_test_api():
    return{
        "message": "This test API is static and has executed successfully. - 29th Aug"
    }
