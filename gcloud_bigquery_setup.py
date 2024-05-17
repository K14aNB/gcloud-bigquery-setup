import os
from importlib import import_module
from dotenv import get_key
from google.cloud import bigquery

def bqsetup(repo_name:str,runtime:str,dataset_ids:list,ds_project_id=None):
    '''
    Function to setup bigquery client and fetch the datasets
    
    Arguments:
    repo_name:str: Local git repository name
    runtime:str: Indicates currently active runtime type. Can be colab, jupyter or python-script
    dataset_ids:list(str): dataset names/ids of datasets in bigquery
    ds_project_id:str: project_id of the project in which dataset is stored in bigquery

    Returns:
    A tuple containing an instance of bigquery Client and list of instances of Dataset retrieved from respective dataset references. 
    '''
    env_var=repo_name.replace('-','_').upper()
    quota_project_id=None

    if runtime=='colab':
        userdata=import_module('google.colab.userdata')    
        quota_project_id=userdata.get(env_var)
    
    elif runtime in ['jupyter','python-script']:
        if os.path.isfile(os.path.join(os.path.expanduser('~'),'.config','gcloud','application_default_credentials.json')):
            if os.path.isfile(os.path.join(os.expanduser('~'),'.env',repo_name,'environment_variables.env')):
                quota_project_id=get_key(dotenv_path=os.path.join(os.expanduser('~'),'.env',repo_name,'environment_variables.env'),key_to_get=env_var,encoding='utf-8')
    
    
    if quota_project_id is not None:
        client=bigquery.Client(project_id=quota_project_id)
        if type(ds_project_id)==str: 
            dataset_refs=[client.dataset(dataset_id,project=ds_project_id) for dataset_id in dataset_ids]
            datasets=[client.get_dataset(dataset_ref) for dataset_ref in dataset_refs]
    
    return client,datasets

        




        
    




        
