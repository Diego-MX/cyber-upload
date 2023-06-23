
databricks --profile <zoras> jobs list 
databricks --profile <zoras> jobs get --job-id <an-id>
databricks --profile <qas> jobs --version 2.1 create --json-file .\refs\dbks_jobs\update_all.json


$env:ENV_TYPE = 'qas'; $env:SERVER_TYPE='local'; ipython 


