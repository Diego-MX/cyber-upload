
databricks --profile <zoras> jobs list 
databricks --profile <zoras> jobs get --job-id <an-id>
databricks --profile <qas> jobs --version 2.1 create --json-file .\refs\databricks\update_all.json


$env:ENV_TYPE = 'qas'; $env:SERVER_TYPE='local'; ipython 


$env:GITHUB_TOKEN = 'PASTE-TOKEN'
databricks --profile <qas> secrets create-scope <user-diego-villamil>
databricks --profile <qas> secrets put --scope <user-diego-villamil> --key <github-access-token> --string-value $env:GITHUB_TOKEN