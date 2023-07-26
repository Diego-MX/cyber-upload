
# Jobs

- Los Jobs de Databricks son:  
  - `collections 1 update`  
  - `collections 1 cyber`  

- A su vez depende de `cx-read-events:events update`. 

- Y también requiere de tener un _cluster_ configurado.  

- Los recursos de Azure se encuentran en `config.py`.  


# Otros recursos

## Librerías y secretos   

- `epic_py` se instala mediante el archivo `user_databricks.yml`  con los respectivos _scope_, llaves y secretos, previamente habilitados.  
  No discrimina versiones, y hay pocos tests asociados:  ¡Aguas!  

- Usamos variables de ambientes `SERVER_TYPE`, `ENV_TYPE`,  `CORE_ENV`, `CRM_ENV` para identificar los nombres de los recursos y secretos que se utilizan.  

- El resto de los recursos se manejan desde `config.py`, la mayoría mediante las llaves anteriores.  



