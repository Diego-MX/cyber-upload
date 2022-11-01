Creamos una base de datos para guardar algunas referencias.  
Y que también servirá para guardar otros catálogos. 


# Server

Para eso hay que crear un _SQL Server_.  Usamos las siguientes características: 
- Authentication:  Only Azure AD  
- Admin: yours truly  
- Networking | Firewall: Allow resources to access this server  
- MS Defender:  Not now  

# Database

- Elastic pool:  No
- Compute Storage:  Gen5, 2 vCores, 32 GB, zone redundance disabled
- Backup redundancy: Zone-redundant storage

- Network | current client IP address:  Yes

- Security | MS Defender: Not now

- Additional | Existing data: 
- Collation | SQL_Latin1_General_CP1_CI_AS
- Maintenance | 5pm-8am

# Permisos 

- Se habilita el de `cx-collections`, y el resto de los accesos se hacen por _Service Principal_. 


