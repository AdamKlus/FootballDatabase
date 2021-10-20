# Sport Database              
                    
TV Research company is in need of a database that will be source of truth for the live football events.             
                
This will help them identify live events in the tv logs from their providers.           
                
I was asked to develop ETL processes for above database in Redshift.           
                
The database can be used for further automations and/or as a base for web app.                


## Database schema design and ETL process              
Database have 2 sources:                 
                  
- Historical data in csv format. Each file represents one day. The data is stored in S3 bucket          
                   
- Current data is scraped from the website Betxplorer.com               
                  
ETL is processing both sources into star schema database optimised for queries that will help the team get live football matches data from one trusted source.             
                 
Fact Table               
                 
1. events - live football games since July 2013                 
`event_id` , `start_time`, `home_id`, `away_id`, `competition_id`                 
               
Dimension Tables               
                
2. teams - table with all the teams                
`team_id`, `name`, `country`, `alternative_names`               
                 
3. copetitions - table with all the copetitions                    
`competition_id`, `name`, `country`, `alternative_names`             
             
4. time - timestamps of records in events broken down into specific units            
`start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`           

![Database schema](/images/schema.png)
  
           
Example query:     
     
```
SELECT e.start_time, home.name home, away.name away  
FROM events e  
JOIN competitions c ON e.competition_id = c.competition_id  
JOIN teams home ON e.home_id = home.team_id  
JOIN teams away ON e.away_id = away.team_id  
JOIN times t ON e.start_time = t.start_time  
WHERE c.name = 'Champions League'  
AND t.year = 2021 AND t.month = 5    
ORDER BY e.start_time
```

Which will give us all Champions League games in May 2021.   
     
![Query result](/images/table.png)    
   
## Files in repository             
            
`slq_queries.py` - queries definitions          
            
`functions.py` - functions to create the requried tables              
        
`etl.py` - extract, transform and load processes            
                
`dwh.cfg` - credentials and settings for AWS (not uploaded to git hub)    

`balnk_dwh.cfg` - blanked version of `dwh.cfg`                

## How to run the python scripts          
           
Before running you need to create Redshift cluster with associated IAM role to read S3 buckets.        
           
Then populate dwh.cfg with necessary credentials.          
       
Run `etl.py` to populate tables.              

## Possible scenarios    
     
1. If the data was increased by 100x.     
        
Redshift should handle this amount of data. If needed we can always scale up the service.    
     
2. If the pipelines were run on a daily basis by 7am.     
     
We can schedule daily running in Python or with use of Apache Airflow. There is no need of running first part of the ETL pipeline for the historical data on schedule. It is one time event. The second part with scraping can be run daily.     
    
3. If the database needed to be accessed by 100+ people.    
     
Redshift can handle this amount of users. If needed we can always scale up the service.     




