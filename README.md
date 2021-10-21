# Sport Database              
                    
TV Research company is in need of a database that will be the source of truth for the live football events.             
                
This will help them identify live events in the tv logs from their providers.           
                
I was asked to develop ETL processes for above database in Redshift.           
                
The database can be used for further automations and/or as a base for a web app.     
     
I have decided to use Redshift, because of its flexibility in scaling and ability to quickly import data from S3.

The first part of the ETL for historical data needs to be run only one time. The second part for scraped data should be run daily to give the team most up to date data.             


## Database schema design and ETL process              
Database have 2 sources:                 
                  
- Historical data in csv format. Each file represents one day. The data is stored in S3 bucket          
                   
- Current data is scraped from the website Betexplorer.com               
                  
ETL is processing both sources into star schema database optimised for queries that will help the team get live football matches data from one trusted source.             
                 
Fact Table               
                 
1. events - live football games since July 2013                 
`event_id` - PRIMARY KEY,   
`start_time` - start time of the event,   
`home_id` - unique ID of home team,     
`away_id` - unique ID of away team,    
`competition_id` - unique ID of competiton.                 
               
Dimension Tables               
                
2. teams - table with all the teams                
`team_id` - unique ID of the team,     
`name` - name of the team,     
`country` - country of the team,      
`alternative_names` - blank space to populate with alternative names.                      
                 
3. competitions - table with all the competitions                    
`competition_id` - unique ID of competition,     
`name` - name of the competition,      
`country` - country of the competition,        
`alternative_names` - blank space to populate with alternative names.              
             
4. time - timestamps of records in events broken down into specific units            
`start_time` - date and time in timestamp format,     
`hour` - number representing an hour (0-24),    
`day` - a day of the month (1-31),    
`week` - week number in the year (1-53),    
`month` - number of the month (1-12),    
`year` - number representing a year,    
`weekday` - number representing day of the week (0-6), 0 is Sunday, 1 is Monday etc.               

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
            
`functions.py` - functions to create the database              
        
`etl.py` - extract, transform and load proces            
                
`dwh.cfg` - credentials and settings for AWS (not uploaded to git hub)    

`blank_dwh.cfg` - blanked version of `dwh.cfg`                

## How to run the python scripts          
           
Before running you need to create Redshift cluster with associated IAM role to read S3 buckets.        
           
Then populate dwh.cfg with necessary credentials.          
       
Run `etl.py` to populate tables.              

## Possible scenarios    
     
1. If the data was increased by 100x.     
        
Redshift should handle this amount of data. If needed we can always scale up the service or even use Spark for loading data from S3.        
     
2. If the pipelines were run on a daily basis by 7am.     
     
We can schedule daily running in Python or with use of Apache Airflow. There is no need of running first part of the ETL pipeline for the historical data on schedule. It is one time event. The second part with scraping can be run daily.     
    
3. If the database needed to be accessed by 100+ people.    
     
Redshift can handle this amount of users. If needed we can always scale up the service.     




