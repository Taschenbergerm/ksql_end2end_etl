# KSQL Experiments ETL 

This repo is a small playground with KSQL and a Full-Stack ETL Pipeline from Data Generation, Transformation, Ingestion, Access and Visualization. 

Technologies;
- Kafka + Zookeeper 
- KSQL DB 
- Python 
- FastAPI 
- Graphana



## Starting the Stack 

the whole docker stack can be started using docker-compose and the compose file within the deployment folder. 


## Entering the CLI 

docker exec -it ksqldb-cli ksql http://ksqldb-server:8088            


## Starting Components 

To start any part of this project a cli is provided in cli.py on the top level 
ATM you can start it py executing 

    $ python cli.py --help 

It support the following commands 

| Command      | Description                                         | Options            |
|--------------|-----------------------------------------------------|--------------------|
| start api    | Starts the rest api using the environment variables | None               | 
| start worker | Starts the worker that executs experiments          | None               |
| init-db      | Initialises all Streams and Tables within KSQLDB    | Host: defaults to "http://0.0.0.0:8088" |
|--------------|-----------------------------------------------------|--------------------|


##  To Do 
- Split the api and routers into different modules
- Grafana view on all data 
- Extend the API with pagination for the GETs
- Api + Worker for managing the Experiment-Simulations
- Own Dashboard for controlling the processes 
