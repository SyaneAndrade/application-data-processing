# App Event Processing

App Event Processing is a project to analyze data from a App using:

* Postgres: Postgres database.
* Spark: Spark for processing data.

## Postgres

All the DLL source is inside the folder  /postgresql_processing_messages/. There is a docker for Postgres when you run the docker-compose a docker container is created with all the Tables and processing tables. 

## Installation

### Pre-requisite: 
* Docker

For UP the container for the Postgres:

```bash
docker-compose up -d
```

For STOP the container:

```bash
docker-compose down
```

## ACESSING
### PgAdmin UI

* URL: 
http://localhost:8080
* USER: 
root@root.com
* PASSWORD: 
root

You can create a connection for Postgres DB with the following config:
* HOSTNAME: postgres
* USER: 
root
* PASSWORD: 
root

The tables can be find in the database with name root in application_db schema.


## Spark

The project with the processing of all events is inside the folder /AppEventProcessing. The source code can be found in the folder /src/ including the main.py of the project.

## Installation

DEV enviroment.

### Pre-requisite: 
* Spark

* Virtualenv for Python

```bash
pip install virtualenv 
```
```bash
python -m venv .appeventprocessingenv
```
```bash
source .\.appeventprocessingenv\Scripts\activate 
```
Install all the dependencies inside the virtualenv.

```bash
pip install -r .\requirements.txt
```

Run the main:

```bash
python main.py
```
