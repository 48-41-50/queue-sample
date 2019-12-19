# queue-sample

A sample implementation of a multi-subscriber queue system in python and postgres

## Requirements

* docker
* docker-compose
* Python 3.7

## Environment

It is recommended that you create a Python virutal environment and then run `pip install -r ./requirements.txt` to get all required packages for your virtual environment.

## Components

* postgres:12 docker container
* python::3.7 docker container (for the queue server)

### PostgreSQL Bootstrap

The bootstrap file `init/10_create_schema.sql` will create the tables to hold topics, topic messages and topic subscribers. Also there are a series of functions to provide a clean and standardized means to access and manipulate the data in these tables.

Functions are used to keep intermediate queries from having to make a full trip to the application layer. This also has an advantage that the application can be a bit simpler in that so many queries are not required in the application code. Also if a better strategy is discovered that does not change the function signature, then the code improvements can be deployed to the database without having a application deploy.

### Python Files

#### Server

The server file `http/queue_server.py` contains the web server code definition for the functionality of the queue server. This is setup to have routes directly mapped to class methods. Inputs and outputs are in url parameters or via json data.

#### Publisher

The publisher file `http/queue_publisher.py` utilizes the requests module to manipulate topics and post messages to topics. Topics can be reset to an arbitrary offset. This actually has the effect of resetting all subscribers of a topic to the arbitrary offset. This will have the effect of not requiring new message publishes and the messge getter code undergoes no change.

#### Subscriber

The subscriber file `http/queue_subscriber.py` utilized the requests module to get messages for a particular topic and acknowledge their receipt.

## Stress Test Script

The script `queue_stress.bash` is intended to push the server, publisher, subscriber code hard to determine errors and to improve the service over time.

## Manual Initialization

Using `docker-compose` the environment can be initialized in the following manner:

1. Activate the virtual environment
2. Execute `docker-compose up -d server` (omit the -d to see log messages directly)
3. Change to the http directory `cd ./http`
4. Launch python
5. Import the `queue_publisher` and/or `queue_subscriber` modules
6. Uitlize `QPublisher` and/or `QSubscriber` class instances to interact with the server


