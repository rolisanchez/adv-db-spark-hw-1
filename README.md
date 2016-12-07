# adv-db-spark-hw-1
Spark Homework for Advanced Databases 2016 NTHU

The Docker files were modified for a Homework on Advanced Databases 
Based on Getty Images "https://github.com/gettyimages/docker-spark"

# Problem description

Using the Weather Dataset provided:

 - Show the top 100 average precipitations for each Station.

Using the Product Rating Dataset provided:

- What are the records that are rated for item_ID 1-10 from user_ID 1-10, and rating is larger than 3 (not included). Order by rating (descending).


# Running this project
To run the container:
    
    docker-compose up
    
If there are changes made, rebuild:
    
    docker-compose build
    
To enter the container and run scripts from inside:

    docker exec -it advdbsparkhw1_master_1 /bin/bash

The python-scripts folder is mounted at /python-scripts inside the container, so files placed there can be executed like:

    python /python-scripts/some_python_script.py

The datasets folder is mounted at /datasets inside the container, so files placed there can be accesed inside the container.