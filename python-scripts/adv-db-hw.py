#!/usr/bin/python
#-*- coding: latin-1 -*-
__author__ = 'Victor R. Sanchez J.'

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from datetime import datetime
import matplotlib
# Define a display when no DISPLAY is undefined - In this case because of Docker
matplotlib.use('Agg')
import numpy as np
import matplotlib.pyplot as plt
from pyspark import SparkConf, SparkContext

def main():
    """
    Main function to execute the HW assignments
    """

    print "Adv DB HW"
    print "#105065428 Victor Rolando Sanchez Jara"


    conf = (SparkConf()
             .setMaster("local")
             .setAppName("Adv DB HW app")
             .set("spark.executor.memory", "1g"))
    sc = SparkContext(conf = conf)

    # Using the Weather Dataset provided:
    # Get the top 100 (from high to less, DECS) average precipitation for each station.
    # Based on this value to get the top 100 station record, and the output format should be like:
    # Station US1TXGG0002 had average precipitations of 3056.000000

    print "Working on weather data"

    # Import weather dataset
    weatherDataset = sc.textFile("/datasets/2016weather.csv")

    # Parse weather dataset
    weatherParse = weatherDataset.map(lambda line : line.split(","))

    # Work on weather dataset
    weatherPrecp = weatherParse.filter(lambda x: x[2]=="PRCP")

    weatherPrecpCountByKey = weatherPrecp.map(lambda x : (x[0], (int(x[3]), 1)))
    print "First weatherPrecipitationCountByKey"
    print weatherPrecpCountByKey.first()

    weatherPrecpAddByKey = weatherPrecpCountByKey.reduceByKey(lambda v1,v2 : (v1[0]+v2[0], v1[1]+v2[1]))
    print "First weatherPrecpAddByKey"
    print weatherPrecpAddByKey.first()

    weatherAverages = weatherPrecpAddByKey.map(lambda k: (k[0], k[1][0] / float(k[1][1] ) ) )
    print "First weatherAverages"
    print weatherAverages.first()

    f = open('/python-scripts/weatherAverages.txt', 'w')

    precTop100=[]
    stationsTop100=[]
    for pair in weatherAverages.map(lambda (x,y) : (y,x)).top(100):
        precTop100.append(pair[0])
        stationsTop100.append(pair[1])
        strWrite = "Station %s had average precipitations of %f \n" % (pair[1],pair[0])
        f.write(strWrite)

    f.close()
    print "Saved top 100 average precipitations to /python-scripts/weatherAverages.txt"

    N = 100
    index = np.arange(N)
    bar_width = 0.5
    plt.bar(index, precTop100, bar_width,
    color='b')
    plt.xlabel('Stations')
    plt.ylabel('Precipitations')
    plt.title('100 stations with the highest average precipitation')
    # plt.xticks(index + bar_width, stationsTop100, rotation=90)
    plt.savefig('/python-scripts/top100precipitations.png')
    print "Saved top 100 average precipitations plot to /python-scripts/top100precipitations.png"


    # Using the Product Rating Dataset provided:
    # What are the records that are rated for item_ID 1-10 from user_ID 1-10,
    # and rating is larger than 3 (not included). Order by rating (descending).

    print "Working on user ratings data"

    # Import user ratings dataset
    userDataset = sc.textFile("/datasets/userdata.csv")

    # Parse user ratings dataset
    userParse = userDataset.map(lambda line : line.split(","))

    # Work on user ratings dataset

    # userRatingsFiltered = userParse.filter(lambda line: line!=small_movies_raw_data_header).map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1])).cache()
    # userRatingsFiltered = userParse.filter(lambda x: (x[0],x[1])).cache()
    # userRatingsFiltered = userParse.filter(lambda x: (x[0],x[1],x[2]))
    # users1to10 = userParse.filter(lambda x: (x[0]>=1 and x[0]<=10))
    # users1to10 = userParse.filter(lambda x: (int(x[0])<=10))

    # users1to10 = userParse.filter(lambda x: (x[0]))

    print "users1to10"
    print users1to10.take(3)

    # print "userRatingsFiltered"
    # print userRatingsFiltered.take(3)
if __name__ == "__main__":
        main()
