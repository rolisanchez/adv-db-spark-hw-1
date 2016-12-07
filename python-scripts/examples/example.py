#!/usr/bin/python
#-*- coding: latin-1 -*-
__author__ = 'Victor R. Sanchez J.'

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from datetime import datetime
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
from pyspark import SparkConf, SparkContext

def main():
    """
    Main function to execute the HW assignments
    """

    print "Adv DB HW"

    conf = (SparkConf()
             .setMaster("local")
             .setAppName("Adv DB HW app")
             .set("spark.executor.memory", "1g"))
    sc = SparkContext(conf = conf)

    # Define a display when no DISPLAY is undefined
    matplotlib.use('Agg')

    # Import weather dataset
    weatherDataset = sc.textFile("/datasets/2016weather.csv")

    # Import user dataset
    userDataset = sc.textFile("/datasets/userdata.csv")

    # Parse datasets
    weatherParse = weatherDataset.map(lambda line : line.split(","))
    userParse = userDataset.map(lambda line : line.split(","))

    # Work on datasets
    weatherSnow = weatherParse.filter(lambda x: x[2]=="SNOW")

    # Just testing
    firstWeather = weatherParse.first()
    print "firstWeather is ", firstWeather

    # instantiate SQLContext object
    sqlContext = SQLContext(sc)

    # Convert each line of snowWeather RDD into a Row object
    snowRows= weatherSnow.map(lambda p: Row(station=p[0], month=datetime.strptime(p[1], '%Y%m%d').month, date=datetime.strptime(p[1], '%Y%m%d').day,metric=p[2], value=int(p[3])))

    # Apply Row schema
    snowSchema = sqlContext.createDataFrame(snowRows)

    # Register 'snow2016' table with 5 columns: station, month, date, metric, and value
    snowSchema.registerTempTable("snow2016")
    sqlContext.cacheTable("snow2016")

    snow_US10chey021 = sqlContext.sql("SELECT month, COUNT(*) AS snowdays FROM snow2016 WHERE station='US10chey021' GROUP BY month ORDER BY month").collect()

    snow_US10chey021[0]

    # list of 12 values are 0
    US10chey021_snowdays_y=[0] * 12
    for row in snow_US10chey021:
        US10chey021_snowdays_y[row.month - 1]=row.snowdays

    print (US10chey021_snowdays_y)

    snow_USW00094985 = sqlContext.sql("SELECT  month, COUNT(*) AS snowdays FROM snow2016 WHERE station='USW00094985' GROUP BY month ORDER BY month").collect()

    USW00094985_snowdays_y=[0] * 12
    for row in snow_USW00094985:
        USW00094985_snowdays_y[row.month -1]=row.snowdays

    print (USW00094985_snowdays_y)

    """
        Print figures
    """

    N=12
    ind=np.arange(N)
    width = 0.35
    pUS10chey021 = plt.bar(ind, US10chey021_snowdays_y, width, color='g', label='US10chey021')
    pUSW00094985 = plt.bar(ind+width, USW00094985_snowdays_y, width, color='y', label='USW00094985')

    plt.ylabel('SNOW DAYS')
    plt.xlabel('MONTH')
    plt.title('Snow Days in 2016 at Stations US10chey021 vs. USW00094985')
    plt.xticks(ind+width, ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'))
    plt.legend()

    plt.savefig('/python-scripts/examples/weatherfig.png')

    plt.show()

if __name__ == "__main__":
        main()
