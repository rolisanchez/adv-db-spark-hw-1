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

if __name__ == "__main__":
        main()
