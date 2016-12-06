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

    weather = sc.textFile("/datasets/2016.csv")

    weatherParse = weather.map(lambda line : line.split(","))
    weatherSnow = weatherParse.filter(lambda x: x[2]=="SNOW")

    test = weatherParse.first()
    print "test is ", test


if __name__ == "__main__":
        main()
