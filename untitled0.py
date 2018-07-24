# -*- coding: utf-8 -*-
"""
Created on Tue Jul 17 20:27:03 2018

@author: Think
"""

from pyspark import SparkContext
sc = SparkContext.getOrCreate()
#第二个参数2代表的是分区数，默认为1
old=sc.parallelize([1,2,3,4,5],2)
newMap = old.map(lambda x:(x,x**2))
newReduce = old.reduce(lambda a,b : a+b)
print(newMap.glom().collect())
print(newReduce)
