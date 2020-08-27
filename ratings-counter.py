#!/usr/bin/env python
# coding: utf-8

# In[4]:


import findspark


# In[7]:


findspark.init('/home/ubuntu/spark-3.0.0-bin-hadoop2.7')


# In[9]:


import pyspark


# In[11]:


from pyspark import SparkConf, SparkContext
import collections


# In[12]:


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")


# In[13]:


sc = SparkContext(conf = conf)


# In[15]:


lines = sc.textFile("file:///home/ubuntu/Documents/PySpark/DownloadedData/ml-100k/u.data")


# In[17]:


ratings = lines.map(lambda x: x.split()[2])


# In[18]:


result = ratings.countByValue()


# In[19]:


sortedResults = collections.OrderedDict(sorted(result.items()))


# In[23]:


sortedResults


# In[29]:


for key, value in sortedResults.items():
    print("Key: %s Value: %i"% (key, value))

