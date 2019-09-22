# Hadoop
MapReduce Word Count project
> In MapReduce word count example, we find out the frequency of each word. Here, the role of Mapper is to map the keys to the existing values and the role of Reducer is to aggregate the keys of common values. So, everything is represented in the form of Key-value pair.

## Requirements

* Java Installation - Check whether the Java is installed or not using the following command.
  
  ``` java -version ```
* Hadoop Installation - Check whether the Hadoop is installed or not using the following command.
  
  ``` hadoop version ```
  
> <b>Step 1</b>.
  Create a directory in HDFS, where to kept text file.
  
  ``` hdfs dfs -mkdir /microtema ```
  
> <b>Step 2</b>.
  Upload the data.txt file on HDFS in the specific directory.
  
  ``` hdfs dfs -put /Users/mtema/Documents/DEVELOPER/PROJECTS/HADOOP/hadooptest/hdfs/data.txt /microtema ```
  
> You can do this step over http://localhost:9870/explorer.html#/

> <b>Step 3</b>
  Write the MapReduce program
  

