# Datamonster
Datamonster is a MapReduce abstraction library for SQL and HBase-oriented applications

# What is it really?
It's a library to do SQL/HBase-oriented hadoop jobs. It provide an abtraction layer that makes job creation easier than in "pure hadoop"
It's primary usage is to make pertinence score calculations, but it should find an usage in many other kind of tasks.

# Exemples
Some examples are (or will be) available in the examples folder:
* SqlToSql -> fetch data from a SQL (MariaDB/MySQL) database and write it in the same database

# Libraries
When building my jobs using Datamonster, i use these libraries
* datamonster (obviously)
* hadoop-annotations
* hadoop-common
* hadoop-mapreduce-client-core
* hbase-client
* hbase-common
* hbase-hadoop2-compat
* hbase-protocol
* hbase-server
* high-scale-lib
* htrace-core


A more complete documentation will come soon, i'm still writing it, there's javadoc comments in the source, though.
