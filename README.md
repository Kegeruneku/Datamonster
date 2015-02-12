# Datamonster
Datamonster is a MapReduce abstraction library for SQL and HBase-oriented applications

# What is it really?
It's a library to do SQL/HBase-oriented hadoop jobs. It provide an abtraction layer that makes job creation easier than in "pure hadoop"

# Howto (Examples will be published soon)
Example for a SQL to SQL job  
```java
Datamonster sqlmonster = new Datamonster("SQLMonster");
sqlmonster.setSQLCredentials("URL", "USER", "PASS");
sqlmonster.setWritables(IntWritable.class, DBInputWritable.class, DBOutputWritable.class);
sqlmonster.mapperConfiguration("YOUR SQL QUERY", Map.class);
// outputTable -> String // outputFields and conditionalFields -> String[]
sqlmonster.reducerConfiguration("UPDATE", outputTable, outputFields, conditionalFields, Reduce.class);
System.exit(ToolRunner.run(job, args));
```

A more complete documentation will come soon, i'm still writing it, there's javadoc comments in the source, though.  
