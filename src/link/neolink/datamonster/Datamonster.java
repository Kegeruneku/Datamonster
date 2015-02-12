package link.neolink.datamonster;

/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Hadoop general imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Tool;

// MapReduce imports
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

// HBase imports
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A MapReduce abstraction class for SQL and HBase-oriented jobs
 *
 * {@link DBUpdateOutputFormat} accepts SQL and HBase
 * for both input and output, SQL output can be INSERT, UPDATE,
 * or the so-called DELINSERT,which act like INSERT,
 * but does a TRUNCAT before running the job.
 * It doesn't support HBase filters for now (don't hesitate to add it ;) )
 *
 */
public class Datamonster extends Configured implements Tool
{
   // Some may argue that i should name rename MAGIC as MOREMAGIC...
   public enum Type {MAGIC, SELECT, UPDATE, INSERT, DELINSERT, HBASE};

   // The (italian) job
   private Job job = null;

   // General configuration
   private Type inType = Type.MAGIC;
   private Type outType = Type.MAGIC;

   // Writable class
   private Class<?> dataWritable = null;
   private Class<? extends DBWritable> inputWritable = null;
   private Class<? extends DBWritable> outputWritable = null;
   private Class<? extends DBWritable> ioWritable = null;

   // SQL driver/type and credentials
   private String sqlDriver = "com.mysql.jdbc.Driver";
   private String sqlURLType = "mysql";
   private String sqlURL = "localhost";
   private String sqlUser = "root";
   private String sqlPassword = "";

   // SQL-specific configuration
   private String outTable = "";
   private DBConfiguration sqlConfig = null;

   /**
    * Create the job and set it's name
    *
    * @param name
    *    The job's name
    */
   public Datamonster(String name) throws Exception
   {
      Configuration conf = HBaseConfiguration.create();

      this.setConf(conf);
      this.job = Job.getInstance(this.getConf(), name);
      this.job.setJarByClass(super.getClass());
   }

   /**
    * Set the SQL credentials
    *
    * @param URL
    *    The SQL URI, in JDBC format
    * @param user
    *    The SQL username
    * @param password
    *    The SQL password
    */
   public void setSQLCredentials(String URL, String user, String password)
   {
      this.sqlURL = URL;
      this.sqlUser = user;
      this.sqlPassword = password;
   }

   /**
    * Set the SQL credentials
    *
    * @param host
    *    The SQL host
    * @param database
    *    The SQL database
    * @param user
    *    The SQL username
    * @param password
    *    The SQL password
    */
   public void setSQLCredentials(String host, String database, String user, String password)
   {
      this.sqlURL = "jdbc:"+this.sqlURLType+"://"+host+"/"+database;
      this.sqlUser = user;
      this.sqlPassword = password;
   }

   /**
    * configure the JDBC driver
    *
    * @param driver
    *          The SQL Driver, default is com.mysql.jdbc.Driver
    */
   public void setSQLDriver(String driver, String URLType)
   {
      this.sqlDriver = driver;
      this.sqlURLType = URLType;
   }

   /**
    * Writables configuration
    *
    * @param dataWritable
    *    The writable that goes between the mapper and the reducer
    */
   public void setWritables(Class<?> dataWritable)
   {
      this.dataWritable = dataWritable;
   }

   /**
    * Writables configuration
    *
    * @param dataWritable
    *    The writable that goes between the mapper and the reducer
    * @param ioWritable
    *    The writable that goes either between the SQL and the mapper or the reducer and SQL
    */
   public void setWritables(Class<?> dataWritable, Class<? extends DBWritable> ioWritable)
   {
      this.dataWritable = dataWritable;
      this.ioWritable = ioWritable;
   }

   /**
    * Writables configuration
    *
    * @param dataWritable
    *    The Writable that goes between the mapper and the reducer
    * @param inputWritable
    *    The Writable that goes between the SQL and the mapper
    * @param outputWritable
    *    The Writable that goes between the reducer and the SQL
    */
   public void setWritables(Class<?> dataWritable, Class<? extends DBWritable> inputWritable, Class<? extends DBWritable> outputWritable)
   {
      this.dataWritable = dataWritable;
      this.inputWritable = inputWritable;
      this.outputWritable = outputWritable;
   }

   /**
    * Convert a string to a Type
    *
    * @param outType
    *    The output type, as a string.
    */
   private void setOutType(String outType)
   {
      if (outType.equals("INSERT"))
      {
         this.outType = Type.INSERT;
      }
      else if (outType.equals("UPDATE"))
      {
         this.outType = Type.UPDATE;
      }
      else if (outType.equals("DELINSERT"))
      {
         this.outType = Type.DELINSERT;
      }
      else
      {
         System.out.println("Warning: Your SQL OutType is invalid");
      }
   }

   /**
    * Mapper configuration, with a dataset from SQL/*
    *
    * @param query
    *    The SQL Query
    * @param mapper
    *    The mapper class
    */
   public void mapperConfiguration(String query, Class<? extends Mapper> mapper)
   {
      if (this.inType != Type.MAGIC)
      {
         System.err.println("Input type already defined.");
         System.exit(-1);
      }
      this.inType = Type.SELECT;
      this.job.setMapperClass(mapper);
      this.job.setMapOutputKeyClass(Text.class);
      this.job.setMapOutputValueClass(this.dataWritable);
      this.job.setInputFormatClass(DBInputFormat.class);

      if (this.inputWritable != null)
      {
         DBInputFormat.setInput(this.job, this.inputWritable, query, "SELECT COUNT(*) FROM (" + query + ") AS query_count");
      }
      else if (this.ioWritable != null)
      {
         DBInputFormat.setInput(this.job, this.ioWritable, query, "SELECT COUNT(*) FROM (" + query + ") AS query_count");
      }
      else
      {
         System.err.println("Input is SQL, but no input Writable class defined");
         System.exit(-1);
      }
   }

   /**
    * Mapper configuration, with a dataset from HBase
    *
    * @param inputTable
    *    The HBase table
    * @param families
    *    Input table's families
    * @param mapper
    *    La classe du mapper
    */
   public void mapperConfiguration(String inputTable, String[] families, Class<? extends TableMapper> mapper) throws Exception
   {
      if (this.inType != Type.MAGIC)
      {
         System.err.println("Input type already defined");
         System.exit(-1);
      }

      this.inType = Type.HBASE;

      // Add families to the query
      Scan scan = new Scan();
      for (String family : families)
      {
         scan.addFamily(Bytes.toBytes(family));
      }
      TableMapReduceUtil.initTableMapperJob(inputTable, scan, mapper, Text.class, this.dataWritable, this.job);
   }

   /**
    * Reducer configuration, with SQL
    *
    * @param outType
    *    Kind of output query, either "INSERT", "UPDATE" or "DELINSERT"
    * @param outTable
    *    The output table
    * @param outFields
    *    The output table's fields
    * @param reducer
    *    The reducer class
    */
   public void reducerConfiguration(String outType, String outTable, String[] outFields, Class<? extends Reducer> reducer) throws Exception
   {
      if (this.outType != Type.MAGIC)
      {
         System.err.println("Output type already defined");
         System.exit(-1);
      }

      this.job.setReducerClass(reducer);
      this.setOutType(outType);
      job.setOutputValueClass(NullWritable.class);

      this.outTable = outTable;

      if (this.outputWritable != null)
      {
         job.setOutputKeyClass(this.outputWritable);
      }
      else if (this.ioWritable != null)
      {
         job.setOutputKeyClass(this.ioWritable);
      }
      else
      {
         System.err.println("Output is SQL, but no output Writable class defined");
         System.exit(-1);
      }

      // Define the OutputFormatClass (it's not the same between an INSERT/DELINSERT and a UPDATE)
      if (this.outType == Type.INSERT || this.outType == Type.DELINSERT)
      {
         this.job.setOutputFormatClass(DBOutputFormat.class);
      }
      else if (this.outType == Type.UPDATE)
      {
         this.job.setOutputFormatClass(DBUpdateOutputFormat.class);
      }

      // Configura the DBOutputFormat (same thing, it differs bewteen INSERT and UPDATE)
      if (this.outType == Type.INSERT || this.outType == Type.DELINSERT)
      {
         DBOutputFormat.setOutput(job, outTable, outFields);
      }
      else if (this.outType == Type.UPDATE)
      {
         DBUpdateOutputFormat.setOutput(job, outTable, outFields);
      }
   }

   /**
    * Reducer configuration, for SQL
    *
    * @param outType
    *    Kind of output query, either "INSERT", "UPDATE" or "DELINSERT"
    * @param outTable
    *    The output table
    * @param outFields
    *    The output table's fields
    * @param outConditions
    *    The output table's conditions
    * @param reducer
    *    The reducer class
    */
   public void reducerConfiguration(String outType, String outTable, String[] outFields, String[] outConditions, Class<? extends Reducer> reducer) throws Exception
   {
      if (this.outType != Type.MAGIC)
      {
         System.err.println("Output type already defined!");
         System.exit(-1);
      }

      if (!outType.equals("UPDATE"))
      {
         System.err.println("Conditional fields are for UPDATEs only");
         System.exit(-1);
      }

      this.job.setReducerClass(reducer);
      this.setOutType(outType);
      this.outTable = outTable;
      if (this.outputWritable != null)
      {
         job.setOutputKeyClass(this.outputWritable);
      }
      else if (this.ioWritable != null)
      {
         job.setOutputKeyClass(this.ioWritable);
      }
      else
      {
         System.err.println("Output is SQL, but no output Writable class defined");
         System.exit(-1);
      }
      job.setOutputValueClass(NullWritable.class);
      this.job.setOutputFormatClass(DBUpdateOutputFormat.class);

      DBUpdateOutputFormat.setOutput(job, outTable, outFields, outConditions);
   }

   /**
    * Reducer configuration, for HBase
    *
    * @param outTable
    *    The output table
    * @param reducer
    *    The reducer class
    */
   public void reducerConfiguration(String outTable, Class<? extends TableReducer> reducer) throws Exception
   {
      if (this.outType != Type.MAGIC)
      {
         System.err.println("Output type already defined");
         System.exit(-1);
      }
      this.outType = Type.HBASE;
      TableMapReduceUtil.initTableReducerJob(outTable, reducer, job);
   }

   @Override
   public int run(String[] args) throws Exception
   {
      if (inType == Type.MAGIC || outType == Type.MAGIC)
      {
         System.err.println("Input and/or output type not defined");
         return -1;
      }

      // If you have both input and output as SQL, and only configured ioWritable, you gonna have a bad time.
      if (inType != Type.HBASE && outType != Type.HBASE && (this.inputWritable == null || this.outputWritable == null))
      {
         System.out.println("Warning: Both input and output are SQL, but Writables class saren't corretly defined, maybe you only defined one class?");
      }

      // SQL configuration (if either input or output isn't HBase)
      if (inType != Type.HBASE || outType != Type.HBASE)
      {
         DBConfiguration.configureDB(job.getConfiguration(), this.sqlDriver, this.sqlURL, this.sqlUser, this.sqlPassword);
      }

      // Do an armageddon on the output table (if we do a DELINSERT)
      if (this.outType == Type.DELINSERT)
      {
         this.sqlConfig = new DBConfiguration(job.getConfiguration());
         this.sqlConfig.getConnection().createStatement().executeUpdate("DELETE FROM " + outTable);
      }

      // Run and wait for the job
      return this.job.waitForCompletion(true)? 0:1;
   }
}
