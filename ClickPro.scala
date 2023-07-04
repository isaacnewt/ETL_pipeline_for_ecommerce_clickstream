/*
 * The program describes ETL pipeline design for Clickstream analytics using 
 * Apache Kafka as an ingestion engine and Apache Spark as the Sink for analysing
 * the data. The program includes a database engine such as Postgre DB for   
 * storage purposes 
 * 
 */

// scalastyle:off println

package com.spark_kafka // package definition 

/**
 * Consumes messages from one or more topics in Kafka and does clickstream
 * analytics.
 * Usage: StructuredKafkaWordCount <bootstrap-servers> <subscribe-type> <topics>
 *     [<checkpoint-location>]
 *   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
 *   comma-separated list of host:port.
 *   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
 *   'subscribePattern'.
 *   |- <assign> Specific TopicPartitions to consume. Json string
 *   |  {"topicA":[0,1],"topicB":[2,4]}.
 *   |- <subscribe> The topic list to subscribe. A comma-separated list of
 *   |  topics.
 *   |- <subscribePattern> The pattern used to subscribe to topic(s).
 *   |  Java regex string.
 *   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
 *   |  specified for Kafka source.
 *   <topics> Different value format depends on the value of 'subscribe-type'.
 *   <checkpoint-location> Directory in which to create checkpoints. If not
 *   provided, defaults to a randomized directory in /tmp.
 *
 * Example:
 *    `$ spark-submit \
 *      com.spark_kafka.ClickProgram localhost:9092 \
 *      subscribe topic1,topic2 --packages \
 *      org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.3`
 */

//import the required libraries 

import org.apache.spark.sql.{SparkSession,DataFrame,ForeachWriter,Dataset,Row}  
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col 
//import za.co.absa.abris.avro.functions.from_avro
//import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}
import spark.implicits._

import java.util.{Properties, UUID}


import java.util.UUID
import java.sql._

object ClickPro {

  def main(args: Array[String]): Unit = {
  
  if (args.length < 3) {
      System.err.println("Usage: Clicktream analytics <bootstrap-servers> " +
        "<subscribe-type> <topics> [<checkpoint-location>]")
      System.exit(1)
    }

    val Array(bootstrapServers, subscribeType, topics, _*) = args
    val checkpointLocation =
      if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString
    
    val spark = SparkSession
      .builder
      .appName("ClickstreamETL")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR") 
    //org.apache.spark.sql.kafka010.KafkaSourceProvider
    val kafkaStructuredStream = spark.readStream.format("kafka") 
                  .option("subscribe", "clickers") //KafkaDataConsumer
                  .option("failOnDataLoss", "false")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .load()

    // var kafkaSelectDS = kafkaStructuredStream
    //      .select((dg.cast("string"), "$.zip").alias("county")).groupBy($"curr").count()
         
    // display(kafkaSelectDF)
    
import spark.implicits._   
    
    val kafkaSelectDS = kafkaStructuredStream
         .selectExpr("CAST (value AS STRING)").as[String] 
         
    val kafkaSplit = kafkaSelectDS.withColumn("value", split(col("value"), "\t"))

    val prev = regexp_replace(kafkaSplit.col("value").getItem(0), "\\[\\w+\\]:","")
    val curr = regexp_replace(kafkaSplit.col("value").getItem(1), "\\[\\w+\\]:","")
    val link = regexp_replace(kafkaSplit.col("value").getItem(2), "\\[\\w+\\]:","")
    val n = regexp_replace(kafkaSplit.col("value").getItem(3), "\\[\\w+\\]:","")

import org.apache.spark.sql.types._

    val reformedDF = kafkaSplit
         .withColumn("prev", prev)
         .withColumn("curr", curr)
         .withColumn("link", link)
         .withColumn("n", n cast LongType) // (col("n").cast(Long)))
         .drop("value")
         //.where("address = '/cursor'")
         
    // trafic per zip code for a 10 minute window interval, with sliding duration of 5 minutes starting 2 minutes past the hour    
    
    
   val postgreswriter = new JDBCSinkClass(url, user, pwd)
   
   val query = reformedDF
        .writeStream              
        .foreach(postgreswriter)
        .outputMode("append")
        .trigger(ProcessingTime("40 seconds"))         
        .start()
   
   print("Connection starting...")
   query.awaitTermination(5)
   
   }
    
    // var reformedDataDF = reformedDF.groupBy($"zip", window($"hit".cast("timestamp"), "10 minute", "5 minute", "2 minute")).count()
         
    // display(reformedDataDF)


// Postgres Database class 

// Database implementaion code
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import java.sql._

      
   val url = "jdbc:postgresql://localhost:5432/test" //<dbserver>:port/test
   val user = "postgres"
   val pwd = "postgres"

   class JDBCSinkClass(url:String, user:String, pwd:String) extends ForeachWriter[Row] {
       val driver = "org.postgresql.Driver"
       var connection:Connection = _
       var statement:PreparedStatement = _
       
       val query_statement = "INSERT INTO clickstreamTable (prev, curr, link, n) VALUES (?, ?, ?,(? cast as long))"
       
       def open(partitionId: Long, version: Long): Boolean = {
          Class.forName(driver)
          connection = DriverManager.getConnection(url,user,pwd)
          connection.setAutoCommit(false)
          statement = connection.prepareStatement(query_statement)
          true
       }
       // "INSERT INTO clickstreamTable" + "VALUES (" +
       //   value._1 + "," + value._2 + "," + value._3 + "," + value._4 + ")"
           
       def process(value: Row): Unit = {
           statement.setString(0, value(0).toString)
           statement.setString(1, value(1).toString)
           statement.setString(2, value(2).toString)
           statement.setString(3, value(3).toString)
           statement.executeUpdate()
       }
       
       def close(errorOrNull: Throwable): Unit = {
           connection.commit()
           connection.close
       }
   
   }
   
   }
}
}

****************************************************************
//println(query.status)
//println(query.lastProgress)
//println(query.exception)
//query.explain()

/** CREATE TABLE clickstreamTable(
 *    prev VARCHAR,
 *    curr VARCHAR,
 *    link VARCHAR,
 *    n int
 *  );
 * SELECT * FROM clickstreamTable;
 */
// Database implementaion code
package com.spark_kafka // package definition 


import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import java.sql._


//import the required libraries 

import org.apache.spark.sql.{SparkSession,DataFrame,ForeachWriter,Dataset,Row}  
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col 
//import za.co.absa.abris.avro.functions.from_avro
//import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}
import spark.implicits._

import java.util.{Properties, UUID}


import java.util.UUID
import java.sql._

object ClickPro extends App {
  
    val spark = SparkSession
      .builder
      .appName("ClickstreamETL")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR") 
    //org.apache.spark.sql.kafka010.KafkaSourceProvider
    val kafkaStructuredStream = spark.readStream.format("kafka") 
                  .option("subscribe", "clickers") //KafkaDataConsumer
                  .option("failOnDataLoss", "false")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .load()
    
import spark.implicits._   
    
    val kafkaSelectDS = kafkaStructuredStream
         .selectExpr("CAST (value AS STRING)").as[String] 
         
    val kafkaSplit = kafkaSelectDS.withColumn("value", split(col("value"), "\t"))

    val prev = regexp_replace(kafkaSplit.col("value").getItem(0), "\\[\\w+\\]:","")
    val curr = regexp_replace(kafkaSplit.col("value").getItem(1), "\\[\\w+\\]:","")
    val link = regexp_replace(kafkaSplit.col("value").getItem(2), "\\[\\w+\\]:","")
    val n = regexp_replace(kafkaSplit.col("value").getItem(3), "\\[\\w+\\]:","")

import org.apache.spark.sql.types._

    val reformedDF = kafkaSplit
         .withColumn("prev", prev)
         .withColumn("curr", curr)
         .withColumn("link", link)
         .withColumn("n", n cast LongType) // (col("n").cast(Long)))
         .drop("value")
         //.where("address = '/cursor'")

      
   val url = "jdbc:postgresql://localhost:5432/test" //<dbserver>:port/test
   val user = "postgres"
   val pwd = "postgres"
   
   val postgreswriter = new ClickPro(url, user, pwd)
   
   val query = reformedDF
        .writeStream              
        .format("console")
        .outputMode("append")
        .trigger(ProcessingTime("40 seconds"))         
        .start()
        
   print("Connection starting...")
   query.awaitTermination(5)

// Database implementaion code
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import java.sql._
  
   class ClickPro(url:String, user:String, pwd:String) extends ForeachWriter[Row] {
       val driver = "org.postgresql.Driver"
       var connection:Connection = _
       var statement:Statement = _
       
       def open(partitionId: Long, version: Long): Boolean = {
          Class.forName(driver)
          connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test","postgres","postgres")
          statement = connection.createStatement
          true
       }
           
       def process(value: Row): Unit = {
           statement.executeUpdate("replace into clickstreamTable(prev, curr, link, n) values("
                                    + "'" + value.getString(0) + "'" + "," //prev
                                    + "'" + value.getString(1) + "'" + "," //curr
                                    + "'" + value.getString(2) + "'" + "," //link
                                    +  value.getLong(3) //n
                                    )
       }
       
       def close(errorOrNull: Throwable): Unit = {
           connection.commit()
           connection.close
       }
   
   }

}  

//*****************************************************
   val query = reformedDF
        .writeStream              
        .foreach(postgreswriter)      
        .start()
        
   print("Connection starting...")
   query.awaitTermination(5)
   
val query = reformedDF
        .writeStream              
        .format("console")
        .outputMode("append")
        .trigger(ProcessingTime("40 seconds"))         
        .start()
        
   print("Connection starting...")
   query.awaitTermination(5)
   
//*************************************************************
// kafkaSplit.writeStream.format("console").start()

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import java.sql._


//import the required libraries 
import org.apache.spark.sql.{SparkSession,DataFrame,ForeachWriter,Dataset,Row}  
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col 
import org.apache.spark.sql.types._
//import za.co.absa.abris.avro.functions.from_avro
//import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}

import java.util.{Properties, UUID}


import java.util.UUID
  
    val spark = SparkSession
      .builder
      .appName("ClickstreamETL")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR") 
    //org.apache.spark.sql.kafka010.KafkaSourceProvider
    val kafkaStructuredStream = spark.readStream.format("kafka") 
                  .option("subscribe", "clickers") //KafkaDataConsumer
                  .option("failOnDataLoss", "false")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .load()
    
import spark.implicits._   
    
    val kafkaSelectDS = kafkaStructuredStream
         .selectExpr("CAST (value AS STRING)").as[String] 
         
    val kafkaSplit = kafkaSelectDS.withColumn("value", split(col("value"), "\\t"))
    
    kafkaSplit.printSchema()
    
    //kafkaSplit.writeStream.format("console").start()
    //val kafkaSplit2 = spark.createDataFrame(kafkaSplit)
    //df = df.select("col_1", *[col("gen_cols").getItem(i).alias("gen_cols{i+1}") for i in range(0,4)])

    val reformedDF = kafkaSplit.select(
                 kafkaSplit.col("value").getItem(0).alias("prev"),
                 kafkaSplit.col("value").getItem(1).alias("curr"),
                 kafkaSplit.col("value").getItem(2).alias("link"),
                 kafkaSplit.col("value").getItem(3).alias("n")
                 )
                 .registerTempTable("clickQuery")
                 //.groupBy("curr")
                 //.count()
      
   val url = "jdbc:postgresql://localhost:5432/test" //<dbserver>:port/test
   val user = "postgres"
   val pwd = "postgres"
   
   val postgreswriter = new ClickPro(url, user, pwd)
        
   val query = reformedDF
        .writeStream              
        .format("console")
        .outputMode("append")
        .trigger(ProcessingTime("40 seconds"))         
        .start()
        
   reformedDF.writeStream.foreach(postgreswriter).start()
        
        
   print("Connection starting...")
   query.awaitTermination(5)

// Database implementaion code
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row

   class ClickPro(url:String, user:String, pwd:String) extends ForeachWriter[Row] {
       val driver = "org.postgresql.Driver"
       var connection:Connection = _
       var statement:PreparedStatement = _
       
       def open(partitionId: Long, version: Long): Boolean = {
          Class.forName(driver)
          connection = DriverManager.getConnection(url,user,pwd)
          statement = connection.createStatement
          true
       }
           
       def process(value: Row): Unit = {
           statement.executeUpdate("replace into clickstreamTable(prev, curr, link, n) values("
                                    + "'" + value.getString(0) + "'" + "," //prev
                                    + "'" + value.getString(1) + "'" + "," //curr
                                    + "'" + value.getString(2) + "'" + "," //link
                                    +  value.getLong(3) //n
                                    )
       }
       
       def close(errorOrNull: Throwable): Unit = {
           connection.commit()
           connection.close
       }
   
   }

// *************************************************
   reformedDF.writeStream.foreachBatch { (micro_batchDF: DataFrame,batchId: Long) =>
   batchDF.persist()
   batchDF.write.format("csv").save(...)
   batchDF.write.format("jdbc").save(...)
   batchDF.unpersist()
        }
        
// *************************
//import za.co.absa.abris.avro.functions.from_avro
//import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}

   val baseCheckpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString
   val PostgreDb = "test"

  def writePostgreTable(df: DataFrame, name: String) = {     
      val fullTableName = s"$PostgreDb.name"        
      df.writeStream         
        .format("jdbc")         
        .option("user", "postgres")         
        .option("dbtable", fullTableName)         
        .option("checkpointLocation", s"$baseCheckpointLocation $name")         
        .option("retries", "3")         
        .outputMode("update")         
        .start()     
        } 
   
    val abrisConfig: FromAvroConfig = AbrisConfig
        .fromConfluentAvro
        .downloadReaderSchemaByLatestVersion
        .andTopicNameStrategy("clickers")
        .usingSchemaRegistry("http://localhost:8070")
 
    val df = sparkConsumer.readStream     
        .format("kafka")              
        .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS)              
        .option("subscribe", TOPIC)              
        .option("startingOffsets", "earliest")             
        .load() 
        
    val deserializedAvro = df.select(from_avro(col("value"), abrisConfig).as("data"))
        .select(col("data.*")) 
 
    df.printSchema()  
    
    /*deserializedAvro.writeStream
       .outputMode("append")
       .format("console")
       .start()
       .awaitTermination()
     */          

 
// Stream data to database...       
            
sparkConsumer.streams.awaitAnyTermination() 
    
}  

    /*queryDF.writeStream      
        .outputMode("complete")     
        .option("truncate", "false")         
        .format("jdbc")      
        .option("url", url)     
        .option("dbtable", clickers)     
        .option("user", airflow)     
        .option("password",passw)     
        .outputMode("append")      
        .save()     
        .start() 
        */

}

