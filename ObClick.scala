//import the required libraries 

import org.apache.spark.sql.{SparkSession,DataFrame,ForeachWriter,Dataset,Row}  
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.functions.col 
//import za.co.absa.abris.avro.functions.from_avro
//import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}

import org.apache.spark.sql.types._
import java.util.{Properties, UUID}
import java.util.UUID
import java.sql._

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
         
val kafkaSplit = kafkaSelectDS.withColumn("value", split(col("value"), "/t"))

val prev = regexp_replace(kafkaSplit.col("value").getItem(0), "\\[\\w+\\]:","")
val curr = regexp_replace(kafkaSplit.col("value").getItem(1), "\\[\\w+\\]:","")
val link = regexp_replace(kafkaSplit.col("value").getItem(2), "\\[\\w+\\]:","")
val n = regexp_replace(kafkaSplit.col("value").getItem(3), "\\[\\w+\\]:","")

val reformedDF = kafkaSplit
         .withColumn("prev", prev)
         .withColumn("curr", curr)
         .withColumn("link", link)
         .withColumn("n", n cast LongType) // (col("n").cast(Long)))
         .drop("value")
         //.where("address = '/cursor'")

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
   

val postgreswriter = new JDBCSinkClass(url, user, pwd)

val query = reformedDF
        .writeStream              
        .format("console")
        .outputMode("append")
        .trigger(ProcessingTime("40 seconds"))         
        .start()
        
   print("Connection starting...")
   query.awaitTermination(5)
   
val query = reformedDF
        .writeStream              
        .foreach(postgreswriter)
        .outputMode("append")
        .trigger(ProcessingTime("40 seconds"))         
        .start()
        
   print("Connection starting...")
   query.awaitTermination(5)
   
//*****************************************************

val query = reformedDF
        .writeStream              
        .format("console")
        .outputMode("update")
        .trigger(ProcessingTime("40 seconds"))         
        .start()
        
   print("Connection starting...")
   query.awaitTermination(5)
   
//*****************************************************
package com.spark_kafka // package definition 


import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import java.sql._


//import the required libraries 
import org.apache.spark.sql.{SparkSession,DataFrame,ForeachWriter,Dataset,Row}  
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col 
import org.apache.spark.sql.types._

import java.util.{Properties, UUID}
import java.util.UUID

// Database implementaion code
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
  
  
class ClickPro(url:String, user:String, pwd:String) extends ForeachWriter[Row] {
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
   

//object ClickSpark3{
    
    val spark: SparkSession = SparkSession
        .builder()
        .appName("ClickSpark for clicksstream analytics")
        //.master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()    
    
    import spark.implicits._     

    spark.sparkContext.setLogLevel("ERROR") 
    
    val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"        
    val SCHEMA_REGISTRY_URL = "http://localhost:8081"        
    val TOPIC = "clickers"

    //org.apache.spark.sql.kafka010.KafkaSourceProvider
    val kafkaStructuredStream = spark
      .readStream
      .format("kafka") 
      .option("subscribe", TOPIC) //KafkaDataConsumer
      .option("startingOffsets", "earliest")
      //.option("startingOffsets", "latest")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .load()
      .selectExpr("CAST (value AS STRING)").as[String] 
         
      val kafkaSplit = kafkaStructuredStream.withColumn("value", split(col("value"), "\\t"))
    
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
                 
                 
                 reformedDF.registerTempTable("clickQuery")
                 //.groupBy("curr")
                 //.count()
      
    val url  = "jdbc:postgresql://localhost:5432/test" //<dbserver>:port/test
    val user = "postgres"
    val pwd  = "postgres"
       
    val postgreswriter = new ClickPro(url, user, pwd)
            
    val query = reformedDF
            .writeStream              
            .format("console")
            //.outputMode("update")
            //.trigger(ProcessingTime("40 seconds"))         
            .start()
            
    reformedDF.writeStream.foreach(postgreswriter).start()
    
     
// Question 1 - What are the top 10 articles requested from wikipedia ?            

     val enrichedTransactions1 = spark.sql(       
     """         
     SELECT curr as Current_title, 
     SUM(n) as Top_Articles 
     FROM clickQuery 
     GROUP BY Current_title 
     ORDER BY Top_Articles 
     DESC LIMIT(5)       
     """.stripMargin) 
     
     enrichedTransactions1.writeStream.format("console").start()
 
//Question 2 - Who sent the most traffic to wikipedia in feb2015 ? So, Who were the top referers to wikipedia ? 
           
     val top_referers = spark.sql(
     """
     SELECT prev_title as Referers, 
     SUM(n) as Number_of_times_refered 
     FROM clickstream 
     GROUP BY prev_title 
     ORDER BY Number_of_times_refered 
     DESC LIMIT(10)"
     """.stripMargin)   
     
     top_referers.writeStream.format("console").start()
     
     val enrichedTransactions2 = spark.sql(       
     """
     SELECT prev as Referers,            
     SUM(n) as Number_ofTimes_refered  
     FROM clickQuery           
     GROUP BY Referers            
     ORDER BY Number_ofTimes_refered 
     DESC LIMIT(10)       
     """.stripMargin) 
 
     enrichedTransactions2.writeStream.format("console").start()      
     
// Question 3 - What were the top 5 trending articles on twitter Feb 2015 ?                
           
     val enrichedTransactions3 = spark.sql(       
     """SELECT curr as Top_articles_In_Twitter,           
     SUM(n) as Number_of_times_refered           
     FROM clickQuery WHERE prev = 'other-twitter'           
     GROUP BY curr           
     ORDER BY Number_of_times_refered 
     DESC LIMIT(5)       
     """.stripMargin) 
 
     enrichedTransactions3.writeStream.format("console").start()      
     
// Question 4 - What are the most requested missing pages ? 
     // df.select($"name").show()     
         
     val enrichedTransactions4 = spark.sql(       
     """SELECT curr as Top_missing_pages,           
     SUM(n) as Number_of_times_refered           
     FROM clickQuery WHERE type = 'redlink'           
     GROUP BY curr           
     ORDER BY Number_of_times_refered 
     DESC LIMIT(10)       
     """.stripMargin) 
 
     enrichedTransactions4.writeStream.format("console").start()       
     
//Question 5 - What does the traffic inflow vs outflow look like for the most requested pages ? 
     // df.createOrReplaceTempView(clickQuery) 
     // val result = sparkSession.sql("select ") 
     // Lets first find out pageviews per article         
     val page_views_df = spark.sql(
     """
     SELECT curr as Article, 
     SUM(n) as in_count 
     FROM clickQuery 
     GROUP BY curr
     """.stripMargin)         
     
     page_views_df.registerTempTable("intable")
     //.CreateOrReplaceTempView("intable")         
     page_views_df.cache()         
     page_views_df.writeStream.format("console").start() 
             
     // then, find the link clicks per article         
     val link_click_per_article_df = spark.sql(
     """
     SELECT prev as link_clicked, 
     SUM(n) as out_count 
     FROM clickstream 
     GROUP BY prev_title
     """.stripMargin)   
     
     link_click_per_article_df.registerTempTable("outtable")            
     link_click_per_article_df.cache()          
     link_click_per_article_df.writeStream.format("console").start()      
     
// so, when the clients went to the 'David_Janson' article 340 times they clicked on a link in that article to go to next article          
// Join the 2 dataframes to get the wholistic picture         
      val in_out_df = spark.sql(
      """
      SELECT * FROM intable i 
      JOIN outtable o ON i.Article = o.link_clicked 
      ORDER BY in_count DESC
      """.stripMargin
      )         
      
      in_out_df.writeStream.format("console").start()      
      
// add  a new ratio column to easily see whether there is more in_count or out_count for an article          
      val in_out_ratio_df = in_out_df.withColumn("ratio", col("out_count")/col("in_count"))  
              
      val in_out_ratio_df2 = in_out_ratio_df
           .select("Article","in_count","out_count","ratio")                  
           .registerTempTable("ratiotable")            
          
// So, we can infer that 49% of the people who visited the "Fifty Shades of Grey" article clicked on a link in the article and continued to browse wikipedia 
      //in_out_ratio_df2.writeStream.format("console").start()
 
// Question 6 -  What does the traffic flow pattern look like for the Deaths_in_2015 article?                   
         val traffic = spark.sql(
         """
         SELECT * FROM ratiotable 
         WHERE Article = 'Deaths_in_2015'
         """.stripMargin)
         
         traffic.writeStream.format("console").start()            
     
// Question 7 -  which referers send the most traffic to the Deaths in 2015 article ?          
               
         val top_referers_deaths = spark.sql(
         """
         SELECT * FROM clickQuery 
         WHERE curr LIKE 'Deaths_in_2015' 
         ORDER BY n DESC LIMIT(10)
         """.stripMargin)
         
         top_referers_deaths.writeStream.format("console").start()  
                                 
                 
//     val referers_deaths = top_referers_deaths.collect()       
     
// Question 8 -  And which future articles does the deaths in 2015 article send most traffic onward to?                  
            
         val top_leavers_deaths = spark.sql(
         """
         SELECT * FROM clickQuery 
         WHERE prev 
         LIKE 'Deaths_in_2015' 
         ORDER BY n DESC LIMIT(10)
         """.stripMargin)
         
         top_leavers_deaths.writeStream.format("console").start()                
               
        // val leavers_deaths = top_leavers_deaths.collect()
     
// Question 9 -  Persist the union inct()                  
        val data_fsog = spark.sql(
         """
         SELECT *
         FROM t1 
         UNION SELECT * FROM t2 
         """.stripMargin)
         
         data_fsog.writeStream.format("console").start()    //     
       
    print("Connection starting...")
    query.awaitTermination(5)

//}

