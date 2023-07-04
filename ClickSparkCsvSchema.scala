/*
 * The program describes ETL pipeline design for Clickstream analytics using 
 * Apache Kafka as an ingestion engine and Apache Spark as the Sink for analysing
 * the data. The program includes a database engine such as Postgre DB for   
 * storage purposes 
 * 
 */

// scalastyle:off println

/* How to submit this program:
 *
 * Example:
 *    `$ spark-submit \
 *      com.spark_kafka.ClickProgram localhost:9092 \
 *      --packages \
 *      org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.3`
 */


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

object ClickSpark3 extends Apps{

    val schema: structType(
           Seq(structField("prev", "StringType", true),
               structField("curr", "StringType", true),
               structField("link", "StringType", true),
               structField("n", "LongType", true)
           )
    )
    def main(args: Array[String]) {
    
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
      .option("subscribe", TOPIC) 
      .option("startingOffsets", "earliest")
      //.option("startingOffsets", "latest")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .load()
      .select(from_json($"value".cast(StringType), schema).as("data_in_json_format"))
    
    //kafkaStructuredStream.createOrReplaceTempView("clickQuery")
    kafkaStructuredStream.registerTempTable("clickQuery")
    
    kafkaStructuredStream.printSchema()
     
    val query = kafkaStructuredStream
            .writeStream              
            .format("console")
            .outputMode("update")
            //.option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
            .trigger(ProcessingTime("40 seconds"))         
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
     
     enrichedTransactions1.show()
 
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
     
     top_referers.writeStream.show()
     
     val enrichedTransactions2 = spark.sql(       
     """
     SELECT prev as Referers,            
     SUM(n) as Number_ofTimes_refered  
     FROM clickQuery           
     GROUP BY Referers            
     ORDER BY Number_ofTimes_refered 
     DESC LIMIT(10)       
     """.stripMargin) 
 
     enrichedTransactions2.show()   
     
// Question 3 - What were the top 5 trending articles on twitter Feb 2015 ?                
           
     val enrichedTransactions3 = spark.sql(       
     """SELECT curr as Top_articles_In_Twitter,           
     SUM(n) as Number_of_times_refered           
     FROM clickQuery WHERE prev = 'other-twitter'           
     GROUP BY curr           
     ORDER BY Number_of_times_refered 
     DESC LIMIT(5)       
     """.stripMargin) 
 
     enrichedTransactions3.show() 
     
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
 
     enrichedTransactions4.show()      
     
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
     page_views_df.show() 
             
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
     link_click_per_article_df.show()      
     
// so, when the clients went to the 'David_Janson' article 340 times they clicked on a link in that article to go to next article          
// Join the 2 dataframes to get the wholistic picture         
      val in_out_df = spark.sql(
      """
      SELECT * FROM intable i 
      JOIN outtable o ON i.Article = o.link_clicked 
      ORDER BY in_count DESC
      """.stripMargin
      )         
      
      in_out_df.show()    
      
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
         
         traffic.show()          
     
// Question 7 -  which referers send the most traffic to the Deaths in 2015 article ?          
               
         val top_referers_deaths = spark.sql(
         """
         SELECT * FROM clickQuery 
         WHERE curr LIKE 'Deaths_in_2015' 
         ORDER BY n DESC LIMIT(10)
         """.stripMargin)
         
         top_referers_deaths.show() 
                                 
                 
//     val referers_deaths = top_referers_deaths.collect()       
     
// Question 8 -  And which future articles does the deaths in 2015 article send most traffic onward to?                  
            
         val top_leavers_deaths = spark.sql(
         """
         SELECT * FROM clickQuery 
         WHERE prev 
         LIKE 'Deaths_in_2015' 
         ORDER BY n DESC LIMIT(10)
         """.stripMargin)
         
         top_leavers_deaths.show()              
               
        // val leavers_deaths = top_leavers_deaths.collect()
     
// Question 9 -  Persist the union inct()                  
        val data_fsog = spark.sql(
         """
         SELECT *
         FROM t1 
         UNION SELECT * FROM t2 
         """.stripMargin)
         
         data_fsog.show() 
       
    print("Connection starting...")

}

