scala> val lines = sc.textFile("clickstream-dewiki-2023-01.tsv") 
//val lines = spark.read.options(Map("delimeter"->"\t", "inferSchema"->"true", "header"->"false")).csv("path") or spark.read.format("csv").load("path")
//lines.isStreaming
//lines.printSchema
//
val userSchema = new StructType().add("prev", "string")
    .add("curr","string")
    .add("type","string")
    .add("n","long")

val cvDF = spark.read.option("sep", "\t").schema(userSchema).csv("../clickstream-dewiki-2023-01.tsv")

val header = cvDF.first

 
scala> cvDF.count 
res3: Long = 1349
 
case class Click(prev: String, curr: String, link: String, n: Long)

 
//val noheader = lines.filter(_ != header) 

val au = lines.map(_.split(",")).map(r => Click(r(0), r(1), r(2), r(3).toLong)) 
 
val df = au.toDF 

df.printSchema 
root
 |-- prev: string (nullable = true)
 |-- curr: string (nullable = true)
 |-- link: string (nullable = true)
 |-- n: long (nullable = false)

//import the required libraries 
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}  
import org.apache.spark.sql.functions.col 
//import za.co.absa.abris.avro.functions.from_avro
//import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}
import spark.implicits._

import java.util.{Properties, UUID}

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row} 
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import scala.util.Try

object Main extends App {

case class KafkaClick(prev: String, curr: String, link: String, n: Long)
//.master("local[*]")
def parseVal(x: Array[Byte]): Option[KafkaClick] = {
    val split: Array[String] = new Predef.String(x).split("\\t")
    if (split.length == 4) {
      Try(KafkaClick(split(0), split(1), split(2), split(3).toLong)).toOption
    } else
      None
  }

//org.apache.spark.sql.kafka010.KafkaSourceProvider
val records = spark.readStream.format("kafka") 
                  .option("subscribe", "clickers") //KafkaDataConsumer
                  .option("failOnDataLoss", "false")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .load()

// records.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)").as[(String, String)]

val messages = records.select("value").as[Array[Byte]]
                     .flatMap(x => parseVal(x))
                     .createOrReplaceTempView("clickQuery")
                     .groupBy("curr")
                     .agg(Map("n" -> "sum"))
                     .sort(col("sum(n)").desc)
                     
val r1 = messages.writeStream
        .outputMode("complete")
        .format("console")
        .start()
//  show()

//  val wordCounts = records.as[String].flatMap(_.split("\t")).groupBy("value").count()
    
    
//   wordCounts.show()

// Question 1 - What are the top 10 articles requested from wikipedia ?            
     val tbname1 = "top_10_articles"            
     val enrichedTransactions1 = spark.sql(       
     """         
     SELECT curr as Current_title, 
     SUM(n) as Top_Articles 
     FROM clickQuery 
     GROUP BY Current_title 
     ORDER BY Top_Articles 
     DESC LIMIT(5)       
     """.stripMargin)

val q1 = enrichedTransactions1.writeStream
        .outputMode("complete")
        .format("console")
        .start()

//    writePostgreTable(enrichedTransactions1, tbname1) 

//Question 2 - Who sent the most traffic to wikipedia in feb2015 ? So, Who were the top referers to wikipedia ? 
 
val tbname2 = "top_10_referers" 
                
val top_referers = spark.sql(
     """
     SELECT prev_title as Referers, 
     SUM(n) as Number_of_times_refered 
     FROM clickQuery 
     GROUP BY prev_title 
     ORDER BY Number_of_times_refered 
     DESC LIMIT(10)
     """.stripMargin)   

top_referers.show()

val q2 = top_referers.writeStream
        .outputMode("complete")
        .format("console")
        .start()

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

val q3 = enrichedTransactions2.writeStream
        .outputMode("complete")
        .format("console")
        .start()
     
// Question 3 - What were the top 5 trending articles on twitter Feb 2015 ?               

val tbname3 = "top_trending_articles"    
             
val enrichedTransactions3 = spark.sql(       
     """
     SELECT curr as Top_articles_In_Twitter,           
     SUM(n) as Number_of_times_refered           
     FROM clickQuery WHERE prev = 'other-twitter'           
     GROUP BY curr           
     ORDER BY Number_of_times_refered 
     DESC LIMIT(5)       
     """.stripMargin) 

enrichedTransactions3.show()

val q4 = enrichedTransactions3.writeStream
        .outputMode("complete")
        .format("console")
        .start()
// writePostgreTable(enrichedTransactions3, tbname3)   
     
// Question 4 - What are the most requested missing pages ? 
// df.select($"name").show()     
val tbname4 = "most_requested_articles" 
                
val enrichedTransactions4 = spark.sql(       
     """
     SELECT curr as Top_missing_pages,           
     SUM(n) as Number_of_times_refered           
     FROM clickQuery WHERE type = 'redlink'           
     GROUP BY curr           
     ORDER BY Number_of_times_refered 
     DESC LIMIT(10)       
     """.stripMargin) 
     
enrichedTransactions4.show()
//  writePostgreTable(enrichedTransactions4, tbname4)

val q5 = enrichedTransactions1.writeStream
        .outputMode("complete")
        .format("console")
        .start()

// Question 5 - What does the traffic inflow vs outflow look like for the most requested pages ? 
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
     
//   page_views_df.registerTempTable("intable")  
//   CreateOrReplaceTempView("intable")         
page_views_df.cache()         
page_views_df.show()  
             
// then, find the link clicks per article         
val link_click_per_article_df = spark.sql(
     """
     SELECT prev as link_clicked, 
     SUM(n) as out_count 
     FROM clickQuery
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
      
      
// add  a new ratio column to easily see whether there is more in_count or out_count for an article          
    
val in_out_ratio_df = in_out_df.withColumn("ratio", col("out_count")/col("in_count"))  
              
val in_out_ratio_df2 = in_out_ratio_df.select("Article","in_count","out_count","ratio")                  
            
in_out_ratio_df2.cache().show()                                             
       
in_out_ratio_df.registerTempTable("ratiotable")     
          
// So, we can infer that 49% of the people who visited the "Fifty Shades of Grey" article clicked on a link in the article and continued to browse wikipedia 

 
// Question 6 -  What does the traffic flow pattern look like for the Deaths_in_2015 article?                   
val traffic_flow = spark.sql(
     """
     SELECT * FROM ratiotable 
     WHERE Article = 'Deaths_in_2015'
     """.stripMargin)
     .show()     
     
// Question 7 -  which referers send the most traffic to the Deaths in 2015 article ?             
         
val tbname7 = "most_refered_deaths" 
               
val top_referers_deaths = spark.sql(
     """
     SELECT * FROM clickQuery 
     WHERE curr LIKE 'Deaths_in_2015' 
     ORDER BY n DESC LIMIT(10)
     """.stripMargin)                      
     
top_referers_deaths.show()                  
//  val referers_deaths = top_referers_deaths.collect()
 
//  writePostgreTable(top_referers_deaths, tbname7)        
     
// Question 8 -  And which future articles does the deaths in 2015 article send most traffic onward to?                  

val tbname8 = "most_deaths"       
             
val top_leavers_deaths = spark.sql(
     """
     SELECT * FROM clickQuery 
     WHERE prev 
     LIKE 'Deaths_in_2015' 
     ORDER BY n DESC LIMIT(10)
     """.stripMargin)         
     
top_leavers_deaths.show()                  
//  val leavers_deaths = top_leavers_deaths.collect()
     
     
// Question 9 -  Persist the union inct()                  
    val data_fsog = top_referers_deaths.join(top_leavers_deaths)      

// writePostgreTable(data_fsog, s"$PostgreDb.$tbname8") 
data_fsog.show()

val query = data_fsog.writeStream
              .outputMode("append")
              .option("truncate", "false")
              .format("console")
              .start()

// Stream data to database... 
    
 
// writePostgreTable(enrichedTransactions2, tbname2)
// Start running the query that prints the running counts to the console
// Kafka class for data ingestion

import java.util.Properties
import org.apache.spark..sql.ForeachWriter
import kafkashaded.org.apache.kafka.clients.producer._

class KafkaSinkClass(topic: String, servers:String) extends ForeachWriter[(String, String, String, Long)] {

   val kafkaPro = new Properties()
   val kafkaPro.put("kafka.bootstrap.servers", "localhost:9092") //"servers:localhost:9092"
   val kafkaPro.put("key.serializer", "kafkashaded.org.apache.common.serialization.StringSerializer")
   val kafkaPro.put("value.serializer", "kafkashaded.org.apache.common.serialization.StringSerializer")
   
   val results = new scala.collection.mutable.HashMap[String, String, String, Long]
   var producer: KafkaProducer[String, String, String, Long] = _
   
   def open(partitionId: Long, version: Long): Boolean = {
      producer = new KafkaProducer(kafkaProperties)
      true
   } 
   
   def process(value: (String, String, String, Long)): Unit = {
      producer.send(new ProducerRecord(topic, value._1 + ":" + value._2 + ":" + value._3 + ":" + value._4))
   }
   
   def close(errorOrNull: Throwable): Unit = {
      producer.close()
   }
 }
 
    
// Kafka implementation code

   val topic = "clickers"
   val brokers = "localhost:9092"
   
   val writer = new KafkaSinkClass(topic, brokers)
   
   val query = df.writeStream
       .foreach(writer)
       .outputMode("update")
       .trigger(ProcessingTime("40 seconds"))
       .start()
       
}
