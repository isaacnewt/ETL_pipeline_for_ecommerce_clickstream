//import the required libraries 
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}  
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
//import za.co.absa.abris.avro.functions.from_avro
//import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}
import spark.implicits._

import java.util.{Properties, UUID}

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row} 
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import scala.util.Try

// userSchema = new StructType([StructField("id",IntegerType(), False), StructField("country",IntegerType(), False),...])

val userSchema = new StructType().add("prev", "string")
    .add("curr","string")
    .add("type","string")
    .add("n","long")

val cvDF = spark.read.option("sep", "\t").schema(userSchema).csv("clickstream-dewiki-2023-01.tsv")

// exampleDF = spark.createDataFrame(ampledata, amplecolumn)
// amplecolumn = ["prev","curr","link","n"]
// ampledata = [("ada","adre","ear.com",4),("ada","adre","ear.com",4),("ada","adre","ear.com",4)]
// 

val header = cvDF.first

cvDF.printSchema

val q1 = cvDF.createOrReplaceTempView("clickQuery")

val enrichedTransactions1 = spark.sql(       
           """         
           SELECT curr as Current_title, 
           SUM(n) as Top_Articles 
           FROM clickQuery 
           GROUP BY Current_title 
           ORDER BY Top_Articles 
           DESC LIMIT(5)       
           """.stripMargin)

enrichedTransactions1.printSchema

enrichedTransactions1.show()

//Question 2 - Who sent the most traffic to wikipedia in feb2015 ? So, Who were the top referers to wikipedia ? 
 
//val tbname2 = "top_10_referers" 
                
val top_referers = spark.sql(
     """
     SELECT prev as Referers, 
     SUM(n) as Number_of_times_refered 
     FROM clickQuery
     GROUP BY Referers
     ORDER BY Number_of_times_refered 
     DESC LIMIT(10)
     """.stripMargin)
     
top_referers.show()

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

//   writePostgreTable(enrichedTransactions3, tbname3)   
     
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
//page_views_df.CreateOrReplaceTempView("intable")         
page_views_df.cache()         
page_views_df.show()  
             
//   then, find the link clicks per article         

val link_click_per_article_df = spark.sql(
     """
     SELECT prev as link_clicked, 
     SUM(n) as out_count 
     FROM clickQuery
     GROUP BY prev
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
      """.stripMargin)         
      
      
// add  a new ratio column to easily see whether there is more in_count or out_count for an article          
    
val in_out_ratio_df = in_out_df.withColumn("ratio", col("out_count")/col("in_count"))  
              
val in_out_ratio_df2 = in_out_ratio_df.select("Article","in_count","out_count","ratio")                  
            
in_out_ratio_df2.cache().show()                                             
       
in_out_ratio_df.registerTempTable("ratiotable")     
          
// So, we can infer that 49% of the people who visited the "Fifty Shades of Grey" article clicked on a link in the article and continued to browse wikipedia 

 
// Question 6 -  What does the traffic flow pattern look like for the Deaths_in_2015 article?                   
     spark.sql(
     """
     SELECT * FROM ratiotable 
     WHERE Article = 'Deaths_in_2015'
     """.stripMargin)
     .show()     
     
// Question 7 -  which referers send the most traffic to the Deaths in 2015 article ?             

//     import spark.implicits._
         
val tbname7 = "most_refered_deaths" 
               
val top_referers_deaths = spark.sql(
     """
     SELECT * FROM clickQuery 
     WHERE curr LIKE 'Deaths_in_2015' 
     ORDER BY n DESC LIMIT(10)
     """.stripMargin)                      
     
top_referers_deaths.show()                  
val referers_deaths = top_referers_deaths.registerTempTable("t2") 
 
//  writePostgreTable(top_referers_deaths, tbname7)        
     
//  Question 8 -  And which future articles does the deaths in 2015 article send most traffic onward to?                  

val tbname8 = "most_deaths" 
                   
val top_leavers_deaths = spark.sql(
     """
     SELECT * FROM clickQuery 
     WHERE prev 
     LIKE 'Deaths_in_2015' 
     ORDER BY n DESC LIMIT(10)
     """.stripMargin)         
     
top_leavers_deaths.show()                  
val leavers_deaths = top_leavers_deaths.registerTempTable("t1") 
     
     
// Question 9 -  Persist the union inct()        
// val data_fsog = top_leavers_deaths.join(top_referers_deaths, "prev").show()

val data_fsog = spark.sql(
     """
     SELECT *
     FROM t1 
     UNION SELECT * FROM t2 
     """.stripMargin) //      SELECT * FROM t1 INNER JOIN t2 ON (t1.prev=t2.curr)
     
data_fsog.show()
    
   val baseCheckpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString
   val PostgreDb = "clickers"

  def writePostgreTable(df: DataFrame, name: String): Unit = {     
      //val fullTableName = s"$PostgreDb.$name"        
      df.writeStream        
        .format("jdbc") 
        .option("url","jdbc:postgresql:clickers")       
        .option("user", "postgres")
        .option("password", "postgres")         
        .option("dbtable", name)         
        .option("checkpointLocation", s"$baseCheckpointLocation/$name")         
        .option("retries", "3")         
        //.save() //
        .outputMode("update") //.trigger(ProcessingTime("40 seconds"))  
        .start()
        } 

  writePostgreTable(top_referers_deaths, tbname7) 
  
  //*************************************************
          
  def writePostgreTable(df: DataFrame, name: String): Unit = {     
      //val fullTableName = s"$PostgreDb"        
      df.write       
        .format("jdbc") 
        .option("url","jdbc:postgresql:clickers")       
        .option("user", "postgres")
        .option("password", "postgres")         
        .option("dbtable", "most_refered_deaths")         
        .option("checkpointLocation", s"$baseCheckpointLocation/$name")         
        .option("retries", "3")         
        .save() 
        //.outputMode("update") //.trigger(ProcessingTime("40 seconds"))  

        }           
          

// val data_fsog = leavers_deaths.join(referers_deaths, $"prev"===$"prev", "inner")

// val data_fsog = leavers_deaths.join(referers_deaths).where($"prev"===$"prev")

// val data_fsog = leavers_deaths.join(referers_deaths).filter($"prev"===$"prev")

//val data_fsog = leavers_deaths.joinWith(referers_deaths(col("top_leavers_deaths")).equalTo(col("top_leavers_deaths.prev")), "inner")      

// writePostgreTable(data_fsog, s"$PostgreDb.$tbname8") 



