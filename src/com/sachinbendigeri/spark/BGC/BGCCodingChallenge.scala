package com.sachinbendigeri.spark.BGC
import org.apache.spark.SparkContext._
import com.sachinbendigeri.spark.BGC.constants.Constants
import com.sachinbendigeri.spark.BGC.setup.SparkSetup
import com.sachinbendigeri.spark.BGC.ingestion._
import com.sachinbendigeri.spark.BGC.utils.Utils
import org.apache.log4j._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object  BGCCodingChallenge {
      def main(args: Array[String]) {
      
        
      val ClusterMachine = true  

  
      // Set the Spark Context
      implicit val sparkSession = SparkSetup.GetSparkContext("BCGCodingChallenge")
      import sparkSession.implicits._
 
      
      // Read Parameters from Hive table
      //var parameters = SparkSetup.ReadParametersTable(args(0), sparkSession,ClusterMachine)
      var parameters = SparkSetup.ReadParametersTable("Defalut", sparkSession,ClusterMachine)
      
      //Instantiate the Objects for Serialised processing
      val title_Basics = new com.sachinbendigeri.spark.BGC.ingestion.Title_Basics(sparkSession,parameters)
      val title_Ratings = new com.sachinbendigeri.spark.BGC.ingestion.Title_Ratings(sparkSession,parameters)      
      val title_Principles = new com.sachinbendigeri.spark.BGC.ingestion.Title_Principles(sparkSession,parameters)
      val title_AKAs = new com.sachinbendigeri.spark.BGC.ingestion.Title_AKAs(sparkSession,parameters)     
      val name_Basics = new com.sachinbendigeri.spark.BGC.ingestion.Name_Basics(sparkSession,parameters)
      
      /************************************************************************      
      //Identify Top20 best rated movies
      ***********************************************************************/
      //***Load the required data***
      // Get the titles basic data to filter on titleType = movie
      val title_BasicsData =title_Basics.ReadFromTSV(parameters(Constants.PARAM_TITLE_BASICS_FILEPATH))
                            .filter("titleType = 'movie'")
                            //Assist in efficient execution of join step further
                            .repartition(20, $"tconst")
                            
      val title_RatingsData=title_Ratings.ReadFromTSV(parameters(Constants.PARAM_TITLE_RATINGS_FILEPATH))
                            //Assist in efficient execution of join step further
                            .repartition(20, $"tconst") 
      

      val title_Movie20Ratings =title_RatingsData
                            //select ratings for movies alone
                            .join(title_BasicsData, title_RatingsData.col("tconst")===title_BasicsData.col("tconst"))
                            // Set window as the whole dataset and get the Average Number of Votes for Movies
                            .withColumn("averageNumberOfVotes",
                                         avg("numVotes")
                                         .over(Window.partitionBy($"titleType"))
                                        )
                            // calculate the BGCRating for Movies           
                            .withColumn("BGCRating", ($"numVotes"/$"averageRating")*$"averageNumberOfVotes")
                            .withColumn("BGCRatingOrder", 
                                         row_number()
                                         .over(Window.orderBy($"BGCRating".desc))
                                       )
                            .filter("BGCRatingOrder <=20 ")    
                            //persist it as its used multiple times
                            .select(
                              title_BasicsData.col("tconst").as("tconst"),
                              title_RatingsData.col("averageRating"),
                              title_RatingsData.col("numVotes"),
                              title_BasicsData.col("titleType"),
                              title_BasicsData.col("primaryTitle"),
                              title_BasicsData.col("originalTitle"),
                              title_BasicsData.col("isAdult"),
                              title_BasicsData.col("startYear"),
                              title_BasicsData.col("endYear"),
                              title_BasicsData.col("runtimeMinutes"),
                              title_BasicsData.col("genres"),
                              col("averageNumberOfVotes"),
                              col("BGCRating"),
                              col("BGCRatingOrder")
                            )
                            .persist()
      Utils.OverwriteTable(sparkSession,title_Movie20Ratings.toDF(),parameters(Constants.PARAM_BGC_TOP20MOVIES_TABLE), parameters(Constants.PARAM_RUN_INSTANCE_TIME))              
      title_Movie20Ratings.show(false)
      
      /************************************************************************
      //For the Top20 best rated movies pull most accredited persons from principles and names
       ***********************************************************************/      
      //***Load the required data***    
      val title_PrinciplesData=title_Principles.ReadFromTSV(parameters(Constants.PARAM_TITLE_PRINCIPLES_FILEPATH))
      val title_PrinciplesTop20Data = title_PrinciplesData
                            //Broadcast the 20 rows of top20 for the join
                            .join(broadcast(title_Movie20Ratings.select(
                                       col("tconst"),
                                       col("originalTitle"),
                                       col("genres"),
                                       col("BGCRating"),
                                       col("BGCRatingOrder")
                                )).as("Top20"), $"Top20.tconst"=== title_PrinciplesData.col("tconst"))
                                .select(
                                    col("Top20.BGCRatingOrder").as("BGCRatingOrder"),
                                    col("Top20.BGCRating").as("BGCRating"),
                                    col("Top20.tconst").as("tconst"),
                                    col("Top20.originalTitle").as("originalTitle"),                                  
                                    title_PrinciplesData.col("ordering").as("ordering"),
                                    title_PrinciplesData.col("nconst").as("nconst"),
                                    title_PrinciplesData.col("job").as("job"),
                                    title_PrinciplesData.col("characters").as("characters")                                
                                )
      
      val name_BasicsData =name_Basics.ReadFromTSV(parameters(Constants.PARAM_NAME_BASICS_FILEPATH))
      
      val PrincipleCharactersTop20Data = name_BasicsData
                             //Broadcast the 20 rows of top20 for the join
                            .join(broadcast(title_PrinciplesTop20Data).as("Top20"), $"Top20.nconst"=== name_BasicsData.col("nconst"))
                            .select(
                                    col("Top20.BGCRatingOrder").as("BGCRatingOrder"),
                                    col("Top20.BGCRating").as("BGCRating"),
                                    col("Top20.tconst").as("tconst"),
                                    col("Top20.originalTitle").as("originalTitle"),
                                    col("Top20.ordering").as("ordering"),
                                    col("Top20.job").as("job"),                      
                                    name_BasicsData.col("primaryName").as("primaryName"),
                                    name_BasicsData.col("primaryProfession").as("primaryProfession"),
                                    name_BasicsData.col("knownForTitles").as("knownForTitles")                         
                             )
                            .persist() 
    
      Utils.OverwriteTable(sparkSession,PrincipleCharactersTop20Data.toDF(),parameters(Constants.PARAM_BGC_TOP20MOVIES_PRICNCIPLES_TABLE), parameters(Constants.PARAM_RUN_INSTANCE_TIME))                                       
      PrincipleCharactersTop20Data.orderBy("BGCRatingOrder","ordering").show(200) 
 
      /************************************************************************
       *For the Top20 best rated movies pull all the other titles release 
       ***********************************************************************/
      val title_AKAsData =title_AKAs.ReadFromTSV(parameters(Constants.PARAM_TITLE_AKAS_FILEPATH))
      val Top20TitleAKAs = title_AKAsData
                            .join(broadcast(title_Movie20Ratings.select(
                                       col("tconst"),
                                       col("originalTitle"),
                                       col("genres"),
                                       col("BGCRating"),
                                       col("BGCRatingOrder")
                                )).as("Top20"), $"Top20.tconst"=== title_AKAsData.col("titleId"))
                            .select(
                                    col("Top20.BGCRatingOrder").as("BGCRatingOrder"),
                                    col("Top20.BGCRating").as("BGCRating"),
                                    col("Top20.tconst").as("tconst"),
                                    col("Top20.originalTitle").as("originalTitle"),                 
                                    title_AKAsData.col("ordering").as("AKAordering"),
                                    title_AKAsData.col("title").as("AKATitle"),
                                    title_AKAsData.col("region").as("AKAregion"),                         
                                    title_AKAsData.col("attributes").as("AKAattributes")
                            )
      Utils.OverwriteTable(sparkSession,Top20TitleAKAs.toDF(),parameters(Constants.PARAM_BGC_TOP20MOVIES_AKATITLES_TABLE), parameters(Constants.PARAM_RUN_INSTANCE_TIME))                                            
      Top20TitleAKAs.orderBy("BGCRatingOrder","ordering").show(1000) 

      }
}