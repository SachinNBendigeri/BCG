package com.sachinbendigeri.spark.BGC.ingestion 

import com.sachinbendigeri.spark.BGC.constants.Constants
import com.sachinbendigeri.spark.BGC.utils.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

case class Title_RatingsStructure(
        tconst                              :String,
        averageRating                       :Decimal,
        numVotes                            :Int
)


   
class Title_Ratings(sparkSession: SparkSession, parameters: Map[String, String]) extends Serializable {
  

   import sparkSession.implicits._

   //Logger to log messages
   val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  
        
   def ReadFromTSV(FilePath: String): Dataset[Title_RatingsStructure] = {  
      val Title_RatingsSchema = ScalaReflection.schemaFor[Title_RatingsStructure].dataType.asInstanceOf[StructType]
      val df =  Utils.ReadFromTSV(sparkSession,FilePath,Title_RatingsSchema)                       
      return    df.as[Title_RatingsStructure]
   }
    
  def FullRefreshFromTSV(FilePath :String) = {
      val Title_RatingsDS = ReadFromTSV(FilePath)
      Utils.OverwriteTable(sparkSession,Title_RatingsDS.toDF(),Constants.PARAM_TITLE_RATINGS_TABLE,parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
    
    
   def DeltaRefreshFromTSV(FilePath :String) = {
      val Title_RatingsDS = ReadFromTSV(FilePath)
      Utils.AppendTable(sparkSession,Title_RatingsDS.toDF(),Constants.PARAM_TITLE_RATINGS_TABLE,parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
   
    
}