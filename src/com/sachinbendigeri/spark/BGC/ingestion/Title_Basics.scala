package com.sachinbendigeri.spark.BGC.ingestion 

import com.sachinbendigeri.spark.BGC.constants.Constants
import com.sachinbendigeri.spark.BGC.utils.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

case class Title_BasicsStructure(
        tconst                              :String,
        titleType                           :String,
        primaryTitle                        :String,
        originalTitle                       :String, 
        isAdult                             :Short,
        startYear                           :String,
        endYear                             :String,
        runtimeMinutes                      :Short,
        genres                              :String
)


   
class Title_Basics(sparkSession: SparkSession, parameters: Map[String, String]) extends Serializable {
  

   import sparkSession.implicits._

   //Logger to log messages
   val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  
        
   def ReadFromTSV(FilePath: String): Dataset[Title_BasicsStructure] = {  
      val Title_BasicsSchema = ScalaReflection.schemaFor[Title_BasicsStructure].dataType.asInstanceOf[StructType]
      val df =  Utils.ReadFromTSV(sparkSession,FilePath,Title_BasicsSchema)                       
      return    df.as[Title_BasicsStructure]
   }
    
  def FullRefreshFromTSV(FilePath :String) = {
      val Title_BasicsDS = ReadFromTSV(FilePath)
      Utils.OverwriteTable(sparkSession,Title_BasicsDS.toDF(),Constants.PARAM_TITLE_BASICS_TABLE,parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
    
    
   def DeltaRefreshFromTSV(FilePath :String) = {
      val Title_BasicsDS = ReadFromTSV(FilePath)
      Utils.AppendTable(sparkSession,Title_BasicsDS.toDF(),Constants.PARAM_TITLE_BASICS_TABLE,parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
   
    
}