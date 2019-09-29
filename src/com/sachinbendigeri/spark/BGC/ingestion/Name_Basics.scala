package com.sachinbendigeri.spark.BGC.ingestion 

import com.sachinbendigeri.spark.BGC.constants.Constants
import com.sachinbendigeri.spark.BGC.utils.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

case class Name_BasicsStructure(
        nconst                              :String,
        primaryName                         :String,
        birthYear                           :String,
        deathYear                           :String, 
        primaryProfession                   :String,
        knownForTitles                      :String
)


   
class Name_Basics(sparkSession: SparkSession, parameters: Map[String, String]) extends Serializable {
  

   import sparkSession.implicits._

   //Logger to log messages
   val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  
        
   def ReadFromTSV(FilePath: String): Dataset[Name_BasicsStructure] = {  
      val Name_BasicsSchema = ScalaReflection.schemaFor[Name_BasicsStructure].dataType.asInstanceOf[StructType]
      val df =  Utils.ReadFromTSV(sparkSession,FilePath,Name_BasicsSchema)                       
      return    df.as[Name_BasicsStructure]
   }
    
  def FullRefreshFromCSV(FilePath :String) = {
      val Name_BasicsDS = ReadFromTSV(FilePath)
      Utils.OverwriteTable(sparkSession,Name_BasicsDS.toDF(),Constants.PARAM_NAME_BASICS_TABLE,parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
    
    
   def DeltaRefreshFromCSV(FilePath :String) = {
      val Name_BasicsDS = ReadFromTSV(FilePath)
      Utils.AppendTable(sparkSession,Name_BasicsDS.toDF(),Constants.PARAM_NAME_BASICS_TABLE,parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
   
    
}