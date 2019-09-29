package com.sachinbendigeri.spark.BGC.ingestion 

import com.sachinbendigeri.spark.BGC.constants.Constants
import com.sachinbendigeri.spark.BGC.utils.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

case class Title_AKAsStructure(
        titleId                             :String,
        ordering                            :Int,
        title                               :String,
        region                              :String, 
        language                            :String,
        types                               :String,
        attributes                          :String,
        isOriginalTitle                     :Short
)


   
class Title_AKAs(sparkSession: SparkSession, parameters: Map[String, String]) extends Serializable {
  

   import sparkSession.implicits._

   //Logger to log messages
   val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  
        
   def ReadFromTSV(FilePath: String): Dataset[Title_AKAsStructure] = {  
      val Title_AKAsSchema = ScalaReflection.schemaFor[Title_AKAsStructure].dataType.asInstanceOf[StructType]
      val df =  Utils.ReadFromTSV(sparkSession,FilePath,Title_AKAsSchema)                       
      return    df.as[Title_AKAsStructure]
   }
    
  def FullRefreshFromTSV(FilePath :String) = {
      val Title_AKAsDS = ReadFromTSV(FilePath)
      Utils.OverwriteTable(sparkSession,Title_AKAsDS.toDF(),Constants.PARAM_TITLE_AKAS_TABLE,parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
    
    
   def DeltaRefreshFromTSV(FilePath :String) = {
      val Title_AKAsDS = ReadFromTSV(FilePath)
      Utils.AppendTable(sparkSession,Title_AKAsDS.toDF(),Constants.PARAM_TITLE_AKAS_TABLE,parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
   
    
}