package com.sachinbendigeri.spark.BGC.ingestion 

import com.sachinbendigeri.spark.BGC.constants.Constants
import com.sachinbendigeri.spark.BGC.utils.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

case class Title_PrinciplesStructure(
        tconst                              :String,
        ordering                            :Int,
        nconst                              :String,
        category                            :String, 
        job                                 :String,
        characters                          :String
)


   
class Title_Principles(sparkSession: SparkSession, parameters: Map[String, String]) extends Serializable {
  

   import sparkSession.implicits._

   //Logger to log messages
   val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  
        
   def ReadFromTSV(FilePath: String): Dataset[Title_PrinciplesStructure] = {  
      val Title_PrinciplesSchema = ScalaReflection.schemaFor[Title_PrinciplesStructure].dataType.asInstanceOf[StructType]
      val df =  Utils.ReadFromTSV(sparkSession,FilePath,Title_PrinciplesSchema)                       
      return    df.as[Title_PrinciplesStructure]
   }
    
  def FullRefreshFromTSV(FilePath :String) = {
      val Title_PrinciplesDS = ReadFromTSV(FilePath)
      Utils.OverwriteTable(sparkSession,Title_PrinciplesDS.toDF(),Constants.PARAM_TITLE_PRINCIPLES_TABLE,parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
    
    
   def DeltaRefreshFromTSV(FilePath :String) = {
      val Title_PrinciplesDS = ReadFromTSV(FilePath)
      Utils.AppendTable(sparkSession,Title_PrinciplesDS.toDF(),Constants.PARAM_TITLE_PRINCIPLES_TABLE,parameters(Constants.PARAM_RUN_INSTANCE_TIME))  
   }  
   
    
}