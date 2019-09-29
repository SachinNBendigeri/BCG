package com.sachinbendigeri.spark.BGC.utils
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, SparkSession,SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.Dataset
import java.time.format.DateTimeFormatter
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions.{lit, _}
import java.io.File



class RefStructure (
   Type_Code :Short,
   Type :String
)

object Utils {
   
  //Logger to log messages
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  

  def FilesInFolder(DirectoryPath:String): Array[Array[String]]= {

    // list only the folders directly under this directory (does not recurse)
    val files: Array[Array[String]] = (new File(DirectoryPath))
        .listFiles
        .filter(_.isFile)  //isFile to find files
        .map(f => Array(f.getAbsoluteFile().toString(),f.getName().replaceAll(".csv","")))
        
    return files
  }  
  
  
    /********************************************************************
   //  Get Current Time in String Format
    *********************************************************************/
   def getCurrentTime: String = {
       DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(java.time.LocalDateTime.now())
    }
    


    /********************************************************************
    //  FUNCTION TO OVERWRITE DATA INTO THE HIVE TABLE with PARITION
    *********************************************************************/   
    def ReadFromTSV(sparkSession : SparkSession, FilePath: String, FileSchema :StructType): DataFrame = {     
      logger.info("Utils.ReadFromCSV(" + FilePath + ")- Started @ " + java.time.LocalDateTime.now)                 
      val df =  sparkSession.read.format("csv")
                .option("delimiter", "\t")
                .option("header", "true") //first line in file has headers
           //   .option("mode", "DROPMALFORMED")
                .schema(FileSchema)
                .load(FilePath)
      logger.info("Utils.ReadFromCSV(" + FilePath + ")- Completed @ " + java.time.LocalDateTime.now)                 
      return df
    }

    /********************************************************************
    *  FUNCTION TO OVERWRITE DATA INTO THE HIVE TABLE with PARITION
    *
    *********************************************************************/
    def OverwriteTable(sparkSession : SparkSession, DF :DataFrame, TableName :String, RunInstanceTime :String):DataFrame= {
      logger.info("Utils.OverwriteTable(" + TableName + ")- Started @ " + java.time.LocalDateTime.now)                 
      val DFwithIngestFlags =  DF.withColumn("IngestYear",lit(RunInstanceTime.substring(0,4)))
          .withColumn("IngestMonth",lit(RunInstanceTime.substring(5,7)))
          .withColumn("IngestDay",lit(RunInstanceTime.substring(8,10)))
          
      DFwithIngestFlags.write
          .partitionBy("IngestYear","IngestMonth","IngestDay")
          .mode(SaveMode.Overwrite)
          .format("orc")
          .saveAsTable(TableName)
      logger.info("Utils.OverwriteTable(" + TableName + ")- Completed @ " + java.time.LocalDateTime.now)                 
      return DFwithIngestFlags    
    }

   
    /********************************************************************
    //  FUNCTION TO APPEND DATA INTO THE HIVE TABLE
    *********************************************************************/
    def AppendTable(sparkSession : SparkSession, DF :DataFrame, TableName :String, RunInstanceTime :String) :DataFrame= {
        logger.info("Utils.AppendTable(" + TableName + ")- Started @ " + java.time.LocalDateTime.now)                     
        // Build Paritition Specification that needs to be dropped before added the new dataset
        var partitionSpecification = "IngestYear='"+RunInstanceTime.substring(0,4)+ "',"
        partitionSpecification = partitionSpecification +  "IngestMonth='"+RunInstanceTime.substring(5,7)+ "',"
        partitionSpecification = partitionSpecification +  "IngestDay='"+RunInstanceTime.substring(8,10)+ "'"
        // Drop the Partition
        try {
          val sourceDF = sparkSession.sql("Alter table " + TableName + " drop if exists partition("+partitionSpecification+")")
           }
        catch {
          case err: Throwable =>
            logger.error("Error while dropping partition " + partitionSpecification + " from the table " + TableName  + " ")
            throw new RuntimeException(err)
        }
       
        val DFwithIngestFlags =  DF.withColumn("IngestYear",lit(RunInstanceTime.substring(0,4)))
                     .withColumn("IngestMonth",lit(RunInstanceTime.substring(5,7)))
                     .withColumn("IngestDay",lit(RunInstanceTime.substring(8,10)))
        // Add the new data into a new day partition     
        DFwithIngestFlags.write
              .partitionBy("IngestYear","IngestMonth","IngestDay")   
              .mode(SaveMode.Append)
              .format("orc")
              .saveAsTable(TableName)
        logger.info("Utils.AppendTable(" + TableName + ")- Completed @ " + java.time.LocalDateTime.now)                 
        return DFwithIngestFlags
    }
        
    
    /********************************************************************
    //  FUNCTION TO VALIDATE THE DATA IS LOADED CORRECTLY
    *********************************************************************/  
    def ValidateLoadCounts(sparkSession : SparkSession, InputFilePath: String, TableName :String,RunInstanceTime :String, Tolerance :Long): Boolean = {     
      logger.info("Utils.ValidateLoadCounts(" +InputFilePath +","+ TableName + ")- Started @ " + java.time.LocalDateTime.now)                                             
      //get the count of the file
      val countFile =  sparkSession.read.text(InputFilePath).count()
      //get the count of records loaded into the partition
      val countDF = sparkSession.sql("select * from " + TableName)
                       .filter("  IngestYear = '" + RunInstanceTime.substring(0,4) +"' and " +
                               "  IngestYear = '" + RunInstanceTime.substring(5,7) +"' and " +
                               "  IngestDay = '" + RunInstanceTime.substring(8,10) +"'" ).count()
      logger.info("Utils.ValidateLoadCounts(" +InputFilePath +","+ TableName + ")- Completed @ " + java.time.LocalDateTime.now)                                             
      //validate that the counts are under the tolerance
      if(countFile >= countDF - Tolerance && countFile <= countDF + Tolerance) {
        return true    
      }
      return false
    }
    

}