package com.sachinbendigeri.spark.BGC.setup

import org.apache.spark.sql.{SparkSession}
import org.apache.log4j._
import com.sachinbendigeri.spark.BGC.utils.Utils
import com.sachinbendigeri.spark.BGC.constants.Constants
import scala.reflect.api.materializeTypeTag

object SparkSetup {
  /** ************************************************************
    * Set up Spark Context
    * *************************************************************/
  def GetSparkContext(AppName: String): SparkSession = {
    
    val sparkSession =  SparkSession
      .builder
      .appName(AppName)
     // .master("local[*]")
     // .config("spark.sql.warehouse.dir", "file:///C:/temp") 
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sql("set hive.exec.dynamic.partition=true")
    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sparkSession.sql("set hive.auto.convert.join=true")
    sparkSession.sql("set hive.enforce.bucketmap.join=true")
    sparkSession.sql("set hive.optimize.bucketmap.join=true")
    sparkSession.sql("set hive.exec.parallel=true")
    sparkSession.sql("set hive.execution.engine=tez")
    sparkSession.sql("set hive.vectorized.execution.enabled=true")
    sparkSession.sql("set hive.vectorized.execution.reduce.enabled=true")


    sparkSession.conf.set("hive.exec.dynamic.partition", "true")
    sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    sparkSession.conf.set("hive.auto.convert.join", "true")
    sparkSession.conf.set("hive.enforce.bucketmap.join", "true")
    sparkSession.conf.set("hive.optimize.bucketmap.join", "true")
    sparkSession.conf.set("hive.exec.parallel", "true")
    sparkSession.conf.set("hive.execution.engine", "tez")
    sparkSession.conf.set("hive.vectorized.execution.enabled", "true")
    sparkSession.conf.set("hive.vectorized.execution.reduce.enabled", "true")

    sparkSession.conf.set("spark.broadcast.compress", "true")
    sparkSession.conf.set("spark.shuffle.compress", "true")
    sparkSession.conf.set("spark.shuffle.spill.compress", "true")
    sparkSession.conf.set("spark.hive.mapred.supports.subdirectories", "true")
    sparkSession.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    sparkSession.conf.set("spark.sql.broadcastTimeout", "3600")
    sparkSession.conf.set("spark.sql.orc.filterPushdown", "true")
    sparkSession.conf.set("spark.sql.orc.splits.include.file.footer", "true")
    sparkSession.conf.set("spark.sql.orc.cache.stripe.details.size", "10000")
    sparkSession.conf.set("spark.sql.hive.metastorePartitionPruning", "true")
    sparkSession.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    sparkSession.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped", "true")
    sparkSession
  }

    /**************************************************************
    * Reads the Parameter table into a Map for use by the rest of the
    * programs
    * *************************************************************/
  def ReadParametersTable(TableName: String, sparkSession: SparkSession, ClusterMachine :Boolean): Map[String, String] = {
    
   /* Code to read from Parameters table from hive
    val parameters = sparkSession.sql(
      "Select Field , " +
        " COALESCE(FieldValue,'') AS FieldValue  " +
        " From " + TableName +
        " where field is not null")
      .collect()
      .map(x => (x.get(0).toString.trim, x.get(1).toString.trim))
      .toMap
     */ 
       //Set The Parameters or can be passed as a HIVE Table Name with args(0) as table name 
      //Set the RunTime Parameter used to tag IngestDate Time for the generated records  
      var parameters = Map(Constants.PARAM_RUN_INSTANCE_TIME -> Utils.getCurrentTime)
      //Set the Tables Name Parameters
      parameters = parameters ++ Map(Constants.PARAM_TITLE_AKAS_TABLE->"BGC_TITLE_AKA")
      parameters = parameters ++ Map(Constants.PARAM_TITLE_BASICS_TABLE->"BGC_TITLE_BASICS")
      parameters = parameters ++ Map(Constants.PARAM_TITLE_PRINCIPLES_TABLE->"BGC_TITLE_PRINCIPLES")
      parameters = parameters ++ Map(Constants.PARAM_TITLE_RATINGS_TABLE->"BGC_TITLE_RATINGS")
      parameters = parameters ++ Map(Constants.PARAM_NAME_BASICS_TABLE->"BGC_NAME_BASICS")
      //Set the File Path Parameters
      if( ClusterMachine) {
        parameters = parameters ++ Map(Constants.PARAM_TITLE_AKAS_FILEPATH->"title.akas.tsv")
        parameters = parameters ++ Map(Constants.PARAM_TITLE_BASICS_FILEPATH->"title.basics.tsv")
        parameters = parameters ++ Map(Constants.PARAM_TITLE_PRINCIPLES_FILEPATH->"title.principles.tsv")
        parameters = parameters ++ Map(Constants.PARAM_TITLE_RATINGS_FILEPATH->"title.ratings.tsv")
        parameters = parameters ++ Map(Constants.PARAM_NAME_BASICS_FILEPATH->"name.basics.tsv")
      }
      else {
        parameters = parameters ++ Map(Constants.PARAM_TITLE_AKAS_FILEPATH->"D:\\Contracting\\20190929 - BGC Partners - Code Challenge\\title.akas.tsv")
        parameters = parameters ++ Map(Constants.PARAM_TITLE_BASICS_FILEPATH->"D:\\Contracting\\20190929 - BGC Partners - Code Challenge\\title.basics.tsv")
        parameters = parameters ++ Map(Constants.PARAM_TITLE_PRINCIPLES_FILEPATH->"D:\\Contracting\\20190929 - BGC Partners - Code Challenge\\title.principles.tsv")
        parameters = parameters ++ Map(Constants.PARAM_TITLE_RATINGS_FILEPATH->"D:\\Contracting\\20190929 - BGC Partners - Code Challenge\\title.ratings.tsv")
        parameters = parameters ++ Map(Constants.PARAM_NAME_BASICS_FILEPATH->"D:\\Contracting\\20190929 - BGC Partners - Code Challenge\\name.basics.tsv")
      }
      //Set the output table Parameters
      parameters = parameters ++ Map(Constants.PARAM_BGC_TOP20MOVIES_TABLE->"BGC_TOP20MOVIES")
      parameters = parameters ++ Map(Constants.PARAM_BGC_TOP20MOVIES_PRICNCIPLES_TABLE->"BGC_TOP20MOVIES_PRINCIPLES")
      parameters = parameters ++ Map(Constants.PARAM_BGC_TOP20MOVIES_AKATITLES_TABLE->"BGC_TOP20MOVIES_AKATITLES")

    
    parameters
  }


   /** ************************************************************
    * Updates the Parameter table back into a HIVE table for other programs to use
    * programs
    * *************************************************************/
  def UpdateParametersTable(Parameters: Map[String, String], TableName: String, sparkSession: SparkSession): Unit = {
    sparkSession.createDataFrame(Parameters.toSeq)
      .toDF("Field", "FieldValue")
      .write.mode("Overwrite")
      .saveAsTable(TableName)
  }


}