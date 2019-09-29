# BCG Coding Challenge
The coding challenge has been broken down in to following set of activities
1. setup: To hold all the functions to setup the spark application, including parameters reading [currently disabled]
2. ingestion: To hold a serializable class for each kind of input that the BCG challenge needs
3. utils: To hold generic functions required for the application
4. BCGCodingChallenge is the spark in scala script for processing the data and generating the required output.

## Assumptions 
1. The code is currently set up to execute on the cluster and it can be changed in the SparkSetup.scala
2. The code assumes that the jar file wil be placed in the same folder as the unziped data files donwloaded from imdb

## constants/Constants.scala
All the required constants are set here. The Constants are names to the variables and not the variables themselves.

## utils/Utils.scala
 *FilesInFolder - Function returns the files in the folder [not used in the BCGCodingChallenge]* 

getCurrentTime - gets the current time to set the RunInstanceTime which is used to generate IngestYear, IngestMonth and IngestDay columns for partitioned HIVE tables

ReadFromTSV - Reads the TSV file based on the schema provided and return the Dataframe

OverwriteTable - Overwrites the content of the table by dynamically loading to appropriate partitions

 *AppendTable  - drops the partitions related to current RunInstanceTime and appends a new partition to cater for delta processing [not used in the BCG Coding Challenge]* 

 *ValidateLoadCounts  - Checks the counts in the table loaded and the file   [not used in the BCG Coding Challenge]* 

## setup/SparkSetup.scala
GetSparkContext - Sets the spark context and sets the appropriate config parameters for optimal solution

ReadParametersTable - Sets the default parameters but has code to read from hive table

 *UpdateParametersTable - Not currently used but can be used to save updates to parameters* 

## ingestion/Name_Basics.scala	
Name_BasicsStructure - Case class for the format of the name.basics.tsv file

ReadFromTSV - Applies the Schema and reads the name.basics.tsv file and returns a Dataset

 *FullRefreshFromCSV - Applies the Schema and reads the name.basics.tsv file and loads a hive table [not used in BCGCodingChallenge]* 

 *DeltaRefreshFromCSV - Applies the Schema and reads the name.basics.tsv file and loads a delta partition into hive table [not used in BCGCodingChallenge]* 

## ingestion/Title_AKAs.scala	
Title_AKAsStructure - Case class for the format of the title.akas.tsv file

ReadFromTSV - Applies the Schema and reads the title.akas.tsv file and returns a Dataset

 *FullRefreshFromCSV - Applies the Schema and reads the title.akas.tsv file and loads a hive table [not used in BCGCodingChallenge]* 

 *DeltaRefreshFromCSV - Applies the Schema and reads the title.akas.tsv file and loads a delta partition into hive table [not used in BCGCodingChallenge]* 

## ingestion/Title_Basics.scala	
Title_BasicsStructure - Case class for the format of the title.basics.tsv file

ReadFromTSV - Applies the Schema and reads the title.basics.tsv file and returns a Dataset

 *FullRefreshFromCSV - Applies the Schema and reads the title.basics.tsv file and loads a hive table [not used in BCGCodingChallenge]* 

 *DeltaRefreshFromCSV - Applies the Schema and reads the title.basics.tsv file and loads a delta partition into hive table [not used in BCGCodingChallenge]* 

## ingestion/Title_Principles.scala	
Title_PrinciplesStructure - Case class for the format of the title.principles.tsv file

ReadFromTSV - Applies the Schema and reads the title.principles.tsv file and returns a Dataset

 *FullRefreshFromCSV - Applies the Schema and reads the title.principles.tsv file and loads a hive table [not used in BCGCodingChallenge]* 

 *DeltaRefreshFromCSV - Applies the Schema and reads the title.principles.tsv file and loads a delta partition into hive table [not used in BCGCodingChallenge]* 

## ingestion/Title_Ratings.scala	
Title_RatingsStructure - Case class for the format of the title.ratings.tsv file

ReadFromTSV - Applies the Schema and reads the title.ratings.tsv file and returns a Dataset

 *FullRefreshFromCSV - Applies the Schema and reads the title.ratings.tsv file and loads a hive table [not used in BCGCodingChallenge]* 

 *DeltaRefreshFromCSV - Applies the Schema and reads the title.ratings.tsv file and loads a delta partition into hive table [not used in BCGCodingChallenge]* 


# BGCCodingChallenge.scala
### The main script which performs the bulk of the processing
### 1. Sets the spark context [currently set to cluster but tested locally]
### 2. Reads the parameters into a Map[String,String] for ease of use throughout the script
### 3. Instantiate all the ingestion related Objects for Serialised processing 
### 4. Loads the hive table BGC_TOP20MOVIES based on BGC movie rating method

  4.a - Get the titles basic data to filter on titleType = movie - repartition appropriately
  
  4.b - Get the ratings data and repartition appropriately
  
  4.c - Join the two datasets
  
  4.d - Set window as the whole dataset and get the Average Number of Votes for Movies
  
  4.e - calculate the BGCRating for Movies  
  
  4.f - calculate the ordering by the BCG rating using row_number()
  
  4.g - select the required columns
  
  4.h - persist it as its used multiple times
  
  4.i - overwrite the data into a HIVE table BGC_TOP20MOVIES 
  
  4.j - Show the rows to be captured in the log
  
### 5. Loads the hive table BGC_TOPMOVIES_PRINCIPLES

  5.a - Get the principles data 
  
  5.b - perform a broadcast join with small 20 row dataset from Step4 
  
  5.c - get the names data for the principle characters
  
  5.d - join with principles data from output of step 5.b as a broadcase join
  
  5.e - select the required columns
  
  5.f - overwrite the data into a HIVE table BGC_TOP20MOVIES_PRINCIPLES 
  
  5.g - Show the rows to be captured in the log
  
### 6. Loads the hive table BGC_TOPMOVIES_AKAS

  6.a - Get the AKA Titles data 
  
  6.b - perform a broadcast join with small 20 row dataset from Step4 
  
  6.c - select the required columns
  
  6.d - overwrite the data into a HIVE table BGC_TOP20MOVIES_AKAS 
  
  6.e - Show the rows to be captured in the log
  
# BGCCodingChallengeLocalMachineExecutionLogs.log
### Holds the log when executed locally and also includes the outputs of the dataset for validation.
