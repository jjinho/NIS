library(dplyr)
library(RSQLite)
library(DBI)

# Input
# year = NIS Year
# dx_list = list of ICD9 diagnoses (list of strings)
# Output
# Dataframe with all variables as Characters
nis_ecode_df <- function(year, ecode) {
  # Preprocess the ICD9 list
  # Remove all periods
  ecode <- gsub("\\.", "", ecode)
  
	# Convert to a string with each ICD9 ECODE encased in single quotes
  ecode <- paste("(", toString(paste("'", ecode, "'", sep="")), ")", sep="")
  
  # Get path to DB by year
  db_path <- gsub("y_", toString(year), "~/NIS/y_/nis_y_.db")
	
  # Connection
  con <- dbConnect(SQLite(), db_path)
  
  nis_ecode_query <- "SELECT * FROM core_y_ WHERE ECODE1 IN ecode OR ECODE2 IN ecode OR ECODE3 IN ecode OR ECODE4 IN ecode"; 
  
  # Put the correct year
  nis_ecode_query <- gsub("y_", toString(year), nis_ecode_query)
  nis_ecode_query <- gsub("ecode", ecode, nis_ecode_query)
  
  # Perform the query
  q <- dbSendQuery(con, nis_ecode_query)
  
  # Get the data
  ecode_df <- dbFetch(q, n=-1)
  
	# Clear the query to prevent memory leaks
  dbClearResult(q)
	
	return(ecode_df)
}