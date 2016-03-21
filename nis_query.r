library(RSQLite)
library(DBI)
library(dplyr)

# Schemas for formating NIS data frames by years
format_2005 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Num","Num","Char","Char")
format_2006 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Num","Num","Char","Char")
format_2007 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char")
format_2008 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char","Char")
format_2009 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char","Char")
format_2010 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char","Char","Char")
format_2011 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char","Char","Char")
format_2012 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Num","Char","Char","Char","Char")

# Input
# df = data frame containing the samples to be converted
# year = year of NIS data
# format = format for that year
reformat_nis_dataframe <- function(df, format) {
  counter = 1
  for(f in format) {
    # Remove unknowns
    df[,counter][grep("-[0-9]+", df[,counter])] <- NA
    
    if(f == "Num") {
      # Num
      df[,counter] <- as.numeric(df[,counter])
    } else {
      # Char
      df[,counter] <- as.factor(df[,counter])
    }
    counter <- counter + 1
  }
  return(df)
}

# Input
# year = NIS Year
# dx_list = list of ICD9 diagnoses (list of strings)
# Output
# Dataframe with all variables as Characters
get_by_dx <- function(year, dx_list) {
  # Preprocess the ICD9 list
  # Remove all periods
  dx_list <- gsub("\\.", "", dx_list)
  # Convert to a string with each ICD9 diagnosis encased in single quotes
  dx_list <- paste("(", toString(paste("'", dx_list, "'", sep="")), ")", sep="")
  
  # Get path to DB by year
  db_path <- gsub("y", toString(year), "y/nis_y.db")

  # Connection
  con = dbConnect(SQLite(), db_path)
  
  if(year > 2008) {
    # Years that have 25 DXs
    dx_query <- "SELECT * FROM core_y WHERE DX1 IN dx OR DX2 IN dx OR DX3 IN dx OR DX4 IN dx OR DX5 IN dx OR DX6 IN dx OR DX7 IN dx OR DX8 IN dx OR DX9 IN dx OR DX10 IN dx OR DX11 IN dx OR DX12 IN dx OR DX13 IN dx OR DX14 IN dx OR DX15 IN dx OR DX16 IN dx OR DX17 IN dx OR DX18 IN dx OR DX19 IN dx OR DX20 IN dx OR DX21 IN dx OR DX22 IN dx OR DX23 IN dx OR DX24 IN dx OR DX25 IN dx;"
  } else {
    # Years that have 15 Dx
    dx_query <- "SELECT * FROM core_y WHERE DX1 IN dx OR DX2 IN dx OR DX3 IN dx OR DX4 IN dx OR DX5 IN dx OR DX6 IN dx OR DX7 IN dx OR DX8 IN dx OR DX9 IN dx OR DX10 IN dx OR DX11 IN dx OR DX12 IN dx OR DX13 IN dx OR DX14 IN dx OR DX15 IN dx;"
  }
  # Put the correct year
  dx_query <- gsub("y", toString(year), dx_query)
  # Put the correct list of ICD9 diagnoses
  dx_query <- gsub("dx", dx_list, dx_query)
  
  # Perform the query
  q <- dbSendQuery(con, dx_query)
  
  # Get the data
  db_data <- dbFetch(q, n=-1)
  
  # Clear the query to prevent memory leaks
  dbClearResult(q)
  
  # Return all results
  return(db_data)
}

# Input
# year = NIS Year
# dx_list = list of ICD9 procedures (list of strings)
# Output
# Dataframe with all variables as Characters
get_by_pr <- function(year, pr_list) {
  # Preprocess the ICD9 list
  # Remove all periods
  pr_list <- gsub("\\.", "", pr_list)
  # Convert to a string with each ICD9 diagnosis encased in single quotes
  pr_list <- paste("(", toString(paste("'", pr_list, "'", sep="")), ")", sep="")
  
  # Get path to DB by year
  db_path <- gsub("y", toString(year), "y/nis_y.db")
  
  # Connection
  con = dbConnect(SQLite(), db_path)

  # All years have 15 PRs
  pr_query <- "SELECT * FROM core_y WHERE PR1 IN pr OR PR2 IN pr OR PR3 IN pr OR PR4 IN pr OR PR5 IN pr OR PR6 IN pr OR PR7 IN pr OR PR8 IN pr OR PR9 IN pr OR PR10 IN pr OR PR11 IN pr OR PR12 IN pr OR PR13 IN pr OR PR14 IN pr OR PR15 IN pr;"
  
  # Put the correct year
  pr_query <- gsub("y", toString(year), pr_query)
  # Put the correct list of ICD9 diagnoses
  pr_query <- gsub("pr", pr_list, pr_query)
  
  # Perform the query
  q <- dbSendQuery(con, pr_query)
  
  # Get the data
  db_data <- dbFetch(q, n=-1)
  
  # Clear the query to prevent memory leaks
  dbClearResult(q)
  
  # Return all results
  return(db_data)
}