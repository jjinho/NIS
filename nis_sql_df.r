# Input
# year = NIS Year
# dx_list = list of ICD9 diagnoses (list of strings)
# Output
# Dataframe with all variables as Characters
nis_sql_df <- function(year, inc_dx, exc_dx, inc_pr, exc_pr) {
  # Preprocess the ICD9 list
  # Remove all periods
  inc_dx <- gsub("\\.", "", inc_dx)
	exc_dx <- gsub("\\.", "", exc_dx)
  inc_pr <- gsub("\\.", "", inc_pr)
  exc_pr <- gsub("\\.", "", exc_pr)
  
	# Convert to a string with each ICD9 diagnosis encased in single quotes
  inc_dx <- paste("(", toString(paste("'", inc_dx, "'", sep="")), ")", sep="")
  exc_dx <- paste("(", toString(paste("'", exc_dx, "'", sep="")), ")", sep="")
	inc_pr <- paste("(", toString(paste("'", inc_pr, "'", sep="")), ")", sep="")
	exc_pr <- paste("(", toString(paste("'", exc_pr, "'", sep="")), ")", sep="")
	
  # Get path to DB by year
  db_path <- gsub("y_", toString(year), "y_/nis_y_.db")
	print(db_path)
	
  # Connection
  con <- dbConnect(SQLite(), db_path)
  
  if(year > 2008) {
    # Years that have 25 DXs
		nis_core_query <- "SELECT * FROM core_y_ WHERE DX1 IN inc_dx OR DX2 IN inc_dx OR DX3 IN inc_dx OR DX4 IN inc_dx OR DX5 IN inc_dx OR DX6 IN inc_dx OR DX7 IN inc_dx OR DX8 IN inc_dx OR DX9 IN inc_dx OR DX10 IN inc_dx OR DX11 IN inc_dx OR DX12 IN inc_dx OR DX13 IN inc_dx OR DX14 IN inc_dx OR DX15 IN inc_dx OR DX16 IN inc_dx OR DX17 IN inc_dx OR DX18 IN inc_dx OR DX19 IN inc_dx OR DX20 IN inc_dx OR DX21 IN inc_dx OR DX22 IN inc_dx OR DX23 IN inc_dx OR DX24 IN inc_dx OR DX25 IN inc_dx AND (DX1 NOT IN exc_dx AND DX2 NOT IN exc_dx AND DX3 NOT IN exc_dx AND DX4 NOT IN exc_dx AND DX5 NOT IN exc_dx AND DX6 NOT IN exc_dx AND DX7 NOT IN exc_dx AND DX8 NOT IN exc_dx AND DX9 NOT IN exc_dx AND DX10 NOT IN exc_dx AND DX11 NOT IN exc_dx AND DX12 NOT IN exc_dx AND DX13 NOT IN exc_dx AND DX14 NOT IN exc_dx AND DX15 NOT IN exc_dx AND DX16 NOT IN exc_dx AND DX17 NOT IN exc_dx AND DX18 NOT IN exc_dx AND DX19 NOT IN exc_dx AND DX20 NOT IN exc_dx AND DX21 NOT IN exc_dx AND DX22 NOT IN exc_dx AND DX23 NOT IN exc_dx AND DX24 NOT IN exc_dx AND DX25 NOT IN exc_dx) AND (PR1 IN inc_pr OR PR2 IN inc_pr OR PR3 IN inc_pr OR PR4 IN inc_pr OR PR5 IN inc_pr OR PR6 IN inc_pr OR PR7 IN inc_pr OR PR8 IN inc_pr OR PR9 IN inc_pr OR PR10 IN inc_pr OR PR11 IN inc_pr OR PR12 IN inc_pr OR PR13 IN inc_pr OR PR14 IN inc_pr OR PR15 IN inc_pr) AND (PR1 NOT IN exc_pr AND PR2 NOT IN exc_pr AND PR3 NOT IN exc_pr AND PR4 NOT IN exc_pr AND PR5 NOT IN exc_pr AND PR6 NOT IN exc_pr AND PR7 NOT IN exc_pr AND PR8 NOT IN exc_pr AND PR9 NOT IN exc_pr AND PR10 NOT IN exc_pr AND PR11 NOT IN exc_pr AND PR12 NOT IN exc_pr AND PR13 NOT IN exc_pr AND PR14 NOT IN exc_pr AND PR15 NOT IN exc_pr)"; 
  } else {
    # Years that have 15 Dx
    nis_core_query <- "SELECT * FROM core_y_ WHERE (DX1 IN inc_dx OR DX2 IN inc_dx OR DX3 IN inc_dx OR DX4 IN inc_dx OR DX5 IN inc_dx OR DX6 IN inc_dx OR DX7 IN inc_dx OR DX8 IN inc_dx OR DX9 IN inc_dx OR DX10 IN inc_dx OR DX11 IN inc_dx OR DX12 IN inc_dx OR DX13 IN inc_dx OR DX14 IN inc_dx OR DX15 IN inc_dx) AND (DX1 NOT IN exc_dx AND DX2 NOT IN exc_dx AND DX3 NOT IN exc_dx AND DX4 NOT IN exc_dx AND DX5 NOT IN exc_dx AND DX6 NOT IN exc_dx AND DX7 NOT IN exc_dx AND DX8 NOT IN exc_dx AND DX9 NOT IN exc_dx AND DX10 NOT IN exc_dx AND DX11 NOT IN exc_dx AND DX12 NOT IN exc_dx AND DX13 NOT IN exc_dx AND DX14 NOT IN exc_dx AND DX15 NOT IN exc_dx) AND (PR1 IN inc_pr OR PR2 IN inc_pr OR PR3 IN inc_pr OR PR4 IN inc_pr OR PR5 IN inc_pr OR PR6 IN inc_pr OR PR7 IN inc_pr OR PR8 IN inc_pr OR PR9 IN inc_pr OR PR10 IN inc_pr OR PR11 IN inc_pr OR PR12 IN inc_pr OR PR13 IN inc_pr OR PR14 IN inc_pr OR PR15 IN inc_pr) AND (PR1  NOT IN exc_pr AND PR2  NOT IN exc_pr AND PR3  NOT IN exc_pr AND PR4  NOT IN exc_pr AND PR5  NOT IN exc_pr AND PR6  NOT IN exc_pr AND PR7  NOT IN exc_pr AND PR8  NOT IN exc_pr AND PR9  NOT IN exc_pr AND PR10 NOT IN exc_pr AND PR11 NOT IN exc_pr AND PR12 NOT IN exc_pr AND PR13 NOT IN exc_pr AND PR14 NOT IN exc_pr AND PR15 NOT IN exc_pr)"; 
  }
  # Put the correct year
  nis_core_query <- gsub("y_", toString(year), nis_core_query)
  # Put the correct list of ICD9 diagnoses
  nis_core_query <- gsub("inc_dx", inc_dx, nis_core_query)
  nis_core_query <- gsub("exc_dx", exc_dx, nis_core_query)
	nis_core_query <- gsub("inc_pr", inc_pr, nis_core_query)
	nis_core_query <- gsub("exc_pr", exc_pr, nis_core_query)
	
  # Perform the query
  q <- dbSendQuery(con, nis_core_query)
  
  # Get the data
  core_df <- dbFetch(q, n=-1)
  
	# Clear the query to prevent memory leaks
  dbClearResult(q)
	
	# Obtaining the Severity data
	if(year == 2012) {
		key_list <- paste("(", toString(paste("'", core_df$KEY_NIS, "'", sep="")), ")", sep="")
	} else {
		key_list <- paste("(", toString(paste("'", core_df$KEY, "'", sep="")), ")", sep="")
	}
  nis_sev_query <- "SELECT * FROM severity_year_ WHERE KEY in list;"
  nis_sev_query <- gsub("year_", toString(year), nis_sev_query)
  nis_sev_query <- gsub("list", key_list, nis_sev_query)
  
	
	# Perform the query
  q <- dbSendQuery(con, nis_sev_query)
  
  # Get the data
  sev_df <- dbFetch(q, n=-1)
	
  # Clear the query to prevent memory leaks
  dbClearResult(q)
  
	# Format
	format_nis_sev_ <- function(df) {
		for(n in names(df)) {
			df[[n]] <- as.factor(df[[n]])
		}
		return(df)
	}
	
	sev_df <- format_nis_sev_(sev_df)

	#----------------------------------------------------------------------------
	# Reformating
	format_2005 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Num","Num","Char","Char")
	format_2006 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Num","Num","Char","Char")
	format_2007 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char")
	format_2008 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char","Char")
	format_2009 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char","Char")
	format_2010 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char","Char","Char")
	format_2011 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char","Char","Char")
	format_2012 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Num","Char","Char","Char","Char")

	format_nis_core_	<- function(df, format) {
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
	
	if(year == 2005) core_df <- format_nis_core_(core_df, format_2005)
	if(year == 2006) core_df <- format_nis_core_(core_df, format_2006)
	if(year == 2007) core_df <- format_nis_core_(core_df, format_2007)
	if(year == 2008) core_df <- format_nis_core_(core_df, format_2008)
	if(year == 2009) core_df <- format_nis_core_(core_df, format_2009)
	if(year == 2010) core_df <- format_nis_core_(core_df, format_2010)
	if(year == 2011) core_df <- format_nis_core_(core_df, format_2011)
	if(year == 2012) core_df <- format_nis_core_(core_df, format_2012)

	# Merge core_df and sev_df
	if(year == 2012) {
		core_df <- merge(core_df, sev_df, by="KEY_NIS")	
	} else {
		core_df <- merge(core_df, sev_df, by="KEY")
	}
	rm(sev_df)
	
		#----------------------------------------------------------------------------
	# Cleaning the data
	# Rename FEMALE to GENDER
	names(core_df)[names(core_df) == "FEMALE"] <- "GENDER"

	relevel_gender_ <- function(female) {
		levels(female)[levels(female) == "0"] <- "M"
		levels(female)[levels(female) == "1"] <- "F"
		return(female)
	}

	relevel_0_1_ <- function(f) {
		levels(f)[levels(f) == "0"] <- "N"
		levels(f)[levels(f) == "1"] <- "Y"
		return(f)
	}

	relevel_race_ <- function(f) {
		levels(f)[levels(f) == "1"] <- "White"
		levels(f)[levels(f) == "2"] <- "Black"
		levels(f)[levels(f) == "3"] <- "Hispanic"
		levels(f)[levels(f) == "4"] <- "Asian_Pacific"
		levels(f)[levels(f) == "5"] <- "Native_American"
		levels(f)[levels(f) == "6"] <- "Other"
		return(f)
	}

	relevel_pay_ <- function(f) {
		levels(f)[levels(f) == "1"] <- "Medicare"
		levels(f)[levels(f) == "2"] <- "Medicaid"
		levels(f)[levels(f) == "3"] <- "Private_Insurance"
		levels(f)[levels(f) == "4"] <- "Self_Pay"
		levels(f)[levels(f) == "5"] <- "No Charge"
		levels(f)[levels(f) == "6"] <- "Other"
		return(f)
	}

	relevel_zipinc_qrtl_ <- function(f) {
		levels(f)[levels(f) == "1"] <- "Quartile1"
		levels(f)[levels(f) == "2"] <- "Quartile2"
		levels(f)[levels(f) == "3"] <- "Quartile3"
		levels(f)[levels(f) == "4"] <- "Quartile4"
		return(f)
	}

	# Remove duplicates
	core_df <- unique(core_df)
	
	core_df$CM_AIDS        <- relevel_0_1(core_df$CM_AIDS)
	core_df$CM_ALCOHOL     <- relevel_0_1(core_df$CM_ALCOHOL)
	core_df$CM_ANEMDEF     <- relevel_0_1(core_df$CM_ANEMDEF)
	core_df$CM_ARTH        <- relevel_0_1(core_df$CM_ARTH)
	core_df$CM_BLDLOSS     <- relevel_0_1(core_df$CM_BLDLOSS)
	core_df$CM_CHF         <- relevel_0_1(core_df$CM_CHF)
	core_df$CM_CHRNLUNG    <- relevel_0_1(core_df$CM_CHRNLUNG)
	core_df$CM_COAG        <- relevel_0_1(core_df$CM_COAG)
	core_df$CM_DEPRESS     <- relevel_0_1(core_df$CM_DEPRESS)
	core_df$CM_DM          <- relevel_0_1(core_df$CM_DM)
	core_df$CM_DMCX  			 <- relevel_0_1(core_df$CM_DMCX)
	core_df$CM_DRUG        <- relevel_0_1(core_df$CM_DRUG)
	core_df$CM_HTN_C       <- relevel_0_1(core_df$CM_HTN_C)
	core_df$CM_HYPOTHY     <- relevel_0_1(core_df$CM_HYPOTHY)
	core_df$CM_LIVER       <- relevel_0_1(core_df$CM_LIVER)
	core_df$CM_LYMPH       <- relevel_0_1(core_df$CM_LYMPH)
	core_df$CM_LYTES       <- relevel_0_1(core_df$CM_LYTES)
	core_df$CM_METS        <- relevel_0_1(core_df$CM_METS)
	core_df$CM_NEURO       <- relevel_0_1(core_df$CM_NEURO)
	core_df$CM_OBESE       <- relevel_0_1(core_df$CM_OBESE)
	core_df$CM_PARA        <- relevel_0_1(core_df$CM_PARA)
	core_df$CM_PERIVASC    <- relevel_0_1(core_df$CM_PERIVASC)
	core_df$CM_PSYCH       <- relevel_0_1(core_df$CM_PSYCH)
	core_df$CM_PULMCIRC    <- relevel_0_1(core_df$CM_PULMCIRC)
	core_df$CM_RENLFAIL    <- relevel_0_1(core_df$CM_RENLFAIL)
	core_df$CM_TUMOR       <- relevel_0_1(core_df$CM_TUMOR)
	core_df$CM_ULCER       <- relevel_0_1(core_df$CM_ULCER)
	core_df$CM_VALVE       <- relevel_0_1(core_df$CM_VALVE)
	core_df$CM_WGHTLOSS    <- relevel_0_1(core_df$CM_WGHTLOSS)
	
	core_df$ELECTIVE    <- relevel_0_1(					core_df$ELECTIVE)
	core_df$GENDER      <- relevel_gender(			core_df$GENDER)
	core_df$DIED        <- relevel_0_1(					core_df$DIED)
	core_df$RACE        <- relevel_race(				core_df$RACE)
	core_df$PAY1        <- relevel_pay(					core_df$PAY1)
	if(year == 2005) {
		core_df$ZIPInc_Qrtl <- relevel_zipinc_qrtl(	core_df$ZIPInc_Qrtl)
	} else {
		core_df$ZIPINC_QRTL <- relevel_zipinc_qrtl(	core_df$ZIPINC_QRTL)
	}
	
	# TODO
	# Convert -9 to NA
	# Convert blank spaces to NA
		
	return(core_df)
}
