library(data.table)
library(stringr)
library(doParallel)
library(lubridate)
library(foreach)

# set directory
setwd("~/scratch-midway2/ECMA31330/")

`%ni%` <- Negate(`%in%`)

# parallelization cluster
cl <- makeCluster(getOption('cl.cores', detectCores()))
registerDoParallel(cl)
###############################################################################################################
################################################## OHIO #######################################################
###############################################################################################################
# function to read Ohio data
readOhio <- function(x) {
  require(data.table)
  fread(paste("Data/OH", x, sep = "/"))
}
# parallelized Ohio import
oh_voter <- rbindlist(parLapply(cl, dir("Data/OH"), readOhio))
# supplementary columns - defined for uniformity
oh_voter$state <- "OH"
oh_voter$gender <- NA
oh_voter$race <- NA

# remove non-primary/general elections
oh_identifying <- c("SOS_VOTERID", "DATE_OF_BIRTH", "PARTY_AFFILIATION", "CONGRESSIONAL_DISTRICT",
                    "gender", "race", "state", "PRECINCT_CODE", "RESIDENTIAL_ZIP")
oh_election <- colnames(oh_voter)[grepl("GENERAL", colnames(oh_voter)) | grepl("PRIMARY", colnames(oh_voter))]
# select desired columns for final wide table version
oh_voter_final_wide <- oh_voter[,.SD,.SDcols = c(oh_identifying,oh_election)]
# melt table from wide to long
oh_voter_final <- melt(oh_voter_final_wide, id.vars = oh_identifying, measure.vars = oh_election,
                       variable.name = "election_desc")

# remove data
rm(list = c("oh_voter_final_wide", "oh_voter"))

# remove cases where individual did not vote
#oh_voter_final <- oh_voter_final[value != ""]

# define supplementary columns
oh_voter_final[,birth_year := year(ymd(DATE_OF_BIRTH))]

# remove individuals 20 or older in first election (2000)
oh_voter_final <- oh_voter_final[birth_year >= 1980]
# rename state id
oh_voter_final[,SOS_VOTERID:=paste("OH", SOS_VOTERID, sep = "_")]
oh_voter_final[,election_lbl := tstrsplit(election_desc, "-")[2]]
oh_voter_final[,election_year := year(mdy(election_lbl))]

# keep only presidential and congressional elections
oh_voter_final <- oh_voter_final[election_year %% 2 == 0]
# 18-20 year olds voting in first election
oh_voter_final <- oh_voter_final[election_year - birth_year >= 18]
oh_voter_final[,voting_method := NA]
oh_voter_final[, voted := as.numeric(value != "")]

ohio_nevervote <- oh_voter_final[, all(voted == 0), by = SOS_VOTERID][V1 == TRUE, SOS_VOTERID]
ohio_nevervote_df <- oh_voter_final[SOS_VOTERID %in% ohio_nevervote]
nullcols <- c("election_desc", "election_lbl", "election_year")
ohio_nevervote_df[, (nullcols) := NA]
ohio_nevervote_df <- unique(ohio_nevervote_df)

oh_voter_final <- rbind(oh_voter_final[voted == 1], ohio_nevervote_df)

oh_voter_final[,election_type := tstrsplit(election_desc, "-")[1]]
oh_voter_final[election_type == "GENERAL",first_election := min(election_year,na.rm = TRUE), by = SOS_VOTERID]
oh_voter_final[,voted_primary := any(election_type == "PRIMARY"), by = c("election_year", "SOS_VOTERID")]
oh_voter_final[,presidential_election := election_year%%4 == 0]
oh_voter_final[,age_at_first_election := first_election - birth_year]
oh_voter_final[,precinct_code := paste(PRECINCT_CODE, "OH", sep = "_")]
oh_voter_final[,congressional_district := paste(CONGRESSIONAL_DISTRICT, "OH", sep = "_")]
oh_voter_final[,nth_election:= 1 + (election_year - first_election)/2]
oh_voter_final[,party := ifelse(PARTY_AFFILIATION == "R", "REP",
                                ifelse(PARTY_AFFILIATION == "D", "DEM",
                                       ifelse(PARTY_AFFILIATION == "X", "UNA",
                                              "OTHER")))]
oh_voter_final[is.na(nth_election), party := NA]
oh_voter_final <- oh_voter_final[election_type %in% c("GENERAL", NA)]
# remove columns
oh_voter_final <- oh_voter_final[, -c("election_desc", "election_type", "PRECINCT_CODE",
                                      "CONGRESSIONAL_DISTRICT", "election_lbl","DATE_OF_BIRTH",
                                      "value", "PARTY_AFFILIATION")]
# standardized renaming
setnames(oh_voter_final,
         c("SOS_VOTERID", "birth_year", "party", "congressional_district", "gender", "race", "state",
           "precinct_code", "RESIDENTIAL_ZIP","election_year", "voting_method", "first_election", "voted_primary",
           "presidential_election","age_at_first_election", "nth_election"),
         c("state_id", "birth_yr", "party", "congressional_district", "gender", "race", "state",
           "precinct", "zip", "election_yr", "voting_method", "first_election_yr", "voted_primary",
           "presidential_election","age_at_first_election", "nth_election"))


###############################################################################################################
############################################# CONNECTICUT #####################################################
###############################################################################################################
# import Connecticut Data
readCT <- function(x) {
  require(data.table)
  fread(paste("Data/CT/SSP/ELCT/VOTER", x, sep = "/"))
}
# parallelized import
ct_voter <- rbindlist(parLapply(cl, dir("Data/CT/SSP/ELCT/VOTER"), readCT))
# remove last 4 null columns
ct_voter[,c("V101", "V102", "V103", "V104"):=NULL]
# import table with column names
ct_cols <- fread("Data/CT/CT_cols.txt", sep = "", col.names = c("cols"), header = FALSE)
# filter descriptions
ct_cols <- ct_cols[!grepl("COMM", cols)]
ct_cols <- unlist(lapply(strsplit(ct_cols$cols, "\\."), function (x) str_trim(x[2])))
# generate names for election columns and rename
elect_info <- unlist(lapply(1:19, function (x) paste(c("ELECTION DATE", "ELECTION TYPE", "ABSENTEE BALLOT"), x)))
colnames(ct_voter) <- c(ct_cols, elect_info)

#supplementary columns - added for uniformity
ct_voter$gender <- NA
ct_voter$race <- NA
ct_voter$state <- "CT"

#filter for voters who were 20 or younger in earliest election available
ct_voter[, birth_year := year(mdy(`DATE OF BIRTH`))]
earliest_election <- min(year(mdy(unique(ct_voter$`ELECTION DATE 1`))), na.rm = TRUE)
earliest_election <- ifelse(earliest_election%%2 == 0, earliest_election, earliest_election+1)
ct_voter <- ct_voter[birth_year >= earliest_election-20]

# combine each triplet of election cols into one
combined_cols <- foreach(i = 1:19, .combine = "cbind") %dopar% {
  selcols <- paste(c("ELECTION DATE", "ELECTION TYPE", "ABSENTEE BALLOT"), i)
  ct_voter[, do.call(paste, c(.SD, sep = "_")),
           .SDcols = selcols]
}
combined_colnames <-  paste("ELECTION",1:19,sep="_")
colnames(combined_cols) <- combined_colnames
# add triplet to dataset
ct_voter <- cbind(ct_voter, combined_cols)

# filter for desired columns to make final wide table
ct_identifying <- c("VOTER ID", "birth_year", "PARTY CODE SEE PARTY CODES SPREADSHEET",
                    "STATE/FED VOTING DISTRICT", "gender", "race", "state", "STATE/FED VOTING PRECINCT",
                    "ZIP5")
ct_voter_final_wide <- ct_voter[,.SD, .SDcols = c(ct_identifying, combined_colnames)]

# melt from wide to long
ct_voter_final <- melt(ct_voter_final_wide, id.vars = ct_identifying, measure.vars = combined_colnames)
# remove data
rm(list = c("ct_voter_final_wide", "ct_voter"))

# remove rows with no election records
voter_ids <- ct_voter_final[, all(value == "__"), by = `VOTER ID`][V1 == TRUE, `VOTER ID`]
nevervoted <- ct_voter_final[`VOTER ID` %in% voter_ids]
nevervoted[,variable := NA]
nevervoted <- unique(nevervoted)
nevervoted$voted <- 0
ct_voter_final <- rbind(ct_voter_final[value != "__"], nevervoted, fill = TRUE)
ct_voter_final$voted <- 1

# split election information into desired columns
ct_voter_final[, c("election_desc", "election_type", "voting_method") := tstrsplit(value, "_")]
# supplementary colulmns
ct_voter_final[,election_year := year(mdy(election_desc))]
ct_voter_final <- ct_voter_final[election_year%%2 == 0 | is.na(election_year)]
ct_voter_final <- ct_voter_final[election_type %in% c("E", "P", "")]
ct_voter_final[,election_type:=ifelse(election_type == "E", "GENERAL", 
                                      ifelse(election_type == "", "", "PRIMARY"))]
ct_voter_final[election_type == "GENERAL",first_election := min(election_year), by = "VOTER ID"]
ct_voter_final[,voted_primary := any(election_type == "PRIMARY"), by = c("election_year", "VOTER ID")]
ct_voter_final <- ct_voter_final[election_type %in% c("GENERAL", "")]
ct_voter_final[,presidential_election := election_year%%4 == 0]
ct_voter_final[,age_at_first_election := first_election - birth_year]
ct_voter_final[,precinct_code := paste(`STATE/FED VOTING PRECINCT`, "CT", sep = "_")]
ct_voter_final[,congressional_district := paste(`STATE/FED VOTING DISTRICT`, "CT", sep = "_")]
ct_voter_final[,nth_election:= 1 + (election_year - first_election)/2]
ct_voter_final[,party := ifelse(`PARTY CODE SEE PARTY CODES SPREADSHEET` == "R", "REP",
                                ifelse(`PARTY CODE SEE PARTY CODES SPREADSHEET` == "D", "DEM",
                                       ifelse(`PARTY CODE SEE PARTY CODES SPREADSHEET` == "U", "UNA",
                                              "OTHER")))]
ct_voter_final[, voting_method := ifelse(voting_method == "N", "IN-PERSON", "ABSENTEE")]
# 18-20 year olds voting in first election
ct_voter_final <- ct_voter_final[age_at_first_election >= 18 | is.na(age_at_first_election)]

# remove unneeded columns
ct_voter_final <- ct_voter_final[, -c("variable", "value", "election_desc", "STATE/FED VOTING PRECINCT", 
                                      "PARTY CODE SEE PARTY CODES SPREADSHEET", "STATE/FED VOTING DISTRICT",
                                      "election_type")]
# standardized renaming
setnames(ct_voter_final,
         c("VOTER ID", "birth_year", "party", "congressional_district", "gender", "race", "state",
           "precinct_code", "ZIP5","election_year", "voting_method", "first_election", "voted_primary",
           "presidential_election","age_at_first_election", "nth_election"),
         c("state_id", "birth_yr", "party", "congressional_district", "gender", "race", "state",
           "precinct", "zip", "election_yr", "voting_method", "first_election_yr", "voted_primary",
           "presidential_election","age_at_first_election", "nth_election"))


###############################################################################################################
############################################### OKLAHOMA ######################################################
###############################################################################################################
# function to read Oklahoma data
readOK <- function(x) {
  require(data.table)
  ok <- fread(paste("Data/OK/voter_registration/CD0", x, "_vr.csv", sep = ""))
  ok$congressional_district <- x
  ok
}

# paralellized import 
ok_voter <- rbindlist(parLapply(cl, 1:5, readOK))

# supplementary columns
ok_voter$gender <- NA
ok_voter$race <- NA
ok_voter$state <- "OK"

# combine each triplet of election cols into one
combined_cols <- foreach(i = 1:10, .combine = "cbind") %dopar% {
  selcols <- paste(c("VoterHist", "HistMethod"), i, sep = "")
  ok_voter[, do.call(paste, c(.SD, sep = "_")),
           .SDcols = selcols]
}
combined_colnames <-  paste("ELECTION",1:10,sep="_")
colnames(combined_cols) <- combined_colnames
# add triplet to dataset
ok_voter <- cbind(ok_voter, combined_cols)

# filter columns
ok_identifying <-  c("VoterID", "DateOfBirth", "gender", "race", "state", 
                     "Zip", "PolitalAff", "Precinct", "congressional_district")
ok_voter_final_wide <- ok_voter[,.SD,.SDcols = c(ok_identifying, combined_colnames)]

# melt from wide to long
ok_voter_final <- melt(ok_voter_final_wide, id.vars = ok_identifying, measure.vars = combined_colnames)
# remove data
rm(list = c("ok_voter_final_wide", "ok_voter"))

# split election information into desired columns
ok_voter_final[, c("election_desc", "voting_method") := tstrsplit(value, "_")]

nevervoted <- ok_voter_final[,all(is.na(voting_method) | voting_method == "**"),
                             by = VoterID][V1 == TRUE, VoterID]
nevervoted_df <- ok_voter_final[VoterID %in% nevervoted]
remcols <- c("variable", "value", "election_desc")
nevervoted_df[, (remcols) := NA]
nevervoted_df <- unique(nevervoted_df)
nevervoted_df[,voted := 0]

# remove null
ok_voter_final <- rbind(ok_voter_final[voting_method %ni% c("**", NA)],
                        nevervoted_df, fill = TRUE)
ok_voter_final[is.na(voted), voted := 1]
# supplementary colulmns
# keep individuals 20 years or younger at first election
ok_voter_final[, birth_year := year(mdy(DateOfBirth))]

ok_voter_final[,election_year := year(mdy(election_desc))]
ok_voter_final <- ok_voter_final[election_year%%2 == 0 | is.na(election_year)]

min_electionyear <- min(ok_voter_final$election_year, na.rm = TRUE)
ok_voter_final <- ok_voter_final[birth_year >= min_electionyear - 20]
# define primaries as march or june (depending on election year type)
# define general election months as november, remove all other months
ok_voter_final[,election_month := month(mdy(election_desc))]

ok_voter_final <- ok_voter_final[(election_year %% 4 == 0 & election_month %in% c(3, 6, 11)) |
                                   (election_year %% 4 == 2 & election_month %in% c(6, 11)) |
                                   is.na(election_year)]
ok_voter_final[, election_type := ifelse(election_month == 11, "GENERAL", 
                                         ifelse(is.na(election_month), NA, "PRIMARY"))]
ok_voter_final[election_type == "GENERAL",first_election := min(election_year), by = "VoterID"]
ok_voter_final[,voted_primary := any(election_type == "PRIMARY"), by = c("election_year", "VoterID")]
ok_voter_final <- ok_voter_final[election_type %in% c("GENERAL", NA)]
ok_voter_final[,presidential_election := election_year%%4 == 0]
ok_voter_final[,age_at_first_election := first_election - birth_year]
ok_voter_final[,precinct_code := paste(`Precinct`, "OK", sep = "_")]
ok_voter_final[,nth_election:= 1 + (election_year - first_election)/2]
ok_voter_final[,party := ifelse(PolitalAff %in% c("DEM", "REP"), PolitalAff,
                                ifelse(PolitalAff == "IND", "UNA", "OTHER"))]
ok_voter_final[, voting_method := ifelse(voting_method == "IP", "IN-PERSON", 
                                         ifelse(is.na(voting_method), NA, "ABSENTEE"))]
# 18-20 year olds voting in first election
ok_voter_final <- ok_voter_final[age_at_first_election >= 18 | is.na(age_at_first_election)]
ok_voter_final[,VoterID := paste("OK", VoterID, sep = "_")]
ok_voter_final[,congressional_district := paste(congressional_district,"OK", sep = "_")]

# remove unneeded columns
ok_voter_final <- ok_voter_final[, -c("variable", "value", "election_desc", "Precinct", 
                                      "PolitalAff", "DateOfBirth","election_month","election_type")]

# standardized renaming
setnames(ok_voter_final,
         c("VoterID", "birth_year", "party", "congressional_district", "gender", "race", "state",
           "precinct_code", "Zip","election_year", "voting_method", "first_election", "voted_primary",
           "presidential_election","age_at_first_election", "nth_election"),
         c("state_id", "birth_yr", "party", "congressional_district", "gender", "race", "state",
           "precinct", "zip", "election_yr", "voting_method", "first_election_yr", "voted_primary",
           "presidential_election","age_at_first_election", "nth_election"))

complete_voter_dt <- rbind(oh_voter_final, ct_voter_final, ok_voter_final)
#remove data
rm(list = c("oh_voter_final", "ct_voter_final", "ok_voter_final"))

# turn characters into numeric values
complete_voter_dt[, state_id := .GRP, by = state_id]

complete_voter_dt <- complete_voter_dt[,-c("gender", "race")]
complete_voter_dt[, voting_method := ifelse(voting_method == "ABSENTEE", 0, 
                                            ifelse(is.na(voting_method), NA, 1))]
setnames(complete_voter_dt, "voting_method", "voted_inperson")

# factorize data
factorized <- c("party", "congressional_district", "state", "precinct")
complete_voter_dt[, (factorized) := lapply(.SD, function (x) unclass(as.factor(x))), 
                  .SDcols = factorized]
boolean_cols <- c("voted_primary", "presidential_election")
complete_voter_dt[, (boolean_cols) := lapply(.SD, as.numeric), 
                  .SDcols = boolean_cols]
# create level codes for unclassed data
level_codes <- foreach(i = factorized, .combine = "rbind") %dopar% {
  level_codes <- levels(complete_voter_dt[,get(i)])
  res <- data.table(level_codes)
  res$column <- i
  res$level_vals <- 1:length(level_codes)
  res
}

complete_voter_dt <- complete_voter_dt[order(state_id, election_yr)]
# export data
fwrite(complete_voter_dt,"data.csv")
fwrite(level_codes,"levels.csv")
