
#  MOH Mortality Preparation 2019

#  Last Modified: August 2019

# ------ File Information -------
# 
#   File 1 (All records)
#   Date Recieved: 17/11/15 
#   File name: mos3363 
#   Number of records: 754,312
# 
#   File 2 (Updater add-on)
#   Date Recieved: 28/01/16
#   File name: mos3377 
#   Number of records: 82,916
# 
#   File 3 (Updater add-on)
#   Date Recieved: 26/08/16
#   Last DOD = 2016-01-28
#   File name: mos3445 
#   Number of records: 157,817
# 
#   File 4 (Updater add-on)
#   Date Recieved: 18/08/17 
#   Last DOD = 2017-01-28
#   File name: mos3569
#   Number of records: 156,393
# 
#   File 5 (Updater add-on)
#   Date Recieved 20/07/2018
#   Last DOD = 2017-06-29
#   File name: mos3692
#   Number of records: 172,337
# 
#   File 6 (Updater add-on)
#   Date Recieved 06/03/2019
#   Last DOD = 2017-12-30
#   File name: mos3764
#   Number of records: 159,388
# 
#   File 7 (Updater add-on)
#   Date Recieved 08/08/2019
#   Last DOD = 2017-12-30
#   File name: mos3827
#   Number of records: 190,115
# 
# Important: Where there are overlapping records, always use the latest extract!
# 
# ------ Begin Script -------

library(haven)
library(data.table)
library(dplyr)
library(fst)

# ---- A.   Synthesize mortality dataset into a single working object

# i.    Import raw SAS files
#       NB: There are 6 updates so far - MOS3363, MOS3377, MOS3445, MOS3569, MOS3692, mos3764, mos3827
#       These are top-ups that need to be binded
MOS3363 <- read_sas("source_data/SAS/MOS/mos3363.sas7bdat")
MOS3377 <- read_sas("source_data/SAS/MOS/mos3377.sas7bdat")
MOS3445 <- read_sas("source_data/SAS/MOS/mos3445.sas7bdat")
MOS3569 <- read_sas("source_data/SAS/MOS/mos3569.sas7bdat")
MOS3692 <- read_sas("source_data/SAS/MOS/mos3692.sas7bdat")
MOS3764 <- read_sas("source_data/SAS/MOS/mos3764.sas7bdat")
MOS3827 <- read_sas("source_data/SAS/MOS/mos3827.sas7bdat")

# VERY IMPORTANT - sequence order matters - put latest set first (for prioritisation)!!
death.sets <- c("MOS3827", "MOS3764", "MOS3692", "MOS3569", "MOS3445", "MOS3377", "MOS3363")

# ii.  Clean variable classes
for(set in death.sets){
   
   DATA <- setDT(get(set))
   
   # Remove HAVEN attributes
   DATA[, (names(DATA)) := lapply(.SD, function(x){
      attr(x, "format.sas") <- NULL
      attr(x, "label") <- NULL
      return(x)
   })]
   
   # Upper-case
   names(DATA) <- toupper(names(DATA))
   
   # numerics
   num.vars <- c("REG_YEAR", "COUNTRY_CODE", "AGE_AT_DEATH_YRS", "POST_MORTEM_CODE","DEATH_CERTIFIER_CODE","DEATH_INFO_SRC_CODE","DEATH_FACILITY_CODE", "CC_SYS")
   
   DATA[, (num.vars) := lapply(.SD,function(x) 
      as.numeric(x)), .SDcols=num.vars]
   
   # Drop eth vars
   DATA[, c("ETHNICG1", "ETHNICG2", "ETHNICG3") := NULL]
   
   # Set Priorty Marker (based on new-ness): Latest update = highest priority (ie. 1)
   # This step is important because an older death record may have been superceeded (eg. coroner's deaths.)
   DATA$PRIORITY <- which(death.sets %in% set)

   assign(set, DATA)
   
   print(paste(set, " completed")); rm(DATA, num.vars)
   
}

rm(set)

# iii.   Unify Variables

# i.  Examine differences / determine renaming, keeping, dropping
# nb: Limiting to fewer ICD vars only affect 1-4 people.
names_list <- lapply(death.sets, function(x){
   names(get(x))
})

common <- Reduce(intersect, lapply(death.sets, function(x){
   names(get(x))
}))
  
diff_list <- lapply(death.sets, function(x){
   setdiff(names(get(x)), common)
})

# ii. Convention
# - Use only SECONDARY + rename all to ENIGMA_SECONDARY
# - ICDF to 8 (all tables have this); ICDG to 6 (all tables have this); ICDN to 40 (all tables have this); ICDC to 4 (all tables have this); 
# - The earliest tables (i.e. MOS3363 and MOS3377) do not contain: DOCUMENT_STATUS_CODE, ICDF9, ICDF10
# - The table MOS3377 does not contain: ICDG7, ICDG8, ICDC5, ICDC6

for(set in death.sets){
   
   DATA <- get(set)
   
   # Rename ENHI
   if(set %in% c("MOS3827", "MOS3764", "MOS3692", "MOS3569", "MOS3445")){
      DATA[, "ENIGMA_MASTER" := NULL]
   } else if(set=="MOS3377"){
      setnames(DATA, "ENIGMA_MASTER", "ENIGMA_SECONDARY")
   } else {
      setnames(DATA, "VIEW_MOH_ENHI", "ENIGMA_SECONDARY")
   }
   
   # Add ICD Vars
   # - all tables have DOCUMENT_STATUS_CODE, ICDF9, ICDF10 except for MOS3363 & MOS3377
   if(set=="MOS3363" | set=="MOS3377"){
      DATA[,c("DOCUMENT_STATUS_CODE","ICDF9","ICDF10") := .("","","")]
   }
   # - all tables have ICDG7, ICDG8, ICDC5, ICDC6 except for MOS3377 & MOS3764
   if(set=="MOS3377" | set=="MOS3764"){
      DATA[,c("ICDG7", "ICDG8", "ICDC5", "ICDC6") := .("","","","")]
   }
   
   # Drop
   DATA[, c("YRS_IN_NZ","ICDD_BLANK_FLAG") := NULL]
   
   assign(paste0("MOD_", set), DATA)
   
   print(paste(set, " completed")); rm(DATA)
   
}

# iii.  Rename key variables variables
names_list <- lapply(death.sets, function(x){
   names(get(x))
})

common <- Reduce(intersect, lapply(death.sets, function(x){
   names(get(x))
}))
  
# iv. Combine
death.sets <- c("MOD_MOS3827", "MOD_MOS3764", "MOD_MOS3692", "MOD_MOS3569", "MOD_MOS3445", "MOD_MOS3377", "MOD_MOS3363")

ALL_DATA <- do.call("rbind", lapply(death.sets,
                                    function(x){
                                       get(x)[,common,with=F]
                                    }))


# ---- B.   Find the most recent record
# NB: There are 869300 unique SECONDARIES

# Create record priority for each person
ALL_DATA <- ALL_DATA[order(PRIORITY), 
                     RECORD_PRIORITY := seq_len(.N), 
                     by=ENIGMA_SECONDARY]

# i.  Find & Remove Duplicates in each set
# Single Death Records (648185 people)
SINGLE_REC <- ALL_DATA[ALL_DATA[,.I[max(RECORD_PRIORITY)==1], by=ENIGMA_SECONDARY]$V1]

# Multiple Death Records (221119 people)
MULTI_RECS <- ALL_DATA[ALL_DATA[,.I[max(RECORD_PRIORITY)>1], by=ENIGMA_SECONDARY]$V1]

  # Select the newest death record (priority 1 / lowest value) 
  # NB: min(PRIORITY) to resolve ties 
  MULTI_RECS_SNG <- MULTI_RECS[MULTI_RECS[,.I[PRIORITY==min(PRIORITY) & RECORD_PRIORITY==1]
                                          , by=ENIGMA_SECONDARY]$V1]
  
# Recombine
# Still 869300 unique individuals
ALL_MORTALITY <- rbind(SINGLE_REC, MULTI_RECS_SNG)

# Reorder Variables
core <- names(ALL_MORTALITY)[1:23]
icdf <- names(ALL_MORTALITY)[startsWith(names(ALL_MORTALITY), "ICDF")]
icdg <- names(ALL_MORTALITY)[startsWith(names(ALL_MORTALITY), "ICDG")] 
icdc <- names(ALL_MORTALITY)[startsWith(names(ALL_MORTALITY), "ICDC")] 
icdn <- names(ALL_MORTALITY)[startsWith(names(ALL_MORTALITY), "ICDN")] 
icds <- names(ALL_MORTALITY)[startsWith(names(ALL_MORTALITY), "ICDS")]

ALL_MORTALITY <- ALL_MORTALITY[,c(core, icdf, icdg, icdc, icdn, icds, "PRIORITY"), with=F]

# Fill charvars with NA
char.vars <- names(ALL_MORTALITY)[sapply(ALL_MORTALITY, class)=="character"]

ALL_MORTALITY[, (char.vars) := lapply(.SD, function(x)
   replace(x, x=="", NA)), 
   .SDcols = char.vars]

# ENHI Swapover
VIEW_ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/VIEW_VSIMP_MOH__PS_AUG2019.fst")

ALL_MORTALITY <- merge(setDT(VIEW_ENHI)[,.(VSIMPLE_INDEX_2NDARY, MOH_ENHI_2NDARY)], ALL_MORTALITY,
                       by.x = "MOH_ENHI_2NDARY", by.y = "ENIGMA_SECONDARY",
                       all.y=F)

ALL_MORTALITY[, "MOH_ENHI_2NDARY" := NULL]

# Variable Selection
# Remove extreme missing fields - i.e. variables containing <100 values
missing.vars <- names(ALL_MORTALITY)[sapply(ALL_MORTALITY, function(x) 
   sum(!is.na(x)))<100]

ALL_MORTALITY <- ALL_MORTALITY[, -missing.vars, with=F]

# Keep traceback variable (traces record to table of origin)
setnames(ALL_MORTALITY, "PRIORITY", "DATASET_TRACEBACK")

# Export
write.fst(ALL_MORTALITY, "source_data/R/MORTALITY/Working/VSIMPLE_MORTALITY_DEC2017_v2.fst", 75)

# NB! Dataset needs to be syncronyised with latest NHI to be complete!
# -- VERY IMPORTANT -- PROCEDURE NOT FINISHED - TBC BY SYNCING WITH NHI!!
