
# Laboratory Claims Data Preparation

# This script imports source LAB (LAB Claims) without ENHI conversion or cleaning.
# Data format is a Stata file - which is used as the transport data format


library(foreign)
library(dplyr)
library(data.table)

# Functions
ClearAttr <- function(x, type){
    attr(x, type) <- NULL
    return(x)
  }
  
ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/VIEW_P__VIEW_MOH_S_JUL2018.fst")
setDT(ENHI)

# Import all available STATA files
for(year in 2014:2015){
  
  DATA <- read.dta(paste0("U:/VIEW_Data/source_data/STATA/LAB/LAB_", year, ".dta"))
  name <- paste0("LAB_", year)
  assign(name, DATA)
  rm(DATA)
  
  print(paste0(year, " imported"))
  
}

# 2003 - 2012 uses shape 1; replace enhi only
for(year in 2008:2012){
  
  DATA <- as.data.table(get(paste0("LAB_", year)))
  setkey(DATA, "view_moh_enhi")
  name <- paste0("LAB_", year)
  DATA <- ENHI[DATA, nomatch=0]
  DATA[,"NHB_ENHI_2NDARY":=NULL]
  assign(name, DATA); rm(DATA)
  print(paste0(year, " completed"))
  gc()
}
saveRDS(LAB_2008, "D:/VIEW/SOURCE/LAB/ALL_LAB_2008.rds")
saveRDS(LAB_2009, "D:/VIEW/SOURCE/LAB/ALL_LAB_2009.rds")
saveRDS(LAB_2010, "D:/VIEW/SOURCE/LAB/ALL_LAB_2010.rds")
saveRDS(LAB_2011, "D:/VIEW/SOURCE/LAB/ALL_LAB_2011.rds")
saveRDS(LAB_2012, "D:/VIEW/SOURCE/LAB/ALL_LAB_2012.rds")

rm(LAB_2008, LAB_2009, LAB_2010, LAB_2011, LAB_2012); gc()

# 2014 - 2015 uses shape 2: remove primary, replace enhi
for(year in 2014:2015){
  
  DATA <- as.data.table(get(paste0("LAB_", year)))
  DATA[,"enigma_master" :=NULL]
  setkey(DATA, "enigma_secondary")
  name <- paste0("LAB_", year)
  DATA <- ENHI[DATA, nomatch=0]
  DATA[,"NHB_ENHI_2NDARY":=NULL]
  assign(name, DATA); rm(DATA)
  print(paste0(year, " completed"))
}

saveRDS(LAB_2014, "D:/VIEW/SOURCE/LAB/ALL_LAB_2014.rds")
saveRDS(LAB_2015, "D:/VIEW/SOURCE/LAB/ALL_LAB_2015.rds")

rm(LAB_2014, LAB_2015); gc()


# 2013 is special: has two two shapes (Jan-Apr & May-Dec)
LAB_2013_A <- read.dta("U:/VIEW_Data/source_data/STATA/LAB/LAB_2013_A.dta")
LAB_2013_B <- read.dta("U:/VIEW_Data/source_data/STATA/LAB/LAB_2013_B.dta")

LAB_2013_A <- as.data.table(LAB_2013_A)
setkey(LAB_2013_A, "view_moh_enhi")
LAB_2013_A <- ENHI[LAB_2013_A, nomatch=0]
LAB_2013_A[,"NHB_ENHI_2NDARY":=NULL]

LAB_2013_B <- as.data.table(LAB_2013_B)
LAB_2013_B[,"enigma_master" :=NULL]
setkey(LAB_2013_B, "enigma_secondary")
LAB_2013_B <- ENHI[LAB_2013_B, nomatch=0]
LAB_2013_B[,"NHB_ENHI_2NDARY":=NULL]

LAB_2013 <- rbind(LAB_2013_A, LAB_2013_B)
saveRDS(LAB_2013, "D:/VIEW/SOURCE/LAB/ALL_LAB_2013.rds")

# 2016
LAB_2016 <- read.dta("U:/VIEW_Data/source_data/STATA/LAB/LAB_2016.dta")
setDT(LAB_2016)
setkey(LAB_2016, "enigma_secondary")

LAB_2016 <- ENHI[LAB_2016, nomatch=0]
LAB_2016[,c("MOH_ENHI_2NDARY", "enigma_master"):=NULL]

saveRDS(LAB_2016, "D:/VIEW/SOURCE/LAB/ALL_LAB_2016.rds")

# 2017
# Import all available SAS files
months <- c("01","02","03","04","05","06","07","08","09","10","11","12")

for(i in months){
  
  DATA <- read_sas(paste0("source_data/SAS/LAB/lab0324_2017_", i, ".sas7bdat"))
  
  setDT(DATA)
  setnames(DATA, names(DATA), toupper(names(DATA)))
  
  # Clean variable classes
  remove.attr <- names(DATA)[2:length(DATA)]
  
  DATA[, (remove.attr) := lapply(.SD,
                                function(x)
                                  ClearAttr(x, "label")), .SDcols=remove.attr]
  
  if(i=="01"){
    LAB <- DATA
  } else {
    LAB <- rbind(LAB, DATA)
  }
  
  print(paste0(i, " completed"))
}

BACKUP <- copy(LAB)

setkey(LAB, "ENIGMA_MASTER")

DATA <- ENHI[LAB, nomatch=0]
DATA[, c("MOH_ENHI_2NDARY","ENIGMA_SECONDARY","VIEW_ENHI_2NDARY"):=NULL]

saveRDS(DATA, "source_data/R/LAB/ALL_LAB_2017.rds")



