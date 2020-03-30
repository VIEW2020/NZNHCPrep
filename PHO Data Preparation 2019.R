
# PHO Data Preparation - 2019

# Last modified July, 2019

# Imported from SAS
# Uses VSIMP ENHI for SIMPLE Dataset
# Designed to load fast and reduce overhead in VM or local machines

library(haven)
library(data.table)
library(fst)

ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/VIEW_SIMPLE_PS_JUN2019.fst", as.data.table = T)

setkey(ENHI, VSIMPLE_INDEX_2NDARY)

files.list <- list.files("V:/source_data/SAS/PHO/SIMPLE/", pattern=".sas7bdat$")

for(file in files.list){
   
   DATA <- read_sas(paste0("V:/source_data/SAS/PHO/SIMPLE/", file))
   
   setDT(DATA); setkey(DATA, VSIMPLE_INDEX_SECONDARY)
   
   DATA <- ENHI[DATA, nomatch = 0][, c("VIEW_ENHI_MASTER", "VSIMPLE_INDEX_2NDARY") := NULL]
   
   DATA[, (names(DATA)) := lapply(.SD, function(x){
      attr(x, "format.sas") <- NULL
      attr(x, "label") <- NULL
      return(x)
   })]
   
   num.vars <- c("EN_DEPRIVATION_QUINTILE", "ADDRESS_UNCERTAINTY", "EN_MESHBLOCK")
   
   DATA[,(num.vars) := lapply(.SD, function(x){
      return(as.numeric(x))
   }), .SDcols = num.vars]
   
   file.yr <- substr(unlist(strsplit(file, "_"))[3],1,4)
   file.qt <- substr(unlist(strsplit(file, "_"))[3],6,6)
   
   DATA[, CALENDAR_YEAR_AND_QUARTER := NULL]
   DATA[, CALENDAR_YEAR_AND_QUARTER := as.numeric(paste(file.yr, file.qt, sep="."))]
   
   write.fst(DATA, paste0("V:/source_data/R/PHO/SIMPLE/VSIMP_PHO_", file.yr, "_Q", file.qt, ".fst"), compress = 75)
   
   print(paste0(file.yr, " completed"))
   
}
