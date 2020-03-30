
# NNPAC Data Preparation - 2019

# Last modified November, 2019

# Imported from SAS
# Uses VSIMP ENHI for SIMPLE Dataset
# Designed to load fast and reduce overhead in VM or local machines

library(haven)
library(data.table)
library(fst)

ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/VIEW_VSIMP_MOH__PS_AUG2019.fst", 
                 as.data.table = TRUE)

setkey(ENHI, MOH_ENHI_2NDARY)

files.list <- list.files("V:/source_data/SAS/NAP/", pattern=".sas7bdat$")

for(file in files.list){
  
  DATA <- read_sas(paste0("V:/source_data/SAS/NAP/", file))
  
  if(file %in% files.list[1:5]){
    
    setnames(DATA, "VIEW_MoH_eNHI", "enigma_secondary")  
    
  } else {
    
    DATA$engima_master <- NULL
    
  }
    
  setDT(DATA); setkey(DATA, enigma_secondary)
  
  DATA <- ENHI[DATA, nomatch = 0][, c("VIEW_ENHI_2NDARY", "MOH_ENHI_2NDARY", "VIEW_ENHI_MASTER", "MOH_ENHI_MASTER") := NULL]
  
  DATA <- DATA[, c("DOB", "GENDER", "ETHNICG1", "ETHNICG2", "ETHNICG3", "DOMICILE_CODE") := NULL]
  
  DATA[, (names(DATA)) := lapply(.SD, function(x){
    attr(x, "format.sas") <- NULL
    attr(x, "label") <- NULL
    return(x)
  })]
  
  DATA$LOCATION <- as.numeric(DATA$LOCATION)
  
  file.yr <- substr(unlist(strsplit(file, "_"))[3],1,4)
  
  write.fst(DATA, paste0("V:/source_data/R/NAP/VSIMP_NAP_", file.yr, ".fst"), compress = 75)
  write_sav(DATA, paste0("V:/source_data/SAS/NAP/VSIMPLE/VSIMP_NAP_", file.yr, ".sav"))
  
  print(paste0(file.yr, " completed"))
  
}
