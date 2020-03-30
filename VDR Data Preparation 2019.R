
# VDR Data Preparation - 2019

# Last modified July, 2019

# Imported from SAS
# Uses VSIMP ENHI for SIMPLE Dataset
# Designed to load fast and reduce overhead in VM or local machines

library(haven)
library(data.table)
library(fst)

ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/VIEW_SIMPLE_PS_JUN2019.fst", as.data.table = TRUE)

setkey(ENHI, VSIMPLE_INDEX_2NDARY)

files.list <- list.files("V:/source_data/SAS/VDR/SIMPLE/", pattern=".sas7bdat$")

for(file in files.list){
   
   DATA <- read_sas(paste0("V:/source_data/SAS/VDR/SIMPLE/", file))
   
   setDT(DATA); setkey(DATA, VSIMPLE_INDEX_SECONDARY)
   
   DATA <- ENHI[DATA, nomatch = 0][, c("VIEW_ENHI_MASTER", "VSIMPLE_INDEX_2NDARY") := NULL]
   
   DATA[, (names(DATA)) := lapply(.SD, function(x){
      attr(x, "format.sas") <- NULL
      attr(x, "label") <- NULL
      return(x)
   })]
  
   file.yr <- substr(unlist(strsplit(file, "_"))[3],1,4)
   
   write.fst(DATA, paste0("V:/source_data/R/VDR/SIMPLE/VSIMP_VDR_", file.yr, ".fst"), compress = 75)
   
   print(paste0(file.yr, " completed"))
   
}


# Direct from SAS
ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/VIEW_VSIMP_MOH__PS_AUG2019.fst", as.data.table = TRUE)

DATA <- read_sas("V:/source_data/SAS/VDR/vdr2018_with_enigma.sas7bdat"); setDT(DATA)

DATA[, (names(DATA)) := lapply(.SD, function(x){
   attr(x, "format.sas") <- NULL
   attr(x, "label") <- NULL
   return(x)
})]

setkey(ENHI, MOH_ENHI_2NDARY)
setkey(DATA, Enigma_Encrypted_NHI)

DATA <- ENHI[DATA, nomatch = 0]

DATA[, names(DATA)[1:5] := NULL]

DATA <- DATA[, intersect(names(VDR_2017), names(DATA)), with = F]

write.fst(DATA, "source_data/R/VDR/SIMPLE/VSIMP_VDR_2018.fst", 75)



