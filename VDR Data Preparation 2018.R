
# Complete Ministry of Health - VDR Data Preparation Procedure

# Created by Billy Wu
# Last Updated November, 2018

# This procedure converts all VDR datasets 2005-2016 to RDS files. 
# It contains ENHI swapout, cleaning, and class change

library(haven)
library(data.table)
library(dplyr)
library(fst)

VIEW_ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/VIEW_P__VIEW_MOH_S_JUL2018.fst")

setkey(setDT(VIEW_ENHI), MOH_ENHI_2NDARY)

# ---- A.   Events ----
# i.    Read in Events 
#       Swap ENHI, tidy, clean

# Fx: Remove attributes
ClearAttr <- function(x, type){
  attr(x, type) <- NULL
  return(x)
}

# 2005 to 2016
for(year in 2005:2016){
    
    VDR <- haven::read_sas(paste0("source_data/SAS/VDR/vdr_", year, ".sas7bdat"))
   
     setkey(setDT(VDR), enigma_master)
  
  # Remove Attr
  VDR[, (names(VDR)) := lapply(.SD,
                            function(x)
                              ClearAttr(x, "label")), .SDcols=names(VDR)]
  
   # Swap ENHI
   VIEW_VDR <- merge(VIEW_ENHI[,.(VIEW_ENHI_MASTER, MOH_ENHI_2NDARY)], VDR, 
                     by.x="MOH_ENHI_2NDARY", by.y="enigma_master", 
                     all.y=F)
 
   # Remove Uncessary Vars
   VIEW_VDR[, c("MOH_ENHI_2NDARY", "enigma_secondary") := NULL]
  
  # Export - for R / SAS using different ENHIs
  write.fst(VIEW_VDR, paste0("source_data/R/VDR/VDR_", year, ".fst"))
  
  print(paste0(year, " completed"))
  
}
