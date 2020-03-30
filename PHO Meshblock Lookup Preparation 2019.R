
# EN Meshblock Lookup Table 2019

# Last updated July 2019

# Creates a lookup table for all ENHIs in entire PHO collection (n = 5832972)
# Provides a meshblock number by year and quarter
# It's important to remove duplicates - since we're dealing with MASTER ENHI
# If multiple records, then LAST_CONSULTATION_DATE is prioritised to capture the most recent MB

library(data.table)
library(fst)

files.list <- list.files("V:/source_data/R/PHO/SIMPLE/", pattern=".fst$")

for(file in files.list){
   
   file.yr <- substr(unlist(strsplit(file, "_"))[3],1,4)
   file.qt <- substr(unlist(strsplit(file, "_"))[4],1,2)
   file.lb <- paste("MB", file.yr, file.qt, sep="_")
   
   PHO <- read.fst(paste0("source_data/R/PHO/SIMPLE/", file))
   PHO <- setDT(PHO)[EN_MESHBLOCK!=0, .(VSIMPLE_INDEX_MASTER, EN_MESHBLOCK, LAST_CONSULTATION_DATE)]
   
   # There might be multiple records for same person. Prioritise by last consultation date!
   PHO <- PHO[order(LAST_CONSULTATION_DATE, decreasing = T), 
              index := seq_len(.N), 
              by = VSIMPLE_INDEX_MASTER][index==1, -c("index","LAST_CONSULTATION_DATE"), with=F]
   
   setnames(PHO, "EN_MESHBLOCK", file.lb)
   
   if(file==files.list[1]){
      LOOKUP <- PHO
   } else {
      LOOKUP <- merge(LOOKUP, PHO, 
                      by = "VSIMPLE_INDEX_MASTER", 
                      all=T, 
                      allow.cartesian = T)
   }
   
   print(paste0(file.lb, " completed")); rm(PHO)
}

# Carry foward Last observation
library(fastmatch)
library(future.apply); plan(multiprocess, workers = 8) 

LOCF <- as.data.table(t(future_apply(LOOKUP[,2:length(LOOKUP), with=F], 1, 
                            function(x){
                               zoo::na.locf(x, na.rm=F)
                               })))

ALL_MB <- cbind(LOOKUP[,.(VSIMPLE_INDEX_MASTER)], LOCF)

# Check
SAMPLE1 <- ALL_MB[sample(.N, 1000)]
SAMPLE2 <- LOOKUP[VSIMPLE_INDEX_MASTER %in% SAMPLE1$VSIMPLE_INDEX_MASTER]

write.fst(ALL_MB, "source_data/R/LOOKUP TABLES/MB NZDep/PHO_EN_MESHBLOCK_2004_2019.fst", compress = 75)
