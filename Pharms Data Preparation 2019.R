
# Pharms data preparation procedure 2019

# Last modified June, 201

# Imported from SAS
# Uses VSIMP ENHI for SIMPLE Dataset
# Designed to load fast and reduce overhead in VM or local machines


library(haven)
library(data.table)
library(fst)

ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/VIEW_SIMPLE_PS_JUN2019.fst", as.data.table = TRUE)

setkey(ENHI, VSIMPLE_INDEX_2NDARY)

files.list <- list.files("V:/source_data/SAS/PHH/SIMPLE/", pattern=".sas7bdat$")

Q1 <- c("01","02","03")
Q2 <- c("04","05","06")
Q3 <- c("07","08","09")
Q4 <- c("10","11","12")

for(year in 2005:2018){
   
   files.yr <- grep(year, files.list, value = T)
   
   for(qtr in c("Q1", "Q2", "Q3", "Q4")){
      
      substr.qtr  <- paste(year, get(qtr), sep="_")
      files.qtr   <- grep(paste0(substr.qtr, collapse = "|"), files.yr, value = T)
      
      DATA <- rbindlist(lapply(files.qtr, 
                               function(x)
                                  read_sas(paste0("V:/source_data/SAS/PHH/SIMPLE/", x))))
      
      setkey(DATA, VSIMPLE_INDEX)
      
      DATA <- ENHI[DATA, nomatch = 0][, c("VIEW_ENHI_MASTER", "VSIMPLE_INDEX_2NDARY") := NULL]
      
      DATA[, (names(DATA)) := lapply(.SD, function(x){
         attr(x, "format.sas") <- NULL
         attr(x, "label") <- NULL
         return(x)
      })]
      
      DATA$DHBDOM <- NULL
      
      DATA[MESH_BLOCK_2006=="unknown", MESH_BLOCK_2006:="0"]
      DATA[FUNDING_DHB_CODE=="UNK", FUNDING_DHB_CODE:="0"]
      
      num.vars <- c("FUNDING_DHB_CODE", "MESH_BLOCK_2006")
      
      DATA[,(num.vars) := lapply(.SD, function(x){
         return(as.numeric(x))
      }), .SDcols = num.vars]
      
      write.fst(DATA, paste0("V:/source_data/R/PHARMS/SIMPLE/VSIMP_PHH_", year, "_", qtr, ".fst"), compress = 75)
      
      print(paste0(qtr, " completed"))
      
   }
   
   if(year==2010 | year==2013 | year==2016){
      gc()
   }
   
   print(paste0(year, " completed"))
}

