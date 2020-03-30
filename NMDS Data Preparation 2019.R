
# NMDS data preparation procedure 2019

# Last modified August, 2019

# Imported from SAS
# Uses VSIMP ENHI for SIMPLE Dataset
# Designed to load fast and reduce overhead in VM or local machines

library(haven)
library(data.table)
library(fst)

ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/VIEW_SIMPLE_PS_JUN2019.fst")
setDT(ENHI); setkey(ENHI, VSIMPLE_INDEX_2NDARY)

# ---- A.   Public Hospitalisation -----

files.list <- list.files("V:/source_data/SAS/PUS/SIMPLE/", pattern=".sas7bdat$")

for(file in files.list){
 
   files.yr <- substr(unlist(strsplit(file, "_"))[4],1,4)
   
   DATA <- read_sas(paste0("V:/source_data/SAS/PUS/SIMPLE/", file))
   
   setDT(DATA)
   names(DATA)[1] <- "VSIMPLE_INDEX_2NDARY"
   setDT(DATA); setkey(DATA, VSIMPLE_INDEX_2NDARY)
   
   DATA[, (names(DATA)) := lapply(.SD, function(x){
      attr(x, "format.sas") <- NULL
      attr(x, "label") <- NULL
      return(x)
   })]
   
   if(grepl("events", file)){
      
      num.vars <- c("FAC_TYPE", "AGENCY", "DRG_31", "CCL", "PCCL", "MDC_CODE")
      
      DATA[,(num.vars) := lapply(.SD, function(x){
         return(as.numeric(x))
      }), .SDcols = num.vars]
         
      DATA <- ENHI[DATA, nomatch = 0]
      DATA[, c("VIEW_ENHI_MASTER","VSIMPLE_INDEX_2NDARY") := NULL]
      
      write.fst(DATA, paste0("V:/source_data/R/HOSPITALISATION/SIMPLE/VSIMP_EVENTS_", files.yr, ".fst"), compress = 75)
      
   } else { #Diag files
      
      num.vars <- "CC_SYS"
      
      DATA[,(num.vars) := lapply(.SD, function(x){
         return(as.numeric(x))
      }), .SDcols = num.vars]
      
      if(as.numeric(files.yr)<=2005){
         # Forward Map ICD9 Codes
      } else {
         DATA[, CLIN_CD_10 := CLIN_CD]
      }
      
      write.fst(DATA, paste0("V:/source_data/R/HOSPITALISATION/SIMPLE/VSIMP_DIAGS_", files.yr, ".fst"), compress = 75)
      
   }
   
 print(paste0(files.yr, " completed"))
 
}

# ---- B.   Private Hospitalisation ----

library(fst)
library(data.table)

ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/VIEW_SIMPLE_MOH_PS_JUN2019.fst", as.data.table = T)
ENHI <- ENHI[,.(VSIMPLE_INDEX_MASTER, MOH_ENHI_2NDARY)]; setkey(ENHI, MOH_ENHI_2NDARY)

# Events
PRIV_EVENTS <- haven::read_sas("V:/source_data/SAS/PRS/prs0572_events.sas7bdat")

  setDT(PRIV_EVENTS)
  PRIV_EVENTS[, enigma_master := NULL]
  setkey(PRIV_EVENTS, enigma_secondary)
   
  PRIV_EVENTS <- ENHI[PRIV_EVENTS, nomatch = 0]
  PRIV_EVENTS[, "MOH_ENHI_2NDARY" := NULL]
  
  PRIV_EVENTS[, (names(PRIV_EVENTS)) := lapply(.SD, function(x){
     attr(x, "format.sas") <- NULL
     attr(x, "label") <- NULL
     return(x)
  })]
  
  PRIV_EVENTS <- PRIV_EVENTS[,.(VSIMPLE_INDEX_MASTER,ADM_TYPE,EVENT_TYPE,END_TYPE,EVSTDATE,EVENDATE,EVENT_ID,FAC_TYPE,PURCHASER,
                                HLTHSPEC,DRG_CURRENT,DRG_GROUPER_TYPE,DRG_31,CCL,PCCL,MDC_CODE,MDC_TYPE)]

  PRIV_EVENTS <- PRIV_EVENTS[EVSTDATE >= "2002-01-01"]
  
  num.vars <- c("FAC_TYPE", "DRG_31", "CCL", "PCCL", "MDC_CODE")
      
      PRIV_EVENTS[,(num.vars) := lapply(.SD, function(x){
         return(as.numeric(x))
      }), .SDcols = num.vars]
  
  write.fst(PRIV_EVENTS, "V:/source_data/R/HOSPITALISATION/PRIVATE/VSIMP_PRIVATE_EVENTS_2004_2017.fst", 75)
 
  
# Diagnosis 
PRIV_DIAGS <- haven::read_sas("V:/source_data/SAS/PRS/prs0572_diags.sas7bdat")

setDT(PRIV_DIAGS)

   PRIV_DIAGS[, (names(PRIV_DIAGS)) := lapply(.SD, function(x){
      attr(x, "format.sas") <- NULL
      attr(x, "label") <- NULL
      return(x)
   })]
   
   PRIV_DIAGS <- PRIV_DIAGS[,.(EVENT_ID, CC_SYS, DIAG_TYP, DIAG_SEQ, CLIN_CD)]
   
   num.vars <- "CC_SYS"
   
   PRIV_DIAGS[,(num.vars) := lapply(.SD, function(x){
      return(as.numeric(x))
   }), .SDcols = num.vars]
   
   PRIV_DIAGS <- PRIV_DIAGS[EVENT_ID %in% PRIV_EVENTS$EVENT_ID]
   
   write.fst(PRIV_DIAGS, "V:/source_data/R/HOSPITALISATION/PRIVATE/VSIMP_PRIVATE_DIAGS_2004_2017.fst", 75)
   
   
# ---- C.   Forward Map ICD-9 Records -----

# Mapping tables (to go from ICD-9 to ICD-10-AM-II)
ICD10    <- readRDS("common_lookups/ICD 10 Mapping/Latest_ICD10_Forward_Index.rds")
ICD9_10  <- as.data.table(readxl::read_excel("common_lookups/ICD 10 Mapping/ICD9 - 10/ICD9_to_10_forward_key.xlsx"))
   
# ICD-9 affects records from 1988 to 2005
files.list <- list.files("V:/source_data/R/HOSPITALISATION/SIMPLE/", pattern="DIAGS")
      
for(file in files.list){

   DIAG_DATA <- setDT(read.fst(paste0("V:/source_data/R/HOSPITALISATION/SIMPLE/", file)))
   
   if(file %in% files.list[grepl(paste(1988:2005, collapse="|"), files.list)]){
      
      DIAGS_ICD9  <- DIAG_DATA[CC_SYS==6]
      DIAGS_ICD10 <- DIAG_DATA[CC_SYS!=6]
      
      ICD9_10_ABV <- ICD9_10[TABLETYP %in% c("A","B","V")]
      ICD9_10_E   <- ICD9_10[TABLETYP=="E"]
      ICD9_10_O   <- ICD9_10[TABLETYP=="O"]
      
      DIAGS_ICD9[DIAG_TYP=="A" | DIAG_TYP=="B", CLIN_CD_X := ICD9_10_ABV$ICD10[match(CLIN_CD, ICD9_10_ABV$ICD9)]]
      DIAGS_ICD9[DIAG_TYP=="E", CLIN_CD_X := ICD9_10_E$ICD10[match(CLIN_CD, ICD9_10_E$ICD9)]]
      DIAGS_ICD9[DIAG_TYP=="O", CLIN_CD_X := ICD9_10_O$ICD10[match(CLIN_CD, ICD9_10_O$ICD9)]]
      
      # 0.05% of all ICD9 diag records cannot be mapped
      # Map ICD10-v1 to ICD10-v2
      DIAGS_ICD9[, CLIN_CD_10 := ICD10$ICD10_2[match(CLIN_CD_X, ICD10$ICD10_1)]][, CLIN_CD_X := NULL]
      DIAGS_ICD10[, CLIN_CD_10 := CLIN_CD]
      
      DIAG_DATA <- rbind(DIAGS_ICD9, DIAGS_ICD10)
      
   } else {
      
      DIAG_DATA[, CLIN_CD_10 := CLIN_CD]
      
   }
   
   write.fst(DIAG_DATA, paste0("V:/source_data/R/HOSPITALISATION/SIMPLE/", file), compress = 75)
   
   print(paste(file, " completed"))
}
   

# Private Diags

PRIV_DIAGS <- read.fst("V:/source_data/R/HOSPITALISATION/PRIVATE/VSIMP_PRIVATE_DIAGS_2004_2017.fst", as.data.table = T)

   DIAGS_ICD9  <- PRIV_DIAGS[CC_SYS==6]
   DIAGS_ICD10 <- PRIV_DIAGS[CC_SYS!=6]
   
   ICD9_10_ABV <- ICD9_10[TABLETYP %in% c("A","B","V")]
   ICD9_10_E   <- ICD9_10[TABLETYP=="E"]
   ICD9_10_O   <- ICD9_10[TABLETYP=="O"]
   
   DIAGS_ICD9[DIAG_TYP=="A" | DIAG_TYP=="B", CLIN_CD_X := ICD9_10_ABV$ICD10[match(CLIN_CD, ICD9_10_ABV$ICD9)]]
   DIAGS_ICD9[DIAG_TYP=="E", CLIN_CD_X := ICD9_10_E$ICD10[match(CLIN_CD, ICD9_10_E$ICD9)]]
   DIAGS_ICD9[DIAG_TYP=="O", CLIN_CD_X := ICD9_10_O$ICD10[match(CLIN_CD, ICD9_10_O$ICD9)]]
   
   # 0.05% of all ICD9 diag records cannot be mapped
   # Map ICD10-v1 to ICD10-v2
   DIAGS_ICD9[, CLIN_CD_10 := ICD10$ICD10_2[match(CLIN_CD_X, ICD10$ICD10_1)]][, CLIN_CD_X := NULL]
   DIAGS_ICD10[, CLIN_CD_10 := CLIN_CD]
   
   PRIV_DIAGS <- rbind(DIAGS_ICD9, DIAGS_ICD10)
   
   write.fst(PRIV_DIAGS, "V:/source_data/R/HOSPITALISATION/PRIVATE/VSIMP_PRIVATE_DIAGS_2004_2017.fst", compress = 75)
   