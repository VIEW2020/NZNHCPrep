
# - NHI & Mortality Collection Syncronysation 2019 v2-

# Last updated August 2019

# New to 2019, this proceedure provides quality checking between NHI and the Mortality Collection!
# The procedure further refines both the NHI and mortality collections to ensure correct MASTER KEY unification and death data for both collections.

library(fst)
library(data.table)
library(lubridate)
 
NHI  <- read.fst("source_data/R/NHI/Working/VSIMPLE_NHI_LOOKUP_AUG2019.fts", as.data.table = T)
MORTALITY <- read.fst("source_data/R/MORTALITY/Working/VSIMPLE_MORTALITY_DEC2017_v2.fst", as.data.table = T)

# ---- A. Quality check using Latest NHI ----

# i.  DOB / DOD
INDEX <- merge(MORTALITY[,.(VSIMPLE_INDEX_2NDARY, DOB, DOD)],
               NHI[,.(VSIMPLE_INDEX_2NDARY, EN_DOB, EN_DOD)],
               by="VSIMPLE_INDEX_2NDARY",
               all.x=T)

INDEX[,c("match_dob", "match_dod") := list(+(DOB==EN_DOB),
                                           +(DOD==EN_DOD))]

INDEX[, no_match := +(match_dob==0 & match_dod==0)]

# There are 204 people with BOTH mismatching DOB and DOD
# The majority of cases are off by 1 or 2 days. 
# Only Remove those records where the dates are super different. Use the date.intersect function (2 of 3 match)
date.intersect <- function(x){
  
  date.1  <- c(year(x[1]), month(x[1]), day(x[1]))
  date.2 <- c(year(x[2]), month(x[2]), day(x[2]))
    
  date.1 %in% date.2
  
}

# 8 people have BOTH very different DOBs & DODs
INDEX <- INDEX[no_match==1][, diff_person := apply(.SD, 1, function(x)
  +(sum(date.intersect(x))<=1)), 
  .SDcols=c("DOB", "EN_DOB")][diff_person==1]

INDEX <- INDEX[no_match==1][, diff_person := apply(.SD, 1, function(x)
  +(sum(date.intersect(x))<=1)), 
  .SDcols=c("DOD", "EN_DOD")][diff_person==1]


# Remove these 8 people from both the mortality collection and NHI!
MORTALITY <- MORTALITY[!VSIMPLE_INDEX_2NDARY %in% INDEX$VSIMPLE_INDEX_2NDARY]
NHI <- NHI[!VSIMPLE_INDEX_2NDARY %in% INDEX$VSIMPLE_INDEX_2NDARY]


# ---- B.   Syncronise DOD Each Collection -----

# i.  Attach Master ENHI to Mortality Collection
MORTALITY <- merge(NHI[,.(VSIMPLE_INDEX_MASTER,VSIMPLE_INDEX_2NDARY)], MORTALITY,
                   by="VSIMPLE_INDEX_2NDARY", all.y=F)

# There are ~500 mistaches containing very very minor differences in dates. The above process has already confirmed the details belong to the same person.
# Overwrite these records using EN_DOD and EN_DOB: i.e. use NHI data as the true value
MORTALITY[match(NHI$VSIMPLE_INDEX_2NDARY, VSIMPLE_INDEX_2NDARY), 
          c("EN_DOD", "EN_DOB") := list(NHI$EN_DOD,
                                        NHI$EN_DOB)]

# Update NHI with DOD (from Mortality Collection) where DOD is missing in NHI
# nb - must use MASTER so that DOD's can be copied across multiple secondaries
DEATH <- MORTALITY[is.na(MORTALITY$EN_DOD)] # Affects 67 people

NHI[is.na(EN_DOD), EN_DOD := DEATH$DOD[match(VSIMPLE_INDEX_MASTER, DEATH$VSIMPLE_INDEX_MASTER)]]

# NHI_DEATHS <- NHI[!is.na(EN_DOD)]
# sum(NHI_DEATHS$VSIMPLE_INDEX_2NDARY %in% MORTALITY$VSIMPLE_INDEX_2NDARY)

# For Mortality Collection - Reduce to one MASTER per row (i.e. make the secondary record redudant)
# There are 115 people (230 records) where MASTERS are duplicated
DUPS <- copy(MORTALITY)[VSIMPLE_INDEX_MASTER %in% VSIMPLE_INDEX_MASTER[which(duplicated(MORTALITY$VSIMPLE_INDEX_MASTER))]]

# Remove DUPS from MORTALITY (rbind non-dups later)
MORTALITY <- MORTALITY[!VSIMPLE_INDEX_2NDARY %in% DUPS$VSIMPLE_INDEX_2NDARY]

# Clean up DUPS
# First, make sure document status code is not missing
# Second, make sure gender is not Unknown
# Third, direct duplicate removal
DUPS <- DUPS[!is.na(DOCUMENT_STATUS_CODE)]
DUPS <- DUPS[GENDER!="U"]
DUPS <- DUPS[, index := seq_len(.N), by = VSIMPLE_INDEX_MASTER][index==1, -"index", with=F]

# Merge back to MORTALITY
MORTALITY <- rbind(MORTALITY, DUPS)

MORTALITY[, VSIMPLE_INDEX_2NDARY := NULL]

# uniqueN(MORTALITY$VSIMPLE_INDEX_MASTER) == nrow(MORTALITY)
# 869179 unique MASTER for 869179 individuals 

# --- Final Export ---

write.fst(NHI, "source_data/R/NHI/VSIMPLE_NHI_LOOKUP_AUG2019.fts", 75)
write.fst(MORTALITY, "source_data/R/MORTALITY/VSIMPLE_MORTALITY_DEC2017_v2.fst", 75)


# For ANZACS-QI / ANZACS-RESEARCH 
ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/DO NOT REMOVE!/VIEW_SIMPLE_ALL_KEYS.fst", as.data.table = TRUE)

# 1.  Mortality
MORTALITY <- read.fst("source_data/R/MORTALITY/VSIMPLE_MORTALITY_DEC2017_v2.fst", as.data.table = TRUE)

setkey(ENHI, VSIMPLE_INDEX)
setkey(MORTALITY, VSIMPLE_INDEX_MASTER)

MORTALITY <- ENHI[MORTALITY, nomatch = 0]
MORTALITY <- MORTALITY[,.(VIEW_MoH_eNHI,REG_YEAR,EN_DOD,CC_SYS,ICDD,ICDF1,ICDF2,ICDF3,ICDF4,ICDF5,ICDF6)]

setnames(MORTALITY, "VIEW_MoH_eNHI", "enigma_master")

haven::write_sav(MORTALITY, "source_data/SAS/MOS/MOH_MORTALITY_DEC2017_v2.sav")


# 2.  NHI
#     nb: make sure index is secondary! 
NHI <- read.fst("source_data/R/NHI/VSIMPLE_NHI_LOOKUP_AUG2019.fts", as.data.table = T)

setkey(ENHI, VSIMPLE_INDEX)
setkey(NHI, VSIMPLE_INDEX_2NDARY)

NHI <- ENHI[NHI, nomatch = 0]
NHI <- NHI[,.(VIEW_MoH_eNHI, GENDER, DEP01, DEP06, DEP13, EN_DOB, EN_DOD,
              EN_ETHNICG1, EN_ETHNICG2, EN_ETHNICG3, EN_PRTSD_ETH)]

setnames(NHI, "VIEW_MoH_eNHI", "enigma_secondary")

# Consider saving to D:/temp first for speed
haven::write_sav(NHI, "source_data/SAS/NHI/NHI_LOOKUP_AUG2019_ANZACS.sav")



