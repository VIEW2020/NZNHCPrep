
# NHI Demographic Data 2019 v3

# Last updated August 2019

# Note - the ENHI both-table prepartion is now embedded into this process as a sub-routine - because it needs to be done each time the NHI is updated!

# ---- Info --- 
# NHI Demographic Index as at August 2019

# Initial 8475022 primary / 9784683 secondary

library(data.table)
library(fst)

NHI  <- haven::read_sas("source_data/SAS/NHI/mis3956.sas7bdat")

setDT(NHI)

# --- A.  Pre-cleaning ----

# i.  Clean variable classes
ClearAttr <- function(x, type){
  attr(x, type) <- NULL
  return(x)
}

remove.attr <- names(NHI)[3:11]

NHI[, (remove.attr) := lapply(.SD,
                              function(x)
                                ClearAttr(x, "format.sas")), .SDcols=remove.attr]

# ii. Numerics classes
names(NHI) <- tolower(names(NHI))

num.vars <- c("ethnicg1","ethnicg2","ethnicg3","ethnicgp","dom_cd")

  NHI[, (num.vars) := lapply(.SD,
                              function(x)
                                as.numeric(x)), .SDcols=num.vars]


#---- B.   Unify secondaries ----

# 1. Seperate people into single ENHI & Multiples
#    ~13% have multiple secondary ENHIs (duplicate masters)
NHI[, index := seq_len(.N), by=enigma_master]

SINGLE_NHI <- NHI[NHI[, .I[max(index)==1], by=enigma_master]$V1]
MULTI_NHI  <- NHI[NHI[, .I[max(index)>1], by=enigma_master]$V1]


# 2.  Date of Birth
SINGLE_NHI[, "en_dob":=dob]

# People with multiple NHI records may have different DOBs across their records
# - split people with SINGLE-DOBs and MULTI-DOBS (among people with Multiple NHI Records)
# - For people with single unique DOBs, they can just be written directly across 

M_SINGLE_DOB <- MULTI_NHI[MULTI_NHI[, .I[uniqueN(na.omit(dob))==1], by=enigma_master]$V1]
M_SINGLE_DOB[, "en_dob" := unique(na.omit(dob)), by=enigma_master]

#     316143 Records ~1.6% (134248 people) have multiple DOBs (where there are multiple ENHI records)
M_MULTI_DOB  <- MULTI_NHI[MULTI_NHI[, .I[uniqueN(na.omit(dob))>=2], by=enigma_master]$V1]

# For the DOB field, the primary (ie. where primary key = secondary key) maybe not have the most complete information for ethnicity or DOD. 
# Therefore, you can't just remove non-primary records! Instead, CAPTURE the DOB from the primary record as the the true DOB. Do not remove records!
# Interestinly, almost all primary codes have the latest updated date.
M_MULTI_DOB[, "en_dob" := dob[which(enigma_master==enigma_secondary)], by=enigma_master]

#     Bind back together
MULTI_NHI <- rbind(M_SINGLE_DOB, M_MULTI_DOB); rm(M_SINGLE_DOB, M_MULTI_DOB)


# 3.  Date of death
SINGLE_NHI[, "en_dod":=as.Date(dod)]

# Split people with deaths vs no deaths - ie. no DOD at all
MULTI_NO_DEATHS <- MULTI_NHI[MULTI_NHI[, .I[all(is.na(dod))], by=enigma_master]$V1]
MULTI_NO_DEATHS[, "en_dod":=as.Date(NA)]

#  NB: ~2.2% (200648 people; 437251 records) have multiple ENHI records + a death in at least one record
MULTI_ANY_DEATH <- MULTI_NHI[MULTI_NHI[, .I[!all(is.na(dod))], by=enigma_master]$V1]

#  The MOH have MOSTLY assigned the DOD to the primary record. Generally [except for ~1800 records], the secondary records contains no DOD.
#  The most direct approach is to remove records without DOD. However, since SOME secondary records could contain useful ethnicity or Dep data,
#  you can't just remove non-primary records! Instead, CAPTURE the DOD from the primary record as the the true DOD. 
#  Note - DO NOT remove records!
MULTI_ANY_DEATH[, "en_dod" := dod[which(enigma_master==enigma_secondary)], by=enigma_master]

# For ~2K records (~800 people), their DOD is actually recorded on the secondary record. Carry over the DOD for these people.
MULTI_ANY_DEATH[is.na(en_dod), "en_dod" := na.omit(unique(dod))[1], by=enigma_master]

#     Bind together
MULTI_NHI <- rbind(MULTI_NO_DEATHS, MULTI_ANY_DEATH); rm(MULTI_NO_DEATHS, MULTI_ANY_DEATH)


# 4.  Ethnicity
eth.vars <- c("ethnicgp", "ethnicg1", "ethnicg2", "ethnicg3")
en.eth.vars <- paste0("en_", eth.vars)
  
SINGLE_NHI[, (en.eth.vars) := lapply(.SD, function(x) 
  return(x)
  ), .SDcols = eth.vars]

# For multi-NHI records, remove codes 61 (other), 90-99 (unknown), then use zoo::locf method
MULTI_NHI[, (en.eth.vars) := lapply(.SD, function(x)
   
  replace(x, list = which(x %in% c(61, 90:99)), 
          values = NA)
  
  ), .SDcols = eth.vars]

MULTI_NHI[order(last_updated_date),
          by = enigma_master,
          (en.eth.vars) := lapply(.SD, function(x){
            
            x <- zoo::na.locf(x, na.rm = FALSE)
            x <- zoo::na.locf(x, fromLast = T)
            return(x)
            
          }), .SDcols = en.eth.vars]

NHI_DATA <- rbind(SINGLE_NHI, MULTI_NHI)

NHI_DATA[, (en.eth.vars) := lapply(.SD, function(x)
   
  replace(x, list = which(is.na(x) | x %in% c(54,61,94,95,97)), 
          values = 99)
  
  ), .SDcols = en.eth.vars]

BACKUP <- copy(NHI_DATA)


# Aggregate Ethnicity
euro    <- c(10,11,12)
maori   <- 21
pacific <- c(30:37)
fijian  <- 36
oth.asn <- c(40,41,44)
chinese <- 42
indian  <- 43
melaa   <- c(51,52,53)
other   <- 99 

# Prioritised Ethnicity
library(fastmatch)
library(future.apply); plan(multiprocess, workers = 8) 

NHI_DATA[, en_prtsd_eth := future_mapply(function(x,y,z){
  
  v <- c(x,y,z)
  
  if(all(v == 99)){
    return(9)
  } else {
    dplyr::case_when(
      any(v %fin% maori) ~ 2,
      any(v %fin% fijian) & any(v %fin% indian) ~ 43,
      any(v %fin% pacific) ~ 3,
      any(v %fin% indian) ~ 43,
      any(v %fin% chinese) ~ 42,
      any(v %fin% oth.asn) ~ 4,
      any(v %fin% melaa) ~ 5,
      TRUE ~ 1)
  }
}, x = en_ethnicg1, y = en_ethnicg2, z = en_ethnicg3)]


# Sole ethnicity
sole.eth <- c("en_sole_ethgp", "en_sole_ethg1")

NHI_DATA[, (sole.eth) := lapply(.SD, function(x){
  
  x <- replace(x, which(x %in% euro), 1)
  x <- replace(x, which(x %in% maori), 2)
  x <- replace(x, which(x %in% pacific), 3)
  x <- replace(x, which(x %in% oth.asn), 4)
  x <- replace(x, which(x %in% chinese), 42)
  x <- replace(x, which(x %in% indian), 43)
  x <- replace(x, which(x %in% melaa), 5)
  x <- replace(x, which(x %in% other), 9)
  
  return(x)

}), .SDcols = c("en_ethnicgp", "en_ethnicg1")]


# Sole / Combination Ethnicity
NHI_DATA[, en_solecombo_eth := future_mapply(function(x,y,z){
  
  v <- c(x,y,z)
  v <- v[!v %in% 99]
  
  if(length(v)==0){
    return(9)
  } else {
    dplyr::case_when(
      all(v %fin% euro) ~ 1,
      all(v %fin% maori) ~ 2,
      all(v %fin% pacific) ~ 3,
      all(v %fin% oth.asn) ~ 4,
      all(v %fin% chinese) ~ 42,
      all(v %fin% indian) ~ 43,
      all(v %fin% melaa) ~ 5,
      TRUE ~ 99)
  }}, x = en_ethnicg1, y = en_ethnicg2, z = en_ethnicg3)]

NHI_DATA[en_solecombo_eth == 99, 
         en_solecombo_eth := future_mapply(function(x,y,z){
           
           v <- c(x,y,z)
           v <- v[!v %in% 99]
           
           dplyr::case_when(
             any(v %fin% fijian) & any(v %fin% indian) ~ 3,
             any(v %fin% maori) & any(v %fin% euro) ~ 2.1,
             any(v %fin% maori) & any(v %fin% pacific) ~ 2.3,
             any(v %fin% pacific) & any(v %fin% euro) ~ 3.1,
             length(v)==2 ~ 9.2,
             TRUE ~ 99)
           }, x = en_ethnicg1, y = en_ethnicg2, z = en_ethnicg3)]


# 5.  Save
# Totaly = 9784683 rows / 8475022 master / 9784683 secondary
setnames(NHI_DATA, names(NHI_DATA), toupper(names(NHI_DATA)))

write.fst(NHI_DATA, "source_data/R/NHI/ENIGMA_NHI_LOOKUP_AUG2019.fts", 75)


#---- C.   VIEW SIMPLE MASTER / SECONDARY----

ALL_ENHI <- read.fst("source_data/R/ENHI BOTH TABLES/DO NOT REMOVE!/VIEW_SIMPLE_ALL_KEYS.fst", as.data.table = T)

ENHI <- copy(NHI_DATA[,.(enigma_master, enigma_secondary)])

# Master
setkey(ALL_ENHI, VIEW_MoH_eNHI)
setkey(ENHI, enigma_master)

ALL_MASTER <- ALL_ENHI[ENHI, nomatch = 0]

setnames(ALL_MASTER, 
         old = c("VIEW_eNHI", "VIEW_MoH_eNHI", "VSIMPLE_INDEX"),
         new = c("VIEW_ENHI_MASTER", "MOH_ENHI_MASTER", "VSIMPLE_INDEX_MASTER"))

# Secondary
setkey(ALL_MASTER, enigma_secondary)

ALL_SECOND <- ALL_ENHI[ALL_MASTER, nomatch = 0]

setnames(ALL_SECOND, 
         old = c("VIEW_eNHI", "VIEW_MoH_eNHI", "VSIMPLE_INDEX"),
         new = c("VIEW_ENHI_2NDARY", "MOH_ENHI_2NDARY", "VSIMPLE_INDEX_2NDARY"))

# Save
write.fst(ALL_SECOND, "source_data/R/ENHI BOTH TABLES/VIEW_VSIMP_MOH__PS_AUG2019.fst", 75)


#---- D.   Attach Latest ENHI to Latest NHI----

# NB: Export the data with secondary keys so that it can be syncronyised with mortality collection
NHI <- merge(ALL_SECOND[,.(VSIMPLE_INDEX_MASTER, VSIMPLE_INDEX_2NDARY, MOH_ENHI_2NDARY)], NHI_DATA,
             by.x = "MOH_ENHI_2NDARY", by.y = "ENIGMA_SECONDARY",
             all.y = F)

NHI[, c("MOH_ENHI_2NDARY","ENIGMA_MASTER") := NULL]
NHI[, c("ETHNICG1","ETHNICG2","ETHNICG3","ETHNICGP",
        "INDEX","DOB","DOD") := NULL]

write.fst(NHI, "source_data/R/NHI/Working/VSIMPLE_NHI_LOOKUP_AUG2019.fts", 75)


# --- E. Create updated ENHI both-tables----

library(data.table)
library(fst)

# There are two categories: 1) Exisiting and 2) New VIEW SIMPLE (introduced in 2019) 

# -- 1. Exisiting --
# Bring in all enigma master list (14 million rows)
# Doesn't need to be repeated with each version!
ALL_ENHI  <- readRDS("source_data/R/ENHI BOTH TABLES/ALL_ENIGMA_ALL_INHOUSE.rds")
ALL_VSIMP <- read.fst("source_data/R/ENHI BOTH TABLES/DO NOT REMOVE!/VIEW_SIMPLE_ALL_KEYS.fst", as.data.table = T)

ALL_VSIMP <- ALL_VSIMP[,.(VSIMPLE_INDEX, VIEW_MoH_eNHI)]

setkey(ALL_ENHI, VIEW_MoH_eNHI)
setkey(ALL_VSIMP, VIEW_MoH_eNHI)

ALL_ENHI <- ALL_ENHI[ALL_VSIMP, nomatch = 0][,.(VSIMPLE_INDEX, VIEW_eNHI, VIEW_MoH_eNHI, VIEW_TESTSAFE_eNHI, VIEW_ANZACS_eNHI, MLEE_INDEX, EXTERNAL_INDEX)]

write.fst(ALL_ENHI, "source_data/R/ENHI BOTH TABLES/DO NOT REMOVE!/VIEW_SIMPLE_ALL_KEYS_EXTENDED.fst", 75)


# Master
NHI <- read.fst("source_data/R/NHI/Working/VSIMPLE_NHI_LOOKUP_AUG2019.fts", as.data.table=T)
ENHI <- NHI[,c("VSIMPLE_INDEX_MASTER", "VSIMPLE_INDEX_2NDARY"), with=F]

setkey(ALL_ENHI, VSIMPLE_INDEX)
setkey(ENHI, VSIMPLE_INDEX_MASTER)

ALL_MASTER <- ALL_ENHI[ENHI, nomatch = 0]

setnames(ALL_MASTER, 
         old = c("VSIMPLE_INDEX", "VIEW_eNHI", "VIEW_MoH_eNHI", "VIEW_TESTSAFE_eNHI", "VIEW_ANZACS_eNHI", "MLEE_INDEX", "EXTERNAL_INDEX"),
         new = c("VSIMPLE_INDEX_MASTER", "VIEW_ENHI_MASTER", "MOH_ENHI_MASTER", "VIEW_TESTSAFE_MASTER", "VIEW_ANZACS_MASTER", "MLEE_INDEX_MASTER", "EXTERNAL_INDEX_MASTER"))

# Secondary
setkey(ALL_MASTER, VSIMPLE_INDEX_2NDARY)

ALL_SECOND <- ALL_ENHI[ALL_MASTER, nomatch = 0]

setnames(ALL_SECOND, 
         old = c("VSIMPLE_INDEX", "VIEW_eNHI", "VIEW_MoH_eNHI", "VIEW_TESTSAFE_eNHI", "VIEW_ANZACS_eNHI", "MLEE_INDEX", "EXTERNAL_INDEX"),
         new = c("VSIMPLE_INDEX_2NDARY", "VIEW_ENHI_2NDARY", "MOH_ENHI_2NDARY", "VIEW_TESTSAFE_2NDARY", "VIEW_ANZACS_2NDARY", "MLEE_INDEX_2NDARY", "EXTERNAL_INDEX_2NDARY"))

# Save all
write.fst(ALL_SECOND, "source_data/R/ENHI BOTH TABLES/ALL__PS_AUG2019.fst", 75)


# -- VERY IMPORTANT -- PROCEDURE NOT FINISHED - TBC BY SYNCING WITH MORTALITY COLLECTION!!


