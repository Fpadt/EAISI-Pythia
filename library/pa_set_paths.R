# Data Access

# Paths to Parquet Files

# Define the path to your Parquet file

PRTP <- file.path(PS02, SYS, "RTP")
PSTK <- file.path(PS02, SYS, "STK")
PDYN <- file.path(PS02, SYS, "DYN")

# Master Data
FN_MATL <- 
  file.path(PRTP, "MD_MATERIAL.parquet") %>% 
  normalizePath() 

FN_MATS <- 
  file.path(PRTP, "MD_MATERIAL_SALES_ORG.parquet") %>% 
  normalizePath() 

FN_MATP <- 
  file.path(PRTP, "MD_MATERIAL_PLANT.parquet") %>% 
  normalizePath() 

FN_CUST <- 
  file.path(PRTP, "MD_SOLD_TO_CUSTOMER.parquet") %>% 
  normalizePath() 

# Transaction Data

## Invoiced Sales for Dynasys Cloud RTP
FN_IRTP <-                # pre-Demand review 
  file.path(PRTP, paste0("DD_SALES_QTY_202*")) 

## Invoiced Sales for Dynasys on-premise 2018
FN_IIPM <-                # pre-Demand review 
  file.path(PRTP, paste0("DD_SALES_QTY_202*")) 

# Stock
FN_STCK <- 
  file.path(PSTK, paste0("IMP03SM1*")) 

# from Dynasys

## Actuals

paths_parquet_files <- fread("
   stype,vtype,ftype,path
   DYN,010,1,SDSFRPR1*.parquet
   DYN,010,5,SDSFRFA1.parquet   
   DYN,010,2,SDSFRPR3*.parquet
   DYN,010,6,SDSFRFA3.parquet   
   DYN,060,1,SDSFRPR2*.parquet
   DYN,060,2,SDSFRPR4*.parquet
   ",
  colClasses = list(character = "vtype"))  %>%    
  .[, path := file.path(PDYN, path)]

FN_FRPR1 <-               # pre-Demand review 
  paths_parquet_files[vtype == '010' & ftype == 1, path]

FN_FRPR5 <-               # pre-Demand review 
  paths_parquet_files[vtype == '010' & ftype == 5, path]

FN_FRPR3 <-               # pst-Demand review
  paths_parquet_files[vtype == '010' & ftype == 2, path]

FN_FRPR6 <-               # pst-Demand review
  paths_parquet_files[vtype == '010' & ftype == 6, path]

## Forecasts

FN_FRPR2 <-               # pre-Demand review
  paths_parquet_files[vtype == '060' & ftype == 1, path]

FN_FRPR4 <-               # pst-Demand review
  paths_parquet_files[vtype == '060' & ftype == 2, path]


