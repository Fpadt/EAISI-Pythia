# Data Access

.construct_path <- function(
    .root,  # 00
    .dat,   # 10
    .layer, # 20
    .sid,   # 30
    .pa_AREA   # 40 
) {
  
  PATHS <- read_xlsx(
    file = normalizePath(file.path(".", "config","IB_OB_FLD.xlsx")), 
    sheet = "PATHS", 
  ) %T>% setDT()

  if(missing(.root)){
    if(grepl(pattern = "OneDrive", x =  PATHS[SUB == 'L00', VALUE])){
      L00 <- Sys.getenv(PATHS[SUB == 'L00', VALUE]) 
    } else {
      L00 <- PATHS[SUB == 'L00', VALUE]
    }  
  } else {
    L00 <- .root
  }
  
  if(missing(.dat)){
    L10 <- PATHS[SUB == 'L10', VALUE]
  } else {
    L10 <- .dat
  }

  if(missing(.layer)){
    L20 <- PATHS[SUB == 'L20' & SELECT == 'X', VALUE]
  } else {
    L20 <- .layer
  }
  
  if(missing(.sid)){
    L30 <- PATHS[SUB == 'L30' & SELECT == 'X', VALUE]
  } else {
    L30 <- .sid
  }
  
  if(missing(.pa_AREA)){
    stop("Please provide a value for .pa_AREA")
  }
  
  full_path <- 
    normalizePath(
      file.path(L00, L10, L20, L30, .pa_AREA),
      winslash = "/")
  
  return(full_path)
}

  
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

# to be used for forecast Accuracy
# to be used for forecast Accuracy
# to be used to  forecast
# to be used to  forecast

.get_parquet_files <- function(
  .vtype, 
  .ftype
) {
  paths_parquet_files <- fread("
   stype, vtype, ftype, file_name
   DYN  , 010  , 1    , SDSFRFR1*.parquet  
   DYN  , 010  , 2    , SDSFRFR2*.parquet   
   DYN  , 010  , 3    , SDSFRLV1*.parquet  
   DYN  , 010  , 4    , SDSFRLV3*.parquet   
   DYN  , 060  , 1    , SDSFRPR2*.parquet
   DYN  , 060  , 2    , SDSFRPR4*.parquet
   ",
  colClasses = list(character = "vtype"))  
  
  paths_parquet_files[
    vtype == .vtype & ftype == .ftype, file_name]
}

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
}
paths_parquet_files <- fread("
   stype,vtype,ftype,path
   DYN,010,1,SDSFRFR1*.parquet  
   DYN,010,2,SDSFRFR2*.parquet   
   DYN,010,3,SDSFRLV1*.parquet  
   DYN,010,4,SDSFRLV3*.parquet   
   DYN,060,1,SDSFRPR2*.parquet
   DYN,060,2,SDSFRPR4*.parquet
   ",
  colClasses = list(character = "vtype"))  %>%    
  .[, path := file.path(PDYN, path)]

# .construct_path(.layer = "S2S", .pa_AREA = "DYN")
FN_FRPR1 <-               # pre-Demand review 
  paths_parquet_files[vtype == '010' & ftype == 1, path]

.construct_path(.layer = "S2S", .pa_AREA = "DYN")
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


