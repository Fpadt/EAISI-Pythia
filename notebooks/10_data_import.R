# Data Access

# Paths to Parquet Files

# Define the path to your Parquet file

PRTP <- file.path(PS02, SYS, "RTP", "OB")
PSTK <- file.path(PS02, SYS, "STK", "OB")
PDYN <- file.path(PS02, SYS, "DYN", "IB")

# Material 
FN_MATL <- 
  file.path(PRTP, "MD_MATERIAL.parquet") %>% 
  normalizePath() 

FN_MATS <- 
  file.path(PRTP, "MD_MATERIAL_SALES_ORG.parquet") %>% 
  normalizePath() 

FN_MATP <- 
  file.path(PRTP, "MD_MATERIAL_PLANT.parquet") %>% 
  normalizePath() 

FN_MATP <- 
  file.path(PRTP, "MD_SOLD_TO_CUSTOMER.parquet") %>% 
  normalizePath() 

# Sales
FN_ISLS <- 
  file.path(PRTP, paste0("DD_SALES_QTY_202*")) 

# Stock
FN_STCK <- 
  file.path(PSTK, paste0("IMP03SM1*")) 

# Dynasys 2018
FN_FRPR <- 
  file.path(PDYN, paste0("SDSFRPR*")) 

