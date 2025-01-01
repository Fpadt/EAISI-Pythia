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
FN_FCST <- 
  paste0("['", 
         normalizePath(file.path(PDYN, "SDSFRPR2.parquet")), "', '", 
         normalizePath(file.path(PDYN, "SDSFRPR4.parquet")),
         "']"
  )

FN_FRPR1 <- 
  file.path(PDYN, paste0("SDSFRPR1.parquet")) 

FN_FRPR2 <- 
  file.path(PDYN, paste0("SDSFRPR2.parquet"))

FN_FRPR3 <- 
  file.path(PDYN, paste0("SDSFRPR3.parquet"))

FN_FRPR4 <- 
  file.path(PDYN, paste0("SDSFRPR4.parquet")) 

