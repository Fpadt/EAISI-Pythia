# Data Access

# Paths to Parquet Files

# Define the path to your Parquet file

PARR <- file.path(PS02, SYS, "RTP", "ARR")

# Material 
FN_MATL <- 
  file.path(PARR, "MD_MATERIAL.parquet") %>% 
  normalizePath() 

FN_MATS <- 
  file.path(PARR, "MD_MATERIAL_SALES_ORG.parquet") %>% 
  normalizePath() 

FN_MATP <- 
  file.path(PARR, "MD_MATERIAL_PLANT.parquet") %>% 
  normalizePath() 

FN_MATP <- 
  file.path(PARR, "MD_SOLD_TO_CUSTOMER.parquet") %>% 
  normalizePath() 

# Sales
FN_ISLS <- 
  file.path(PARR, "DD_HISTO_QTY_21-24.parquet") %>% 
  normalizePath() 