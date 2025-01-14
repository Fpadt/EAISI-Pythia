# https://yihui.name/knitr/options/
knitr::opts_chunk$set(
  cache   = FALSE,
  echo    = FALSE,     # include R source code in the output  
  eval    = TRUE,
  message = FALSE,
  warning = FALSE,
  results = "markup",
  image   = TRUE,  
  include = FALSE      # include the chunk output in the output 
)

computer_name <- 
  Sys.getenv(
    ifelse(.Platform$OS.type == "windows", "COMPUTERNAME", "HOSTNAME")
    )

# folder locations
RW <- 
  switch(
    computer_name,
    "TREX-TOAD" = file.path("U:", "floris", "GH"),
                  file.path("C:", "RW")
  ) 
PRJ  <- file.path(RW , "EAISI-Pythia")
MOD  <- file.path(PRJ, "library")
RAW  <- file.path(PRJ, "10_RawData")
DAT  <- file.path(PRJ, "11_PrepData")
ANA  <- file.path(PRJ, "30_Analysis")
RES  <- file.path(PRJ, "60_Results")
FIG  <- file.path(PRJ, "70_Figures")
SAP  <- DAT 

DWL <- file.path("c:", "Users", "Floris.padt", "Downloads")

ONE <- file.path("C:", "Users", "floris.padt", "OneDrive - Wessanen")
GEN <- file.path(ONE, "General")
OPS <- file.path(ONE, "OPS - Operations")
DEV <- file.path(ONE, "DEV - Development")
EIM <- file.path(ONE, "Business Intelligence & Analytics")

FCT <- file.path(ONE, "Forecasting")
IMM <- file.path(ONE, "Inventory Management")
SCA <- file.path(ONE, "General - EU EIM SCA")

# SharePoint
EBQ <- file.path(ONE, "BI_Documents - Ecotone BI QA")
EPB <- file.path(ONE, "BI_Documents")

# Personal
PET <- file.path("C:", "PW", "OneDrive", "ET")

# Pythia
PPET <- normalizePath(
  file.path(Sys.getenv("OneDriveConsumer"), "ET"), winslash = "/")
PDAT <- file.path(PPET, "pythia", "dat")

PS01 <- file.path(PDAT, "S1B")    # Staging - Bronze
PS02 <- file.path(PDAT, "S2S")    # Staging - Silver
PS03 <- file.path(PDAT, "S3G")    # Staging - Gold
PS04 <- file.path(PDAT, "S4P")    # Staging - Platinum

PPRM <- file.path(PS01, "PRM")    # Pythia - Promotions

# PSYS <- file.path(PS01,  SYS)          # BW SYSTEM

# PRTP <- file.path(PSYS, "RTP")         # Actuals deliverd to RTP project
# PIPM <- file.path(PSYS, "IPM")         # Actuals for Ambient for DSCP 2018
# PDYN <- file.path(PSYS, "DYN")         # Data From Dynasys Actuals & Forecast
# PEDA <- file.path(PSYS, "EDA")         # Data From EDA Actuals & Forecast


# Systems
WPB <- B4P <- c("WPB500")
WPE <- S4P <- c("WPE500")
WQB <- B4Q <- c("WQB500")
WQE <- S4Q <- c("WQE500")
WDB <- B4D <- c("WDB100")
WDE <- S4D <- c("WDE100")
WTB <- B4T <- c("WTB500")
WTE <- S4T <- c("WTE500")

ebw = c(B4T, B4D, B4Q, B4P)
es4 = c(S4T, S4D, S4Q, S4P)

EBW <- ebw
ES4 <- es4

# Load functions
invisible(source(file.path(MOD, "General.R")))
invisible(source(file.path(MOD, "ET_Functions.R")))
invisible(source(file.path(MOD, "pa_functions.R")))
invisible(source(file.path(MOD, "pa_set_paths.R")))
#invisible(source(file.path(MOD, "00_Global", "SAP2R_BODS.R")))
if (computer_name == 'GNPBB54' ){
  devtools::load_all(path = "C:\\RW\\sapyr")
  library(reticulate)
  use_condaenv("sapyr")
} else {
  # library("sapyr", lib.loc = "C:/Users/Floris.Padt/AppData/Local/R/cache/R/renv/library/sapyr-1006ad23/R-4.3/x86_64-w64-mingw32/")
  # invisible(source(file.path(RW , "sapyr", "sapyr.R")))
}
#invisible(source(file.path(MOD, "00_Global", "RDCOMClient.R")))

# gvLOGO_PATH <- file.path(FIG, "GV_LOGO.png") 
# if (!file.exists(gvLOGO_PATH)) {
#   download.file(
#     url      = "http://www.grandvision.com/img/logoGV.png",
#     destfile = gvLOGO_PATH,
#     mode     = 'wb')
# }
# knitr::include_graphics(gvLOGO_PATH)

# https://coolors.co/3e074a-0f431c-fde8e9-f0f600
# Colors

ET_CG = "#0f5e3c"
ET_FG = "#089b35"
ET_LG = "#38e56d"
ET_YL = "#fff200"
ET_JB = "#333333"


# GV_ORNG_0 <- "#FE5000"
# GV_ORNG_1 <- "#F68946"
# GV_ORNG_2 <- "#FCC098"
# GV_ORNG_3 <- "#FFF5ED"
# GV_ORNG_A <- "#EF8200"
# 
# GV_BLUE_0 <- "#005ABB"
# GV_BLUE_1 <- "#5580C1"
# GV_BLUE_2 <- "#AEBDE1"
# GV_BLUE_3 <- "#005ABB"
# GV_BLUE_A <- "#EEF0F8"
# 
# GV_GREY_0 <- "#000000"
# GV_GREY_1 <- "#58595B"
# GV_GREY_2 <- "#BCBEC0"
# GV_GREY_3 <- "#F1F1F2"
# GV_GREY_4 <- "#FFFFFF" 
# GV_GREY_A <- "#818183"

COL_FACTR <- "#9CEB91"
COL___RED <- "#ff7f7f"
