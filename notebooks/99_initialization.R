library(fpp3)
library(feasts)
library(tsibble)
library(ggrepel)
library("padt")

SORG <- 'FR30'

SLS <-                              # 
  pa_td_dyn_get(
    .vtype       = c('010')  , # 010 = Actuals, 060 = Forecast
    .ftype       = c(4)      , # Last Version 1 = PreDR, 2 = Pos
    .salesorg    = 'FR30'    ,
    .scope_matl  = FALSE     , # filter by A,B,C instead 
  ) 


dtSCOPE <- 
  openxlsx::read.xlsx( 
    xlsxFile = "C:\\PW\\OneDrive\\ET\\pythia\\data\\test\\Gold\\master_data\\SCOPING_VALUE.xlsx",
    sheet    = "MAT",
    startRow = 5,
    cols     =  1:3
  ) %T>% setDT() %>%
  .[, MATERIAL:= pa_matn1_input(MATERIAL)] %>%
  .[!MATERIAL %like% 'Result']


# PATH_SLV_SLS <- pa_ds_stageing_path_get(
#   .staging = "silver", 
#   .functional_area = "sales", 
#   "rtp"
# )

# dtACC7 <- readRDS(
#   file = file.path(PATH_SLV_SLS,"dtACC7.rds")
# )

# PATH_GLD_MD <- "C:\\PW\\OneDrive\\ET\\pythia\\data\\test\\Gold\\master_data"
# PATH_GLD_MD <- "C:\\Users\\flori\\OneDrive\\ET\\pythia\\data\\test\\Gold\\master_data"
# 
# 
# dtSCOPE <- fread(
#   file = file.path(PATH_GLD_MD, "SCOPE_FR30.csv")
# ) %>% 
#   .[, MATERIAL:= pa_matn1_input(MATERIAL)]

# dtSCOPE <- MATL[BASE_UOM == 'EA', .(MATERIAL)] %>%
#   .[dtSCOPE, on = .(MATERIAL)] %>%
#   MATS[PRAT7 != 1, .(SALESORG, MATERIAL)][
#     ., on = .(SALESORG, MATERIAL), nomatch = 0
#   ]   %>%
#   MBTS[SALESORG == SORG, 
#        .(SALESORG, MATERIAL = MAT_SALES)][
#          ., on = .(SALESORG, MATERIAL), nomatch = 0]  

# filter the scope 
# MATERIAL == MAT & 
dtTS_LEN_GT_24 <-
  copy(SLS) %>%
  .[Q > 0, .N, by = .(SALESORG, PLANT, MATERIAL)] %>%
  .[N > 24]

dtSLS <- 
  dtSCOPE[,.(SALESORG, PLANT, MATERIAL)]                   %>%
  unique()                                                 %>%
  .[SLS, on = .(SALESORG, PLANT, MATERIAL), nomatch = 0]   %>%
  dtTS_LEN_GT_24[., on = .(SALESORG, PLANT, MATERIAL),
                 , nomatch = 0] 

# dtDUMMY <- 
#   dtSLS[CALMONTH %chin% c('202401', '202402')]  %>%
#   .[, CALMONTH:= sub("2024", "2025", CALMONTH)] %>%
#   .[, Q:= 0]


  
# dtTST <-
#   rbind(dtSLS) %>%
#   .[, .(min  = min(CALMONTH), max = max(CALMONTH),
#         isna = sum(is.na(Q)), Q_0 = sum(Q==0)),
#         by =.(SALESORG, PLANT, MATERIAL)]
# 
# # %>%
# #   .[min == "202101" & max == "202502" & isna == 0 & Q_0 <= 3] 
# 
# PATH_GLD_MD <- "C:\\PW\\OneDrive\\ET\\pythia\\data\\test\\Gold\\master_data"
# PATH_GLD_MD <- "C:\\Users\\flori\\OneDrive\\ET\\pythia\\data\\test\\Gold\\master_data"
# 
# fwrite(
#   unique(dtTST[, .(SALESORG, PLANT, MATERIAL)]),
#   file = file.path(PATH_GLD_MD, "SCP_156.csv")
# ) 

tsSLS <- 
  rbind(dtSLS) %>%
  .[, .(SALESORG, PLANT, MATERIAL, CALMONTH, Q)]     %>%
  .[, YM:= yearmonth(CALMONTH, format = "%Y%m")]     %>% 
  .[, CALMONTH:= NULL]                               %>%  
  as_tsibble(
    key   = c(SALESORG, PLANT, MATERIAL), 
    index = YM
  )                %>%
  group_by_key()   %>%
  fill_gaps(.full = TRUE, Q = 0) 

months_diff <- 
  function(m1, m2) {
    
    d1 <- ymd(paste0(m1, "01"))
    d2 <- ymd(paste0(m2, "01"))
    
    M <- lubridate::interval(d2, d1) / months(1) 
    
    return(as.integer(M))
  }
