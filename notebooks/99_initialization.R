library(fpp3)
library(feasts)
library(tsibble)
library(ggrepel)
library("padt")

SLS <-                              # 
  pa_td_dyn_get(
    .vtype       = c('010')  , # 010 = Actuals, 060 = Forecast
    .ftype       = c(4)      , # Last Version 1 = PreDR, 2 = Pos
    .salesorg    = 'FR30'    ,
    .scope_matl  = TRUE 
  ) 

PATH_SLV_SLS <- pa_ds_stageing_path_get(
  .staging = "silver", 
  .functional_area = "sales", 
  "rtp"
)

dtACC7 <- readRDS(
  file = file.path(PATH_SLV_SLS,"dtACC7.rds")
)

# filter the scope 
# MATERIAL == MAT & 
dtSLS <- 
  dtACC7[SALESORG == SORG, 
         .(SALESORG, PLANT, MATERIAL)]                     %>%
  unique()                                                 %>%
  .[SLS, on = .(SALESORG, PLANT, MATERIAL), nomatch = 0]   %>%
  .[SALESORG == SORG & FTYPE == 4]     

tsSLS <- 
  dtSLS %>%
  .[, .(MATERIAL, PLANT,  CALMONTH, ACT = Q)]              %>%
  .[, YM:= yearmonth(CALMONTH, format = "%Y%m")]   %>% 
  .[, CALMONTH:= NULL]                                     %>%  
  as_tsibble(
    key   = c(MATERIAL, PLANT), 
    index = YM
  ) 

months_diff <- 
  function(m1, m2) {
    
    d1 <- ymd(paste0(m1, "01"))
    d2 <- ymd(paste0(m2, "01"))
    
    M <- lubridate::interval(d2, d1) / months(1) 
    
    return(as.integer(M))
  }