# Floris Padt

#### Library ####
library(knitr      , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
# library(kableExtra , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library("crayon"    , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

library(ggplot2    , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(stringr    , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr      , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate  , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(glue       , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

library(magrittr   , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(tidyverse  , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(purrr      , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

library(data.table , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

# library(fs         , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
# library(broom      , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

library(openxlsx   , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(openxlsx2  , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

# library(forecast)
# library(fpp)


fPrependDateTime <- function(filename){
    filename %>%
    paste0(format(Sys.time(), "%Y%m%d%H%M%S"), "-", .)
}

wp <- function(path) {
  writeClipboard(path)
  gsub("\\\\", "/", readClipboard())
}

# ---- Filter ----
# Function to create a filter to be used when extracting data from SAP
# ---- filter ----
f_or <- 
  function(x, field){
    
    dtL <- as.data.table(x = x)
    # anr <- paste0(and_restrict, "AND", " ")
    
    dtL[         , RET := paste0(field, " = '", x, "' or")]
    dtL[nrow(dtL), RET := sub(pattern = "' or", x = RET, replacement = "'")]
    
    as.list(dtL[, RET])
  }


fTableOverview <- function(pTable){
  
  lstUnique    <- lapply(pTable   , FUN = unique)
  vUniqueCount <- sapply(lstUnique, FUN = length)
  vUniqueCount <- sort(vUniqueCount, decreasing = TRUE)
  
  dtRATIO    <- data.table(FLDNM  = names(vUniqueCount),
                           UCOUNT = vUniqueCount,
                           RATIO  = round(nrow(pTable)/vUniqueCount, 0)) 
  
  fHDR   <- function(x){
    l_ret <- paste(sort(x)[1:ifelse(length(x) < 11, length(x), 10)], collapse = "/")
    if (substr(l_ret, 1, 1) == "/") { l_ret <- paste0("#", l_ret)}
    return(l_ret)
  }
  
  
  dtTMP  <- data.table(FLDNM = names(lstUnique),
                       EXAMP = sapply(lstUnique, FUN = fHDR))
  
  dtRATIO <-
    merge(dtRATIO, dtTMP, by = "FLDNM")[order(UCOUNT, decreasing = TRUE)]
  
  return(list(UCOUNT = matrix(vUniqueCount,
                              ncol = 1,
                              dimnames = list(names(vUniqueCount), "Count")), 
              UNIQUE = lstUnique,
              dtRATIO = dtRATIO ))
}

fTA <- 
  function(pDT){
    fTableOverview(pDT)$dtRATIO 
  }


fOpen_as_xlsx <- 
  function(pDT, pPath = "./Results", pFN, pasTable = TRUE){
    
    if (!dir.exists(pPath)) {
      dir.create(pPath)
    }
    
    if (missing(pFN) == TRUE) {
      pFN <- paste0("~", format(now(), "%Y%m%d-%H%M%S"), ".xlsx")
    }
    
    FFN <- file.path(pPath, pFN)
    openxlsx::write.xlsx(
      x          = pDT, 
      file       = FFN, 
      asTable    = pasTable, 
      tableStyle = "TableStyleMedium4") 
    openXL(FFN)
    
  }

fOpen_csv_in_xl <- 
  function(FFN){
    shell.exec(normalizePath(FFN))    
  }


fPath2Clip <- function(FileName) {
  l_path <- normalizePath(paste0(getwd(), "\\", FileName))
  writeClipboard(l_path)
}

fD3path<- function(FileName) {
  l_path <- normalizePath(FileName)
  writeClipboard(l_path)
}  

fNM <- 
  function(DT){
    cat(names(DT), sep = ", ")    
  }

fSC <- 
  function(x){
    paste0('"', names(x), '"', collapse = ', \n')                    %>%
      clipr::write_clip()
  }

fCatTab <- 
  function(x){
    cat(names(x), sep = ",\n")
  }

fShowFolder <- 
  function(folder){
    shell(
      cmd = paste0("explorer ", normalizePath(folder)),
      intern  = TRUE
    )
  }
