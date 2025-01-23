# Floris Padt
# 4-5-2015

#### Library ####
# install.packages("RDCOMClient", repos = "http://www.omegahat.net/R")
# http://www.omegahat.net/RDCOMClient/
#library(RDCOMClient, verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

library(knitr      , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
# library(kableExtra , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
#library(printr     , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE) # not available for 3.2.2
library("crayon"    , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

suppressMessages(
  library(bit64    , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE))
# library(readxl     , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE) # Still used in deployer
library(ggplot2    , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
# library(reshape2   , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE) #20220623
library(stringr    , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(tidyr      , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(lubridate  , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(glue       , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

library(magrittr   , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
# library(fst        , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE) #20220623
#library(RCurl      , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE) #obsolete
library(curl       , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
# library(readr      , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(tidyverse  , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(purrr      , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

library(data.table , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(DBI        , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

# library(fs         , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
# library(broom      , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

# library(openxlsx   , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE) 
library(openxlsx2  , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

library(arrow      , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)
library(duckdb     , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)


fPrependDateTime <- function(filename){
    filename %>%
    paste0(format(Sys.time(), "%Y%m%d%H%M%S"), "-", .)
}


# library(forecast)
# library(fpp)
# library(RODBC     , verbose = FALSE, quietly = TRUE, warn.conflicts = FALSE)

# ---- ReadingData ----
# fGetTable <- function(pTable, pKey, pSystID = "BA1", pClient = "200"){
#   
#   if(!missing(pSystID)){
#     l_Table <- paste0(pSystID, "C", pClient, "_", pTable )
#   } else {
#     l_Table <- pTable
#   }
# 
#   # Open COnnection to DataBase
#   A2R    <- odbcConnect("ACCESS2R")
#   
#   dtTABLE <- as.data.table(
#     sqlFetch(A2R, l_Table, 
#              stringsAsFactors = FALSE)
#   )
#   
#   # Set Key
#   if(!missing(pKey)){
#     setkeyv(dtTABLE, pKey)  
#   }
#   
#   # Get Attributes
#   if(!missing(pSystID)){
#     dfCMT <- sqlQuery(A2R, 
#                       paste("SELECT START, DTIME, RECORDCOUNT", 
#                             "FROM zsDD02_SLCT",
#                             "WHERE ((SYSTEMID=", 
#                             paste0("'", pSystID, "'"),  
#                             ") AND (CLIENT=", 
#                             pClient, ") AND (TABNAME=", 
#                             paste0("'", pTable , "'"), 
#                             "));")) 
#     
#     attr(dtTABLE, "RefreshDate")      <- dfCMT[["START"]]
#     attr(dtTABLE, "RecordsExtracted") <- dfCMT[["RECORDCOUNT"]]
#   }
#   close(A2R)
#   
#   return(dtTABLE)
# }


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


# ---- ReadingData ----
# Function to Read .txt file from specific System 
# ---- ReadingData ----
fGetEXPTable <- function(pTableName, pKey, pSystID, pClient = "300"){
  
  if (!missing(pSystID)) {
    l_Table <- paste0(pSystID, "C", pClient, "_", pTableName )
  } else {
    l_Table <- pTableName
  }
  
  # set Path
  cRAWDATA    <- file.path("C:", "Users", "floris", "SAPEXPORT")
  cEXT        <- "txt" 
  
  dtTABLE <- as.data.table(
    fread(file.path(cRAWDATA, paste(l_Table, cEXT, sep = ".")), 
          sep = ";",  
          colClasses = "character",  stringsAsFactors = FALSE)
  )
  
  # Set Key
  if (!missing(pKey)) {
    setkeyv(dtTABLE, pKey)  
  }
  
  # Replace Illegal field names
  oNAMES <- names(dtTABLE)[grep(pattern = "/", x = names(dtTABLE))]
  nNAMES <- sub(pattern = "/BIC/", "", x = oNAMES)
  setnames(dtTABLE, oNAMES, nNAMES)
  
  # Get Attributes
  setattr(dtTABLE, "RefreshDate",       
    file.mtime(file.path(cRAWDATA, paste(l_Table, cEXT, sep = "."))))
  setattr(dtTABLE, "FileSize",
    file.size(file.path(cRAWDATA, paste(l_Table, cEXT, sep = "."))))
  
  return(dtTABLE)
}

# Function to Read .txt file from specific System 
# ---- ReadingData ----
fGetOHDTable <- function(pTableName, pKey, pAddHeader = FALSE, 
                         pSystID, pClient){
  
  if (!missing(pSystID)) {
    l_Table <- paste0(pSystID, "C", pClient, "_", pTableName )
  } else {
    l_Table <- pTableName
  }
  
  # set Path
  cFTP       <- file.path("C:", "FTP")
  cEXT        <- "txt" 
  
  dtTABLE <- as.data.table(
    fread(file.path(cFTP, paste(l_Table, cEXT, sep = ".")), 
          sep = ";", header = !pAddHeader, 
          colClasses = "character",  stringsAsFactors = FALSE)
  )
  
  # Set Key
  if (!missing(pKey)) {
    setkeyv(dtTABLE, pKey)  
  }
  
  if (pAddHeader) {
    if (missing(pSystID)) {pSystID <- "BP1"}
    if (missing(pClient)) {pClient <- "300"}
    
    l_hdr <- fGetFieldNM(pTableName = pTableName, 
                         pSystID = pSystID, pClient = pClient)
    setnames(dtTABLE, l_hdr)
  }
  
  # Get Attributes
  attr(dtTABLE, "RefreshDate")      <- 
    file.mtime(file.path(cFTP, paste(l_Table, cEXT, sep = ".")))
  attr(dtTABLE, "FileSize")      <- 
    file.size(file.path(cFTP, paste(l_Table, cEXT, sep = ".")))
  
  return(dtTABLE)
}


fGetFieldNM <- function(
  pTableName, pSystID = "BP1", pClient = "300", pTYPE = "ALL" ){
  
  CSAPEXPORT <- file.path("C:", "SAPexport")
  cFTP       <- file.path("C:", "FTP")
  
  dtOHDEST   <- fread(paste(cFTP, "OHDEST.txt", sep = "/"))
  
  dtRSBOHFIELDS <- as.data.table(
    read.table(paste(CSAPEXPORT, 
                     paste0(pSystID, "C", pClient, "_", "RSBOHFIELDS.txt" ), 
                     sep = "/"),
               dec = ",", sep = ";", header = TRUE, 
               colClasses = c("NULL", "character", 
                              rep("character", 2), "numeric", 
                              rep("character", 3), rep("NULL", 11)))  )
  setkey(dtRSBOHFIELDS, "OHDEST", "POSIT")
  
  lOHDEST       <- dtOHDEST[TABLE == pTableName, OHDEST]
  dtRSBOHFIELDS <- dtRSBOHFIELDS[OHDEST == lOHDEST]
  
  vCHA <- c("CHAR", "CUKY", "UNIT", "DATS", "NUMC", "TIMS")
  vKYF <- c("QUAN", "CURR", "DEC")

  lFLDNM <- switch(
    pTYPE,
    ALL = dtRSBOHFIELDS[, FIELDNM],
    CHA = dtRSBOHFIELDS[DATATYPE %in% vCHA, FIELDNM],
    KYF = dtRSBOHFIELDS[DATATYPE %in% vKYF, FIELDNM])
  
  lFLDNM    <- sub("^/BIC/", "", lFLDNM)
  
  return(lFLDNM)
}

fGetKYF <- function(pTableName, pSystID = "BP1", pClient = "300"){
  
  CSAPEXPORT <- file.path("C:", "SAPexport")
  cFTP       <- file.path("C:", "FTP")
  
  dtOHDEST   <- fread(paste(cFTP, "OHDEST.txt", sep = "/"), sep = ";")
  
  dtRSBOHFIELDS <- as.data.table(
    read.table(paste(CSAPEXPORT, 
                     paste0(pSystID, "C", pClient, "_", "RSBOHFIELDS.txt" ), 
                     sep = "/"),
               dec = ",", sep = ";", header = TRUE, 
               colClasses = c("NULL", "character", 
                              rep("character", 2), "numeric", 
                              rep("character", 3), rep("NULL", 11))) )
  setkey(dtRSBOHFIELDS, "OHDEST", "POSIT")
  
  lOHDEST <- dtOHDEST[TABLE == pTableName]$OHDEST
  
  lDAT    <- dtRSBOHFIELDS[OHDEST == lOHDEST & DATATYPE == "CURR", FIELDNM ]
  lHDR    <- sub("^/BIC/", "", lHDR)
  
  return(lHDR)
}

# # setnames(dtMATPLT, old = names(dtMATPLT), new = fGetHeader(pTableName = "MAT_PLANT"))  
# fGetHeader <- function(pTableName){
#   
#   cRAWDATA      <- file.path(".", "data", "raw")
#   
#   dtOHDEST      <- fread(paste(cRAWDATA, "OHDEST.txt", sep = "/"))
#   
#   dtRSBOHFIELDS <- as.data.table(
#     read.table(paste(cRAWDATA, 
#                      "RSBOHFIELDS.txt", sep = "/"),
#                dec = ",", sep = ";", header = TRUE, 
#                colClasses = c("NULL", "character", 
#                               rep("NULL", 2), "numeric", 
#                               "character", rep("NULL", 13)))  )
#   setkey(dtRSBOHFIELDS, "OHDEST", "POSIT")
#   
#   lOHDEST <- dtOHDEST[TABLE == pTableName]$OHDEST
#   
#   lHDR    <- dtRSBOHFIELDS[OHDEST == lOHDEST, ]$TEMPIOBJNM
#   lHDR    <- sub("0", "", lHDR)
#   
#   return(lHDR)
# }

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


# ---- WriteResults ----
fWriteToSheet <- function(pData, pPath, pFileName, pSheetName, pAppend=FALSE){
  pFileName <- paste0(pPath, "/", pFileName, ".xlsx")
  if (nrow(pData) > 0) {
    write.xlsx(pData, 
               file = pFileName, sheetName = pSheetName, 
               col.names = TRUE, row.names = FALSE, 
               append = pAppend, showNA = TRUE)
  }
}

# --- GetSAPSite
fGetSAPSite <- function(
  pLegacySites, pEANTYP, 
  pBS = "BA1", pBC = "200",
  pES = "RA1", pEC = "250"){
  
  # check if data is available else load it
  if (!exists("dtWERKS")) {
    
    dtPLANT     <- fGetTable(pTable = "/BI0/PPLANT",
                             pKey = "PLANT",
                             pSystID = pBS, pClient = pBC)
    
    dtT001W     <- fGetTable(pTable = "T001W", pKey = c("WERKS"),
                             pSystID = pES, pClient = pEC)
    
    dtADRC      <- fGetTable(pTable = "ADRC", 
                             pSystID = pES, pClient = pEC)
    setnames(dtADRC, "ADDRNUMBER", "ADRNR")
    
    dtWERKS  <- merge(dtT001W[ , .(ADRNR, WERKS, VKORG, VTWEG, VLFKZ)], 
                      dtADRC[  , .(ADRNR, SORT2)],
                      all.x = TRUE, by = "ADRNR")
    
    dtTMP01  <- dtWERKS[ is.na(SORT2)][, SORT2 := NULL]
    dtWERKS  <- dtWERKS[!is.na(SORT2)]
    
    if (pEANTYP == "Z2") {
      dtWERKS  <- dtWERKS[substr(VKORG, 1, 2) %in% c("GB", "IE")]
    } else {
      if (pEANTYP == "Z1") {
        dtWERKS  <- dtWERKS[substr(VKORG, 1, 2) %in% c("NL", "BE")]
      }
    }    
#     setnames(dtWERKS, c("SORT2"), c("LWRKS"))
    
    # Quality check on Duplicates
    dtWERKS  <- dtWERKS[, DUP := duplicated(dtWERKS, 
                                           by = c("SORT2", "VKORG"))]
    
    dtTMP02  <- copy(dtWERKS)
    dtTMP02  <- dtTMP02[, DUP := any(DUP), 
                        by = c("SORT2", "VKORG") ]
    dtTMP02  <- dtTMP02[DUP == TRUE ]
#     fWriteToSheet(dtTMP02, 
#                   pPath, pXLSX, "WERKS_DUP", pAppend = TRUE )
#     fWriteToSheet(dtWERKS[DUP == TRUE], 
#                   pPath, pXLSX, "LWRKS_DEL", pAppend = TRUE )
    
    dtWERKS  <- dtWERKS[DUP == FALSE][, DUP := NULL]
    
    # Information on closed stores
    dtCWRKS  <- fread(file.path(".", "RAW_DATA", "CWRKS.csv"), 
                         sep = ";")
    setkey(dtCWRKS, "WERKS")
    dtCWRKS <- dtCWRKS[EANTYP == pEANTYP]
    
    # Add Sales Org from list which include the closed Stores
    dtCWRKS  <- dtCWRKS[, SORT2 := substr(WERKS, 2,4)]
    setnames(dtCWRKS, c("WERKS"), c("LWRKS"))
    setnames(dtWERKS, 
             c("VKORG"   , "VTWEG"),
             c("SALESORG", "DISTR_CHAN"))

    setnames(dtPLANT,
             c("PLANT", "/BIC/G1LGSTNO"),
             c("WERKS", "LWRKS"))
    dtPLANT <- dtPLANT[substr(WERKS, 1, 1) == "H", 
                       .(WERKS, SALESORG, DISTR_CHAN, LWRKS)]
    dtPLANT[, SORT2 := substr(LWRKS, 2, 4)]

    # Create List of Dummy stores which are not used
    l_DUMMIES_USED <- dtPLANT$WERKS
    l_DUMMIES_FREE <- setdiff(paste0(pLGCINDC, str_pad(1:1000, 3, pad = "0")),
                              l_DUMMIES_USED )

    dtCWRKS <- dtCWRKS[SORT2 %in% setdiff(dtCWRKS$SORT2, dtWERKS$SORT2)]
    dtCWRKS <- dtCWRKS[LWRKS %in% pLegacySites]
    dtCWRKS <- dtCWRKS[, WERKS := l_DUMMIES_FREE[1:nrow(dtCWRKS)]]

    dtWERKS <- dtWERKS[, LWRKS := NA]
    dtWERKS <- rbind(dtWERKS[, .(WERKS, SALESORG, DISTR_CHAN, SORT2, LWRKS)],
                     dtCWRKS[, .(WERKS, SALESORG, DISTR_CHAN, SORT2, LWRKS)],
                     dtPLANT[, .(WERKS, SALESORG, DISTR_CHAN, SORT2, LWRKS)])

 
# dtWERKS   <- merge(dtWERKS, 
#                    dtCWRKS, 
#                    all.y = TRUE, by = "SORT2" )
# dtORG     <- dtWERKS[, .N, by =.(VKORG, SALESORG)]
dtWERKS   <- dtWERKS[, .(WERKS, LWRKS, SALESORG, DISTR_CHAN)]
    
    #     fWriteToSheet(dtTMP01, 
#                   pPath, pXLSX, "WERKS_NO_SORT2", pAppend = TRUE )
    
 
  }

}

fGetTableDAP <- 
  function(pTable, pQAR_OBJECT, 
           pLOGSYSTEMS = c("BD1C100", "BA1C200", "BP1C300")){
 
    # Open COnnection to DataBase
    A2R    <- odbcConnect("ACCESS2R")
    
    for (pLOGSYS in pLOGSYSTEMS) {
      l_Table <- paste0(pLOGSYS, "_", pTable )
      
      dtTABLE <- as.data.table(
        sqlFetch(A2R, l_Table, 
                 stringsAsFactors = FALSE)
      )
      
      if (!exists("dtRETURN")) {
        dtRETURN <- dtTABLE
      } else {
        dtRETURN <- rbind(dtRETURN, dtTABLE)
      }
    }
  
  close(A2R)
  
  dtRETURN[, QAR_OBJ := pQAR_OBJECT]
  
  return(dtRETURN)
}

fGetOverviewDAP <- 
  function(){
    
    dtDAP        <- fGetTable(pTable = "tblALIGN")

    for (i in 1:nrow(dtDAP)) {

      dtTABLE <- fGetTableDAP(dtDAP[i]$TABNAME, dtDAP[i]$Comment)
      dtTABLE  <- dtTABLE[, .(SYSTID, QAR_OBJ)]
      
      if (!exists("dtRETURN")) {
        dtRETURN <- dtTABLE
      } else {
        dtRETURN <- rbind(dtRETURN, dtTABLE)
      }
    }
    
    return(dtRETURN)
  }

# --- GetTRFNMAP

fGetTRFNMAP <- function(pTable) {
  
  # Transformation
  dtRSTRAN  <- fGetEXPTable(pTableName = "RSTRAN", pKey    = "TRANID",
                            pSystID    = "BP1"   , pClient = "300" ) 
  
  # Transformation Fields
  dtRSTRANFIELD  <- fGetEXPTable(pTableName = "RSTRANFIELD", pKey    = "TRANID", 
                                 pSystID    = "BP1"        , pClient = "300" ) 
  
  dtTRFN <- merge(dtRSTRAN, dtRSTRANFIELD, 
                  by = c("SYSTID", "OBJVERS", "TRANID"))
  dtTRFN[, `:=`(RULEPOSIT  = as.integer(sub(",00", "", RULEPOSIT)),
                RULEID     = as.integer(RULEID))]
  setkey(dtTRFN, TRANID, RULEID )
  
  dtTRFN <- dtTRFN[TARGETTYPE      == "IOBJ" & 
                     TARGETSUBTYPE == "ATTR" & 
                     TARGETNAME    == pTable,
                   .(FIELD = paste(FIELDNM, collapse = ";")), 
                   by = .(TRANID, RULEID, FIELDTYPE)]
  
  dtMAPPING <- merge(dtTRFN[FIELDTYPE == "F", 
                            c("TRANID", "RULEID", "FIELD" ), with = FALSE], 
                     dtTRFN[FIELDTYPE == "I", 
                            c("TRANID", "RULEID", "FIELD" ), with = FALSE], 
                     by = c("TRANID", "RULEID"), all = TRUE)
  setnames(dtMAPPING, c("FIELD.x", "FIELD.y"), c("FROM", "ATTRINM"))
  
  return(dtMAPPING) 
}

fGetDD <- function(pTable){
  # DD for the following tables DRAW, EINA, EINE, MAKT, MALG, MAPR, MARA, 
  # MARC, MARM, MAW1, MBEW, MEAN, MLGN, MVKE, PROP, WLK2, WRPL

  # DD03M delivers the field text and more
  dtDD03M <- as.data.table(
    read_excel("./10_RawData/DD03M.XLSX", sheet = "Sheet1", 
               col_names = TRUE, col_types = NULL, na = "",
               skip = 0))
  dtDD03M <- dtDD03M[, POSITION := as.integer(POSITION)]
  
  # Texttable and CheckTable
  dtDD30V <- as.data.table(
    read_excel("./10_RawData/DD30V.XLSX", sheet = "Sheet1", 
               col_names = TRUE, col_types = rep("text", 20), na = "",
               skip = 0))
  dtDD30V <- unique(dtDD30V[, .(SELMETHOD, TEXTTAB)])
  dtDD30V[, `:=`(CHECKTABLE = SELMETHOD)]
  
  dtDD30V <- dtDD30V[,
                     .(TXTTAB = paste(TEXTTAB, collapse = ";")), 
                     by = .(CHECKTABLE)]
  
  dtDD30V <- dtDD30V[!TXTTAB == "NA"]
  
  dtDD03M <- merge(dtDD03M, dtDD30V, by = "CHECKTABLE", all.x = TRUE)
  dtDD03M <- dtDD03M[, .(TABNAME, FIELDNAME, CHECKTABLE, TXTTAB, POSITION, 
                         DATATYPE, LENG, INTLEN, INTTYPE, DECIMALS, 
                         DDTEXT, REPTEXT, SCRTEXT_S, SCRTEXT_M, 
                         SCRTEXT_L, LOWERCASE)]
  setkey(dtDD03M, TABNAME, POSITION)
  
  if (!missing(pTable)) {
    dtDD03M <- dtDD03M[TABNAME == pTable]
  }
  return(dtDD03M)
}

fGetAP245 <- function(pTable){
  
  dtAP245 <- as.data.table(
    read_excel("./10_RawData/AP245_ART.XLSX", sheet = "Sheet1", 
               col_names = TRUE, col_types = rep("text", 52), na = "",
               skip = 0))
  
  setnames(dtAP245, 
           c(2, 3, 4, 49, 50, 51, 52), 
           c("DDTEXT", "TABNAME", "FIELDNAME", "BI", "TXT", "TYPE", "CMT"))
  return(dtAP245[BI == "Y", 
                 .(TABNAME, FIELDNAME, DDTEXT, BI, TXT, TYPE, CMT)])
}

fGetZipTable <- function(pFullNameZip, pTable){
  dtTable <- as.data.table(
    read.table(unz(pFullNameZip, pTable), 
               nrows = -1, header = T, quote = "\"", sep = ";")
  )
  
  return(dtTable)
}


fValidCheckDigit <- function(pEAN){

  fIsValidEAN <- function(x){
    
    x <- lapply(x, as.integer)
    iTMP <- with(x, 
                 3*(C1 + C3 + C5 + C7 + C9  + C11 + C13) + 
                   (C2 + C4 + C6 + C8 + C10 + C12)
    ) 
    
    iTMP   <- 10 * ((iTMP %/% 10) + 1) - iTMP
    fValid <- iTMP %% 10 == x$C14
    
    return(fValid)
  }
  
  # Make length 14 by adding leading zero's
  pEAN  <- str_pad(string = pEAN, width = 14, side = "left", pad = "0")
  dtTMP <- data.table(EAN = pEAN)
  dtTMP[, c(paste0("C", 1:14)) := (tstrsplit(EAN, "", fixed = TRUE))]
  dtTMP[, V := fIsValidEAN(.SD), .SDcols = paste0("C", 1:14)]

  return(dtTMP$V)
}

#' Check Mean
#'
#' @param pEAN 
#'
#' @return data.table with EAN and RC codes
#' RC 0 - Valid EAN
#' RC 5 - Non-numeric EAN
#' RC 6 - Too short for EAN, length < 8
#' RC 7 - Too long for EAN , length > 14
#' RC 8 - Check Digit not valid 
#' @export
#'
#' @examples
fChkEAN <- function(pEAN){
  
  dtRC <-
    data.table(
      RC       = 0L:8L,
      ERR_DESC = c(
        "Valid EAN",
        "Not Used",
        "Not Used",
        "Not Used",
        "Not Used",
        "EAN Contains Characters",
        "Too short for EAN, length <  8",
        "Too long for EAN , length > 14",
        "Check Digit not valid"
      ))
  
  if (!class(pEAN) == "character") {
    return(pEAN)
  }
  
  # Create data.table with all EAN with RC = 0 ( being Valid)
  dtEAN <- data.table(EAN = numeric(0), EANT = numeric(0),
                      RC = numeric(0) , LENGTH = numeric(0))
  dtTMP <- data.table(EAN = pEAN, EANT = pEAN,
                      RC = 0, LENGTH = nchar(pEAN))
  
  # Check if EAN is Numeric, non-numeric = RC 5
  dtTMP[grepl(pattern = "[^[0-9]]*", x = EAN), 
        `:=`(RC = 5, LENGTH = nchar(EAN))]
  dtEAN <- rbind(dtEAN, dtTMP[RC != 0])
  dtTMP <- dtTMP[RC == 0]
  
  if (nrow(dtTMP) > 0) {
    # Check Length of trimmed EAN, without leading/trailing spaces, leading 0
    dtTMP[, EANT := gsub(pattern = "^0*", 
                        replacement = "", 
                        x = str_trim(string = EAN, side = "both"))]
    dtTMP[, LENGTH := nchar(EANT)]
    # Too Short, RC 6
    dtTMP[LENGTH < 8,  RC := 6] 
    dtEAN <- rbind(dtEAN, dtTMP[RC != 0])
    dtTMP <- dtTMP[RC == 0]
  }
  
  if (nrow(dtTMP) > 0) {  
    # Too Long, RC 7
    dtTMP[LENGTH > 14, RC := 7] 
    dtEAN <- rbind(dtEAN, dtTMP[RC != 0])
    dtTMP <- dtTMP[RC == 0]
  }
  
  if (nrow(dtTMP) > 0) {
    # Check the check Digit, invalid = RC 8
    dtTMP[!fValidCheckDigit(EANT), 
          `:=`(RC = 8, LENGTH = nchar(EANT))] 
    dtEAN <- rbind(dtEAN, dtTMP)
  }
  
  dtEAN <- dtRC[dtEAN, on = .(RC)]
  
  return(dtEAN)
}

fRemoveFieldNamePart <- function(pTable, pPart = "^/BIC/"){
  
  setnames(pTable, names(pTable), sub(pattern = pPart, "", x = names(pTable)))

  return(pTable)
}

fGetDS <- 
  function(pSystID = "BP1", pClient = "300"){

    sidclient <- paste0(pSystID, pClient)
      
    # DataSource Meta data
    RSDSSEG  <- 
      fRead_and_Union(
        pSIDCLNT = paste0(pSystID, pClient),    
        pTable   = "RSDSSEG",
        pOptions = list("OBJVERS = 'A'"))
    
    RSDSSEGT  <- 
      fRead_and_Union(
        pSIDCLNT = paste0(pSystID, pClient),    
        pTable   = "RSDSSEGT",
        pOptions = list("OBJVERS = 'A'", "AND", 
                        "LANGU = 'EN'"))
    
    RES <- 
      RSDSSEGT[RSDSSEG, on = .(DATASOURCE, LOGSYS, OBJVERS, SEGID)] %T>%
      fOpen_as_xlsx()
    
    RES
    
  }

fGetDSFields <- 
  function(pSystID = "BP1", pClient = "300"){
    
    sidclient <- paste0(pSystID, pClient)
    
    # DataSource Meta data
    RSDSSEGFD  <- 
      fRead_and_Union(
        pSIDCLNT = paste0(pSystID, pClient),    
        pTable   = "RSDSSEGFD",
        pOptions = list("OBJVERS = 'A'"))
    
    RSDSSEGFDT  <- 
      fRead_and_Union(
        pSIDCLNT = paste0(pSystID, pClient),    
        pTable   = "RSDSSEGFDT",
        pOptions = list("OBJVERS = 'A'", "AND", 
                        "LANGU = 'EN'"))
    
    RES <- 
      RSDSSEGFDT[RSDSSEGFD, on = .(SYSTID , CLIENT, DATASOURCE, LOGSYS, 
                                   OBJVERS, SEGID , POSIT)] %T>%
      fOpen_as_xlsx()
    
    RES
    
  }

# fGetDSfields <- function(pSystID = "BP1", pClient = "300"){
#   
# 
#   dtRSDSSEGFDT <- fGetEXPTable(pTable  = "RSDSSEGFDT", 
#                                pKey    = c("DATASOURCE", "POSIT"), 
#                                pSystID = pSystID, pClient = pClient)
#   dtRSDSSEGFDT <- dtRSDSSEGFDT[LANGU == "E", .(DATASOURCE, SEGID, POSIT, TXTLG)]
#   
#   # Get Datasource fields in the right order
#   dtDS   <- dtRSDSSEGFDT[dtRSDSSEGFD, on = c("DATASOURCE", "SEGID", "POSIT") ]
#   dtDS   <- dtDS[, .(SYSTID , DATASOURCE, POSIT, 
#                      FIELDNM, DATATYPE  , DECIMALS, IOBJNM, TXTLG)]
#   rm(dtRSDSSEGFD, dtRSDSSEGFDT)
#   
#   # Replace Illegal field names and fix the order
#   dtDS     <- dtDS[, FIELDNM := sub(pattern = "/BIC/", "", x = FIELDNM) ]
#   
#   # Order by Datasource and Position
#   dtDS         <- dtDS[, POSIT := as.numeric(
#     sub(pattern = ",", replacement = ".", x = POSIT, fixed = TRUE))] 
#   setkey(dtDS, "DATASOURCE", "POSIT")
#   
# 
#   return(dtDS)
# }

fAlignDS <- function(pDT, pDS, pSystID = "BP1", pClient = "300"){
  
  dtDATA <- pDT
  
  # Get All DS fields
  dtDS  <- fGetDS(pSystID, pClient)
  vDSFD <- dtDS[DATASOURCE == pDS, FIELDNM]
  
  # Add Missing Fields, make those NA
  vAFD  <- setdiff(vDSFD, names(dtDATA)) 
  if (length(vAFD > 0)) {dtDATA[, (vAFD) := NA]}
  
  #Restrict fields to DS fields in the right order
  dtDATA <- dtDATA[, vDSFD, with = FALSE] #

  return(dtDATA)
  
}

# fOpen_in_Excel <- 
#   function(pDT, pPath = "./Results", pFN){
#     
#     if (!dir.exists(pPath)) {
#       dir.create(pPath)
#     }
#     
#     if (missing(pFN) == TRUE) {
#       pFN <- paste0("~", format(now(), "%Y%m%d-%H%M%S"), ".csv")
#     }
#     
#     pFFN <- file.path(pPath, pFN)
#     write.table(
#       x = pDT,
#       file = pFFN, 
#       sep = ";", row.names = F, quote = T)
#     shell.exec(normalizePath(pFFN))    
#   }

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

# fOpen_as_xlsx <- 
#   function(pDT, pPath = "./Results", pFN, pasTable = TRUE){
#     
#     if (!dir.exists(pPath)) {
#       dir.create(pPath)
#     }
#     
#     if (missing(pFN) == TRUE) {
#       pFN <- paste0("~", format(now(), "%Y%m%d-%H%M%S"), ".xlsx")
#     }
#     
#     # http://www.omegahat.net/RDCOMClient/Docs/introduction.html
#     # To access a method, we use the $ operator on the COM object.
#     # To access a DCOM property, the [[ operator with the name of the property.
#     
#     FFN <- file.path(pPath, pFN)
#     write.xlsx(x = pDT, file = FFN, asTable = pasTable)
#     shell.exec(normalizePath(FFN))
#         
#     # ## init com api
#     # ExlApp   <- COMCreate("Excel.Application")
#     # ExlApp[["Visible"]] = TRUE
#     # 
#     # wkbNew   <- ExlApp$workbooks()$Add()
#     # shtDat   <- wkbNew$Worksheets()$item(1)
#     # 
#     # # wkbNew$Queries()$Add(
#     # #   Name:="Article", 
#     # #   Formula:= "let" & Chr(13) & "" & Chr(10) & "    
#     # #   Source = Csv.Document(File.Contents(""C:\Users\fpadt\OneDrive - GrandVision\Documents\Article.csv""),[Delimiter="";"", Columns=2, Encoding=1252, QuoteStyle=QuoteStyle.Csv])," & Chr(13) & "" & Chr(10) & "    #""Changed Type"" = Table.TransformColumnTypes(Source,{{""Column1"", type text}, {""Column2"", type text}})" & Chr(13) & "" & Chr(10) & "in" & Chr(13) & "" & Chr(10) & "    #""Changed Type""")
#     # 
#     # n = nrow(pDT)
#     # B = ncol(pDT)
#     # 
#     # rng <-  shtDat$Range(shtDat$Cells(1, 1), shtDat$Cells(1, B))
#     # rng[["Value"]] = names(pDT)
#     # for (i in 1:B) {
#     #   rng <-  shtDat$Range(shtDat$Cells(2, i), shtDat$Cells(2 + n - 1, i))
#     #   rng[["Value"]] = asCOMArray(pDT[, i, with = FALSE])
#     # }
#     # 
#     # act <- ExlApp$ActiveWindow()
#     # act[["DisplayGridlines"]] = FALSE
#     # 
#     # 
#     # wkbNew$saveas(file.path(pPath, pFN)) 
#   }

sdate2rdate <- 
  function(pSAP_DATE) {
    # pSAP_DATE <- gsub("-", "", pSAP_DATE)
    
    make_date(
      year  = substr(pSAP_DATE, 1, 4), 
      month = substr(pSAP_DATE, 5, 6), 
      day   = substr(pSAP_DATE, 7, 8))}

fGetSAPUsers <- 
  function(pSIDCLNT, pEnv, pType){
    
    if (missing(pSIDCLNT)) {   
      pSID.lng <- fGetSID(pEnv, pType)
    } else {
      pSID.lng <- pSIDCLNT
    }
    
    # Logon Language
    USR01 <- 
      fRead_and_Union(
        pSIDCLNT = pSID.lng, 
        pTable   = "USR01",
        pFields  = list("BNAME", "LANGU"))
    
    # User Id, last logon, validity period and status
    USR02 <- 
      fRead_and_Union(
        pSIDCLNT = pSID.lng, 
        pTable   = "USR02", 
        pOptions = list(),
        pFields  = list(
          'ANAME'   , 'BNAME'     , 'ERDAT', 'CODVN',
          'TRDAT'   , 'LTIME'     , 'GLTGV', 'GLTGB', 
          'USTYP'   , 'CLASS'     , 'LOCNT', 'UFLAG',
          'PWDSTATE', 'PWDINITIAL', 'PWDLOCKDATE'))
    
    USR06 <- 
      fRead_and_Union(
        pSIDCLNT = pSID.lng, 
        pTable   = "TUTYP",
        pFields  = list("USERTYP", "UTYPTEXT"),
        pOptions = list("LANGU = 'E'")) %>% 
      .[fRead_and_Union(
        pSIDCLNT = pSID.lng, 
        pTable   = "USR06",
        pFields  = list("BNAME", "LIC_TYPE")),
        on = .(SYSTID, CLIENT, USERTYP == LIC_TYPE)]
    
    
    # PersNumber
    USR21 <- 
      fRead_and_Union(
        pSIDCLNT = pSID.lng, 
        pTable   = "USR21") %>%
      .[, MANDT := NULL]
    
    # create a filter on PERSUMBER to reduce extraction time of ADR6
    pOptions <- 
      copy(USR21[ , .(PERSNUMBER)])                             %>%
      .[ , PERSNUMBER := str_pad(string = PERSNUMBER,
                                 width  = 10,
                                 side   = "left",
                                 pad    = "0")]                 %>%
      .[, PERSNUMBER]                                           %>%
      f_or(x = . , field = "PERSNUMBER")             
    
    # Email
    ADR6 <- 
      fRead_and_Union(
        pSIDCLNT = pSID.lng, 
        pTable   = "ADR6",
        pOptions = pOptions, 
        pFields  = list("PERSNUMBER", "ADDRNUMBER", "SMTP_ADDR"))
    
    # User Name
    ADRP <-
      fRead_and_Union(
        pSIDCLNT = pSID.lng, 
        
        pTable   = "ADRP",
        pOptions = pOptions, 
        pFields  = list("PERSNUMBER", "NAME_FIRST", "NAME_LAST", "NAME_TEXT"))
    
    # convert SAP date to R-Date
    idx_col <- c("TRDAT", "GLTGB")
    USR02 <-
      USR02                                                    %>%
      # .[, (idx_col) := map(.SD, sdate2rdate),
      #   .SDcols = idx_col]                                   %>%
      .[, LDAYS := {(today() - TRDAT)      %>% as.integer()}]  %>%
      .[, CDAYS := {(today() - ymd(ERDAT)) %>% as.integer()}]
    
    dtUSR_lng <- 
      ADRP                                                     %>%
      .[USR21, 
        on = .(SYSTID, CLIENT, PERSNUMBER)]                    %>%
      .[ADR6, 
        on = .(SYSTID, CLIENT, PERSNUMBER, ADDRNUMBER),
        nomatch = 0]                                           %>%
      .[, .(SYSTID, CLIENT, BNAME, SMTP_ADDR, 
            NAME_FIRST, NAME_LAST, NAME_TEXT)]                 %>%
      .[USR01, on = .(SYSTID, CLIENT, BNAME), 
        nomatch = NA]                                          %>%
      .[USR02, on = .(SYSTID, CLIENT, BNAME), 
        nomatch = NA]                                          %>%
      USR06[., on = .(SYSTID, CLIENT, BNAME), 
            nomatch = NA]    %>%
      .[, UFLAG:= trimws(UFLAG)]                               %>%
      .[, USTATE := ifelse(
        GLTGB >= today() | is.na(GLTGB), 
        "VALID", "INVALID") ]                %>%
      .[USTATE == "VALID",
        USTATE := fcase(
          UFLAG == "0"  , "VALID",     # not locked
          UFLAG == "128", "ULOCK",     # password lock
          default = "SLOCK") ]                                 %>%
      .[, USR_GLTGB:= GLTGB]                                   %>%
      .[, GLTGV:= as.character(GLTGV)]                         %>%
      .[, GLTGB:= as.character(GLTGB)]                         %>%
      .[, ERDAT:= as.character(ERDAT)]                         %>%         
      .[, TRDAT:= as.character(TRDAT)]                        %T>%      
      # setnames(c("GLTGB"), c("USR_GLTGB"))                  %T>%
      setcolorder(
        c("SYSTID"    , "CLIENT"    , "BNAME"      , 
          "SMTP_ADDR" ,
          "NAME_FIRST", "NAME_LAST" , "NAME_TEXT"  ,    
          "LANGU"     , "USERTYP"   , "UTYPTEXT"   ,
          "ANAME"     , "ERDAT"     ,
          "TRDAT"     , "LTIME"     ,       
          "GLTGV"     , "GLTGB"     , "USTYP"      ,
          "CLASS"     , "LOCNT"     , "UFLAG"      ,
          "PWDSTATE"  , "PWDINITIAL", "PWDLOCKDATE",
          "CDAYS"     , "LDAYS"     , 
          "USTATE"    , "USR_GLTGB" )
      ) %T>%
      setkey(BNAME)
    
    return(dtUSR_lng)
  }

fPath2Clip <- function(FileName) {
  l_path <- normalizePath(paste0(getwd(), "\\", FileName))
  writeClipboard(l_path)
}

fD3path<- function(FileName) {
  l_path <- normalizePath(FileName)
  writeClipboard(l_path)
}  

f_teams_fullname <- 
  function(filename, channel){
    f_teams_filenames(filename, channel)["fullname"]
  }

f_teams_archive  <- 
  function(filename, channel){
    f_teams_filenames(filename, channel)["archive"]
  }

f_teams_filenames <- 
  function(filename, channel){

    l_sync_base <- "C:\\users\\floris.padt\\GrandVision" 
    l_fullname  <- paste0(l_sync_base, "\\", channel, "\\", filename)
    l_archive   <- 
      paste0(
        l_sync_base, "\\", channel, "\\", "archive", "\\",
        format(now(), "%Y%m%d-%H%M%S"), " - ",
        "WK", 
        str_pad(string = isoweek(today()),
                width  = 2,
                side   = "left",
                pad    = "0")
        , "_", filename, sep = "")

    c(fullname = l_fullname, archive = l_archive)
  }

# copy ohd header and data to folder 10-raw
f_download_ohd <- 
  function(ohd_name, srcdir){
    
    f_download_ohd_part <-
      function(fn, srcdir, tgtdir, ext = "csv"){
        
        fn <- paste0(fn, ".", ext)
        
        curl_download(
          url      = file.path(srcdir, fn),  
          destfile = file.path(tgtdir, fn))    
        
        file.exists(file.path(tgtdir, fn))
      }
    
    tgtdir <- "10-raw"
    if (!dir.exists(tgtdir)){ dir.create(tgtdir)}
    
    # download Header
    f_download_ohd_part(
      fn = paste0("S_", ohd_name), 
      srcdir = srcdir,  tgtdir = tgtdir, ext = "csv")
    
    # download data    
    f_download_ohd_part(
      fn = ohd_name, 
      srcdir = srcdir,  tgtdir = tgtdir, ext = "csv")
    
  }

# create data table from 10-raw downloaded OHD data 2 files
f_read_ohd <- 
  function(ohd_name, sep = "|", as_char = TRUE){
    
    
    f_get_ohdhdr <- 
      function(fn, srcdir, sep){
        
        fn <- paste0("S_", fn, ".csv")
        fread(file = file.path(srcdir, fn), 
              header = T, skip = 5, sep = sep) %>%
          .[, FIELDNM:= 
              sub(pattern = "/BIC/", replacement = "", x = FIELDNAME)] %>%
          .[, "FIELDNM"]     
      }
    
    hdr <- f_get_ohdhdr(fn = ohd_name, srcdir = file.path("10-raw"), sep = sep)
    
    if (as_char == TRUE) {
      fread(file = file.path("10-raw", paste0(ohd_name, ".csv")),
            header = F, col.names = hdr$FIELDNM, sep = sep,
            colClasses = "character") 
    } else {
      fread(file = file.path("10-raw", paste0(ohd_name, ".csv")),
            header = F, col.names = hdr$FIELDNM, sep = sep)               
    }
    
  }  



# EcoTone -----------------------------------------------------------------

fGetLatestFile <- function(pPATTERN, pDIR) {
  ldir <-  normalizePath(pDIR)
  
  finf <- file.info(dir(
    path = ldir, pattern = pPATTERN, 
    full.names = TRUE), extra_cols = FALSE)
  
  finf$FULLNAME <- row.names(finf)
  
  finf <- as.data.table(finf)
  cat(finf[ mtime == max(mtime), FULLNAME])
  return(finf[ mtime == max(mtime), FULLNAME])
}

fNM <- 
  function(DT){
    cat(names(DT), sep = ", ")    
  }


# Remove Leading 0
RL0 <- 
  function(x){
    # like CONVERSION_EXIT_MATN1_INPUT
    # only remove leading zero's in case it is a number
    is_num <- grepl("^[0-9]+$", x)
    ifelse(
      is_num, 
      sub("^0*", "", x, perl = TRUE),
      x
    )
  }

LTRIM <- RL0

# Left Pad 0 - Leading Zero
LP0 <- 
  function(x, width){
    # like CONVERSION_EXIT_MATN1_INPUT
    # only add leading zero's in case it is a number
    is_num <- grepl("^[0-9]+$", x)
    ifelse(
      is_num, 
      stringr::str_pad(string = x, width = width, side = "left", pad = "0"),
      x
    )
  }

# MATN1 <- 
#   function(x){
#     LP0(x, 18)
#   }

fTC <- 
  function(x, SID = EBW, REC = "Inf", CMT = "# "){
    
    x <- toupper(x)
    paste0(
      '# ', fGetTableDescription(x, SID), ' #### \n',
      fReadableTableName(x, SID),
     ' <- \n',
     '   fRead_and_Union(\n',
     '     pSIDCLNT  = "', SID  , '",\n',
     '     pTable    = "', x    , '",\n',
     '     pOptions  = list()' ,  ',\n',
     '     pFields   = list(\n',
     paste0(fGetTableFields(x, SID, CMT)), '\n',
     '    )', ',\n',
     '     pRowcount = ', REC, '\n',
     '   )'
      )                                          %>%
      clipr::write_clip()
  }

fSC <- 
  function(x){
    paste0('"', names(x), '"', collapse = ', \n')                    %>%
      clipr::write_clip()
  }

fReadableTableName <- 
  function(x, SID){
    
    prefix <- ifelse(grepl("E", SID), "S4_", "B4_")
    
    bwind <- ""
    if(grepl("^/BI[0C]/", x = x)){
      bwind <- paste("", substr(x, 6, 6), sep = "_")
    }
    
    paste0(
      prefix,
      sub("^/BI[0C]/[7AHIKMPRSTXZ]", replacement = "", x = x),
      bwind
      )
  }

fGetTableDescription <- 
  function(x, SID = WPB ){

    DD02T <-
      fRead_and_Union(
        pSIDCLNT  = SID,
        pTable    = "DD02T",
        pOptions  = list("DDLANGUAGE = 'E'", "AND",
                         paste0("TABNAME = '", x, "'")),
        pFields   = list(
          "TABNAME", "DDLANGUAGE", "AS4LOCAL", "DDTEXT"
        ),
        pRowcount = Inf
      )                                                                   %>%
      .[, DDTEXT]
  }

fGetTableFields <- 
  function(x, SID = WPB, CMT = "# " ){

    DD03L <-
      fRead_and_Union(
        pSIDCLNT  = SID,
        pTable    = "DD03L",
        pOptions  = list(paste0("TABNAME = '", x, "'")),
        pFields   = list(
          "TABNAME" , "FIELDNAME", "AS4LOCAL", "AS4VERS", "POSITION", 
          "ROLLNAME", "KEYFLAG" 
        ),
        pRowcount = Inf
      )                                                                   %T>%
      setorder(POSITION)
    
    DD03T <-
      fRead_and_Union(
        pSIDCLNT  = SID,
        pTable    = "DD03T",
        pOptions  = list("DDLANGUAGE = 'E'", "AND",
                         paste0("TABNAME = '", x, "'")),
        pFields   = list(
          "TABNAME", "DDLANGUAGE", "AS4LOCAL", "FIELDNAME", "DDTEXT"
        ),
        pRowcount = Inf
      ) 
    
    OPT <- f_or(DD03L$ROLLNAME, "ROLLNAME")
    DD04T <-
      fRead_and_Union(
        pSIDCLNT  = SID,
        pTable    = "DD04T",
        pOptions  = OPT,
        pFields   = list(
          "ROLLNAME", "DDLANGUAGE", "AS4LOCAL", "AS4VERS", "DDTEXT"
        ),
        pRowcount = Inf
      ) 
    
    if(!is.null(DD04T)){
      DD04T <- DD04T[DDLANGUAGE == "E"]
    } 

    RET <- list()
    if (!is.null(DD03T)){
      RET[[1]] <- 
        DD03T[DD03L, 
              on= .(SYSTID, CLIENT, TABNAME, AS4LOCAL, FIELDNAME)]        %>%
        .[, .(SYSTID, CLIENT, TABNAME, FIELDNAME,
              POSITION, DDTEXT, KEYFLAG)]        
    }
    
    if (!is.null(DD04T)){
      RET[[2]] <- 
        DD04T[DD03L, 
              on= .(SYSTID, CLIENT, ROLLNAME, AS4LOCAL, AS4VERS)]         %>%
        .[, .(SYSTID, CLIENT, TABNAME, FIELDNAME,
              POSITION, DDTEXT, KEYFLAG)]
    }  
      
    rbindlist(RET)                                                        %>%
      unique()                                                            %>% 
      .[, .(FIELDNAME, DDTEXT, KEYFLAG)]                                  %>%
      .[, `:=` (
        SPC1 = max(nchar(FIELDNAME) , na.rm = TRUE) + 2 -
          ifelse(is.na(FIELDNAME)   , 0, nchar(FIELDNAME)),
        SPC2 = max(nchar(DDTEXT)    , na.rm = TRUE) + 2 -
          ifelse(is.na(DDTEXT)      , 0, nchar(DDTEXT))
      )]                                                                  %>%
      .[, RET:= paste0(strrep(' ', 8), CMT,
        '', '"', FIELDNAME, '"', strrep(' ', SPC1),
        ', # '   , DDTEXT      , strrep(' ', SPC2),
        KEYFLAG
      )
      ]                                                                   %>%
      .[.N, RET:= sub(",", " ", RET)]                                     %>%
      .[!FIELDNAME  %flike% ".I", RET]                                    %>%
      paste0(collapse = "\n")   
    
  }

fGetOHFields <- 
  function(pOHDEST, pSID = B4P){
  
    # Get the header of the OHD file
    # Fields of the Open Hub Destination #### 
    RSBOHFIELDS <- 
      fRead_and_Union(
        pSIDCLNT  = pSID,
        pTable    = "RSBOHFIELDS",
        pOptions  = list(
          "OBJVERS = 'A'"    , "AND",
          paste0("OHDEST = '", pOHDEST, "'")
          ),
        pFields   = list(
          # "OHDEST"      , # Open Hub Destination                            X
          # "OBJVERS"     , # Object version                                  X
          "FIELDNM"     , # Field name                                      X
          "POSIT"       # , # Position of the Field in the Structure / Table
          # "TEMPIOBJNM"  , # InfoObject                                      
          # "KEYFLAG"     , # Open Hub: Indicator for Semantic Key            
          # "DATATYPE"    , # Data Type                                       
          # "INTLEN"      , # Internal Length in Bytes                        
          # "LENG"        , # Length (No. of Characters)                      
          # "OUTPUTLEN"   , # Output Length                                   
          # "INTTYPE"     , # ABAP data type (C,D,N,...)                      
          # "CONVEXIT"    , # Conversion Routine                              
          # "DECIMALS"    , # Number of Decimal Places                        
          # "OUTFORMAT"   , # Data Input Format (Internal/External)           
          # "EXT_DTYPE"   , # Data Type                                       
          # "EXT_LENGHT"  , # Output Length                                   
          # "UNIFIELDNM"  , # Name of the relevant unit field                 
          # "DTELNM"        # Data element (semantic domain)                  
        ),
        pRowcount = Inf
      )                                                                   %T>%
      setkey(POSIT)                                                       %>%
      .[, FIELDNM:= sub(pattern = "/BIC/", replacement = "", x = FIELDNM)]
    
    return(RSBOHFIELDS$FIELDNM)
  }

dcast.systid <- 
  function(data, pENV){
    dcast.data.table(
      data          = data,
      formula       = ... ~ SYSTID, 
      fun.aggregate = length,
      value.var     = "CLIENT"
    )                                                    %T>%
      setcolorder(
        substr(pENV, 1,3), 
        after = names(data)[length(names(data)) ]
      )
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
