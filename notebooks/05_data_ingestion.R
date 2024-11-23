CFG <- 
  fread(text =
          "EXP, TYPE, AREA     , FNM                      , OHD      , SAVE , COMP
   OLD, TD  , SALES    , DD_HISTO_QTY.CSV         , ZSOP_ASLS, TRUE , none
   OLD, TD  , ORDER    , DD_OPEN_ORDERS_QTY.CSV   , ZSOP_OSLS, TRUE , none
   OLD, MD  , MATERIAL , MD_MATERIAL.CSV          , ZMAT_ATTR, TRUE , none  
   OLD, MD  , MAT_SALES, MD_MATERIAL_SALES_ORG.CSV, ZMATSALES, TRUE , none
   OLD, MD  , MAT_PLANT, MD_MATERIAL_PLANT.CSV    , ZMATPLANT, TRUE , none
   OLD, MD  , BOM      , MD_BOM.CSV               , ZSOP_BOM , TRUE , none
   OLD, MD  , PLANT    , MD_PLANT.CSV             , ZPLANT   , TRUE , none   
   OLD, MD  , PRICE    , MD_PRICE.CSV             , ZPRICING , TRUE , none
   OLD, MD  , SALES_ORG, MD_SALES_ORG.CSV         , ZSALESORG, TRUE , none
   OLD, MD  , SOLDTO   , MD_SOLD_TO_CUSTOMER.CSV  , CUST_ATTR, TRUE , none  
   NEW, TD  , SALES    , DD_HISTO_QTY.CSV         , DSCP_TRAN, TRUE , none
   NEW, TD  , SLS2123  , DD_SALES_QTY_LE23.csv    , DSCP_TRAN, TRUE , none
   NEW, TD  , SLS2424  , DD_SALES_QTY_GE24.csv    , DSCP_TRAN, TRUE , none   
   NEW, TD  , ORDER    , DD_OPEN_ORDERS_QTY.CSV   , DSCP_TRAN, TRUE , none
   NEW, MD  , MATERIAL , MD_MATERIAL.CSV          , DSCP_MATE, TRUE , none  
   NEW, MD  , MAT_SALES, MD_MATERIAL_SALES_ORG.CSV, DSCP_MATS, TRUE , none
   NEW, MD  , MAT_PLANT, MD_MATERIAL_PLANT.CSV    , DSCP_MATP, TRUE , none
   NEW, MD  , BOM      , MD_BOM.CSV               , DSCP_BOMX, TRUE , none
   NEW, MD  , PLANT    , MD_PLANT.CSV             , DSCP_PLNT, TRUE , none   
   NEW, MD  , SALES_ORG, MD_SALES_ORG.CSV         , DSCP_SORG, TRUE , none
   NEW, MD  , SOLDTO   , MD_SOLD_TO_CUSTOMER.CSV  , DSCP_CUST, TRUE , none     
   NEW, TD  , SDSFRPR1 , SDSFRPR1.csv             , OH_FRPR1 , TRUE , none 
   NEW, TD  , SDSFRPR2 , SDSFRPR2_F.csv           , OH_FRPR2 , TRUE , none
   NEW, TD  , STOCK    , IMP03SM1_F.csv           , OH_STOCK , TRUE , none" 
  ) %>%
  .[, c("BNM", "EXT") := tstrsplit(x = FNM, split = "\\.")] 

# OH_DMMG0
# OH_FCWKF
# OH_FRABM
# OH_FRCAC
# OH_FRCT0
# OH_FRPR3
# OH_FRPR4
# OH_VAIT0
# OH_VSSL5


fGet_FieldNames <- 
  function(ohdest, sid="WDB100"){
    
    if (!exists("dtOHD")){
      dtOHD <<- 
        fRead_and_Union(
          pSIDCLNT = sid,
          pTable   = "RSBOHFIELDS",
          pOptions = list("OBJVERS = 'A'")
        )                                        %T>%
        .[, FIELDNM:= sub("/BIC/", "", FIELDNM)] %T>%
        setkey(OHDEST, POSIT)
      
    }
    
    dtOHD[OHDEST == ohdest, FIELDNM]
  }



fLoadOpenHubExport <- 
  function(
    pAREA, pCFG = CFG , pENV = 'WPB', pPERKZ = 'perkz_w', 
    pKEY , pPTH = PYTH, pEXP = 'NEW'){
   
   OHD <- pCFG[AREA == pAREA & EXP == pEXP, OHD]   
   iFN <- pCFG[AREA == pAREA & EXP == pEXP, FNM]
   wFN <- normalizePath(file.path(pPTH, iFN))

    RETURN <- 
      fread(
        # file        = iFN, 
        header      = FALSE, 
        cmd         = paste("iconv -f WINDOWS-1252 -t UTF-8", shQuote(wFN)),
        colClasses  = "character", 
        strip.white = FALSE
      )                                                              %T>%
      setnames(fGet_FieldNames(OHD))

    if(!missing(pKEY)){setkeyv(x = RETURN, pKEY)}
    
    RETURN
  }


# DSCP --------------------------------------------------------------------




# Data Import -------------------------------------------------------------

# DSCP ####

## Plant ####
fGetPlantDSCP <- 
  function(pSIDCLNT = WPB, pType = c("AMB", "SSL") ) {
    
    if(missing(pType)) {
      pType <- pType[1]
    }
    
    if(pType == "AMB") {
      pOptions <- list("NAME = 'DSCP_AMB_PLANT'")
    } else if(pType == "SSL") {
      pOptions <- list("NAME = 'DSCP_SSL_PLANT'")
    }
    
    fRead_and_Union(
      pSIDCLNT  = pSIDCLNT,
      pTable    = "TVARVC",
      pOptions  = pOptions,
      pFields   = list("LOW"),
      pRowcount = Inf
    ) %>%
      .[, LOW]
  }

## Salesorg ####
fGetSalesorgDSCP <- 
  function(pSIDCLNT = WPB, pType = c("AMB", "SSL") ) {
    
    if(missing(pType)) {
      pType <- pType[1]
    }
    
    if(pType == "AMB") {
      pOptions <- list("NAME = 'DSCP_AMB_PLANT'")
    } else if(pType == "SSL") {
      pOptions <- list("NAME = 'DSCP_SSL_PLANT'")
    }
    
    DSCP_WERKS <- 
      fGetPlantDSCP(pSIDCLNT, pType) %>%
      f_or("WERKS")
    
    fRead_and_Union(
      pSIDCLNT  = WPE,
      pTable    = "T001W",
      pOptions  = DSCP_WERKS,
      pFields   = list(
        
      ),
      pRowcount = Inf
    ) %>%
      .[, VKORG] %>%
      unique()
  }

fSignLeft <- 
  function(x){
    ifelse(
      grepl(pattern = "-", x),
      paste0(
        "-",        
        sub(
          pattern     = "-", 
          replacement = "", 
          x           = x
        )
      ),
      x
    ) %>%
      as.numeric()
  }

