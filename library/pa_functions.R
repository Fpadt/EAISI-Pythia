# Scope
SCOPE_MATL <- MATN1('10023')
SCOPE_SORG <- c('FR30', 'NL10')
SCOPE_PRDH <- c(
  '07',  # ALTER ECO
  '08',  # BJORG
  '10',  # CLIPPER (CUPPER)
  '15',  # ZONNATURA
  '53',  # TANOSHI
  '65'   # NATURELA
)

# Master data Functions ####

fGet_MATL <- 
  function(){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb::duckdb())
    
    # Query the file and load it into a data.table
    MATL <- 
      dbGetQuery(
        con, 
        paste0(
          "SELECT 
          * 
         FROM read_parquet('", FN_MATL, "')
         "
        )
      )                                                                  %>%
      setDT()
    
    
    # Disconnect from DuckDB
    dbDisconnect(con)
    
    return(MATL)
  }

fGet_MATS <- 
  function() {
    #| label: 'Material Sales Data',
    #| eval:   true
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb::duckdb())
    
    # Query the file and load it into a data.table
      MATS <- 
        dbGetQuery(
          con, 
          paste0(
            "SELECT 
          * 
         FROM 
          read_parquet('", FN_MATS, "')
        "
          )
        )                                                            %>%
        setDT()
      
    # Disconnect from DuckDB
    dbDisconnect(con)
    
    return(MATS)
  }

fGet_MATP <- 
  function(){
    #| label: 'Material Plant Data',
    #| eval:   true
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb::duckdb())
    
    # Query the file and load it into a data.table

      MATP <- 
        dbGetQuery(
          con, 
          paste0(
            "SELECT 
          * 
         FROM 
          read_parquet('", FN_MATP, "')
        "
          )
        )                                                                  %>%
        setDT()
      
    # Disconnect from DuckDB
    dbDisconnect(con)
    
    return(MATP)
  }

# Sales Functions ####

fGet_Sales_by_Material_Salesorg <- 
  function(material, salesorg){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    
    MATL <- c(MATN1(material))
    SORG <- c(salesorg)
    TABL <- FN_ISLS
    
    query <-   
      glue_sql("
    SELECT 
      SALESORG,
      MATERIAL,
      CALMONTH,
      sum(SLS_QT_SO + SLS_QT_FOC) as DEMND_QTY
    FROM 
      read_parquet({`TABL`})
    WHERE
      MATERIAL IN ({MATL*}) AND
      SALESORG in ({SORG*}) AND
      CALMONTH < date_trunc('month', current_date)
    GROUP BY 
      ALL
    ORDER BY 
      MATERIAL,
      CALMONTH
    ", .con = con
      )
    
    # return data.table
    dtTS <- 
      dbGetQuery(con, query, n = Inf) %>%
      setDT() %>%
      .[, .(
        unique_id = paste(SALESORG, MATERIAL, sep = "_"),
        ds        = CALMONTH,
        y         = DEMND_QTY
      )
      ]
    
    # Disconnect from DuckDB
    dbDisconnect(con)
    
    return(dtTS)
  }

fGet_PA_sales <- 
  function(){
    #| label: 'Sales Data PA Scope',
    #| eval:   true
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    
    query <-   
      glue_sql("
    SELECT 
      *,
      sum(SLS_QT_SO + SLS_QT_FOC) as DEMND_QTY 
    FROM 
      read_parquet({`FN_ISLS`}) AS ISLS
    INNER JOIN
      read_parquet({`FN_MATL`}) AS MATL 
    ON
      ISLS.MATERIAL = MATL.MATERIAL
    WHERE
      SALESORG IN ({SCOPE_SORG*}) AND
      PRDH1    IN ({SCOPE_PRDH*})
    GROUP BY 
      ALL
    ORDER BY 
      SALESORG,
      CALMONTH
    ", .con = con
      )
    
    ISLS <- 
      dbGetQuery(con, query, n = Inf) %>%
      setDT()
    
    # Disconnect from DuckDB
    dbDisconnect(con)
    
    return(ISLS)
    
  }

# Dynasys results Actuals and forecast ####

fGet_PA_FRPR <- 
  function(){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    
    query <-   
      glue_sql("
    SELECT 
      * 
    FROM 
      read_parquet({`FN_FRPR`}) AS FRPR
    INNER JOIN
      read_parquet({`FN_MATL`}) AS MATL 
    ON
      FRPR.MATERIAL = MATL.MATERIAL
    WHERE
      SALESORG IN ({SCOPE_SORG*}) AND
      PRDH1    IN ({SCOPE_PRDH*})
    GROUP BY 
      ALL
    ORDER BY 
      SALESORG,
      CALMONTH
    ", .con = con
      )
    
    FRPR <- 
      dbGetQuery(con, query, n = 100) %>%
      setDT()
    
    # Disconnect from DuckDB
    dbDisconnect(con)
    
    return(FRPR)
    
  }

# Get pre-demand Actuals 

fGet_PA_FRPR2 <- 
  function(){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    
    query <-   
      glue_sql("
    SELECT 
      * 
    FROM 
      read_parquet({`FN_FRPR2`}) AS FRPR
    INNER JOIN
      read_parquet({`FN_MATL`}) AS MATL 
    ON
      FRPR.MATERIAL = MATL.MATERIAL
    WHERE
      SALESORG IN ({SCOPE_SORG*}) AND
      PRDH1    IN ({SCOPE_PRDH*}) AND
      FRPR.MATERIAL IN ({SCOPE_MATL*})
    GROUP BY 
      ALL
    ORDER BY 
      SALESORG,
      CALMONTH
    ", .con = con
      )
    
    FRPR <- 
      dbGetQuery(con, query, n = Inf) %>%
      setDT()
    
    # Disconnect from DuckDB
    dbDisconnect(con)
    
    return(FRPR)
    
  }

# Get pre-demand Actuals 

fGet_MP_FCST <- 
  function(salesorg, material){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    
    material <- MATN1(material)
    
    query <-   
      glue_sql("
    SELECT 
      SALESORG,
      PLANT,
      MATERIAL,
      CALMONTH,
      VTYPE,
      FTYPE,
      VERSMON,
      BASE_UOM, 
      sum(DEMND_QTY) as Q
    FROM 
      read_parquet([{`FN_FRPR2`}, {`FN_FRPR4`} ])
    WHERE
      SALESORG IN ({salesorg*}) AND
      MATERIAL IN ({material*}) AND
      (
       (CALMONTH > '202312' AND VTYPE = '010') OR
       (VERSMON  > '202212' AND VTYPE = '060')
      )
    GROUP BY 
      SALESORG,
      PLANT,
      MATERIAL,
      CALMONTH,
      VTYPE,
      FTYPE,
      VERSMON,
      BASE_UOM
    ORDER BY 
      SALESORG,
      CALMONTH
    ", .con = con
      )
    
    FRPR <- 
      dbGetQuery(con, query, n = Inf) %>%
      setDT()
    
    # Disconnect from DuckDB
    dbDisconnect(con)
    
    return(FRPR)
    
  }
