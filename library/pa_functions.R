fGet_dtMaterial <- 
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
