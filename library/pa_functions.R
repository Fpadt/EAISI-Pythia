fGet_tsMaterial <- 
  function(x){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    
    MATL <- c(MATN1(x))
    TABL <- FN_ISLS
    
    query <-   
      glue_sql("
    SELECT 
      MATERIAL,
      CALMONTH,
      sum(SLS_QT_SO + SLS_QT_FOC) as DEMND_QTY
    FROM 
      read_parquet({`TABL`})
    WHERE
      MATERIAL IN ({MATL*}) AND
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
      setDT()
    
    # Disconnect from DuckDB
    dbDisconnect(con)
    
    return(dtTS)
  }
