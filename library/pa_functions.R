# Scope

# SCOPE_MATL <- MATN1('10023')
SCOPE_SORG <- c('FR30', 'NL10')
SCOPE_PRDH <- c(
  '07',  # ALTER ECO
  '08',  # BJORG
  '10',  # CLIPPER (CUPPER)
  '15',  # ZONNATURA
  '53',  # TANOSHI
  '65'   # NATURELA
)
SCOPE_MATL <- 
  "WITH SCOPE_MATL AS (
     SELECT DISTINCT 
       MATERIAL
     FROM 
       read_parquet({`FN_MATL`})
     WHERE 
       PRDH1 IN ({SCOPE_PRDH*})
     )
  "
SCOPE_PLNT <- 
  "WITH SCOPE_PLNT AS (
     SELECT DISTINCT 
       PLANT
     FROM 
       read_parquet([{`FN_FRPR1`}, {`FN_FRPR3`}]) 
     WHERE 
       SALESORG IN ({SCOPE_SORG*})
     )
  "

# Master data Functions ####
fGet_MATL <-
  function(
    material    = NULL, # Optional user-supplied material
    apply_scope = TRUE  # restrict to Pythia Scope
    ){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    # ensure we disconnect on function exit
    on.exit(dbDisconnect(con), add = TRUE)  
    
    # ---- CTE Scope Materials ----
    # 1) Conditionally build a CTE for scope_materials if apply_scope=TRUE
    cte_scope_materials <- ""
    if (isTRUE(apply_scope)) {
      cte_scope_materials <- glue_sql(SCOPE_MATL, .con = con)
    }
    
    # ---- WHERE clause list ----
    where_clauses <- list()
    
    # ---- apply scope ----
    # (a) If apply_scope=TRUE, filter by SCOPE_SORG + scope_materials
    if (isTRUE(apply_scope)) {
      # Force material in the CTE scope_materials
      where_clauses <- c(
        where_clauses,
        glue_sql("MATERIAL IN (SELECT MATERIAL FROM SCOPE_MATL)", .con = con)
      )
    }
    
    # (c) Regardless of scope, 
    # filter on material list if existing
    if (!is.null(material) && length(material) > 0) {
      where_clauses <- c(
        where_clauses,
        glue_sql("MATERIAL IN ({vals*})", 
                 vals = MATN1(material), .con = con)
      )
    }
    
    # ---- Final Where clause ----
    # Combine them with AND
    final_where <- paste(where_clauses, collapse = " AND ")
    
    # ---- Query ----
    # Construct the query with or without the WHERE filter
    query <- 
      glue_sql("
          {DBI::SQL(cte_scope_materials)}
          SELECT 
            *
          FROM 
            read_parquet({`FN_MATL`})
                  WHERE 
          {DBI::SQL(final_where)} 
        ", .con = con)
    
    # Fetch results as a data.table
    dbGetQuery(con, query, n = Inf) %>%
    setDT()
    
  }

fGet_MATS <-
  function(
    salesorg    = NULL, # Optional user-supplied salesorg
    material    = NULL, # Optional user-supplied material
    apply_scope = TRUE  # restrict to Pythia Scope
    ){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    # ensure we disconnect on function exit
    on.exit(dbDisconnect(con), add = TRUE)  
    
    # ---- CTE Scope Materials ----
    # 1) Conditionally build a CTE for scope_materials if apply_scope=TRUE
    cte_scope_materials <- ""
    if (isTRUE(apply_scope)) {
      cte_scope_materials <- glue_sql(SCOPE_MATL, .con = con)
    }
    
    # ---- WHERE clause list ----
    where_clauses <- list()
    
    # ---- apply scope ----
    # (a) If apply_scope=TRUE, filter by SCOPE_SORG + scope_materials
    if (isTRUE(apply_scope)) {
      # 1) Force salesorg in the global SCOPE_SORG
      where_clauses <- c(
        where_clauses,
        glue_sql("SALESORG IN ({vals*})", 
                 vals = SCOPE_SORG, .con = con)
      )
      
      # 2) Force material in the CTE scope_materials
      where_clauses <- c(
        where_clauses,
        glue_sql("MAT_SALES IN (SELECT MATERIAL FROM SCOPE_MATL)", 
                 .con = con)
      )
    }
    
    # (b) Regardless of scope, 
    # filter on salesorg if existing
    if (!is.null(salesorg) && length(salesorg) > 0) {
      where_clauses <- c(
        where_clauses,
        glue_sql("SALESORG IN ({vals*})", 
                 vals = salesorg, .con = con)
      )
    }
    
    # (c) Regardless of scope, 
    # filter on material list if existing
    if (!is.null(material) && length(material) > 0) {
      where_clauses <- c(
        where_clauses,
        glue_sql("MAT_SALES IN ({vals*})", 
                 vals = MATN1(material), .con = con)
      )
    }

    # ---- Final Where clause ----
    # Combine them with AND
    final_where <- paste(where_clauses, collapse = " AND ")
    
    # Construct the query with or without the WHERE filter
    query <- 
      glue_sql("
          {DBI::SQL(cte_scope_materials)}
          SELECT 
            *
          FROM 
            read_parquet({`FN_MATS`})
          WHERE 
            {DBI::SQL(final_where)}
        ", .con = con)
    
    # Fetch results as a data.table
    dbGetQuery(con, query, n = Inf) %>%
    setDT()
    
  }

fGet_MATP <- 
  function(
    salesorg    = NULL, # Optional user-supplied salesorg
    material    = NULL, # Optional user-supplied material
    apply_scope = TRUE  # restrict to Pythia Scope
    ){

    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    # ensure we disconnect on function exit
    on.exit(dbDisconnect(con), add = TRUE)  
    
    # ---- CTE Scope Materials ----
    # 1) Conditionally build a CTE for scope_materials if apply_scope=TRUE
    cte_scope_materials <- ""
    if (isTRUE(apply_scope)) {
      cte_scope_materials <- glue_sql(SCOPE_MATL, .con = con)
    }
    
    cte_scope_plants <- ""
    if (isTRUE(apply_scope)) {
      cte_scope_plants <- glue_sql(SCOPE_PLNT, .con = con)
    }
    
    # ---- WHERE clause list ----
    where_clauses <- list("1 = 1")
    
    # ---- apply scope ----
    # (a) If apply_scope=TRUE, filter by SCOPE_SORG + scope_materials
    if (isTRUE(apply_scope)) {
      # 1) Force salesorg in the global SCOPE_SORG
      # where_clauses <- c(
      #   where_clauses,
      #   glue_sql("SALESORG IN ({vals*})", 
      #            vals = SCOPE_SORG, .con = con)
      # )
      
      # 2) Force material in the CTE scope_materials
      where_clauses <- c(
        where_clauses,
        glue_sql("MAT_PLANT IN (SELECT MATERIAL FROM SCOPE_MATL)",
                 .con = con)
      )
    }
    
    # (b) Regardless of scope, 
    # filter on salesorg if existing
    if (!is.null(salesorg) && length(salesorg) > 0) {
      where_clauses <- c(
        where_clauses,
        glue_sql("SALESORG IN (SELECT PLANT FROM SCOPE_PLNT)", 
                 vals = salesorg, .con = con)
      )
    }
    
    # (c) Regardless of scope, 
    # filter on material list if existing
    if (!is.null(material) && length(material) > 0) {
      where_clauses <- c(
        where_clauses,
        glue_sql("MAT_PLANT IN ({vals*})", 
                 vals = MATN1(material), .con = con)
      )
    }
    
    # ---- Final Where clause ----
    # Combine them with AND
    final_where <- paste(where_clauses, collapse = " AND ")
    
    # Construct the query with or without the WHERE filter
    query <- 
      glue_sql("
          {DBI::SQL(cte_scope_materials)}
          {DBI::SQL(cte_scope_plants)}
          SELECT 
            *
          FROM 
            read_parquet({`FN_MATP`})
          WHERE 
            {DBI::SQL(final_where)}
        ", .con = con)
    
    # Fetch results as a data.table
    dbGetQuery(con, query, n = Inf) %>%
      setDT()
    
  }

# Sales Functions ####

fGet_Sales_by_Material_Salesorg <- 
  function(material, salesorg){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    # ensure we disconnect on function exit
    on.exit(dbDisconnect(con), add = TRUE)  
    
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
    # ensure we disconnect on function exit
    on.exit(dbDisconnect(con), add = TRUE)  
    
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

# Dynasys results A & F ####

fGet_PA_FRPR <- 
  function(){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    # ensure we disconnect on function exit
    on.exit(dbDisconnect(con), add = TRUE)  
    
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
    # ensure we disconnect on function exit
    on.exit(dbDisconnect(con), add = TRUE)  
    
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


# Get Forecast data 
fGet_MP_FCST <- 
  function(salesorg, material, step_low = 1, step_high = 18){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    # ensure we disconnect on function exit
    on.exit(dbDisconnect(con), add = TRUE)  
    
    material <- MATN1(material)
    
    query <-   
      glue_sql("
    SELECT 
      SALESORG,
      PLANT,
      MATERIAL,
      date_diff(
        'month',
        -- Parse VERSMON as YYYYMM + '01' into a date
        strptime(VERSMON  || '01', '%Y%m%d'),
        -- Parse CALMONTH as YYYYMM + '01' into a date
        strptime(CALMONTH || '01', '%Y%m%d')
      ) AS STEP,
      VERSMON,
      CALMONTH,
      VTYPE,
      FTYPE,
      BASE_UOM, 
      sum(DEMND_QTY) as F
    FROM 
      read_parquet([{`FN_FRPR2`}, {`FN_FRPR4`} ])
    WHERE
      SALESORG IN ({salesorg*}) AND
      MATERIAL IN ({material*}) AND
      (
       -- Only forecasts generated in 2023 or later
       VERSMON  >= '202301' AND VTYPE = '060'
      )
    GROUP BY 
      ALL
    HAVING 
      STEP BETWEEN {step_low} AND {step_high}
    ORDER BY 
      ALL
    ", .con = con
      )
    
    FRPR <- 
      dbGetQuery(con, query, n = Inf) %>%
      setDT()
    
    # Disconnect from DuckDB
    dbDisconnect(con)
    
    return(FRPR)
    
  }


# Get Actuals 
fGet_DYN_Actuals <- 
  function(
    salesorg    = NULL    ,    # Optional user-supplied salesorg
    material    = NULL    ,    # Optional user-supplied material
    apply_scope = TRUE    ,    # restrict to Pythia Scope
    cm_min      = '202101', 
    cm_max      = '202512',
    n           = Inf){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    # ensure we disconnect on function exit
    on.exit(dbDisconnect(con), add = TRUE)  
    
    # ---- CTE Scope Materials ----
    # 1) Conditionally build a CTE for scope_materials if apply_scope=TRUE
    cte_scope_materials <- ""
    if (isTRUE(apply_scope)) {
      cte_scope_materials <- glue_sql(SCOPE_MATL, .con = con)
    }

    # ---- WHERE clause list ----
    # 2) Build up a list of WHERE clauses
    where_clauses <- list()
    
    # ---- apply scope ----
    # (a) If apply_scope=TRUE, filter by SCOPE_SORG + scope_materials
    if (isTRUE(apply_scope)) {
      # 1) Force salesorg in the global SCOPE_SORG
      where_clauses <- c(
        where_clauses,
        glue_sql("SALESORG IN ({vals*})", vals = SCOPE_SORG, .con = con)
      )

      # 2) Force material in the CTE scope_materials
      where_clauses <- c(
        where_clauses,
        glue_sql("MATERIAL IN (SELECT MATERIAL FROM SCOPE_MATL)", .con = con)
      )
    }
    
    # (b) Regardless of scope, 
    # filter on salesorg if existing
    if (!is.null(salesorg) && length(salesorg) > 0) {
      where_clauses <- c(
        where_clauses,
        glue_sql("SALESORG IN ({vals*})", 
                 vals = salesorg, .con = con)
      )
    }
    
    # (c) Regardless of scope, 
    # filter on material list if existing
    if (!is.null(material) && length(material) > 0) {
      where_clauses <- c(
        where_clauses,
        glue_sql("MATERIAL IN ({vals*})", 
                 vals = MATN1(material), .con = con)
      )
    }
    
    # (d) Always apply CALMONTH filter
    where_clauses <- c(
      where_clauses,
      glue_sql("CALMONTH BETWEEN {cm_min} AND {cm_max}", .con = con)
    )
    
    # ---- Final Where clause ----
    # Combine them with AND
    final_where <- paste(where_clauses, collapse = " AND ")
    
    # ---- Query ----
    # 3) Build the final query, using the CTE + main SELECT
    query <- 
      glue_sql("
        {DBI::SQL(cte_scope_materials)}
        SELECT 
          SALESORG,
          PLANT,
          MATERIAL,
          CALMONTH,
          FTYPE,
          BASE_UOM, 
          SUM(DEMND_QTY) AS A
        FROM 
          read_parquet([{`FN_FRPR1`}, {`FN_FRPR3`}]) 
        WHERE 
          {DBI::SQL(final_where)}
        GROUP BY 
          ALL
        ORDER BY 
          ALL
      ", .con = con)
    
    dbGetQuery(con, query, n = n) %>%
    setDT()
    
  }

fGet_RTP_Actuals <- 
  function(
    salesorg    = NULL    ,    # Optional user-supplied salesorg
    material    = NULL    ,    # Optional user-supplied material
    apply_scope = TRUE    ,    # restrict to Pythia Scope
    cm_min      = '202101', 
    cm_max      = '202512',
    n           = Inf){
    
    # Establish a connection to DuckDB
    con <- dbConnect(duckdb(), dbdir = ":memory:")
    # ensure we disconnect on function exit
    on.exit(dbDisconnect(con), add = TRUE)  
    
    # ---- CTE Scope Materials ----
    # 1) Conditionally build a CTE for scope_materials if apply_scope=TRUE
    cte_scope_materials <- ""
    if (isTRUE(apply_scope)) {
      cte_scope_materials <- glue_sql(SCOPE_MATL, .con = con)
    }
    
    # ---- WHERE clause list ----
    # 2) Build up a list of WHERE clauses
    where_clauses <- list()
    
    # ---- apply scope ----
    # (a) If apply_scope=TRUE, filter by SCOPE_SORG + scope_materials
    if (isTRUE(apply_scope)) {
      # 1) Force salesorg in the global SCOPE_SORG
      where_clauses <- c(
        where_clauses,
        glue_sql("SALESORG IN ({vals*})", vals = SCOPE_SORG, .con = con)
      )
      
      # 2) Force material in the CTE scope_materials
      where_clauses <- c(
        where_clauses,
        glue_sql("MATERIAL IN (SELECT MATERIAL FROM SCOPE_MATL)", .con = con)
      )
    }
    
    # (b) Regardless of scope, 
    # filter on salesorg if existing
    if (!is.null(salesorg) && length(salesorg) > 0) {
      where_clauses <- c(
        where_clauses,
        glue_sql("SALESORG IN ({vals*})", 
                 vals = salesorg, .con = con)
      )
    }
    
    # (c) Regardless of scope, 
    # filter on material list if existing
    if (!is.null(material) && length(material) > 0) {
      where_clauses <- c(
        where_clauses,
        glue_sql("MATERIAL IN ({vals*})", 
                 vals = MATN1(material), .con = con)
      )
    }
    
    # (d) Always apply CALMONTH filter
    # where_clauses <- c(
    #   where_clauses,
    #   glue_sql("CALMONTH BETWEEN {cm_min} AND {cm_max}", .con = con)
    # )
    
    # ---- Final Where clause ----
    # Combine them with AND
    final_where <- paste(where_clauses, collapse = " AND ")
    
    # ---- Query ----
    # 3) Build the final query, using the CTE + main SELECT
    query <- 
      glue_sql("
        {DBI::SQL(cte_scope_materials)}
        SELECT 
          SALESORG,
          PLANT,
          MATERIAL,
          CALMONTH,
          0 AS FTYPE,
        --  BASE_UOM, 
          sum(SLS_QT_SO + SLS_QT_FOC) as A
        FROM 
          read_parquet([{`FN_ISLS`}]) 
        WHERE 
          {DBI::SQL(final_where)}
        GROUP BY 
          ALL
        ORDER BY 
          ALL
      ", .con = con)
    
    dbGetQuery(con, query, n = n) %>%
      setDT()
    
  }
