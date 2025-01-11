# General ####

# duckdb environment
.duckdb_env <- new.env(parent = emptyenv())

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

#' Construct a Scope Materials CTE Clause
#'
#' Internal helper function that returns a SQL snippet (CTE) for scope materials 
#' if \code{.apply_scope} is \code{TRUE}, otherwise returns an empty string.
#'
#' @param .con A database connection object.
#' @param .apply_scope Logical. If \code{TRUE}, returns the scope materials snippet;
#'   otherwise, returns an empty string.
#'
#' @return A character string containing the SQL snippet for scope materials.
#'
#' @keywords internal
#' @noMd
.get_cte_scope_materials <- function(.con, .scope_matl) {
  cte_scope_materials <- ""
  if (isTRUE(.scope_matl)) {
    cte_scope_materials <- 
      glue::glue_sql(SCOPE_MATL, .con = .con)
  }
  return(cte_scope_materials)
}

#' Get (or create) a DuckDB connection
#'
#' Internal function that returns a `DBIConnection` to DuckDB. If no connection
#' exists in the `.duckdb_env`, a new one is created using \code{\link[DBI]{dbConnect}}.
#'
#' @param dbdir Character. Location of the DuckDB database file. Use \code{":memory:"}
#'   for an in-memory database.
#'
#' @return A `DBIConnection` object to DuckDB.
#'
#' @details 
#' This internal helper function checks if there is an existing DuckDB connection in
#' the environment `.duckdb_env$conn`. If none is found, it initializes a new one and
#' caches it. Subsequent calls will reuse the same connection.
#'
#' @keywords internal
.get_duckdb_conn <- function(dbdir = ":memory:") {
  if (!exists("conn", envir = .duckdb_env)) {
    message("Initializing DuckDB connection...")
    .duckdb_env$conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = dbdir)
  }
  .duckdb_env$conn
}


#' Close and Remove the DuckDB Connection
#'
#' This internal helper function checks whether a DuckDB connection (stored in
#' \code{.duckdb_env$conn}) exists. If found, it closes (disconnects) the
#' DuckDB connection using \code{\link[DBI]{dbDisconnect}} and removes the
#' \code{conn} object from \code{.duckdb_env}. If no connection is present,
#' a message is displayed indicating that nothing is closed.
#'
#' @details
#' This function is called to cleanly release resources associated with a
#' DuckDB connection in the internal environment. It can be invoked at the end
#' of your workflow or whenever you wish to reset the internal DuckDB state.
#'
#' @return
#' This function is called for its side effects (closing and removing the
#' connection). It returns \code{NULL} invisibly.
#'
#' @keywords internal
.close_duckdb_conn <- function() {
  if (exists("conn", envir = .duckdb_env)) {
    message("Closing DuckDB connection...")
    DBI::dbDisconnect(.duckdb_env$conn, shutdown = TRUE)
    rm("conn", envir = .duckdb_env)
  } else {
    message("No DuckDB connection found to close.")
  }
}

#' Build a list of WHERE Clauses
#'
#' This internal helper function ensures that the first element in a list
#' of WHERE clauses is "TRUE" (which acts as a no-op condition) and then
#' optionally adds further constraints based on the `what` parameter.
#'
#' @param clauses A list of existing WHERE clauses (character strings).
#' @param what A character string specifying which WHERE clause type to add.
#'   For example, "material" may add a condition on the MATERIAL column.
#' @param apply_scope Logical. If `TRUE`, the function will add a scope-based
#'   WHERE clause (e.g., filtering on MATERIAL) to `clauses`.
#' @param con An optional database connection object, used for safe
#'   parameterization with \code{\link[glue]{glue_sql}}. Required if `apply_scope`
#'   is \code{TRUE} and the `what` switch requires building an SQL clause.
#'
#' @details
#' \enumerate{
#'   \item The first WHERE clause is always "TRUE". If the provided \code{clauses}
#'         list does not have it, it will be appended automatically.
#'   \item Depending on the value of \code{what}, additional constraints can be
#'         appended to \code{clauses} using a \code{switch()} statement.
#'   \item For "material", if \code{apply_scope = TRUE}, a clause such as
#'         \code{"MATERIAL IN (SELECT MATERIAL FROM SCOPE_MATL)"} is appended.
#' }
#'
#' You can extend the \code{switch()} statement to handle more cases, e.g.,
#' filtering on organizational data or other columns, if needed.
#'
#' @return A (possibly modified) list of character strings representing WHERE
#'   clauses for a SQL query.
#'
#' @examples
#' # Minimal example (pseudo-code, no real DB connection):
#' my_clauses <- fGet_Where_Clause(clauses = list(), what = "material",
#'                                 apply_scope = TRUE, con = NULL)
#' my_clauses
#'
#' @keywords internal
.get_where_clause <- 
  function(.clauses     = list(),
           .scope_matl  = TRUE,
           .scope_sorg  = TRUE,
           .salesorg    = NULL,
           .material    = NULL,
           .con         = NULL) {
    
    # 1) Ensure the list has "TRUE" to start.
    #    (any() with identical() helps avoid partial matches.)
  if (!any(vapply(.clauses, function(x) identical(x, "TRUE"), logical(1)))) {
    .clauses <- c(.clauses, "TRUE")
  }
  
    # If scope_matl is applied, filter on MATERIALs in Scope
    if (isTRUE(.scope_matl)) {
      
      .clauses <- c(
        .clauses,
        glue::glue_sql(
          "MATERIAL IN (SELECT MATERIAL FROM SCOPE_MATL)", .con = .con)
      )
    }
  
    # If material is given, filter on MATERIAL
    if (!is.null(.material) && length(.material) > 0) {
      .clauses <- c(
        .clauses,
        glue_sql(
          "MATERIAL IN ({vals*})", vals = MATN1(.material), .con = .con)
      )
    }
    
    # collapse list of where_clasues to 1 clause with AND
    where_clause <- paste(.clauses, collapse = " AND ")
    
  # Return the updated list
  return(where_clause)
}

# Master data Functions ####
fGet_MATL <-
  function(
    .material    = NULL, # Optional user-supplied material
    .scope_matl  = TRUE, # restrict to Pythia Scope
    .n           = Inf   # number of materials to return
    ){
    
    # Establish a connection to DuckDB
    con <- .get_duckdb_conn()
    
    cte_scope_materials <- 
      .get_cte_scope_materials(.con = con, .scope_matl = .scope_matl)
    
    # ---- apply scope ----
    # (a) If apply_scope=TRUE, filter by SCOPE_SORG + scope_materials
    # if (isTRUE(apply_scope)) {
    #   # Force material in the CTE scope_materials
    #   where_clauses <- c(
    #     where_clauses,
    #     glue_sql("MATERIAL IN (SELECT MATERIAL FROM SCOPE_MATL)", .con = con)
    #   )
    # }
    
    where_clause <- 
      .get_where_clause(
        .material   = .material,
        .scope_matl = .scope_matl,
        .con        = con
        )
    
    # # (c) Regardless of scope, 
    # # filter on material list if existing
    # if (!is.null(material) && length(material) > 0) {
    #   where_clauses <- c(
    #     where_clauses,
    #     glue_sql("MATERIAL IN ({vals*})", 
    #              vals = MATN1(material), .con = con)
    #   )
    # }
    
    # ---- Final Where clause ----
    # Combine them with AND
    # final_where <- paste(where_clauses, collapse = " AND ")
    
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
          {DBI::SQL(where_clause)} 
        ", .con = con)
    
    # Fetch results as a data.table
    dbGetQuery(con, query, n = .n) %>%
    setDT()
    
  }

fGet_MATS <-
  function(
    salesorg    = NULL, # Optional user-supplied salesorg
    material    = NULL, # Optional user-supplied material
    apply_scope = TRUE  # restrict to Pythia Scope
    ){
    
    # Establish a connection to DuckDB
    con <- .get_duckdb_conn()
    
    cte_scope_materials <- 
      .Get_CTE_Scope_Materials(.con = con, .apply_scope = apply_scope)
    # cte_scope_materials <- ""
    # if (isTRUE(apply_scope)) {
    #   cte_scope_materials <- glue_sql(SCOPE_MATL, .con = con)
    # }
    
    cte_scope_materials <- 
      .Get_CTE_Scope_Materials(.con = con, .apply_scope = apply_scope)
    
    # ---- WHERE clause list ----
    # first where clause is always true
    where_clauses <- list("1 = 1")
    
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
    material    = NULL, # Optional user-supplied material
    apply_scope = TRUE  # restrict to Pythia Scope
    ){

    # Establish a connection to DuckDB
    con <- .get_duckdb_conn()
    
    cte_scope_materials <- 
      .Get_CTE_Scope_Materials(.con = con, .apply_scope = apply_scope)
        # cte_scope_materials <- ""
    # if (isTRUE(apply_scope)) {
    #   cte_scope_materials <- glue_sql(SCOPE_MATL, .con = con)
    # }
    
    # cte_scope_plants <- ""
    # if (isTRUE(apply_scope)) {
    #   cte_scope_plants <- glue_sql(SCOPE_PLNT, .con = con)
    # }
    
    # ---- WHERE clause list ----
    # first where clause is always true
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
    # if (!is.null(salesorg) && length(salesorg) > 0) {
    #   where_clauses <- c(
    #     where_clauses,
    #     glue_sql("SALESORG IN (SELECT PLANT FROM SCOPE_PLNT)", 
    #              vals = salesorg, .con = con)
    #   )
    # }
    
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

# Get PA sales ####
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

# Get pre-demand Actuals ####
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


# Get Forecast data - OBSOLETE #### 
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


# Get DYN Actuals ####
fGet_DYN_Actuals <- 
  function(
    salesorg    = NULL    ,    # Optional user-supplied salesorg
    material    = NULL    ,    # Optional user-supplied material
    apply_scope = TRUE    ,    # restrict to Pythia Scope
    cm_min      = '202101', 
    cm_max      = '202512',
    step_low    = -1      , 
    step_high   = -1      ,
    n           = Inf){
    
    # Establish a connection to DuckDB
    con <- .get_duckdb_conn()
    
    cte_scope_materials <- 
      .Get_CTE_Scope_Materials(.con = con, .apply_scope = apply_scope)
    # cte_scope_materials <- ""
    # if (isTRUE(apply_scope)) {
    #   cte_scope_materials <- glue_sql(SCOPE_MATL, .con = con)
    # }

    # ---- WHERE clause list ----
    # first where clause is always true
    where_clauses <- list("1 = 1")
    
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
    
    # (e) Always apply STEP filter
    where_clauses <- c(
      where_clauses,
      glue_sql("STEP BETWEEN {step_low} AND {step_high}", .con = con)
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
          date_diff(
            'month',
            -- Parse VERSMON as YYYYMM + '01' into a date
            strptime(VERSMON  || '01', '%Y%m%d'),
            -- Parse CALMONTH as YYYYMM + '01' into a date
            strptime(CALMONTH || '01', '%Y%m%d')
          ) AS STEP,
          CALMONTH,
          VERSMON,
          FTYPE,
          VTYPE,
          BASE_UOM, 
          SUM(DEMND_QTY) AS Q
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

# Get DYN Forecast ####
fGet_DYN_Forecast <- 
  function(
    salesorg    = NULL    ,    # Optional user-supplied salesorg
    material    = NULL    ,    # Optional user-supplied material
    apply_scope = TRUE    ,    # restrict to Pythia Scope
    cm_min      = '202401', 
    cm_max      = '202506',
    step_low    = 1       , 
    step_high   = 18      ,
    n           = Inf){
    
    # Establish a connection to DuckDB
    con <- .get_duckdb_conn()
    
    cte_scope_materials <- 
      .Get_CTE_Scope_Materials(.con = con, .apply_scope = apply_scope)
    # cte_scope_materials <- ""
    # if (isTRUE(apply_scope)) {
    #   cte_scope_materials <- glue_sql(SCOPE_MATL, .con = con)
    # }
    
    # ---- WHERE clause list ----
    # first where clause is always true
    where_clauses <- list("1 = 1")
    
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
    
    # (e) Always apply STEP filter
    where_clauses <- c(
      where_clauses,
      glue_sql("STEP BETWEEN {step_low} AND {step_high}", .con = con)
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
          date_diff(
           'month',
           -- Parse VERSMON as YYYYMM + '01' into a date
           strptime(VERSMON  || '01', '%Y%m%d'),
           -- Parse CALMONTH as YYYYMM + '01' into a date
           strptime(CALMONTH || '01', '%Y%m%d')
          ) AS STEP,
          CALMONTH,
          VERSMON,
          FTYPE,
          VTYPE,
          BASE_UOM, 
          SUM(DEMND_QTY) AS Q
        FROM 
          read_parquet([{`FN_FRPR2`}, {`FN_FRPR4`}]) 
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

# Get RTP Actuals ####
fGet_RTP_Actuals <- 
  function(
    salesorg    = NULL    ,    # Optional user-supplied salesorg
    material    = NULL    ,    # Optional user-supplied material
    apply_scope = TRUE    ,    # restrict to Pythia Scope
    cm_min      = '202101', 
    cm_max      = '202512',
    n           = Inf){
    
    # Establish a connection to DuckDB
    con <- .get_duckdb_conn()
    
    cte_scope_materials <- 
      .Get_CTE_Scope_Materials(.con = con, .apply_scope = apply_scope)
    # cte_scope_materials <- ""
    # if (isTRUE(apply_scope)) {
    #   cte_scope_materials <- glue_sql(SCOPE_MATL, .con = con)
    # }
    
    # ---- WHERE clause list ----
    # first where clause is always true
    where_clauses <- list("1 = 1")
    
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

# R Functions ####

# Open Folder ####
fOpen_Folder <- 
  function(path){
    # Open the folder in Windows Explorer
    shell.exec(normalizePath(path))
  }

