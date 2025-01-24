# General ####

##  ToDO
# %>%
#   .[, .(
#     unique_id = paste(SALESORG, MATERIAL, sep = "_"),
#     ds        = CALMONTH,
#     y         = DEMND_QTY
#   )
#   ]

CM_MIN   <- '2021.01'
CM_MAX   <- '2025.06'
STEP_MIN <- 1
STEP_MAX <- 24
LAGG_MIN <- -999
LAGG_MAX <- 999

# duckdb environment
.duckdb_env <- new.env(parent = emptyenv())

# SCOPE_MATL <- pa_matn1_input('10023')
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

fVerbose <- function(
    function_name   = NULL,
    function_args   = NULL,
    function_return = NULL,
    verbose         = FALSE
) {
  if (isTRUE(verbose)) {
    cat("Function name:\n")
    cat("  ", function_name, "\n\n")
    
    if (!is.null(function_args)) {
      cat("Function arguments:\n")
      print(function_args)
      cat("\n")
    }
    
    if (!is.null(function_return)) {
      cat("Function return value:\n")
      print(function_return)
      cat("\n")
    }
  }
}

fDescribe_Parquet <- 
  function(
    .fn = NULL
  ){
    
    # Establish a connection to DuckDB
    con <- .get_duckdb_conn()
    
    # construct Query 
    query <- 
      glue_sql("
        DESCRIBE 
        SELECT 
          *
        FROM 
          read_parquet({`.fn`})
      ", .con = con)
    
    # Execute and fetch results
    dbGetQuery(con, query) %>%
      print()
    
  }

#' Retrieve DuckDB Config Parts
#'
#' This internal helper function returns a list of three elements:
#' \enumerate{
#'   \item \code{duckdb_con} - The DuckDB connection.
#'   \item \code{cte_scope_materials} - The CTE (common table expression) snippet
#'         for scope materials, constructed if needed.
#'   \item \code{where_clause} - A character vector of one or more WHERE clauses,
#'         built according to the provided parameters.
#' }
#'
#' @param .material A character vector of materials to filter on. If \code{NULL},
#'   no material-based filter is applied (beyond scope constraints).
#' @param .salesorg A character vector of sales organizations to filter on. If \code{NULL},
#'   no salesorg-based filter is applied (beyond scope constraints).
#' @param .scope_matl Logical. If \code{TRUE}, the returned \code{cte_scope_materials}
#'   (and any relevant WHERE clauses) will restrict to materials in the scope definition.
#' @param .scope_sorg Logical. If \code{TRUE}, where clauses will restrict to
#'   the provided sales organization scope (if \code{.salesorg} is not \code{NULL}).
#' @param .cm_min Character. Minimum YYYYMM to apply in date-based filtering. Defaults to
#'   '202101'.
#' @param .cm_max Character. Maximum YYYYMM to apply in date-based filtering. Defaults to
#'   '202512'.
#' @param .step_min Numeric. Minimum step filter. Defaults to -999.
#' @param .step_max Numeric. Maximum step filter. Defaults to 999.
#' @param .n Numeric or Inf. The maximum number of materials to return (if that logic
#'   is implemented in the underlying queries). Defaults to \code{Inf}.
#'
#' @return A named \code{list} with three elements:
#'   \itemize{
#'     \item \code{duckdb_con} - The active DuckDB connection.
#'     \item \code{cte_scope_materials} - SQL snippet for scope materials (may be an
#'            empty string if \code{.scope_matl=FALSE}).
#'     \item \code{where_clause} - A character vector of WHERE clauses.
#'   }
#'
#' @details
#' This function centralizes the logic of creating a DuckDB connection,
#' constructing the \code{cte_scope_materials} snippet, and building the \code{where_clause}.
#' Other functions can call this to avoid repeating code.
#'
#' @keywords internal
.get_duckdb_parts <- function(
    .material    = NULL,
    .salesorg    = NULL,
    .scope_matl  = NULL,
    .scope_sorg  = FALSE,
    .cm_min      = NULL,
    .cm_max      = NULL,
    .step_min    = NULL,
    .step_max    = NULL,
    .lagg_min    = NULL, 
    .lagg_max    = NULL
    
) {
  
  # -- 1) Get or create a DuckDB connection --
  con <- .get_duckdb_conn()   
  
  # -- 2) Build the CTE snippet for scope materials --
  cte_scope_materials <- .get_cte_scope_materials(
    .scope_matl = .scope_matl,
    .con        = con
  )
  
  # -- 3) Build the WHERE clause according to the given parameters --
  where_clause <- .get_where_clause(
    .material   = .material,
    .salesorg   = .salesorg,
    .scope_matl = .scope_matl,
    .scope_sorg = .scope_sorg,
    .cm_min     = .cm_min,
    .cm_max     = .cm_max,
    .step_min   = .step_min,
    .step_max   = .step_max,
    .lagg_min   = .lagg_min, 
    .lagg_max   = .lagg_max,    
    .con        = con
  )
  
  # Return as a named list
  list(
    duckdb_con          = con,
    cte_scope_materials = cte_scope_materials,
    where_clause        = where_clause
  )
}


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
.get_cte_scope_materials <- 
  function(.scope_matl, .con) {
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
.get_where_clause <- function(
    .clauses     = list()  , # Existing WHERE clauses
    .material    = NULL    , # NULL wont apply any filter
    .salesorg    = NULL    , # NULL wont apply any filter      
    .scope_matl  = FALSE   , # FALSE wont apply any filter
    .scope_sorg  = FALSE   , # FALSE wont apply any filter
    .cm_min      = NULL    , # NULL wont apply any filter
    .cm_max      = NULL    , # NULL wont apply any filter
    .step_min    = NULL    , # NULL wont apply any filter
    .step_max    = NULL    , # NULL wont apply any filter
    .lagg_min    = NULL    , # NULL wont apply any filter
    .lagg_max    = NULL    , # NULL wont apply any filter
    .con         = NULL) {
  
    # Ensure the list has "TRUE" in case no other where clauses exist.
    if (!any(
          vapply(.clauses, function(x) identical(x, "TRUE"), logical(1))
          )
        ){
      .clauses <- c(.clauses, "TRUE")
    }
  
    # If .material is given, filter on MATERIAL
    # leading zero's are added by function pa_matn1_input
    if (!is.null(.material) && length(.material) > 0) {
      .clauses <- c(
        .clauses,
        glue_sql(
          "MATERIAL IN ({vals*})", vals = pa_matn1_input(.material), 
          .con = .con)
      )
    }
    
    # If .salesorg is given, filter on SALESORG
    if (!is.null(.salesorg) && length(.salesorg) > 0) {
      .clauses <- c(
        .clauses,
        glue_sql(
          "SALESORG IN ({vals*})", vals = .salesorg, 
          .con = .con)
      )
    }
    
    # If .scope_matl is given, filter on MATERIALs in Scope
    if (isTRUE(.scope_matl)) {
      .clauses <- c(
        .clauses,
        glue::glue_sql(
          "MATERIAL IN (SELECT MATERIAL FROM SCOPE_MATL)", 
          .con = .con)
      )
    }
    
    # If .scope_sorg is given, filter on SALESORGs in Scope
    if (isTRUE(.scope_sorg)) {
      .clauses <- c(
        .clauses,
        glue::glue_sql(
          "SALESORG IN ({vals*})", vals = SCOPE_SORG, 
          .con = .con)
      )
    }
    
    # If .cm_min or .cm_max are given, filter on CALMONTH
    if(!is.null(.cm_min) || !is.null(.cm_max)){

      # If only .cm_max is given, set .cm_min 
      if (is.null(.cm_min) || length(.cm_min) == 0) {
        .cm_min <- CM_MIN
      }
      
      # If only .cm_min is given, set .cm_max 
      if (is.null(.cm_max) || length(.cm_max) == 0) {
        .cm_max <- CM_MAX
      }
      
      .clauses <- c(
        .clauses,
        glue::glue_sql(
          "CALMONTH BETWEEN {.cm_min} AND {.cm_max}",  
          .con = .con)
      )
    }
    
    # If  one of the variables is not NULL add a filter
    if(!all(
      vapply(
        list(.step_min, .step_min, .lagg_min, .lagg_max), 
        is.null, logical(1)))){
    
      # set .step_min if NULL
      if (is.null(.step_min) || length(.step_min) == 0) {
        .step_min <- STEP_MIN
      }
    
      # set .step_max if NULL
      if (is.null(.step_max) || length(.step_max) == 0) {
        .step_max <- STEP_MAX
      }
      
      # set .lagg_min if NULL
      if (is.null(.lagg_min) || length(.lagg_min) == 0) {
        .lagg_min <- LAGG_MIN
      }
      
      # set .lagg_max if NULL
      if (is.null(.lagg_max) || length(.lagg_max) == 0) {
        .lagg_max <- LAGG_MAX
      }
      
      .clauses <- c(
        .clauses,
        glue::glue_sql(
          "((VTYPE = '060' AND 
            STEP BETWEEN {.step_min} AND {.step_max}
           ) OR
           (VTYPE = '010' AND 
            STEP BETWEEN {.lagg_min} AND {.lagg_max}
           ))",  
          .con = .con)
      )
    }

    # collapse list of where_clasues to 1 clause with AND
    where_clause <- paste(.clauses, collapse = " AND ")
    
  # Return the updated list
  return(where_clause)
}

#' Return Parquet Paths by VTYPE and FTYPE
#'
#' An internal helper function that uses a data.table lookup for valid vtype-ftype-path
#' combinations. By default, it returns all available paths if no arguments are supplied.
#'
#' @param .vtype A character vector of valid vtype codes. Defaults to \code{c("010", "060")}.
#' @param .ftype A numeric (or integer) vector of valid ftype codes. Defaults to \code{c(1,2)}.
#'
#' @return A character vector of parquet paths corresponding to all
#'   \code{(.vtype, .ftype)} pairs in the lookup.
#' @keywords internal
.get_parquet_paths <- 
  function(
    .vtype = c("010", "060"), 
    .ftype = c(1, 2)) {
    
  if(is.null(.vtype)){ .vtype <- c("010", "060")}  
  if(is.null(.ftype)){ .ftype <- c(1    , 2    )}    

  # Filter using data.table syntax
  # %chin% is for character matching, %in% for numeric
  result <- paths_parquet_files[
    vtype %chin% .vtype &
      ftype %in% .ftype,
    path
  ]
  
  # Return the matching paths
  return(result)
}


.make_sql_query_dyn <- 
  function(
    .vtype       = NULL    , # NULL will get both 010 and 060
    .ftype       = NULL    , # NULL will get all ftypes
    .material    = NULL    , # NULL wont apply any filter
    .salesorg    = NULL    , # NULL wont apply any filter      
    .scope_matl  = FALSE   , # FALSE wont apply any filter
    .scope_sorg  = FALSE   ,  # FALSE wont apply any filter
    .cm_min      = NULL    , # minimal Cal Month
    .cm_max      = NULL    , # maximal Cal Month
    .step_min    = NULL    , # minimal forecast step ahead
    .step_max    = NULL    , # maximal forecast step ahead
    .lagg_min    = NULL    , # minimal diff. between VERSMON & MONTH
    .lagg_max    = NULL      # maximal diff. between VERSMON & MONTH      
    ) {
    
  # Get Centralized config
  config <- .get_duckdb_parts(
    .material    = .material,
    .salesorg    = .salesorg,
    .scope_matl  = .scope_matl,
    .scope_sorg  = .scope_sorg,
    .cm_min      = .cm_min,
    .cm_max      = .cm_max,
    .step_min    = .step_min,
    .step_max    = .step_max,
    .lagg_min    = .lagg_min,
    .lagg_max    = .lagg_max 
  )  
  
  # Determine Files to read
  file_list <- 
    paste0(
      "'", .get_parquet_paths(.vtype = .vtype, .ftype = .ftype), "'", 
      collapse = ", "
    )
  
  # Construct the query using glue_sql()
  query <- glue::glue_sql("
    {DBI::SQL(config$cte_scope_materials)}
    
    SELECT 
      SALESORG,
      PLANT,
      MATERIAL,
      STEP,
      CALMONTH,
      VERSMON,
      FTYPE,
      VTYPE,
      BASE_UOM, 
      SUM(DEMND_QTY) AS Q
    FROM 
      read_parquet([{DBI::SQL(file_list)}])
    WHERE 
      {DBI::SQL(config$where_clause)}
    GROUP BY 
      ALL
    ORDER BY 
      ALL
  ", .con = config$duckdb_con)
  
  return(query)
}

# # Master data Functions ####
# fGet_MATL <-
#   function(
#     .material    = NULL, # Optional user-supplied material
#     .scope_matl  = TRUE, # restrict to Pythia Scope
#     .n           = Inf   # number of materials to return
#     ){
#   
#     # Get Centralized config
#     config <- .get_duckdb_parts(
#       .material   = .material,
#       .scope_matl = .scope_matl
#     )
# 
#     # construct Query using glue package
#     query <- 
#       glue_sql("
#           {DBI::SQL(config$cte_scope_materials)}
#           SELECT 
#             *
#           FROM 
#             read_parquet({`FN_MATL`})
#                   WHERE 
#           {DBI::SQL(config$where_clause)} 
#         ", .con = config$duckdb_con)
#     
#     # fetch .n results and return as data.table 
#     dbGetQuery(config$duckdb_con, query, n = .n) %>%
#       setDT()
#     
#   }
# 
# fGet_MATS <-
#   function(
#     .material    = NULL, # Optional user-supplied material
#     .salesorg    = NULL, # Optional user-supplied salesorg
#     .scope_matl  = TRUE, # restrict to Pythia Scope
#     .scope_sorg  = TRUE, # restrict to Pythia Scope
#     .n           = Inf   # number of materials to return
#   ){
#     
#     # Get Centralized config
#     config <- .get_duckdb_parts(
#       .material    = .material,
#       .salesorg    = .salesorg,
#       .scope_matl  = .scope_matl,
#       .scope_sorg  = .scope_sorg
#     )
#     
#     # construct Query using glue package 
#     query <- 
#       glue_sql("
#           {DBI::SQL(config$cte_scope_materials)}
#           SELECT 
#             *
#           FROM 
#             read_parquet({`FN_MATS`})
#           WHERE 
#             {DBI::SQL(config$where_clause)}
#         ", .con = config$duckdb_con)
#     
#     # fetch .n results and return as data.table 
#     dbGetQuery(config$duckdb_con, query, n = .n) %>%
#       setDT()
#     
#   }
# 
# fGet_MATP <- 
#   function(
#     .material    = NULL, # Optional user-supplied material
#     .scope_matl  = TRUE, # restrict to Pythia Scope
#     .n           = Inf   # number of materials to return
#   ){
#     
#     # Get Centralized config
#     config <- .get_duckdb_parts(
#       .material   = .material,
#       .scope_matl = .scope_matl
#     )
#     
#     # construct Query using glue package
#     query <- 
#       glue_sql("
#           {DBI::SQL(config$cte_scope_materials)}
#           SELECT 
#             *
#           FROM 
#             read_parquet({`FN_MATP`})
#           WHERE 
#             {DBI::SQL(config$where_clause)}
#         ", .con = config$duckdb_con)
#     
#     # fetch .n results and return as data.table 
#     dbGetQuery(config$duckdb_con, query, n = .n) %>%
#       setDT()
#     
#   }



# Transaction Data Functions ####

## DYN from Dynasys ####
# fGet_DYN <- 
#   function(
#     .vtype       = NULL         , # NULL will get all vtypes
#     .ftype       = NULL         , # NULL will get all ftypes
#     .material    = NULL         , # Optional user-supplied material
#     .salesorg    = NULL         , # Optional user-supplied salesorg
#     .scope_matl  = TRUE         , # restrict to Pythia Scope
#     .scope_sorg  = TRUE         , # restrict to Pythia Scope
#     .cm_min      = '202101'     , # minimal Cal Month
#     .cm_max      = '202506'     , # maximal Cal Month
#     .step_min    = 1            , # minimal forecast step ahead
#     .step_max    = 18           , # maximal forecast step ahead
#     .lagg_min    = NULL         , # minimal diff. between VERSMON & MONTH
#     .lagg_max    = NULL         , # maximal diff. between VERSMON & MONTH      
#     .n           = Inf            # number of materials to return
#   ){
#   
#     # construct Query
#     query <- .make_sql_query_dyn(
#       .vtype       = .vtype, 
#       .ftype       = .ftype,
#       .material    = .material,
#       .salesorg    = .salesorg,
#       .scope_matl  = .scope_matl,
#       .scope_sorg  = .scope_sorg,
#       .cm_min      = .cm_min,
#       .cm_max      = .cm_max,
#       .step_min    = .step_min,
#       .step_max    = .step_max,
#       .lagg_min    = .lagg_min,
#       .lagg_max    = .lagg_max    
#     )
#     
#     # fetch .n results and return as data.table 
#     dbGetQuery(.get_duckdb_conn(), query, n = .n) %>%
#       setDT()
#     
#   }

# ## RTP to Dynasys  ####
# fGet_RTP <- 
#   function(
#     .material    = NULL    , # Optional user-supplied material
#     .salesorg    = NULL    , # Optional user-supplied salesorg
#     .scope_matl  = TRUE   , # restrict to Pythia Scope
#     .scope_sorg  = TRUE    , # restrict to Pythia Scope
#     .cm_min      = '202101', # no data available before this date
#     .cm_max      = '202506', # no data available after this date
#     .n           = Inf       # number of materials to return
#   ){
#     
#     # Get Centralized config
#     config <- .get_duckdb_parts(
#       .material   = .material,
#       .salesorg   = .salesorg,
#       .scope_matl = .scope_matl,
#       .scope_sorg = .scope_sorg,
#       .cm_min     = .cm_min,
#       .cm_max     = .cm_max
#     )
#     
#     # construct Query  
#     query <-
#       glue_sql("
#         {DBI::SQL(config$cte_scope_materials)}
# 
#         SELECT
#           SALESORG,
#           PLANT,
#           MATERIAL,
#        -- SOLDTO,
#           -1                          AS STEP,
#           CALMONTH,
#           '4'                         AS FTYPE,
#           '010'                       AS VTYPE,
#           sum(SLS_QT_SO + SLS_QT_FOC) AS Q,          
#           sum(SLS_QT_SO)              AS SLS,
#           sum(SLS_QT_RET)             AS RET,
#           sum(SLS_QT_FOC)             AS FOC,
#           sum(SLS_QT_DIR)             AS DIR,
#        -- sum(SLS_QT_PRO)             AS PRO,
#           sum(SLS_QT_IC)              AS ICS,
#           sum(MSQTBUO)                AS MSL
#         FROM
#           read_parquet([{`FN_IRTP`}])
#         WHERE
#           {DBI::SQL(config$where_clause)}
#         GROUP BY
#           ALL
#         ORDER BY
#           ALL
#       ", .con = config$duckdb_con)
#     
#     # fetch .n results and return as data.table
#     dbGetQuery(config$duckdb_con, query, n = .n) %>%
#       setDT()
#     
#   }

## RTP to Dynasys  ####
# fGet_IPM <- 
#   function(
#     .material    = NULL    , # Optional user-supplied material
#     .salesorg    = NULL    , # Optional user-supplied salesorg
#     .scope_matl  = TRUE   , # restrict to Pythia Scope
#     .scope_sorg  = TRUE    , # restrict to Pythia Scope
#     .cm_min      = '202101', # no data available before this date
#     .cm_max      = '202506', # no data available after this date
#     .n           = Inf       # number of materials to return
#   ){
#     
#     # Get Centralized config
#     config <- .get_duckdb_parts(
#       .material   = .material,
#       .salesorg   = .salesorg,
#       .scope_matl = .scope_matl,
#       .scope_sorg = .scope_sorg,
#       .cm_min     = .cm_min,
#       .cm_max     = .cm_max
#     )
#     
#     # construct Query  
#     query <-
#       glue_sql("
#         {DBI::SQL(config$cte_scope_materials)}
# 
#         SELECT
#           SALESORG,
#           PLANT,
#           MATERIAL,
#        -- SOLDTO,
#           -1                          AS STEP,
#           CALMONTH,
#           '3'                         AS FTYPE,
#           '010'                       AS VTYPE,
#           sum(SLS_QT_SO + SLS_QT_FOC) AS Q,          
#           sum(SLS_QT_SO)              AS SLS,
#           sum(SLS_QT_RET)             AS RET,
#           sum(SLS_QT_FOC)             AS FOC,
#           sum(SLS_QT_DIR)             AS DIR,
#        -- sum(SLS_QT_PRO)             AS PRO,
#           sum(SLS_QT_IC)              AS ICS,
#           sum(MSQTBUO)                AS MSL
#         FROM
#           read_parquet([{`FN_IIPM`}])
#         WHERE
#           {DBI::SQL(config$where_clause)}
#         GROUP BY
#           ALL
#         ORDER BY
#           ALL
#       ", .con = config$duckdb_con)
#     
#     # fetch .n results and return as data.table
#     dbGetQuery(config$duckdb_con, query, n = .n) %>%
#       setDT()
#     
#   }

# R Functions ####

# Open Folder ####
fOpen_Folder <- 
  function(path){
    # Open the folder in Windows Explorer
    shell.exec(normalizePath(path))
  }

