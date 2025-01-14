# CFG <- 
#   fread(text =
#           "EXP, TYPE, AREA     , FNM                      , OHD      , SAVE , COMP
#    OLD, TD  , SALES    , DD_HISTO_QTY.CSV         , ZSOP_ASLS, TRUE , none
#    OLD, TD  , ORDER    , DD_OPEN_ORDERS_QTY.CSV   , ZSOP_OSLS, TRUE , none
#    OLD, MD  , MATERIAL , MD_MATERIAL.CSV          , ZMAT_ATTR, TRUE , none  
#    OLD, MD  , MAT_SALES, MD_MATERIAL_SALES_ORG.CSV, ZMATSALES, TRUE , none
#    OLD, MD  , MAT_PLANT, MD_MATERIAL_PLANT.CSV    , ZMATPLANT, TRUE , none
#    OLD, MD  , BOM      , MD_BOM.CSV               , ZSOP_BOM , TRUE , none
#    OLD, MD  , PLANT    , MD_PLANT.CSV             , ZPLANT   , TRUE , none   
#    OLD, MD  , PRICE    , MD_PRICE.CSV             , ZPRICING , TRUE , none
#    OLD, MD  , SALES_ORG, MD_SALES_ORG.CSV         , ZSALESORG, TRUE , none
#    OLD, MD  , SOLDTO   , MD_SOLD_TO_CUSTOMER.CSV  , CUST_ATTR, TRUE , none  
#    NEW, TD  , SALES    , DD_HISTO_QTY.CSV         , DSCP_TRAN, TRUE , none
#    NEW, TD  , SLS2123  , DD_SALES_QTY_LE23.csv    , DSCP_TRAN, TRUE , none
#    NEW, TD  , SLS2424  , DD_SALES_QTY_GE24.csv    , DSCP_TRAN, TRUE , none   
#    NEW, TD  , ORDER    , DD_OPEN_ORDERS_QTY.CSV   , DSCP_TRAN, TRUE , none
#    NEW, MD  , MATERIAL , MD_MATERIAL.CSV          , DSCP_MATE, TRUE , none  
#    NEW, MD  , MAT_SALES, MD_MATERIAL_SALES_ORG.CSV, DSCP_MATS, TRUE , none
#    NEW, MD  , MAT_PLANT, MD_MATERIAL_PLANT.CSV    , DSCP_MATP, TRUE , none
#    NEW, MD  , BOM      , MD_BOM.CSV               , DSCP_BOMX, TRUE , none
#    NEW, MD  , PLANT    , MD_PLANT.CSV             , DSCP_PLNT, TRUE , none   
#    NEW, MD  , SALES_ORG, MD_SALES_ORG.CSV         , DSCP_SORG, TRUE , none
#    NEW, MD  , SOLDTO   , MD_SOLD_TO_CUSTOMER.CSV  , DSCP_CUST, TRUE , none     
#    NEW, TD  , SDSFRPR1 , SDSFRPR1.csv             , OH_FRPR1 , TRUE , none 
#    NEW, TD  , SDSFRPR2 , SDSFRPR2_F.csv           , OH_FRPR2 , TRUE , none
#    NEW, TD  , STOCK    , IMP03SM1_F.csv           , OH_STOCK , TRUE , none" 
#   ) %>%
#   .[, c("BNM", "EXT") := tstrsplit(x = FNM, split = "\\.")] 

# OH_DMMG0
# OH_FCWKF
# OH_FRABM
# OH_FRCAC
# OH_FRCT0
# OH_FRPR3
# OH_FRPR4
# OH_VAIT0
# OH_VSSL5


# fGet_FieldNames <- 
#   function(ohdest, sid="WDB100"){
#     
#     if (!exists("dtOHD")){
#       dtOHD <<- 
#         fRead_and_Union(
#           pSIDCLNT = sid,
#           pTable   = "RSBOHFIELDS",
#           pOptions = list("OBJVERS = 'A'")
#         )                                        %T>%
#         .[, FIELDNM:= sub("/BIC/", "", FIELDNM)] %T>%
#         setkey(OHDEST, POSIT)
#       
#     }
#     
#     dtOHD[OHDEST == ohdest, FIELDNM]
#   }
# 
# 
# 
# fLoadOpenHubExport <- 
#   function(
#     pAREA, pCFG = CFG , pENV = 'WPB', pPERKZ = 'perkz_w', 
#     pKEY , pPTH = PYTH, pEXP = 'NEW'){
#    
#    OHD <- pCFG[AREA == pAREA & EXP == pEXP, OHD]   
#    iFN <- pCFG[AREA == pAREA & EXP == pEXP, FNM]
#    wFN <- normalizePath(file.path(pPTH, iFN))
# 
#     RETURN <- 
#       fread(
#         # file        = iFN, 
#         header      = FALSE, 
#         cmd         = paste("iconv -f WINDOWS-1252 -t UTF-8", shQuote(wFN)),
#         colClasses  = "character", 
#         strip.white = FALSE
#       )                                                              %T>%
#       setnames(fGet_FieldNames(OHD))
# 
#     if(!missing(pKEY)){setkeyv(x = RETURN, pKEY)}
#     
#     RETURN
#   }


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
# fGetSalesorgDSCP <- 
#   function(pSIDCLNT = WPB, pType = c("AMB", "SSL") ) {
#     
#     if(missing(pType)) {
#       pType <- pType[1]
#     }
#     
#     if(pType == "AMB") {
#       pOptions <- list("NAME = 'DSCP_AMB_PLANT'")
#     } else if(pType == "SSL") {
#       pOptions <- list("NAME = 'DSCP_SSL_PLANT'")
#     }
#     
#     DSCP_WERKS <- 
#       fGetPlantDSCP(pSIDCLNT, pType) %>%
#       f_or("WERKS")
#     
#     fRead_and_Union(
#       pSIDCLNT  = WPE,
#       pTable    = "T001W",
#       pOptions  = DSCP_WERKS,
#       pFields   = list(
#         
#       ),
#       pRowcount = Inf
#     ) %>%
#       .[, VKORG] %>%
#       unique()
#   }

# fSignLeft <- 
#   function(x){
#     ifelse(
#       grepl(pattern = "-", x),
#       paste0(
#         "-",        
#         sub(
#           pattern     = "-", 
#           replacement = "", 
#           x           = x
#         )
#       ),
#       x
#     ) %>%
#       as.numeric()
#   }

# DuckDB ####

# helper_for_pipe_line <- 
#   function(x){
#     
#     res <- 
#       fread(file = file.path(PS01, SYS, "B4", "B4_RSBOHFIELDS.csv")) %>%
#       .[OHDEST == x, .(
#         FIELDNM = sub("/BIC/", "", FIELDNM), 
#         DATATYPE, POSIT)]                                            %T>%
#       setorder(POSIT)
#       
# 
#     # Calculate the maximum length of FLDNM_IN for alignment
#     res[, no_spc := max(nchar(FIELDNM)) - nchar(FIELDNM) + 3]
#         
#     res <- 
#       res[, glue_data(.SD, 
#                   "{FIELDNM}{strrep(' ', no_spc)}| {DATATYPE} | "
#     )]
#     
#     # Collapse the vector into a single string
#     result <- 
#       paste0(res, collapse = "\n") 
#     
#     clipr::write_clip(result)
#     cat(result) 
#   }

# generate a formatted field PIPE_LINE with field types
gen_fields_in <- 
  function(x) {
    
    # remove unnamed fields
    x <- x[FLDNM_IN != ""]
    
    # Calculate the maximum length of FLDNM_IN for alignment
    x[, no_spc := max(nchar(FLDNM_IN)) - nchar(FLDNM_IN) + 3]
    
    # Generate the formatted string using glue_data
    result <- x[, glue_data(
      .SD, "'{FLDNM_IN}'{strrep(' ', no_spc)}: '{FIELDTP}',")]
    
    # Collapse the vector into a single string
    result <- 
      paste0(result, collapse = "\n") %>%
      substr(1, nchar(.) - 1)
    
    return(result)
  }

# generate a formatted field PIPE_LINE with field types
gen_fields_out <- 
  function(x) {
    
    # remove unnamed fields
    x <- x[FLDNM_OUT != ""]
    
    # Calculate the maximum length of TRNSFRM for alignment
    x[, no_spc := max(nchar(TRNSFRM)) - nchar(TRNSFRM) + 3]
    
    # Generate the formatted string using glue_data
    result <- x[, glue_data(.SD, 
                            "{TRNSFRM}{strrep(' ', no_spc)}AS {FLDNM_OUT},"
    )]
    
    # Collapse the vector into a single string
    result <- paste0(result, collapse = "\n") %>%
      substr(1, nchar(.) - 1)
    
    return(result)
  }

# # generate the read_csv SQL snippet
# gen_sql_to_read_csv <- 
#   function(
#     ffns      ,
#     file_spec ,
#     fields_in,
#     verbose) {
#     
#     sql <- glue(
#      "read_csv('{ffns}',
#         delim      = '{file_spec$DELIM}',
#         header     =  {file_spec$HEADER},
#         dateformat = '{file_spec$DATE_FORMAT}',
#         columns = {{
#           {fields_in}
#         }}
#       )
#      ")
#     
#     # Print the SQL if verbose is TRUE
#     # Use the custom verbose function
#     fVerbose(
#       function_name   = "gen_sql_to_read_csv",
#       function_args   = list(
#         ffns       = ffns,
#         file_spec  = file_spec,
#         fields_in = fields_in
#       ),
#       function_return = sql,
#       verbose         = verbose
#     )
#     
#     return(sql)
#   }

# gen_sql_transformation_rules <- 
#   function(
#     sql_read_csv, 
#     pipe_line   , 
#     con         ,
#     verbose) {
#     
#     # # Use glue_sql to construct SQL query
#     # sql_get_data <- 
#     #   glue_sql("
#     #   SELECT 
#     #    *
#     #   FROM {DBI::SQL(sql_read_csv)}
#     #   ", .con = con)
#     
#     # ({DBI::SQL(sql_get_data)})
#     
#     sql_transformation_rules <- 
#       glue_sql("
#       SELECT 
#         {DBI::SQL(gen_fields_out(pipe_line))}
#       FROM 
#           {DBI::SQL(sql_read_csv)}        
#       ", .con = con)
#     
#     # Print if verbose is TRUE
#     fVerbose(
#       function_name  = "gen_sql_transformation_rules",
#       function_args  = list(
#         sql_read_csv = sql_read_csv,
#         pipe_line    = pipe_line,
#         con          = con
#       ),
#       function_return = sql_transformation_rules,
#       verbose         = verbose
#     )
#     
#     return(sql_transformation_rules)
#   }

# # Function to write data to Parquet
# gen_sql_to_write_data_to_parquet <- 
#   function(
#     sql_transformation_rules, 
#     output_pqt_file   ,
#     verbose
#     ) {
#     
#     sql_write_data <- glue(
#       "COPY ({sql_transformation_rules})
#        TO '{output_pqt_file}'
#        (FORMAT 'parquet', CODEC 'uncompressed')
#       ")
#     
#     # Print if verbose is TRUE
#     fVerbose(
#       function_name   = "gen_sql_to_write_data_to_parquet",
#       function_args   = list(
#         sql_transformation_rules = sql_transformation_rules,
#         output_pqt_file          = output_pqt_file
#       ),
#       function_return = sql_write_data,
#       verbose         = verbose
#     )
#     
#     return(sql_write_data)
#   }

# Main function to transform_csv_to_parquet
# read csv > transform > write parquet
.transform_csv_to_parquet <- 
  function(
    full_file_name, 
    output_path   , 
    file_spec     ,
    pipe_line     ,
    verbose) {

    file_name       <- fs::path_file(full_file_name)
    input_csv_file  <- full_file_name
    output_pqt_file <- file.path(output_path, 
                                 fs::path_ext_set(file_name, "parquet")
                                 )

    # Check if the file extension is "csv"
    if (fs::path_ext(input_csv_file) != "csv") {
      stop(glue("Error: The file is not a CSV file!: {input_csv_file}"))
    }
    
    # Check if input file exists
    if (!file.exists(input_csv_file)) {
      stop(glue("Input CSV file does not exist: {input_csv_file}"))
    }
    
    # Check if the output path exists
    if (!dir.exists(output_path)) {
      # Create the output path
      dir.create(output_path, recursive = TRUE)
    }
    
    # Establish a connection to DuckDB
    con <- .get_duckdb_conn()
    
    # Ensure the connection is closed when the function exits
    on.exit(.close_duckdb_conn(), add = TRUE) 

    # sql_read_csv <- glue("
    #   read_csv('{input_csv_file}',
    #     delim      = '{file_spec$DELIM}',
    #     header     =  {file_spec$HEADER},
    #     dateformat = '{file_spec$DATE_FORMAT}',
    #     columns = {{
    #       {gen_fields_in(pipe_line)}
    #     }} 
    #   )", .con = con) 
    # 
    # sql_transformation_rules <- glue_sql("
    #   SELECT 
    #     {DBI::SQL(gen_fields_out(pipe_line))}
    #   FROM 
    #     {DBI::SQL(sql_read_csv)}        
    #   ", .con = con) 
    # 
    # sql_write_parquet <-  glue("
    #   COPY ({sql_transformation_rules})
    #    TO '{output_pqt_file}'
    #    (FORMAT 'parquet', CODEC 'uncompressed')
    #   ", .con = con) 
    
    # Generate duckdb SQL to transform_csv_to_parquet
    sql_transform_csv_to_parquet <- glue("
      read_csv('{input_csv_file}',
        delim      = '{file_spec$DELIM}',
        header     =  {file_spec$HEADER},
        dateformat = '{file_spec$DATE_FORMAT}',
        columns = {{
          {gen_fields_in(pipe_line)}
        }} 
      )", .con = con
    ) %>% glue_sql("
      SELECT 
        {DBI::SQL(gen_fields_out(pipe_line))}
      FROM 
        {DBI::SQL(sql_read_csv)}        
      ", sql_read_csv = ., .con = con
    ) %>% glue("
      COPY ({sql_transformation_rules})
       TO '{output_pqt_file}'
       (FORMAT 'parquet', CODEC 'uncompressed')
      ", sql_transformation_rules = ., .con = con
    ) 
      
    
    # sql_write_parquet <-  glue("
    #   COPY ({sql_transformation_rules})
    #    TO '{output_pqt_file}'
    #    (FORMAT 'parquet', CODEC 'uncompressed')
    #   ", .con = con) 
 #      # Generate duckdb SQL for reading CSV
 # %>%
 #      # gen_sql_to_read_csv(
 #      #   ffns              = input_csv_file,
 #      #   file_spec         = file_spec,
 #      #   fields_in         = gen_fields_in(pipe_line),
 #      #   verbose           = verbose
 #      # ) %>%
 #      # Generate duckdb SQL for data transformation
 #    %>%
 #      # gen_sql_transformation_rules(
 #      #   pipe_line         = pipe_line,
 #      #   con               = con,
 #      #   verbose           = verbose
 #      # ) %>%
 #      # Generate duckdb SQL for writing data as parquet
 #      gen_sql_to_write_data_to_parquet(
 #        output_pqt_file   = output_pqt_file,
 #        verbose           = verbose
 #      )

    # Execute the SQL to transform_csv_to_parquet
    tryCatch({
      system.time({
        dbExecute(con, sql_transform_csv_to_parquet)
        if (verbose == TRUE){
          print(
            dbGetQuery(con, glue("DESCRIBE SELECT * FROM '{output_pqt_file}';"))
          )
        }
        message(
          cat(black$bgGreen$bold(glue("Data successfully written to {output_pqt_file}")))
        )
      })
    }, error = function(e) {
      message(
        cat(white$bgRed$bold(glue("An error occurred: {e$message}")))
      )
    })
  }

fGetPipeLines <-
  function(){
    
      rbind(
        fread(file = file.path(PS01, SYS, "B4", "B4_PIPELINE_ORG.csv")) %>%
          .[, SRC:= 'O'], 
        fread(file = file.path(PS01, SYS, "B4", "B4_PIPELINE_MOD.csv")) %>%
          .[, SRC:= 'C']
      )                                                                 %T>%
      setorder(SRC, OHDEST, POSIT)                                      %>%
      .[, .SD[1], by = .(OHDEST, POSIT)]
  }

fGetPipeLine <- 
  function(ohdest) {
    
    fGetPipeLines()                                                     %>%
      .[OHDEST == ohdest]                                               %T>%
      setorder(OHDEST, POSIT)                                     
  }

fTransform_csv_to_parquet <- 
  function(
    source_path, 
    output_path, 
    file_pattern,
    file_spec,
    ohdest, 
    verbose){
    
    PIPE_LINE <- fGetPipeLine(ohdest = ohdest)

    # Check if the pipeline for the source exists"
    if (nrow(PIPE_LINE) == 0) {
      stop(glue("Error: The pipeline does not exists!: {ohdest}"))
    }   
 
    # list all relevant files
    fls <- 
      list.files(
        path       = source_path, 
        pattern    = file_pattern, 
        full.names = TRUE
      )
    
    # Check if the at least one soruce file exists
    if (length(fls) == 0) {
      stop(
        glue(
          "Error: No source files!: {source_path} {file_pattern}"))
    }  
    
    # Run the main function to transform data from Bronze to Silver
    purrr::walk(
      .x          = fls, 
      .f          = .transform_csv_to_parquet, 
      output_path = output_path,  
      file_spec   = file_spec, 
      pipe_line   = PIPE_LINE, 
      verbose     = verbose
    ) 
  }

# 
# fTest <- 
#   function(){
# 
#   con <- .get_duckdb_conn()
#   
#   on.exit( .close_duckdb_conn())
#   
#   sql_create_temp_table <- glue_sql("
#     CREATE TABLE SDSFRPR1_1 AS
#     SELECT 
#       *,
#       date_diff(
#         'month',
#         -- Parse VERSMON as YYYYMM + '01' into a date
#         strptime(VERSMON  || '01', '%Y%m%d'),
#         -- Parse CALMONTH as YYYYMM + '01' into a date
#         strptime(CALMONTH || '01', '%Y%m%d')
#       ) as STEP
#     FROM 
#       read_parquet([{`FN_FRPR5`}])
#     WHERE 
#       STEP = -1
#    ", .con = con)
#   
#   dbExecute(con, sql_create_temp_table)
#   
#   FN_FRPR_tmp <- file.path(PDYN, "SDSFRPR1_1.parquet")
#   sql_write_FRPR_tmp_table <- glue_sql("
#     COPY SDSFRPR1_1
#     TO {`FN_FRPR_tmp`}
#     (FORMAT 'parquet', CODEC 'uncompressed')
#     ", .con = con)
#   
#   dbExecute(con, sql_write_FRPR_tmp_table)
#   
#   # delete and rename to original
#   fs::file_delete(FN_FRPR5)
#   fs::file_move(FN_FRPR_tmp, FN_FRPR5)
#   
#   # # Step 2: Read the temporary table
#   # query <- glue_sql("
#   #   SELECT
#   #     *
#   #   FROM 
#   #     read_parquet([{`FN_FRPR5`}])
#   #   ", .con = con)
#   # 
#   # dt <- 
#   #   dbGetQuery(con, query) %>% 
#   #   setDT()
#   # 
#   # 
#   # 
#   # return(dt)
#   
#   }



