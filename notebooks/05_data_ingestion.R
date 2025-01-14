# Description: This script contains the functions to transform the data from Bronze to Silver
# DuckDB ####

#' A Generic Function for Generating Formatted Field Strings
#'
#' @description
#' This internal function filters rows in a \code{data.table}, calculates spacing
#' for alignment, applies a \code{glue} template to generate field strings, and
#' finally collapses them into a single character string.
#'
#' @details
#' \enumerate{
#'   \item Filters out rows where \code{filter_col} is empty (\code{""}).
#'   \item Calculates spacing based on the maximum length of \code{alignment_col}.
#'   \item Applies \code{glue_data} to each row using \code{glue_template}.
#'   \item Collapses the results into a single newline-delimited string, removing
#'         the trailing character (often a comma).
#' }
#'
#' @param pipe_line A \code{data.table} (or similar) containing the fields for
#'   transformations. Must include at least the columns referenced by 
#'   \code{filter_col} and \code{alignment_col}.
#' @param filter_col A string specifying the column used to filter out empty rows.
#' @param alignment_col A string specifying the column used to calculate spacing
#'   for alignment.
#' @param glue_template A string template passed to \code{glue_data}, referencing
#'   columns in \code{pipe_line}. For example, \code{"'{FLDNM_IN}' : '{FIELDTP}',"}.
#'
#' @return A single character string with the formatted field definitions.
#'
#' @keywords internal
.gen_fields_generic <- function(
    pipe_line,
    filter_col,
    alignment_col,
    glue_template
) {
  pipe_line %>%
    # 1. Filter out empty rows based on filter_col
    .[get(filter_col) != ""] %>%
    
    # 2. Calculate spacing for alignment
    .[, no_spc := max(nchar(get(alignment_col))) - nchar(get(alignment_col)) + 3] %>%
    
    # 3. Generate the formatted strings via glue_data
    .[, glue_data(.SD, glue_template)] %>%
    
    # 4. Collapse into a single string with newlines
    glue_collapse(sep = "\n") %>%
    
    # 5. Remove the trailing character (comma or otherwise)
    {\(txt) substr(txt, 1, nchar(txt) - 1)}()
}


#' Generate "Fields In" Using a Generic Template
#'
#' @description
#' Internal wrapper function around \code{.gen_fields_generic} specifically for
#' fields that reference \code{FLDNM_IN} and \code{FIELDTP}.
#'
#' @param pipe_line A \code{data.table} (or similar) with columns 
#'   \code{FLDNM_IN} and \code{FIELDTP} at least.
#'
#' @return A single character string with the formatted \code{FLDNM_IN} fields.
#'
#' @keywords internal
.gen_fields_in <- function(pipe_line) {
  .gen_fields_generic(
    pipe_line     = pipe_line,
    filter_col    = "FLDNM_IN",
    alignment_col = "FLDNM_IN",
    glue_template = "'{FLDNM_IN}'{strrep(' ', no_spc)}: '{FIELDTP}',"
  )
}


#' Generate "Fields Out" Using a Generic Template
#'
#' @description
#' Internal wrapper function around \code{.gen_fields_generic} specifically for
#' fields that reference \code{FLDNM_OUT} and \code{TRNSFRM}.
#'
#' @param pipe_line A \code{data.table} (or similar) with columns 
#'   \code{FLDNM_OUT} and \code{TRNSFRM} at least.
#'
#' @return A single character string with the formatted \code{FLDNM_OUT} fields.
#'
#' @keywords internal
.gen_fields_out <- function(pipe_line) {
  .gen_fields_generic(
    pipe_line     = pipe_line,
    filter_col    = "FLDNM_OUT",
    alignment_col = "FLDNM_OUT",
    glue_template = "{TRNSFRM}{strrep(' ', no_spc)}AS {FLDNM_OUT},"
  )
}

#' Generate a WHERE Clause Using the Generic Fields Function
#'
#' @description
#' A wrapper around \code{.gen_fields_generic} to produce the same output as the
#' original \code{.gen_where_clause()} function. Specifically, it filters out any
#' empty \code{WHERE_CLAUSE} rows, calculates alignment based on \code{WHERE_CLAUSE},
#' and applies a glue template of the form:
#'
#' \code{"\{FLDNM_OUT\}\{strrep(' ', no_spc)\} \{WHERE_CLAUSE\},"}
#'
#' @details
#' This function:
#' \enumerate{
#'   \item Filters rows where \code{WHERE_CLAUSE} is non-empty.
#'   \item Aligns based on the length of \code{WHERE_CLAUSE}.
#'   \item Inserts the columns \code{FLDNM_OUT} and \code{WHERE_CLAUSE} into a glue
#'         template that ends with a comma.
#'   \item Collapses them into one string with newlines, then strips the final comma.
#' }
#'
#' @param pipe_line A \code{data.table} (or similar) that includes at least
#'   \code{FLDNM_OUT} and \code{WHERE_CLAUSE}.
#'
#' @return A single character string identical to what the original
#'   \code{.gen_where_clause()} produced.
#'
#' @keywords internal
.gen_where_clause <- function(pipe_line) {
  
  
  if(nrow(pipe_line[WHERE_CLAUSE != ""]) == 0){
    return("TRUE")
  } else {
    .gen_fields_generic(
      pipe_line     = pipe_line,
      filter_col    = "WHERE_CLAUSE",   # Filter out empty WHERE_CLAUSE rows
      alignment_col = "WHERE_CLAUSE",   # Align based on the length of WHERE_CLAUSE
      glue_template = "{FLDNM_OUT}{strrep(' ', no_spc)} {WHERE_CLAUSE},"
    )
  }
}


#' @title Transform a Single CSV File to Parquet (Internal)
#'
#' @description
#' Internal function that reads a single CSV file, applies transformations, 
#' and writes the result to a Parquet file.
#'
#' @details
#' This function:
#' \enumerate{
#'   \item Ensures the file is indeed a CSV.
#'   \item Reads the CSV using \code{duckdb}, applies transformations.
#'   \item Writes the results to a Parquet file.
#' }
#'
#' @param full_file_name A character string with the full path to the CSV file.
#' @param output_path    A character string with the directory path where the 
#'   Parquet file should be written.
#' @param file_spec      A list-like object with the CSV specifications (e.g. 
#'   delim, header, date format).
#' @param pipe_line      A \code{data.table} (or similar) detailing how 
#'   fields should be transformed.
#' @param verbose        A logical indicating whether to print additional 
#'   information (\code{TRUE}) or not (\code{FALSE}).
#'
#' @return
#' Returns \code{NULL} invisibly. The side effect is creation of a Parquet 
#' file in \code{output_path}.
#'
#' @internal
#'
.transform_csv_to_parquet <- function(
    full_file_name,
    output_path,
    file_spec,
    pipe_line,
    verbose
) {
    file_name       <- fs::path_file(full_file_name)
    input_csv_file  <- full_file_name
    output_pqt_file <- file.path(
      output_path,
      fs::path_ext_set(file_name, "parquet")
    )

    # Check if the file extension is "csv"
    if (fs::path_ext(input_csv_file) != "csv") {
      stop(
        cat(white$bgRed$bold(
          glue("Error: The file is not a CSV file!: {input_csv_file}")
        ))
      )
    }

    # Establish a connection to DuckDB
    con <- .get_duckdb_conn()

    # Ensure the connection is closed when the function exits
    on.exit(.close_duckdb_conn(), add = TRUE)

    # Generate duckdb SQL to transform_csv_to_parquet
    sql_transform_csv_to_parquet <- glue("
      read_csv('{input_csv_file}',
        delim      = '{file_spec$DELIM}',
        header     =  {file_spec$HEADER},
        dateformat = '{file_spec$DATE_FORMAT}',
        columns = {{
          {.gen_fields_in(pipe_line)}
        }}
      )
    ", .con = con ) %>% glue_sql("
      SELECT
        {DBI::SQL(.gen_fields_out(pipe_line))}
      FROM
        {DBI::SQL(sql_read_csv)}
      WHERE
        {DBI::SQL(where_clause)} AND
        TRUE
      ",
        sql_read_csv = .,
        where_clause = .gen_where_clause(pipe_line),
        .con = con ) %>% glue("
      COPY ({sql_transformation_rules})
       TO '{output_pqt_file}'
       (FORMAT 'parquet', CODEC 'uncompressed')
      ", sql_transformation_rules = ., .con = con
    )

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

#' @title Internal Helper: Retrieve a Pipeline by ohdest
#'
#' @description
#' Internal helper function that queries all available pipelines (via \code{fGetPipeLines()})
#' and returns only those relevant for a given \code{ohdest}. The returned data.table is then sorted.
#'
#' @param ohdest A character string indicating which pipeline to filter on.
#'
#' @return
#' Returns a \code{data.table} with pipeline steps (sorted by \code{OHDEST}, \code{POSIT}) for the specified \code{ohdest}.
#'
#' @details
#' This is an internal function (\emph{not exported}) and is intended to be used by other functions
#' within this package. It relies on \code{fGetPipeLines()} to load the master pipeline data first.
#'
#' @keywords internal
.get_pipe_line <- function(ohdest) {
  fGetPipeLines() %>%
    .[OHDEST == ohdest] %T>%
    setorder(OHDEST, POSIT)
}


#' Retrieve All Pipeline Definitions
#'
#' @description
#' Reads two CSV files (\emph{B4_PIPELINE_ORG.csv} and \emph{B4_PIPELINE_MOD.csv}) from a predefined
#' directory structure, merges them, and sorts the resulting data.
#'
#' @details
#' The function:
#' \enumerate{
#'   \item Reads original pipeline definitions from \emph{B4_PIPELINE_ORG.csv}.
#'   \item Reads modified pipeline definitions from \emph{B4_PIPELINE_MOD.csv}.
#'   \item Binds them into a single \code{data.table}.
#'   \item Adds columns like \code{SRC} and \code{WHERE_CLAUSE} as needed.
#'   \item Sorts the final table by \code{SRC}, \code{OHDEST}, and \code{POSIT}.
#'   \item Returns the first row per (\code{OHDEST}, \code{POSIT}) group.
#' }
#'
#' @return
#' A \code{data.table} containing the merged pipeline definitions, with one row per
#' combination of (\code{OHDEST}, \code{POSIT}).
#'
#' @examples
#' \dontrun{
#'   # Fetch pipeline data
#'   dt_pipe <- fGetPipeLines()
#'   head(dt_pipe)
#' }
#'
#' @export
fGetPipeLines <- function() {
  rbind(
    fread(file = file.path(PS01, SYS, "B4", "B4_PIPELINE_ORG.csv")) %>%
      .[, `:=`(SRC = "O", WHERE_CLAUSE = "")],
    fread(file = file.path(PS01, SYS, "B4", "B4_PIPELINE_MOD.csv")) %>%
      .[, `:=`(SRC = "C")]
  ) %T>%
    setorder(SRC, OHDEST, POSIT) %>%
    .[, .SD[1], by = .(OHDEST, POSIT)]
}


#' Transform CSV Files to Parquet
#'
#' @description
#' This function transforms one or more CSV files into Parquet format using a predefined
#' data transformation pipeline (the “pipeline for ohdest”).
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Checks whether both \code{source_path} and \code{output_path} exist.
#'   \item Lists files matching \code{file_pattern} in \code{source_path}.
#'   \item Retrieves the pipeline definition for the specified \code{ohdest}.
#'   \item Invokes an internal function \code{.transform_csv_to_parquet} (called via \code{purrr::walk})
#'         on each file to do the actual transformation.
#' }
#'
#' @param source_path  A character string specifying the path to the source directory containing CSV files.
#' @param output_path  A character string specifying the path to the output directory where Parquet files will be saved.
#' @param file_pattern A character string with a regex pattern to filter which source CSV files to include.
#' @param file_spec    A list or object defining file specifications (e.g., delimiter, headers).
#' @param ohdest       A character string indicating which “ohdest” pipeline configuration to retrieve and use.
#' @param verbose      A logical indicating whether to print verbose messages (\code{TRUE}) or not (\code{FALSE}).
#'
#' @return
#' Returns \code{NULL} invisibly. The side-effect is that new Parquet files are written to \code{output_path}.
#'
#' @seealso
#' \code{\link{.get_pipe_line}}, \code{\link{.transform_csv_to_parquet}}
#'
#' @export
fTransform_csv_to_parquet <- function(
    source_path, 
    output_path, 
    file_pattern,
    file_spec,
    ohdest, 
    verbose
) {
  #--- Check if the source path exists -----------------------------------------
  if (!dir.exists(source_path)) {
    stop(
      cat(
        crayon::white$bgRed$bold(
          glue::glue("Error: The source_path does not exist!: {source_path}")
        )
      )
    )
  }
  
  #--- Check if the output path exists -----------------------------------------
  if (!dir.exists(output_path)) {
    stop(
      cat(
        crayon::white$bgRed$bold(
          glue::glue("Error: The output_path does not exist!: {output_path}")
        )
      )
    )
  }
  
  #--- List all relevant files in source_path ----------------------------------
  fls <- list.files(
    path       = source_path, 
    pattern    = file_pattern, 
    full.names = TRUE
  )
  
  #--- Check if at least one source file exists --------------------------------
  if (length(fls) == 0) {
    stop(
      cat(
        crayon::white$bgRed$bold(
          glue::glue("Error: No source files!: {source_path} {file_pattern}")
        )
      )
    )
  }  
  
  #--- Get Transformation Pipeline ---------------------------------------------
  PIPE_LINE <- .get_pipe_line(ohdest = ohdest)
  
  #--- Check if the pipeline for the source exists -----------------------------
  if (nrow(PIPE_LINE) == 0) {
    stop(
      cat(
        crayon::white$bgRed$bold(
          glue::glue("Error: The pipeline does not exist!: {ohdest}")
        )
      )
    )
  }   
  
  #--- Run the main function to transform data from CSV to Parquet -------------
  purrr::walk(
    .x          = fls, 
    .f          = .transform_csv_to_parquet, 
    output_path = output_path,  
    file_spec   = file_spec, 
    pipe_line   = PIPE_LINE, 
    verbose     = verbose
  ) 
  
  invisible(NULL)
}

