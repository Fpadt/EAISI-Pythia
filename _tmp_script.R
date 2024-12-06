yr               <- 24
sys_value        <- "WPB"
input_base_path  <- PS01
output_base_path <- PS02

input_csv_file  <- 
  file.path(
    input_base_path , sys_value, "RTP",  
    paste0("DD_SALES_QTY_20", yr, ".CSV")
  )
output_pqt_file <- 
  file.path(
    output_base_path, sys_value, "RTP", 
    paste0("DD_SALES_QTY_20", yr, ".parquet")
  )

formatted_columns <- generate_formatted_columns(DSCP_TRAN$COLUMN_DEF)

# Generate SLS_CSV
duckdb_read_csv <- 
  generate_duckdb_read_csv(
    ffns              = input_csv_file,
    delim             = DSCP_TRAN$DELIM,
    header            = DSCP_TRAN$HEADER,
    date_format       = DSCP_TRAN$DATE_FORMAT,
    formatted_columns = formatted_columns
  )

# Establish a connection to DuckDB
con <- dbConnect(duckdb(), dbdir = ":memory:")

duckdb_read_csv_sql <- duckdb_read_csv

# Generate SQL for data transformation
sql_transform_data <- 
  transform_data_sql(duckdb_read_csv_sql = duckdb_read_csv, con = con)

