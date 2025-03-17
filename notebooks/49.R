
# PATH_SLV_SLS <- pa_ds_stageing_path_get(
#   .staging = "silver", 
#   .functional_area = "sales", 
#   "rtp"
# )

#' @title Export Forecasts for Business Intelligence
#' @description Formats and exports forecasts for use in BI systems
#' @param forecasts The forecast data from extract_detail_forecasts
#' @param file_path Path to export the data
#' @param format Export format ("csv", "rds", "xlsx")
#' @return Invisible, exports file to specified path
#' @import data.table
#' @export
export_for_bi <- function(
    forecasts, 
    date_from  = '202401',
    date_to    = '202412',
    file_path  = "C:\\Users\\flori\\OneDrive\\ET\\pythia\\data\\test\\Platinum\\sales\\", 
    file_name  = 'OUT_PA_DATA_POSTDR_FORECASTS',
    format     = "csv") {
  
  # Store the original scipen value
  old_scipen <- getOption("scipen")
  # Increase scipen to avoid scientific notation
  options(scipen = 999)
  
  # Prepare forecast data for export
  export_data <- copy(forecasts)
  
  # Filter data to the requested date range
  export_data <- 
    export_data[CALMONTH %between% c(date_from, date_to)] %>%
    .[.model == 'ets_reconciled', .model:= '210' ]         %>%
    .[ mean   < 0               , mean  := 0     ]         %>%    
    # Select only relevant columns    
    .[, .(
      VERSMON            ,
      VTYPE      = .model,
      FTYPE      = 2     ,
      MATERIAL           ,
      CL3        = ""    ,
      CL2        = ""    ,
      CL1        = ""    ,
      PLANT      = 'FR30',
      SALESORG   = 'FR30',
      CALMONTH           ,
      DEMND_QTY  = mean  ,
      BSELN_QTY  = 0     ,
      PROMO_QTY  = 0     ,
      DMDCP_QTY  = 0     ,
      PRMCP_QTY  = 0     ,
      BUOM       = 'EA'
    )]                                                     %T>%
    setorder(VERSMON, PLANT, MATERIAL) 
  
  # Export in the requested format
  FN <- paste0(Sys.time() %>% format("%Y%m%d_%H%M%S_"), file_name)
  if (format == "csv") {
    data.table::fwrite(
      x      = export_data, 
      file   = file.path(file_path, paste0(FN, ".", toupper(format))),
      sep    = ";",
      quote  = FALSE)
  } else if (format == "rds") {
    saveRDS(
      object = export_data, 
      file   = file.path(file_path, paste0(FN, ".", format)))
  } else if (format == "xlsx") {
    openxlsx::write.xlsx(
      x      = export_data, 
      file   = file.path(file_path, paste0(FN, ".", format)))
  } else {
    stop("Unsupported format. Use 'csv', 'rds', or 'xlsx'.")
  }
  
  # Revert back to the original scipen value
  options(scipen = old_scipen)
  options(scipen=999)
  
  message(paste("Forecast data exported to", file_path))
  return(invisible(export_data))
}
