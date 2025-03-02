#' Extract only the most granular level forecasts from a fable object
#'
#' This function takes a reconciled fable object and returns only the
#' bottom-level (most granular) forecasts.
#'
#' @param model A reconciled hierarchical model from fable
#' @param h Forecast horizon (number of periods to forecast)
#' @param level Confidence level for prediction intervals (default: 95)
#' @param hierarchy_spec String specifying the hierarchy format (e.g., "REGION/STORE/MATERIAL")
#' @param filter_aggregated Logical, whether to filter out aggregated levels (default: TRUE)
#'
#' @return A data.table with granular forecasts
#'
#' @importFrom fabletools forecast
#' @importFrom data.table as.data.table setDT
#' @export
forecast_granular_level <- function(model, h, level = 95, hierarchy_spec, filter_aggregated = TRUE) {
  # Extract hierarchy columns from the hierarchy specification
  hierarchy_cols <- unlist(strsplit(gsub(" ", "", hierarchy_spec), "/"))
  cat("Hierarchy columns extracted:", paste(hierarchy_cols, collapse=", "), "\n")
  
  # Generate forecasts
  cat("Generating forecasts...\n")
  forecasts <- fabletools::forecast(model, h = h)
  
  # Create intervals
  cat("Adding prediction intervals...\n")
  # First, identify the distribution column to use for prediction intervals
  # Check for common column names: "value", "Q", ".mean"
  potential_columns <- c("value", "Q", ".mean")
  dist_column <- NULL
  
  for (col in potential_columns) {
    if (col %in% colnames(forecasts)) {
      dist_column <- col
      cat("  Using distribution column:", dist_column, "\n")
      break
    }
  }
  
  if (is.null(dist_column)) {
    # If none of the expected columns are found, try to find any distribution column
    # Distribution columns in fable are typically complex S3 objects
    all_cols <- colnames(forecasts)
    for (col in all_cols) {
      # Try to access the first value to check if it's a distribution
      first_val <- tryCatch({
        val <- forecasts[[col]][1]
        if (inherits(val, c("fbl_ts", "dist"))) {
          dist_column <- col
          cat("  Found distribution column:", dist_column, "\n")
          break
        }
      }, error = function(e) NULL)
    }
  }
  
  if (is.null(dist_column)) {
    stop("Could not identify the forecast distribution column. Available columns: ", 
         paste(colnames(forecasts), collapse = ", "))
  }
  
  # Create the intervals using the identified column
  forecasts_with_intervals <- forecasts %>%
    dplyr::mutate(
      PI = fabletools::hilo(!!dplyr::sym(dist_column), level = level)
    )
  
  # Convert to data.table for further processing
  cat("Converting to data.table...\n")
  dt_forecasts <- as.data.table(forecasts_with_intervals)
  
  if (filter_aggregated) {
    cat("Filtering out aggregated levels...\n")
    # Filter to keep only non-aggregated levels
    for (col in hierarchy_cols) {
      if (col %in% names(dt_forecasts)) {
        # Create a logical vector to identify aggregated rows
        is_agg <- sapply(dt_forecasts[[col]], function(x) {
          # Check if the column value is an agg_vec and if it's aggregated
          if (inherits(x, "agg_vec")) {
            # Get the aggregate attribute safely
            agg_attr <- attr(x, "aggregate", exact = TRUE)
            return(!is.null(agg_attr) && agg_attr %in% c(TRUE, 1))
          }
          return(FALSE)
        })
        
        # Filter out aggregated rows
        dt_forecasts <- dt_forecasts[!is_agg]
      }
    }
  }
  
  cat("Extracting forecast values...\n")
  # Extract the point forecast value - check multiple possible column names
  potential_columns <- c("value", "Q", ".mean")
  forecast_column <- NULL
  
  for (col in potential_columns) {
    if (col %in% names(dt_forecasts)) {
      forecast_column <- col
      cat("  Using forecast column:", forecast_column, "\n")
      dt_forecasts[, forecast_mean := as.numeric(get(forecast_column))]
      break
    }
  }
  
  if (is.null(forecast_column)) {
    cat("  Warning: Could not identify standard forecast column. Checking all columns.\n")
    # If not found, look for any numeric column that might be the forecast
    all_cols <- names(dt_forecasts)
    for (col in all_cols) {
      # Check if column has numeric values
      if (!col %in% c(hierarchy_cols) && is.numeric(dt_forecasts[[col]])) {
        tryCatch({
          dt_forecasts[, forecast_mean := as.numeric(get(col))]
          cat("  Using column as forecast:", col, "\n")
          forecast_column <- col
          break
        }, error = function(e) NULL)
      }
    }
  }
  
  if (is.null(forecast_column)) {
    cat("  Warning: Could not identify forecast column. No forecast_mean column will be created.\n")
  }
  
  # Extract prediction intervals
  if ("PI" %in% names(dt_forecasts)) {
    dt_forecasts[, `:=`(
      lower_bound = as.numeric(PI$lower),
      upper_bound = as.numeric(PI$upper)
    )]
  }
  
  cat("Converting hierarchy columns to character...\n")
  # Convert hierarchy columns from agg_vec to character
  for (col in hierarchy_cols) {
    if (col %in% names(dt_forecasts) && inherits(dt_forecasts[[col]], "agg_vec")) {
      dt_forecasts[[col]] <- sapply(dt_forecasts[[col]], function(x) {
        if (is.na(x)) return(NA_character_)
        return(as.character(x))
      })
    }
  }
  
  cat("Forecast generation complete\n")
  return(dt_forecasts)
}

process_forecast_window <- function(
    window_data, 
    w_id, 
    hierarchy_spec,
    forecast_horizon      = 12, 
    pred_interval_level   = 95,
    reconciliation_method = "mint_shrink") {
  
  
  cat("Processing window", w_id, "\n")
  
  # Extract hierarchy columns from the hierarchy specification
  hierarchy_cols <- unlist(strsplit(gsub(" ", "", hierarchy_spec), "/"))
  cat("Hierarchy columns extracted:", paste(hierarchy_cols, collapse=", "), "\n")
  
  # Process this window
  result <- tryCatch({
    # Create tsibble
    ts_data <- 
      window_data                                      %>%
      # add yearmonth and delete CALMONTH      
      .[, `:=` (
        YM       = yearmonth(CALMONTH, format = "%Y%m"),
        CALMONTH = NULL
      )]                                              %>%
      # coerce to tsibble with key and index
      as_tsibble(
        key = all_of(hierarchy_cols),
        index = YM
      )                                               %>%
      # Fill gaps
      fill_gaps(Q = 0, .full = TRUE)
    
    # Apply hierarchical aggregation
    cat("Creating hierarchical aggregation...\n")
    hierarchical_data <- ts_data %>% 
      aggregate_key(
        !!rlang::parse_expr(hierarchy_spec), 
        Q = sum(Q, na.rm = TRUE)
      )
    
    tic("h_fit")
    # Fit ETS models
    cat("Fitting hierarchical models...\n")
    hierarchical_fits <- tryCatch({
      # Try automatic ETS first
      hierarchical_data %>% 
        model(
          `ets` = ETS(Q)
        )
    }, error = function(e) {
      cat("  Using simpler ETS model specification\n")
      # Try simpler model if automatic fails
      hierarchical_data %>% 
        model(
          ets = ETS(Q ~ error("A") + trend("N") + season("N"))
        )
    })
    toc()
    
    tic("recon_fit")
    # Reconcile forecasts
    cat("Reconciling forecasts using method:", reconciliation_method, "\n")
    
    # Create the reconciliation formula dynamically based on the method
    reconciled_fits <- switch(
      reconciliation_method,
      "ols" = {
        hierarchical_fits %>%
          reconcile(ets_reconciled = ols(ets))
      },
      "wls_var" = {
        hierarchical_fits %>%
          reconcile(ets_reconciled = wls_var(ets))
      },
      "mint_shrink" = {
        hierarchical_fits %>%
          reconcile(ets_reconciled = min_trace(models = ets, method = "mint_shrink"))
      },
      "mint_cov" = {
        hierarchical_fits %>%
          reconcile(ets_reconciled = min_trace(models = ets, method = "mint_cov"))
      },
      "wls_struct" = {
        hierarchical_fits %>%
          reconcile(ets_reconciled = min_trace(models = ets, method = "wls_struct"))
      },
      {
        # Default case if an unrecognized method is provided
        warning("Unrecognized reconciliation method: ", reconciliation_method, 
                ". Using mint_shrink instead.")
        hierarchical_fits %>%
          reconcile(ets_reconciled = min_trace(models = ets, method = "mint_shrink"))
      }
    )
    toc()
    
    tic("h_fct")
    # Generate forecasts
    cat("Generating granular forecasts...\n")
    all_forecasts <- forecast(reconciled_fits, h = forecast_horizon)
    
    # Filter to get only reconciled model
    model_name <- "ets_reconciled"
    cat("Filtering for model:", model_name, "\n")
    all_forecasts <- dplyr::filter(all_forecasts, .model == model_name)
    toc()
    
    tic("dt_ext")
    # Convert to data.table with simplified approach
    dt_forecasts <- all_forecasts                          %>%
      as_tibble()                                          %>%
      # Handle agg_vec columns immediately
      mutate(across(where(~inherits(., "agg_vec")), 
                    ~sapply(., as.character)))             %>%
      as.data.table()                                      %>%
      .[MATERIAL != '<aggregated>'] 
    
    # Add the forecast intervals and mean values
    dt_forecasts[, `:=`(
      VERSMON       = w_id,
      STEP          = months_diff(format(YM, "%Y%m"), w_id),
      CALMONTH      = format(YM, "%Y%m"),      
      .distribution = Q,
      mean          = .mean,
      lower_bound   = sapply(Q, function(dist) as.numeric(quantile(dist, 0.025))),
      upper_bound   = sapply(Q, function(dist) as.numeric(quantile(dist, 0.975))),
      .mean         = NULL,
      YM            = NULL,
      Q             = NULL
    )]
    
    # # Filter to keep only the most granular level if needed
    # # For most hierarchical forecasts, we want just the bottom level
    # if (nrow(dt_forecasts) > 10) {  # Only filter if we have many rows
    #   cat("Filtering to keep only the most granular level...\n")
    #   
    #   # This is a simplified approach that keeps only rows where 
    #   # none of the hierarchy columns have an aggregation marker
    #   # (We could skip this if you only need all levels)
    #   granular_rows <- dt_forecasts[, 
    #                                 !Reduce(`|`, lapply(.SD, function(x) 
    #                                   grepl("^\\[.*\\]$", x) | x == "Total")), 
    #                                 .SDcols = hierarchy_cols]
    #   
    #   dt_forecasts <- dt_forecasts[granular_rows]
    #   cat("After filtering:", nrow(dt_forecasts), "rows remaining\n")
    # }
    # 
    # # Keep only relevant columns
    # required_cols <- c("CALMONTH", "YM", "VERSMON", "STEP", 
    #                    hierarchy_cols, 
    #                    "forecast_mean", "lower_bound", "upper_bound", 
    #                    ".model")
    # 
    # # Identify columns present in the data
    # available_cols <- intersect(required_cols, names(dt_forecasts))
    # 
    # # Return only those columns
    # result <- dt_forecasts[, ..available_cols]
    result <- dt_forecasts
    
    toc()
    cat("  Generated forecasts with", nrow(result), "rows\n")
    
    return(result)
  }, error = function(e) {
    cat("ERROR in window", w_id, ":", conditionMessage(e), "\n")
    cat("Error traceback:\n")
    print(sys.calls())
    return(NULL)
  })
  
  return(result)
}

#' Process hierarchical forecasts across multiple windows
#'
#' This function processes data for multiple windows, creating hierarchical
#' forecasts for each window and combining the results.
#'
#' @param windowed_data Data segmented by window_id
#' @param hierarchy_spec String specifying the hierarchy format (e.g., "REGION/STORE/MATERIAL")
#' @param forecast_horizon Number of periods to forecast
#' @param pred_interval_level Confidence level for prediction intervals
#' @param reconciliation_method Character string specifying the reconciliation method (default: "mint_shrink")
#'
#' @details
#' For reconciliation methods, use one of:
#'   - "ols": Ordinary least squares reconciliation
#'   - "wls_var": Weighted least squares using variance scaling
#'   - "mint_shrink": Minimum trace with shrinkage estimation (recommended)
#'   - "mint_cov": Minimum trace using covariance information
#'   - "wls_struct": Weighted least squares using structural information
#'
#' @return A data.table with processed forecasts for all windows
#' @export
forecast_hierarchical_windows <- function(windowed_data, hierarchy_spec,
                                          forecast_horizon = 12, 
                                          pred_interval_level = 95,
                                          reconciliation_method = "mint_shrink") {
  require(data.table)
  require(tictoc)
  
  # Get unique window IDs
  window_ids <- unique(windowed_data$window_id)
  
  # Process each window
  all_forecasts <- lapply(window_ids, function(w_id) {
    # Extract data for this window
    window_data <- windowed_data[window_id == w_id]
    
    tic(paste("Window", w_id))
    # Process this window
    result <- process_forecast_window(
      window_data = window_data,
      w_id = w_id,
      hierarchy_spec = hierarchy_spec,
      forecast_horizon = forecast_horizon,
      pred_interval_level = pred_interval_level,
      reconciliation_method = reconciliation_method
    )
    toc()
    
    return(result)
  })
  
  # Combine all results
  combined_forecasts <- rbindlist(all_forecasts, fill = TRUE)
  
  return(combined_forecasts)
}