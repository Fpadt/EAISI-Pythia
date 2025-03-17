process_forecast_window <- function(
    window_data, 
    w_id, 
    product_hierarchy_spec,
    customer_hierarchy_spec = NULL,  # New parameter for customer hierarchy
    use_cross_hierarchy = FALSE,     # Flag to use cross-product of hierarchies
    forecast_horizon      = 12, 
    pred_interval_level   = 95,
    reconciliation_method = "mint_shrink") {
  
  cat("Processing window", w_id, "\n")
  
  # Extract hierarchy columns from the hierarchy specifications
  product_hierarchy_cols <- unlist(strsplit(gsub(" ", "", product_hierarchy_spec), "/"))
  cat("Product hierarchy columns extracted:", paste(product_hierarchy_cols, collapse=", "), "\n")
  
  # Process customer hierarchy if provided
  customer_hierarchy_cols <- NULL
  if (!is.null(customer_hierarchy_spec) && nchar(customer_hierarchy_spec) > 0) {
    customer_hierarchy_cols <- unlist(strsplit(gsub(" ", "", customer_hierarchy_spec), "/"))
    cat("Customer hierarchy columns extracted:", paste(customer_hierarchy_cols, collapse=", "), "\n")
  }
  
  # Combine hierarchies for key columns
  key_cols <- product_hierarchy_cols
  if (!is.null(customer_hierarchy_cols)) {
    key_cols <- c(key_cols, customer_hierarchy_cols)
  }
  
  # Build combined hierarchy specification if using cross-hierarchy
  hierarchy_spec <- product_hierarchy_spec
  if (!is.null(customer_hierarchy_spec) && nchar(customer_hierarchy_spec) > 0) {
    if (use_cross_hierarchy) {
      # Create cross-product hierarchy: (Product) * (Customer)
      hierarchy_spec <- paste0("(", product_hierarchy_spec, ") * (", customer_hierarchy_spec, ")")
      cat("Using cross-hierarchy specification:", hierarchy_spec, "\n")
    } else {
      # Create simple concatenated hierarchy: Product/Customer
      hierarchy_spec <- paste0(product_hierarchy_spec, "/", customer_hierarchy_spec)
      cat("Using concatenated hierarchy specification:", hierarchy_spec, "\n")
    }
  }
  
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
        key = all_of(key_cols),
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
      as.data.table()
    
    # Get both hierarchies' most detailed level
    filter_expr <- NULL
    
    # Filter for most detailed level of product hierarchy
    if (length(product_hierarchy_cols) > 0) {
      # Get the lowest level column of product hierarchy
      lowest_product_level <- product_hierarchy_cols[length(product_hierarchy_cols)]
      product_filter <- paste0(lowest_product_level, " != '<aggregated>'")
      filter_expr <- product_filter
    }
    
    # Filter for most detailed level of customer hierarchy if provided
    if (length(customer_hierarchy_cols) > 0) {
      # Get the lowest level column of customer hierarchy
      lowest_customer_level <- customer_hierarchy_cols[length(customer_hierarchy_cols)]
      customer_filter <- paste0(lowest_customer_level, " != '<aggregated>'")
      
      # Combine with product filter if it exists
      if (!is.null(filter_expr)) {
        filter_expr <- paste0(filter_expr, " & ", customer_filter)
      } else {
        filter_expr <- customer_filter
      }
    }
    
    # Apply the filter if we have one
    if (!is.null(filter_expr)) {
      cat("Filtering for most detailed level with expression:", filter_expr, "\n")
      dt_forecasts <- dt_forecasts[eval(parse(text = filter_expr))]
    }
    
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

#' Process hierarchical forecasts across multiple windows with multiple hierarchies
#'
#' This function processes data for multiple windows, creating hierarchical
#' forecasts for each window and combining the results. Supports both product
#' and customer hierarchies.
#'
#' @param windowed_data Data segmented by window_id
#' @param product_hierarchy_spec String specifying the product hierarchy format (e.g., "REGION/STORE/MATERIAL")
#' @param customer_hierarchy_spec Optional string specifying the customer hierarchy format (e.g., "CUST1/CUST2/CUSTOMER")
#' @param use_cross_hierarchy Logical, whether to use cross-product of hierarchies (TRUE) or simple concatenation (FALSE)
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
forecast_hierarchical_windows <- function(
    windowed_data, 
    product_hierarchy_spec,
    customer_hierarchy_spec = NULL,
    use_cross_hierarchy = FALSE,
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
      product_hierarchy_spec = product_hierarchy_spec,
      customer_hierarchy_spec = customer_hierarchy_spec,
      use_cross_hierarchy = use_cross_hierarchy,
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

#' Example usage:
#' 
#' # Create windowed data
#' windows <- create_forecast_windows(
#'   dt = your_data,
#'   start_date = as.IDate("2021-01-01"),
#'   forecast_start_date = as.IDate("2023-01-01"),
#'   forecast_end_date = as.IDate("2023-12-01"),
#'   max_horizon = 12,
#'   step = 1
#' )
#' 
#' # Run forecast with both hierarchies
#' results <- forecast_hierarchical_windows(
#'   windowed_data = windows,
#'   product_hierarchy_spec = "PRDH1/PRDH2/PRDH3/PRDH4/MATERIAL",
#'   customer_hierarchy_spec = "CUST1/CUST2/CUSTOMER",
#'   use_cross_hierarchy = TRUE,  # Use cross-product hierarchical structure
#'   forecast_horizon = 12,
#'   pred_interval_level = 95,
#'   reconciliation_method = "mint_shrink"
#' )