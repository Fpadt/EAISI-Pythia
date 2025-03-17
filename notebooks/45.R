#' @title Fit Hierarchical Model with Multiple Dimensions and Groups
#' @description Fits forecasting models across multiple hierarchical dimensions with optional groups
#' @param data Prepared tsibble data from prepare_multi_hierarchy_data function
#' @param value_col Column name containing the value to forecast
#' @param forecast_periods Number of periods to forecast
#' @param product_hierarchy Character vector specifying the product hierarchy levels
#' @param customer_hierarchy Character vector specifying the customer hierarchy levels, NULL if not used
#' @param group_cols Character vector specifying columns to use for grouping, NULL if not used
#' @param models Named list of model functions to apply at different levels
#' @param reconciliation_method Method for reconciliation (e.g., "MinT", "OLS", "WLS")
#' @param use_fallback Logical, whether to use fallback models if primary models fail
#' @return A hierarchical forecast model with reconciled forecasts
#' @import fabletools fable data.table
#' @export
fit_multi_hierarchy_forecast <- function(data,
                                         value_col = "Sales",
                                         forecast_periods = 12,
                                         product_hierarchy = c("PRDH1", "PRDH2", "PRDH3", "PRDH4", "MATERIAL"),
                                         customer_hierarchy = c("CUST1", "CUST2", "CUSTOMER"),
                                         group_cols = NULL,
                                         models = list(
                                           top = function(x) fable::ARIMA(x),
                                           middle = function(x) fable::ETS(x),
                                           bottom = function(x) fable::SNAIVE(x)
                                         ),
                                         fallback_models = list(
                                           top = function(x) fable::ETS(x),
                                           middle = function(x) fable::SNAIVE(x),
                                           bottom = function(x) fable::MEAN(x)
                                         ),
                                         reconciliation_method = "MinT",
                                         use_fallback = TRUE) {
  
  # Create hierarchy specification with optional groups
  hierarchy_spec <- create_multi_hierarchy_spec(
    data = data,
    product_hierarchy = product_hierarchy,
    customer_hierarchy = customer_hierarchy,
    group_cols = group_cols,
    value_col = value_col
  )
  
  # Create model formula - this needs to reference the value column properly
  formula_str <- paste0(value_col, " ~ 1")
  
  # Determine hierarchy levels
  hierarchy_levels <- c()
  if (!is.null(product_hierarchy) && length(product_hierarchy) > 0) {
    hierarchy_levels <- c(hierarchy_levels, product_hierarchy)
  }
  if (!is.null(customer_hierarchy) && length(customer_hierarchy) > 0) {
    hierarchy_levels <- c(hierarchy_levels, customer_hierarchy)
  }
  
  # Safe modeling function that tries primary model first, then fallback
  # Ensure the formula is properly evaluated
  safe_model <- function(model_func, fallback_func = NULL, use_fallback = TRUE) {
    function(formula) {
      force(formula)  # Force evaluation of the formula argument
      tryCatch({
        model_func(formula)
      }, 
      error = function(e) {
        message(paste("Primary model failed with error:", e$message))
        if (use_fallback && !is.null(fallback_func)) {
          message("Using fallback model.")
          tryCatch({
            fallback_func(formula)
          }, 
          error = function(e2) {
            message(paste("Fallback model also failed with error:", e2$message))
            message("Using simple mean model.")
            fable::MEAN(formula)
          })
        } else {
          message("Using simple mean model.")
          fable::MEAN(formula)
        }
      })
    }
  }
  
  # Create the formula
  model_formula <- as.formula(formula_str)
  
  # Build model specifications more directly
  model_spec <- list()
  
  # Prepare the model functions
  top_model <- safe_model(models$top, fallback_models$top, use_fallback)
  middle_model <- safe_model(models$middle, fallback_models$middle, use_fallback)
  bottom_model <- safe_model(models$bottom, fallback_models$bottom, use_fallback)
  
  # Create a model specification for each level
  if (length(hierarchy_levels) > 0) {
    model_spec <- list(
      # Use the specific model functions with the value column
      top = top_model(model_formula),
      bottom = bottom_model(model_formula)
    )
    
    # Add middle level if we have enough hierarchy levels
    if (length(hierarchy_levels) >= 3) {
      model_spec$middle <- middle_model(model_formula)
    }
  } else {
    # If no hierarchy levels are available, just use the bottom model
    model_spec <- list(
      model = bottom_model(model_formula)
    )
  }
  
  # Apply grouping if specified
  if (!is.null(group_cols) && length(group_cols) > 0) {
    # First, apply grouping
    grouped_data <- data
    for (group in group_cols) {
      grouped_data <- grouped_data %>% dplyr::group_by(!!rlang::sym(group), .add = TRUE)
    }
    
    # Then model with the grouped data
    model_fit <- grouped_data %>%
      fabletools::model(!!!model_spec)
  } else {
    # No grouping needed
    model_fit <- data %>%
      fabletools::model(!!!model_spec)
  }
  
  # Generate reconciled forecasts
  # Only include reconciliation methods that make sense for the data
  reconcile_spec <- list()
  
  # Always include bottom-up
  reconcile_spec$bottom_up <- fabletools::bottom_up()
  
  # Include top-down if we have a hierarchy
  if (length(hierarchy_levels) > 0) {
    reconcile_spec$top_down <- fabletools::top_down()
  }
  
  # Include middle-out if we have at least 3 levels
  if (length(hierarchy_levels) >= 3) {
    reconcile_spec$middle_out <- fabletools::middle_out()
  }
  
  # Add the specified reconciliation method
  # Handle the special case of "mint_shrink"
  if (reconciliation_method == "mint_shrink") {
    reconcile_spec$mint <- fabletools::min_trace(method = "shrink")
  } else {
    reconcile_spec$mint <- fabletools::min_trace(method = reconciliation_method)
  }
  
  # Apply reconciliation and forecast
  reconciled_forecast <- model_fit %>%
    fabletools::reconcile(!!!reconcile_spec) %>%
    fabletools::forecast(h = forecast_periods)
  
  return(list(
    model = model_fit,
    forecast = reconciled_forecast
  ))
}

#' @title Complete Hierarchical Forecasting Workflow
#' @description Runs a complete workflow for hierarchical forecasting with both product and customer dimensions
#' @param data Input data.table with historical data
#' @param date_col Column name for date
#' @param value_col Column name for the value to forecast
#' @param product_hierarchy Vector of product hierarchy columns
#' @param customer_hierarchy Vector of customer hierarchy columns
#' @param forecast_periods Number of periods to forecast
#' @param test_periods Number of periods to hold out for testing
#' @param reconciliation_method Method for reconciliation
#' @return A list with the complete workflow results
#' @import data.table fabletools
#' @export
hierarchical_forecast_workflow <- function(
    data,
    date_col              = "Date",
    value_col             = "Q",
    product_hierarchy     = c("PRDH1", "PRDH2", "PRDH3", "PRDH4", "MATERIAL"),
    customer_hierarchy    = c("CUST1", "CUST2", "CUSTOMER"),
    forecast_periods      = 12,
    test_periods          = 6,
    reconciliation_method = "mint_shrink") {
  
  # Ensure data is a data.table
  if (!data.table::is.data.table(data)) {
    data <- data.table::as.data.table(data)
  }
  
  # Add additional debugging information
  message("Starting hierarchical_forecast_workflow")
  message(paste("Unique dates in data:", paste(head(unique(data[[date_col]])), collapse=", ")))
  message(paste("Number of rows in data:", nrow(data)))
  
  # Split data into training and test sets
  unique_dates <- sort(unique(data[[date_col]]))
  
  # Ensure we have enough data for the requested test periods
  if (length(unique_dates) <= test_periods) {
    stop("Not enough unique dates for the requested test_periods")
  }
  
  train_dates  <- unique_dates[1:(length(unique_dates) - test_periods)]
  test_dates   <- unique_dates[(length(unique_dates) - test_periods + 1):length(unique_dates)]
  
  message(paste("Train dates:", paste(head(train_dates), collapse=", ")))
  message(paste("Test dates:", paste(test_dates, collapse=", ")))
  
  train_data   <- data[get(date_col) %in% train_dates]
  test_data    <- data[get(date_col) %in% test_dates]
  
  message(paste("Train data rows:", nrow(train_data)))
  message(paste("Test data rows:", nrow(test_data)))
  
  # Define simplified model functions to avoid the 'x' not found error
  # Use explicit formulas instead of relying on parameter passing
  simple_models <- list(
    top = function(formula) fable::ARIMA(formula),
    middle = function(formula) fable::ETS(formula),
    bottom = function(formula) fable::SNAIVE(formula)
  )
  
  simple_fallback_models <- list(
    top = function(formula) fable::ETS(formula),
    middle = function(formula) fable::SNAIVE(formula),
    bottom = function(formula) fable::MEAN(formula)
  )
  
  # Initialize result structure
  result <- list()
  
  # 1. Product hierarchy only approach
  if (!is.null(product_hierarchy) && length(product_hierarchy) > 0) {
    message("Processing product hierarchy approach")
    
    tryCatch({
      product_only_data <- prepare_multi_hierarchy_data(
        data                  = train_data,
        date_col              = date_col,
        value_col             = value_col,
        product_hierarchy     = product_hierarchy,
        customer_hierarchy    = NULL
      )
      
      product_only_fit <- fit_multi_hierarchy_forecast(
        data                  = product_only_data,
        value_col             = value_col,
        forecast_periods      = forecast_periods,
        product_hierarchy     = product_hierarchy,
        customer_hierarchy    = NULL,
        models                = simple_models,               # Explicitly pass the models
        fallback_models       = simple_fallback_models,      # Explicitly pass the fallback models
        reconciliation_method = reconciliation_method
      )
      
      result$product_only <- product_only_fit
    }, error = function(e) {
      message(paste("Error in product hierarchy approach:", e$message))
      result$product_only <- NULL
    })
  }
  
  # 2. Customer hierarchy only approach
  if (!is.null(customer_hierarchy) && length(customer_hierarchy) > 0) {
    message("Processing customer hierarchy approach")
    
    tryCatch({
      customer_only_data <- prepare_multi_hierarchy_data(
        data                  = train_data,
        date_col              = date_col,
        value_col             = value_col,
        product_hierarchy     = NULL,
        customer_hierarchy    = customer_hierarchy
      )
      
      customer_only_fit <- fit_multi_hierarchy_forecast(
        data                  = customer_only_data,
        value_col             = value_col,
        forecast_periods      = forecast_periods,
        product_hierarchy     = NULL,
        customer_hierarchy    = customer_hierarchy,
        models                = simple_models,               # Explicitly pass the models
        fallback_models       = simple_fallback_models,      # Explicitly pass the fallback models
        reconciliation_method = reconciliation_method
      )
      
      result$customer_only <- customer_only_fit
    }, error = function(e) {
      message(paste("Error in customer hierarchy approach:", e$message))
      result$customer_only <- NULL
    })
  }
  
  # 3. Combined hierarchies approach
  if (!is.null(product_hierarchy) && length(product_hierarchy) > 0 &&
      !is.null(customer_hierarchy) && length(customer_hierarchy) > 0) {
    message("Processing combined hierarchy approach")
    
    tryCatch({
      combined_data <- prepare_multi_hierarchy_data(
        data                  = train_data,
        date_col              = date_col,
        value_col             = value_col,
        product_hierarchy     = product_hierarchy,
        customer_hierarchy    = customer_hierarchy
      )
      
      combined_fit <- fit_multi_hierarchy_forecast(
        data                  = combined_data,
        value_col             = value_col,
        forecast_periods      = forecast_periods,
        product_hierarchy     = product_hierarchy,
        customer_hierarchy    = customer_hierarchy,
        models                = simple_models,               # Explicitly pass the models
        fallback_models       = simple_fallback_models,      # Explicitly pass the fallback models
        reconciliation_method = reconciliation_method
      )
      
      result$combined <- combined_fit
    }, error = function(e) {
      message(paste("Error in combined hierarchy approach:", e$message))
      result$combined <- NULL
    })
  }
  
  # Determine which approaches were successfully processed
  successful_approaches <- names(result)[!sapply(result, is.null)]
  
  if (length(successful_approaches) == 0) {
    stop("All hierarchical forecasting approaches failed")
  }
  
  message(paste("Successful approaches:", paste(successful_approaches, collapse=", ")))
  
  # Add results to the output
  result$successful_approaches <- successful_approaches
  
  # Attempt to compare approaches if multiple were successful
  if (length(successful_approaches) > 1 && !is.null(test_data) && nrow(test_data) > 0) {
    message("Comparing successful approaches")
    
    tryCatch({
      comparison <- compare_hierarchy_approaches(
        data              = train_data,
        test_data         = test_data,
        value_col         = value_col,
        product_hierarchy = product_hierarchy,
        customer_hierarchy = customer_hierarchy
      )
      
      result$comparison <- comparison
      
      # Analyze results if comparison was successful
      if (!is.null(comparison) && nrow(comparison) > 0) {
        analysis <- analyze_hierarchy_influence(comparison)
        result$analysis <- analysis
        
        # Extract the best approach based on RMSE if available
        if (!is.null(analysis$approach_summary) && nrow(analysis$approach_summary) > 0) {
          best_approach <- analysis$approach_summary[which.min(RMSE), approach]
          result$best_approach <- best_approach
        }
      }
    }, error = function(e) {
      message(paste("Error in approach comparison:", e$message))
    })
  }
  
  # Return the successful parts of the workflow
  return(result)
}