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
                                           top = function(x) ARIMA(x),
                                           middle = function(x) ETS(x),
                                           bottom = function(x) SNAIVE(x)
                                         ),
                                         fallback_models = list(
                                           top = function(x) ETS(x),
                                           middle = function(x) SNAIVE(x),
                                           bottom = function(x) MEAN(x)
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
  
  # Create model formula
  formula_str <- paste0(value_col, " ~ ")
  
  # Determine hierarchy levels
  hierarchy_levels <- c()
  if (!is.null(product_hierarchy) && length(product_hierarchy) > 0) {
    hierarchy_levels <- c(hierarchy_levels, product_hierarchy)
  }
  if (!is.null(customer_hierarchy) && length(customer_hierarchy) > 0) {
    hierarchy_levels <- c(hierarchy_levels, customer_hierarchy)
  }
  
  # Safe modeling function that tries primary model first, then fallback
  safe_model <- function(model_func, fallback_func = NULL, use_fallback = TRUE) {
    function(data) {
      tryCatch({
        model_func(data)
      }, 
      error = function(e) {
        if (use_fallback && !is.null(fallback_func)) {
          message("Primary model failed. Using fallback model.")
          tryCatch({
            fallback_func(data)
          }, 
          error = function(e2) {
            message("Fallback model also failed. Using simple mean model.")
            MEAN(data)
          })
        } else {
          message("Model failed and no fallback specified. Using simple mean model.")
          MEAN(data)
        }
      })
    }
  }
  
  # Create proper hierarchical specifications for each level
  # Use create_multi_hierarchy_spec to create proper hierarchical specifications
  top_spec <- NULL
  middle_spec <- NULL
  
  # For top level, we need a separate specification
  if (length(hierarchy_levels) > 0) {
    # For top level, create a spec with just the top level
    if (!is.null(product_hierarchy) && length(product_hierarchy) > 0) {
      top_level_product <- product_hierarchy[1, drop = FALSE]
      top_level_customer <- if (!is.null(customer_hierarchy) && length(customer_hierarchy) > 0) 
        customer_hierarchy[1, drop = FALSE] else NULL
    } else {
      top_level_product <- NULL
      top_level_customer <- if (!is.null(customer_hierarchy) && length(customer_hierarchy) > 0) 
        customer_hierarchy[1, drop = FALSE] else NULL
    }
    
    top_spec <- create_multi_hierarchy_spec(
      data = data,
      product_hierarchy = top_level_product,
      customer_hierarchy = top_level_customer,
      group_cols = group_cols,
      value_col = value_col
    )
    
    # For middle level, create a spec with the middle level
    if (length(hierarchy_levels) >= 3) {
      middle_idx <- ceiling(length(hierarchy_levels)/2)
      
      if (!is.null(product_hierarchy) && length(product_hierarchy) > 0) {
        if (middle_idx <= length(product_hierarchy)) {
          middle_level_product <- product_hierarchy[1:middle_idx, drop = FALSE]
          middle_level_customer <- customer_hierarchy
        } else {
          middle_level_product <- product_hierarchy
          middle_idx_cust <- middle_idx - length(product_hierarchy)
          middle_level_customer <- if (!is.null(customer_hierarchy) && length(customer_hierarchy) >= middle_idx_cust)
            customer_hierarchy[1:middle_idx_cust, drop = FALSE] else customer_hierarchy
        }
      } else {
        middle_level_product <- NULL
        middle_level_customer <- if (!is.null(customer_hierarchy) && length(customer_hierarchy) >= middle_idx)
          customer_hierarchy[1:middle_idx, drop = FALSE] else customer_hierarchy
      }
      
      middle_spec <- create_multi_hierarchy_spec(
        data = data,
        product_hierarchy = middle_level_product,
        customer_hierarchy = middle_level_customer,
        group_cols = group_cols,
        value_col = value_col
      )
    }
  }
  
  # Fit models with different approaches based on hierarchy level and optional groups
  model_spec <- list()
  
  # Only add specifications if they were successfully created
  if (!is.null(top_spec)) {
    model_spec$top <- fabletools::model(data, 
                                        top = safe_model(models$top, fallback_models$top, use_fallback)(as.formula(formula_str))
    )
  }
  
  if (!is.null(middle_spec)) {
    model_spec$middle <- fabletools::model(data,
                                           middle = safe_model(models$middle, fallback_models$middle, use_fallback)(as.formula(formula_str))
    )
  }
  
  # Bottom level always uses the full hierarchy spec
  model_spec$bottom <- fabletools::model(data,
                                         bottom = safe_model(models$bottom, fallback_models$bottom, use_fallback)(as.formula(formula_str))
  )
  
  # Apply grouping if specified
  if (!is.null(group_cols) && length(group_cols) > 0) {
    model_fit <- data %>%
      dplyr::group_by_at(dplyr::vars(dplyr::one_of(group_cols))) %>%
      fabletools::model(!!!model_spec)
  } else {
    model_fit <- fabletools::model(data, !!!model_spec)
  }
  
  # Generate reconciled forecasts
  reconcile_spec <- list(
    bottom_up = fabletools::bottom_up()
  )
  
  if (!is.null(top_spec)) {
    reconcile_spec$top_down <- fabletools::top_down()
  }
  
  if (!is.null(middle_spec)) {
    reconcile_spec$middle_out <- fabletools::middle_out()
  }
  
  reconcile_spec$mint <- fabletools::min_trace(method = reconciliation_method)
  
  reconciled_forecast <- model_fit %>%
    fabletools::reconcile(!!!reconcile_spec) %>%
    fabletools::forecast(h = forecast_periods)
  
  return(list(
    model = model_fit,
    forecast = reconciled_forecast
  ))
}