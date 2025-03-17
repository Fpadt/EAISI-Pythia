#' @title Create Hierarchical Specification with Multiple Dimensions and Groups
#' @description Generate a hierarchical specification for reconciliation with multiple dimensions and optional groups
#' @param data A data.table containing the hierarchical columns
#' @param product_hierarchy Character vector specifying the product hierarchy levels
#' @param customer_hierarchy Character vector specifying the customer hierarchy levels, NULL if not used
#' @param group_cols Character vector specifying columns to use for grouping, NULL if not used
#' @param value_col Column name containing the value to forecast
#' @return A hierarchical specification object for use with fabletools
#' @import data.table fabletools rlang
#' @export
create_multi_hierarchy_spec <- function(data, 
                                        product_hierarchy = c("PRDH1", "PRDH2", "PRDH3", "PRDH4", "MATERIAL"),
                                        customer_hierarchy = c("CUST1", "CUST2", "CUSTOMER"),
                                        group_cols = NULL,
                                        value_col = "Q") {
  
  # Build hierarchy spec string
  if (!is.null(product_hierarchy) && length(product_hierarchy) > 0 && 
      !is.null(customer_hierarchy) && length(customer_hierarchy) > 0) {
    # Both hierarchies - create cross-product
    hierarchy_spec <- paste0(
      "(",
      paste(product_hierarchy, collapse = " / "),
      ") * (",
      paste(customer_hierarchy, collapse = " / "),
      ")"
    )
  } else if (!is.null(product_hierarchy) && length(product_hierarchy) > 0) {
    # Product hierarchy only
    hierarchy_spec <- paste(product_hierarchy, collapse = " / ")
  } else if (!is.null(customer_hierarchy) && length(customer_hierarchy) > 0) {
    # Customer hierarchy only
    hierarchy_spec <- paste(customer_hierarchy, collapse = " / ")
  } else {
    stop("At least one hierarchy must be specified.")
  }
  
  # Directly adapt your working code pattern
  if (!is.null(group_cols) && length(group_cols) > 0) {
    # Check if groups exist in the data
    missing_groups <- setdiff(group_cols, names(data))
    if (length(missing_groups) > 0) {
      stop(paste("Group columns not found in data:", paste(missing_groups, collapse = ", ")))
    }
    
    # First, apply grouping to data
    grouped_data <- data
    for (group in group_cols) {
      grouped_data <- grouped_data %>% dplyr::group_by(!!rlang::sym(group), .add = TRUE)
    }
    
    # Create aggregation with the parse_expr approach similar to your working code
    hierarchy_spec_obj <- grouped_data %>%
      fabletools::aggregate_key(
        !!rlang::parse_expr(hierarchy_spec),
        !!rlang::sym(value_col) := sum(!!rlang::sym(value_col), na.rm = TRUE)
      )
  } else {
    # No grouping
    hierarchy_spec_obj <- data %>%
      fabletools::aggregate_key(
        !!rlang::parse_expr(hierarchy_spec),
        !!rlang::sym(value_col) := sum(!!rlang::sym(value_col), na.rm = TRUE)
      )
  }
  
  return(hierarchy_spec_obj)
}

#' @title Prepare Data for Multi-Dimensional Hierarchical Forecasting with Groups
#' @description Transforms and prepares data for hierarchical forecasting with multiple dimensions and optional groups
#' @param data A data.table containing sales data
#' @param date_col Column name containing the date
#' @param value_col Column name containing the value to forecast
#' @param product_hierarchy Character vector specifying the product hierarchy levels
#' @param customer_hierarchy Character vector specifying the customer hierarchy levels, NULL if not used
#' @param group_cols Character vector specifying columns to use for grouping, NULL if not used
#' @param frequency Frequency of the time series (e.g., "month", "week")
#' @return A tsibble object with proper grouping for hierarchical forecasting
#' @import data.table tsibble
#' @export
prepare_multi_hierarchy_data <- function(
    data,
    date_col           = "Date",
    value_col          = "Q",
    product_hierarchy  = c("PRDH1", "PRDH2", "PRDH3", "PRDH4", "MATERIAL"),
    customer_hierarchy = c("CUST1", "CUST2", "CUSTOMER"),
    group_cols         = NULL,
    frequency          = "month") {
  
  # Ensure data is a data.table
  if (!data.table::is.data.table(data)) {
    data <- data.table::as.data.table(data)
  }
  
  # Validate column existence
  required_cols <- c(date_col, value_col)
  if (!is.null(product_hierarchy)) {
    required_cols <- c(required_cols, product_hierarchy)
  }
  if (!is.null(customer_hierarchy)) {
    required_cols <- c(required_cols, customer_hierarchy)
  }
  if (!is.null(group_cols)) {
    required_cols <- c(required_cols, group_cols)
  }
  
  missing_cols <- setdiff(required_cols, names(data))
  if (length(missing_cols) > 0) {
    stop(paste("Missing columns in data:", paste(missing_cols, collapse = ", ")))
  }
  
  # Define key columns based on hierarchies in use
  key_cols <- c()
  if (!is.null(product_hierarchy) && length(product_hierarchy) > 0) {
    key_cols <- c(key_cols, product_hierarchy)
  }
  if (!is.null(customer_hierarchy) && length(customer_hierarchy) > 0) {
    key_cols <- c(key_cols, customer_hierarchy)
  }
  if (length(key_cols) == 0) {
    stop("At least one hierarchy must be specified.")
  }
  
  # Convert to tsibble with proper indexing
  if (frequency == "month") {
    data[, date_temp := as.Date(get(date_col))]
    data[, yearmonth := tsibble::yearmonth(date_temp)]
    ts_data <- tsibble::as_tsibble(
      data,
      key = key_cols,
      index = yearmonth
    )
  } else if (frequency == "week") {
    data[, date_temp := as.Date(get(date_col))]
    data[, yearweek := tsibble::yearweek(date_temp)]
    ts_data <- tsibble::as_tsibble(
      data,
      key = key_cols,
      index = yearweek
    )
  } else if (frequency == "day") {
    data[, date_temp := as.Date(get(date_col))]
    ts_data <- tsibble::as_tsibble(
      data,
      key = key_cols,
      index = date_temp
    )
  } else {
    stop("Unsupported frequency. Supported values: 'month', 'week', 'day'")
  }
  
  # Apply grouping if group columns are specified
  if (!is.null(group_cols) && length(group_cols) > 0) {
    ts_data <- ts_data %>%
      dplyr::group_by_at(dplyr::vars(dplyr::one_of(group_cols)))
  }
  
  # Clean up temporary columns
  if (frequency != "day") {
    ts_data <- ts_data %>% dplyr::select(-date_temp)
  }
  
  return(ts_data)
}

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
    group_cols = group_cols
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
  
  # Fit models with different approaches based on hierarchy level and optional groups
  model_spec <- list(
    top = fabletools::aggregate_key(
      hierarchy_levels[1],
      safe_model(models$top, fallback_models$top, use_fallback)(as.formula(formula_str))
    ),
    middle = fabletools::aggregate_key(
      hierarchy_levels[ceiling(length(hierarchy_levels)/2)],
      safe_model(models$middle, fallback_models$middle, use_fallback)(as.formula(formula_str))
    ),
    bottom = fabletools::aggregate_key(
      hierarchy_spec,
      safe_model(models$bottom, fallback_models$bottom, use_fallback)(as.formula(formula_str))
    )
  )
  
  # Apply grouping if specified
  if (!is.null(group_cols) && length(group_cols) > 0) {
    model_fit <- data %>%
      dplyr::group_by_at(dplyr::vars(dplyr::one_of(group_cols))) %>%
      fabletools::model(!!!model_spec)
  } else {
    model_fit <- data %>%
      fabletools::model(!!!model_spec)
  }
  
  # Generate reconciled forecasts
  reconciled_forecast <- model_fit %>%
    fabletools::reconcile(
      top_down = fabletools::top_down(agg_mask = hierarchy_levels[1]),
      middle_out = fabletools::middle_out(agg_mask = hierarchy_levels[ceiling(length(hierarchy_levels)/2)]),
      bottom_up = fabletools::bottom_up(),
      mint = fabletools::min_trace(method = reconciliation_method)
    ) %>%
    fabletools::forecast(h = forecast_periods)
  
  return(list(
    model = model_fit,
    forecast = reconciled_forecast
  ))
}


#' @title Extract Material-Customer Level Forecasts
#' @description Extracts the most detailed level forecasts (MATERIAL/CUSTOMER)
#' @param reconciled_forecast The reconciled forecast object from fit_multi_hierarchy_forecast
#' @param method The reconciliation method to extract (e.g., "mint", "bottom_up")
#' @param product_level The product level to extract, typically the lowest level ("MATERIAL")
#' @param customer_level The customer level to extract, typically the lowest level ("CUSTOMER")
#' @return A data.table with the extracted forecasts at MATERIAL/CUSTOMER level
#' @import data.table fabletools
#' @export
extract_detail_forecasts <- function(
    reconciled_forecast,
    method         = "mint",
    product_level  = "MATERIAL",
    customer_level = "CUSTOMER") {
  
  # Filter for the selected reconciliation method
  method_forecast <- reconciled_forecast %>%
    dplyr::filter(.model == method)
  
  # Extract the most detailed level (MATERIAL/CUSTOMER)
  if (!is.null(customer_level)) {
    detail_forecast <- method_forecast %>%
      dplyr::filter(get(product_level) != "Total" & get(customer_level) != "Total")
  } else {
    detail_forecast <- method_forecast %>%
      dplyr::filter(get(product_level) != "Total")
  }
  
  # Convert to data.table for easier manipulation
  result <- data.table::as.data.table(detail_forecast)
  
  return(result)
}

#' @title Create Hierarchical Aggregates with Groups
#' @description Creates hierarchical aggregates with support for groups as provided function
#' @param dt Data table with raw data
#' @param hierarchy_cols Character vector of hierarchy columns from most detailed to most aggregate
#' @param group_cols Character vector of grouping columns
#' @param value_col Column name containing the value to aggregate
#' @param date_col Column name containing the date
#' @param level_col Column name to store hierarchy level info
#' @return A data.table with hierarchical aggregates for all group combinations
#' @import data.table purrr
#' @export
create_hierarchical_aggregates_with_groups <- function(
    dt, 
    hierarchy_cols, 
    group_cols = NULL,
    value_col = "Q", 
    date_col = "YM", 
    level_col = "level") {
  # Make a copy of the data
  dt_copy <- copy(dt)
  
  # Create combinations of all grouping variables
  if (!is.null(group_cols) && length(group_cols) > 0) {
    # List to hold data for each group combination
    grouped_results <- list()
    
    # Get all unique combinations of group variables
    group_combinations <- unique(dt_copy[, ..group_cols])
    
    # For each combination of group variables
    for (i in 1:nrow(group_combinations)) {
      # Extract the current combination
      current_combo <- group_combinations[i]
      
      # Create a filter condition to match this combination
      filter_condition <- paste0(
        sapply(names(current_combo), function(col) {
          paste0(col, " == '", current_combo[[col]], "'")
        }),
        collapse = " & "
      )
      
      # Filter data to get just this group combination
      group_data <- dt_copy[eval(parse(text = filter_condition))]
      
      # Now create hierarchical aggregates within this group
      if (nrow(group_data) > 0) {
        # Mark the original data with the most detailed level name
        group_data[, (level_col) := hierarchy_cols[1]]
        
        # Function to create aggregates for a specific level
        create_level_aggregate <- function(level_idx) {
          # Current level name
          current_level <- hierarchy_cols[level_idx]
          
          # Columns to group by (current level, more aggregated levels, and all group columns)
          group_cols_for_agg <- c(date_col, 
                                  hierarchy_cols[level_idx:length(hierarchy_cols)],
                                  group_cols)
          
          # Create aggregation formula
          agg_formula   <- paste0(".(", value_col, " = sum(", value_col, ", na.rm = TRUE))")
          
          # Perform aggregation
          level_dt <- group_data[, eval(parse(text = agg_formula)), by = group_cols_for_agg]
          
          # Add level identifier
          level_dt[, (level_col) := current_level]
          
          return(level_dt)
        }
        
        # Generate aggregates for all levels except the most detailed
        level_indices <- 2:length(hierarchy_cols)
        
        # Use purrr to create aggregated levels
        if (length(level_indices) > 0) {
          aggregated_levels <- purrr::map(level_indices, create_level_aggregate)
          all_levels <- c(list(group_data), aggregated_levels)
        } else {
          all_levels <- list(group_data)
        }
        
        # Combine all levels for this group
        grouped_results[[i]] <- rbindlist(all_levels, fill = TRUE)
      }
    }
    
    # Combine all group results
    dt_hierarchical <- rbindlist(grouped_results, fill = TRUE)
    
  } else {
    # No groups - just do regular hierarchical aggregation
    dt_copy[, (level_col) := hierarchy_cols[1]]
    
    # Function to create aggregates for a specific level
    create_level_aggregate <- function(level_idx) {
      
      current_level <- hierarchy_cols[level_idx]
      group_cols    <- c(date_col, hierarchy_cols[level_idx:length(hierarchy_cols)])
      agg_formula   <- paste0(".(", value_col, " = sum(", value_col, ", na.rm = TRUE))")
      
      level_dt      <- dt_copy[, eval(parse(text = agg_formula)), by = group_cols]
      level_dt[, (level_col) := current_level]
      
      return(level_dt)
    }
    
    level_indices <- 2:length(hierarchy_cols)
    
    if (length(level_indices) > 0) {
      aggregated_levels <- purrr::map(level_indices, create_level_aggregate)
      all_levels <- c(list(dt_copy), aggregated_levels)
    } else {
      all_levels <- list(dt_copy)
    }
    
    dt_hierarchical <- rbindlist(all_levels, fill = TRUE)
  }
  
  return(dt_hierarchical)
}

#' @title Compare Forecast Accuracy Across Hierarchies and Groups
#' @description Evaluates and compares forecasting accuracy with different hierarchical approaches and grouping strategies
#' @param data Original data used for training
#' @param test_data Holdout data for evaluation
#' @param value_col Column name containing the value to forecast
#' @param product_hierarchy Character vector specifying the product hierarchy levels
#' @param customer_hierarchy Character vector specifying the customer hierarchy levels
#' @param potential_group_cols List of potential grouping variables to test
#' @param approaches List of hierarchical approaches to compare (product only, customer only, both)
#' @param reconciliation_methods Vector of reconciliation methods to compare
#' @return A data.table with accuracy metrics for each approach, grouping strategy, and method
#' @import data.table fabletools
#' @export
compare_hierarchy_approaches <- function(
    data,
    test_data,
    value_col = "Q",
    product_hierarchy = c("PRDH1", "PRDH2", "PRDH3", "PRDH4", "MATERIAL"),
    customer_hierarchy = c("CUST1", "CUST2", "CUSTOMER"),
    potential_group_cols = NULL,
    approaches = list(
      product_only = list(
        product = product_hierarchy,
        customer = NULL
      ),
      customer_only = list(
        product = NULL,
        customer = customer_hierarchy
      ),
      both = list(
        product = product_hierarchy,
        customer = customer_hierarchy
      )
    ),
    reconciliation_methods = c("mint", "bottom_up", "top_down", "middle_out")) {
  
  results <- data.table::data.table()
  
  # Create a list of grouping options to try
  group_options <- list(NULL)  # Start with no grouping
  
  # Add individual potential grouping variables
  if (!is.null(potential_group_cols) && length(potential_group_cols) > 0) {
    for (col in potential_group_cols) {
      group_options <- c(group_options, list(col))
    }
    
    # Optionally add combinations of group columns if there are multiple
    if (length(potential_group_cols) > 1) {
      for (i in 2:min(length(potential_group_cols), 3)) {  # Limit to at most 3 group combos
        combos <- utils::combn(potential_group_cols, i, simplify = FALSE)
        group_options <- c(group_options, combos)
      }
    }
  }
  
  # For each approach and grouping option, evaluate forecast accuracy
  for (approach_name in names(approaches)) {
    approach <- approaches[[approach_name]]
    
    for (group_cols in group_options) {
      # Create a label for this grouping
      if (is.null(group_cols)) {
        group_label <- "no_groups"
      } else if (length(group_cols) == 1) {
        group_label <- group_cols
      } else {
        group_label <- paste(group_cols, collapse = "_")
      }
      
      # Prepare data with current approach and grouping
      prepared_data <- prepare_multi_hierarchy_data(
        data = data,
        value_col = value_col,
        product_hierarchy = approach$product,
        customer_hierarchy = approach$customer,
        group_cols = group_cols
      )
      
      # Fit model with current approach and grouping
      fit <- fit_multi_hierarchy_forecast(
        data = prepared_data,
        value_col = value_col,
        product_hierarchy = approach$product,
        customer_hierarchy = approach$customer,
        group_cols = group_cols
      )
      
      # Evaluate each reconciliation method
      for (method in reconciliation_methods) {
        # Extract forecasts for this method
        if (!is.null(approach$product) && !is.null(approach$customer)) {
          # Both hierarchies
          forecasts <- extract_detail_forecasts(
            fit$forecast,
            method = method,
            product_level = approach$product[length(approach$product)],
            customer_level = approach$customer[length(approach$customer)]
          )
        } else if (!is.null(approach$product)) {
          # Product hierarchy only
          forecasts <- extract_detail_forecasts(
            fit$forecast,
            method = method,
            product_level = approach$product[length(approach$product)],
            customer_level = NULL
          )
        } else {
          # Customer hierarchy only
          forecasts <- extract_detail_forecasts(
            fit$forecast,
            method = method,
            product_level = NULL,
            customer_level = approach$customer[length(approach$customer)]
          )
        }
        
        # Merge with test data and calculate accuracy metrics
        accuracy <- calculate_accuracy(forecasts, test_data, value_col)
        
        # Add approach, grouping, and method info
        accuracy[, approach := approach_name]
        accuracy[, grouping := group_label]
        accuracy[, method := method]
        
        # Combine results
        results <- rbind(results, accuracy, fill = TRUE)
      }
    }
  }
  
  return(results)
}

#' @title Calculate Accuracy Metrics
#' @description Calculates forecast accuracy metrics by comparing forecasts with actual values
#' @param forecasts Forecast data.table from extract_detail_forecasts
#' @param actuals Actual values data.table
#' @param value_col Column name containing the actual value
#' @return A data.table with accuracy metrics
#' @import data.table
#' @export
calculate_accuracy <- function(forecasts, actuals, value_col = "Q") {
  
  # Assumes forecasts and actuals can be joined on appropriate keys
  # The actual joining logic depends on your data structure
  
  # For example, if joining on MATERIAL, CUSTOMER, and date:
  comparison <- merge(
    forecasts,
    actuals,
    by.x = c("MATERIAL", "CUSTOMER", "index"),
    by.y = c("MATERIAL", "CUSTOMER", "Date"),
    all.x = TRUE
  )
  
  # Calculate metrics
  comparison[, error     := get(paste0(value_col, ".y")) - get(paste0(".mean"))]
  comparison[, abs_error := abs(error)]
  comparison[, pct_error := abs(error) / abs(get(paste0(value_col, ".y"))) * 100]
  
  # Aggregate metrics
  results <- comparison[, .(
    RMSE = sqrt(mean(error^2, na.rm = TRUE)),
    MAE  = mean(abs_error   , na.rm = TRUE),
    MAPE = mean(pct_error   , na.rm = TRUE)
  )]
  
  return(results)
}



#' @title Visualize Forecasts Across Hierarchies
#' @description Creates visualizations for forecasts at various hierarchical levels
#' @param reconciled_forecast The reconciled forecast object from fit_multi_hierarchy_forecast
#' @param historical_data Original historical data for comparison
#' @param value_col Column name containing the value to forecast
#' @param method The reconciliation method to visualize (e.g., "mint")
#' @param level The hierarchical level to visualize (e.g., "MATERIAL", "CUSTOMER")
#' @param top_n Number of top items to visualize (e.g., top 5 materials)
#' @return A ggplot2 object with the visualization
#' @import ggplot2 data.table
#' @export
visualize_hierarchical_forecast <- function(
    reconciled_forecast,
    historical_data,
    value_col = "Q",
    method    = "mint",
    level     = "MATERIAL",
    top_n     = 5) {
  
  # Filter forecast data for the selected method
  forecast_data <- reconciled_forecast %>%
    dplyr::filter(.model == method)
  
  # Convert historical data to long format if needed
  if (data.table::is.data.table(historical_data)) {
    historical_dt <- historical_data
  } else {
    historical_dt <- data.table::as.data.table(historical_data)
  }
  
  # Find top n items by total value
  top_items <- historical_dt[, .(total = sum(get(value_col), na.rm = TRUE)), by = level]
  top_items <- top_items[order(-total)][1:top_n, get(level)]
  
  # Filter data for top items
  historical_filtered <- historical_dt[get(level) %in% top_items]
  forecast_filtered   <- forecast_data[get(level) %in% top_items]
  
  # Convert forecast to data.table for plotting
  forecast_dt         <- data.table::as.data.table(forecast_filtered)
  
  # Create plot
  p <- ggplot2::ggplot() +
    # Historical data
    ggplot2::geom_line(
      data = historical_filtered,
      ggplot2::aes(x = Date, y = get(value_col), color = get(level)),
      size = 1
    ) +
    # Forecast
    ggplot2::geom_line(
      data = forecast_dt,
      ggplot2::aes(x = index, y = .mean, color = get(level)),
      linetype = "dashed",
      size = 1
    ) +
    # Confidence intervals
    ggplot2::geom_ribbon(
      data = forecast_dt,
      ggplot2::aes(
        x = index,
        ymin = .lower,
        ymax = .upper,
        fill = get(level)
      ),
      alpha = 0.2
    ) +
    # Labels and theme
    ggplot2::labs(
      title = paste("Forecast for Top", top_n, level, "Items"),
      subtitle = paste("Using", method, "reconciliation method"),
      x = "Time",
      y = value_col,
      color = level,
      fill = level
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.position = "bottom")
  
  return(p)
}

#' @title Analyze Hierarchical Influence
#' @description Analyzes how different hierarchical levels influence forecast accuracy
#' @param comparison_results Results from compare_hierarchy_approaches function
#' @return A list with analysis results and plots
#' @import ggplot2 data.table
#' @export
analyze_hierarchy_influence <- function(comparison_results) {
  
  # Summary by approach
  approach_summary <- comparison_results[, .(
    RMSE = mean(RMSE, na.rm = TRUE),
    MAE  = mean(MAE , na.rm = TRUE),
    MAPE = mean(MAPE, na.rm = TRUE)
  ), by = .(approach)]
  
  # Summary by approach and method
  method_summary <- comparison_results[, .(
    RMSE = mean(RMSE, na.rm = TRUE),
    MAE  = mean(MAE , na.rm = TRUE),
    MAPE = mean(MAPE, na.rm = TRUE)
  ), by = .(approach, method)]
  
  # Create plots
  
  # RMSE comparison
  rmse_plot <- ggplot2::ggplot(method_summary, ggplot2::aes(x = approach, y = RMSE, fill = method)) +
    ggplot2::geom_bar(stat = "identity", position = "dodge") +
    ggplot2::labs(
      title = "RMSE by Hierarchical Approach and Method",
      x = "Approach",
      y = "RMSE"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.position = "bottom")
  
  # MAPE comparison
  mape_plot <- ggplot2::ggplot(method_summary, ggplot2::aes(x = approach, y = MAPE, fill = method)) +
    ggplot2::geom_bar(stat = "identity", position = "dodge") +
    ggplot2::labs(
      title = "MAPE by Hierarchical Approach and Method",
      x = "Approach",
      y = "MAPE (%)"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.position = "bottom")
  
  return(list(
    approach_summary = approach_summary,
    method_summary   = method_summary,
    rmse_plot        = rmse_plot,
    mape_plot        = mape_plot
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
  
  # Split data into training and test sets
  unique_dates <- sort(unique(data[[date_col]]))
  train_dates  <- unique_dates[1:(length(unique_dates) - test_periods)]
  test_dates   <- unique_dates[(length(unique_dates) - test_periods + 1):length(unique_dates)]
  
  train_data   <- data[get(date_col) %in% train_dates]
  test_data    <- data[get(date_col) %in% test_dates]
  
  # 1. Product hierarchy only approach
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
    reconciliation_method = reconciliation_method
  )
  
  # 2. Customer hierarchy only approach
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
    reconciliation_method = reconciliation_method
  )
  
  # 3. Combined hierarchies approach
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
    reconciliation_method = reconciliation_method
  )
  
  # Compare approaches
  comparison <- compare_hierarchy_approaches(
    data                  = train_data,
    test_data             = test_data,
    value_col             = value_col,
    product_hierarchy     = product_hierarchy,
    customer_hierarchy    = customer_hierarchy
  )
  
  # Analyze results
  analysis <- analyze_hierarchy_influence(comparison)
  
  # Extract detail level forecasts from best approach
  # Determine best approach based on RMSE
  best_approach <- analysis$approach_summary[which.min(RMSE), approach]
  
  if (best_approach == "product_only") {
    best_fit <- product_only_fit
    detail_level <- list(
      product  = tail(product_hierarchy, 1),
      customer = NULL
    )
  } else if (best_approach == "customer_only") {
    best_fit <- customer_only_fit
    detail_level <- list(
      product  = NULL,
      customer = tail(customer_hierarchy, 1)
    )
  } else {
    best_fit <- combined_fit
    detail_level <- list(
      product  = tail(product_hierarchy, 1),
      customer = tail(customer_hierarchy, 1)
    )
  }
  
  # Extract detail forecasts from best approach
  detail_forecasts <- extract_detail_forecasts(
    best_fit$forecast,
    method         = "mint",
    product_level  = detail_level$product,
    customer_level = detail_level$customer
  )
  
  # Return complete workflow results
  return(list(
    product_only     = product_only_fit,
    customer_only    = customer_only_fit,
    combined         = combined_fit,
    comparison       = comparison,
    analysis         = analysis,
    best_approach    = best_approach,
    detail_forecasts = detail_forecasts
  ))
}