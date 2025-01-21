library(fs)
library(yaml)

setup_project_structure <- function(
    root_dir         = ".", 
    dtap_levels      = c("1_Development", "2_Test", 
                         "3_Acceptance", "4_Production"),
    bsgp_levels      = c("1_Bronze", "2_Silver", 
                         "3_Gold", "4_Platinum"),
    functional_areas = c("sales", "stock"),
    project_dir      = "."
) {
  
  # Normalize and validate root_dir
  root_dir <- tryCatch({
    normalizePath(root_dir, mustWork = FALSE)
  }, error = function(e) {
    stop("Invalid root directory path: ", root_dir, "\n", e$message)
  })
  
  # Attempt to create the root directory
  tryCatch({
    dir_create(root_dir)
  }, error = function(e) {
    stop("Failed to create the root directory: ", root_dir, "\n", e$message)
  })
  
  # Check if the directory exists after the creation attempt
  if (!dir_exists(root_dir)) {
    stop("Failed to create the root directory, even though no error was thrown: ", root_dir)
  }
  
  # Create DTAP structure
  for (dtap in dtap_levels) {
    dtap_path <- path(root_dir, dtap)
    dir_create(dtap_path) # Create DTAP directory
    
    # Create BSGP structure
    for (bsgp in bsgp_levels) {
      bsgp_path <- path(dtap_path, bsgp)
      dir_create(bsgp_path) # Create BSGP directory
      
      # Create functional area directories
      for (area in functional_areas) {
        area_path <- path(bsgp_path, area)
        dir_create(area_path) # Create functional area directory
      }
    }
  }
  
  # Save the root_dir to the YAML file
  save_config_to_yaml(
    list(root_dir = root_dir), 
    project_dir   = project_dir
    )
  
  message("Project structure created at: ", root_dir)
  
  return(invisible(root_dir))
}

save_config_to_yaml <- function(config_list, project_dir = ".") {
  # Normalize project directory
  project_dir <- normalizePath(project_dir, mustWork = TRUE)
  
  # Define the path for the .config file
  config_file <- file.path(project_dir, ".config.yaml")
  
  # Write the configuration data to the YAML file
  write_yaml(config_list, config_file)
  
  message("Configuration saved to YAML file: ", config_file)
}

read_config_from_yaml <- function(project_dir = ".") {
  # Normalize project directory
  project_dir <- normalizePath(project_dir, mustWork = TRUE)
  
  # Define the path for the .config file
  config_file <- file.path(project_dir, ".config.yaml")
  
  # Check if the config file exists
  if (!file.exists(config_file)) {
    stop("Configuration file not found: ", config_file)
  }
  
  # Read the YAML file
  config_data <- read_yaml(config_file)
  
  return(config_data)
}

update_or_insert_config_in_yaml <- 
  function(
    .key, 
    .value, 
    .project_dir = ".") {
    
  # Normalize project directory
  project_dir <- normalizePath(.project_dir, mustWork = TRUE)
  
  # Define the path for the .config file
  config_file <- file.path(project_dir, ".config.yaml")
  
  # Check if the config file exists
  if (file.exists(config_file)) {
    # Read the existing configuration
    config_data <- read_yaml(config_file)
  } else {
    # Initialize an empty configuration if the file does not exist
    config_data <- list()
  }
  
  # Check if the key exists and update or insert
  if (!.key %in% names(config_data)) {
    message("Key '", .key, "' does not exist. Adding it to the configuration.")
  }
  config_data[[.key]] <- .value
  
  # Write the updated configuration back to the YAML file
  write_yaml(config_data, config_file)
  
  message("Key '", .key, "' updated/added in the configuration file: ", config_file)
}

# Get the current environment path
set_current_environment <- function(
    .project_dir = ".", 
    .environment = "Production") {
  
  update_or_insert_config_in_yaml(
    .key         = "environment",
    .value       = .environment,
    .project_dir = .project_dir
  )
}

get_environment_path <- function(project_dir = ".") {
  # Normalize project directory
  project_dir <- normalizePath(project_dir, mustWork = TRUE)
  
  # Define the path for the .config file
  config_file <- file.path(project_dir, ".config.yaml")
  
  # Check if the config file exists
  if (!file.exists(config_file)) {
    stop("Configuration file not found: ", config_file)
  }
  
  # Read the configuration data
  config_data <- read_yaml(config_file)
  
  # Check if root_dir and environment keys exist in the config
  if (!"root_dir" %in% names(config_data)) {
    stop("Key 'root_dir' is missing in the configuration file.")
  }
  if (!"environment" %in% names(config_data)) {
    stop("Key 'environment' is missing in the configuration file.")
  }
  
  # Construct and return the normalized environment path
  environment_path <- file.path(config_data$root_dir, config_data$environment)
  return(normalizePath(environment_path, mustWork = FALSE))
}
