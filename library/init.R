library(fs)
library(yaml)
library(crayon)

DTAP <- c("Development", "Test"  , "Acceptance", "Production")
BSGP <- c("Bronze"     , "Silver", "Gold"      , "Platinum")
AREA <- c("sales"      , "stock" , "promotions", "master_data")

.save_config_to_yaml <- function(
    config_list, 
    project_dir = ".") {
  
  # Normalize project directory
  project_dir <- normalizePath(project_dir, mustWork = TRUE)
  
  # Define the path for the .config file
  config_file <- file.path(project_dir, ".config.yaml")
  
  # Write the configuration data to the YAML file
  write_yaml(config_list, config_file)
  
  message("Configuration saved to YAML file: ", config_file)
}

# .read_config_from_yaml <- function(
#     project_dir = ".") {
#   
#   # Normalize project directory
#   project_dir <- normalizePath(project_dir, mustWork = TRUE)
#   
#   # Define the path for the .config file
#   config_file <- file.path(project_dir, ".config.yaml")
#   
#   # Check if the config file exists
#   if (!file.exists(config_file)) {
#     stop("Configuration file not found: ", config_file)
#   }
#   
#   # Read the YAML file
#   config_data <- read_yaml(config_file)
#   
#   return(config_data)
# }

.upsert_config_in_yaml <- 
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

setup_project_structure <- function(
    root_dir         = ".",  
    dtap_levels      = DTAP,  # DTAP levels
    bsgp_levels      = BSGP,  # staging levels
    functional_areas = AREA,  # functional area's
    project_dir      = "."    # to save .config.yaml
) {
  # Get OneDrive paths from environment variables
  onedrive_consumer <- path_abs(Sys.getenv("OneDriveConsumer"))
  onedrive_commercial <- path_abs(Sys.getenv("OneDriveCommercial"))
  
  # Normalize and validate root_dir
  root_dir <- tryCatch({
    path_abs(root_dir)
  }, error = function(e) {
    stop("Invalid root directory path: ", root_dir, "\n", e$message)
  })
  
  # Determine the value to store for root_dir in the YAML
  yaml_root_dir <- 
    if (path_has_parent(root_dir, onedrive_consumer)) {
      rel_path <- path_rel(root_dir, start = onedrive_consumer)
      path("OneDriveConsumer", rel_path)
    } else if (path_has_parent(root_dir, onedrive_commercial)) {
      rel_path <- path_rel(root_dir, start = onedrive_commercial)
      path("OneDriveBusiness", rel_path)
    } else {
      root_dir
    }
  
  # Attempt to create the root directory
  tryCatch({
    dir_create(root_dir)
  }, error = function(e) {
    stop("Failed to create the root directory: ", root_dir, "\n", e$message)
  })
  
  # Create the DTAP, BSGP, and functional area directory structure
  for (dtap in dtap_levels) {
    for (bsgp in bsgp_levels) {
      for (area in functional_areas) {
        dir_create(path(root_dir, dtap, bsgp, area))
      }
    }
  }
  
  # Save the root_dir to the YAML file
  .save_config_to_yaml(
    config_list = list(root_dir = yaml_root_dir), 
    project_dir = project_dir
  )
  
  message("Project structure created successfully at: ", root_dir)
  return(invisible(root_dir))
}


# Get the current environment path
set_current_environment <- function(
    .project_dir = ".", 
    .environment = "Production") {
  
  if(!.environment %in% DTAP){
    message(green(paste0("Valid environments: ", paste(DTAP, collapse = ", "))))
    stop("Invalid environment: ", .environment)
  }
  
  .upsert_config_in_yaml(
    .key         = "environment",
    .value       = .environment,
    .project_dir = .project_dir
  )
}

get_environment_path <- function(
    project_dir = ".") {
  
  # Normalize project directory
  project_dir <- path_abs(project_dir)
  
  # Define the path for the .config file
  config_file <- path(project_dir, ".config.yaml")
  
  # Check if the config file exists
  if (!file_exists(config_file)) {
    stop("Configuration file not found: ", config_file)
  }
  
  # Read the configuration data
  config_data <- read_yaml(config_file)
  
  # Validate keys in the config
  if (!"root_dir" %in% names(config_data)) {
    stop("Key 'root_dir' is missing in the configuration file.")
  }
  if (!"environment" %in% names(config_data)) {
    stop("Key 'environment' is missing in the configuration file.")
  }
  
  # Resolve placeholders in root_dir
  root_dir <- path_abs(config_data$root_dir)
  onedrive_consumer   <- path_abs(Sys.getenv("OneDriveConsumer", ""))
  onedrive_commercial <- path_abs(Sys.getenv("OneDriveCommercial", ""))
  
  if (path_has_parent(root_dir, "OneDriveConsumer")) {
    relative_path <- path_rel(root_dir, start = "OneDriveConsumer")
    root_dir <- path(onedrive_consumer, relative_path)
  } else if (path_has_parent(root_dir, "OneDriveBusiness")) {
    relative_path <- path_rel(root_dir, start = "OneDriveBusiness")
    root_dir <- path(onedrive_commercial, relative_path)
  }
  
  # Construct and return the normalized environment path
  environment_path <- path(root_dir, config_data$environment)
  return(path_abs(environment_path))
}
