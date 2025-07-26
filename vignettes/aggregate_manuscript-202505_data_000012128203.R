
## ----libraries--------------------------------------------------------------------------------------------------------------------------------
library(tidyverse)
library(pool)
library(tictoc)
library(DBI)
library(RPostgres)
library(digest)
library(furrr)
library(yaml)

args = commandArgs(trailingOnly=TRUE)

if (length(args) < 3) {
  stop(paste0(length(args), " arguments were provided. Usage: Rscript aggregate_data.R <session_id> <yml_path> <staining_layout_version>"), call. = FALSE)
}

# session_id is the name of the specific 'reading' of the plate, i.e. "000012126003__2025-01-16T14_31_24-Measurement_1"
session_id = args[1] # takes the value from the first argument when calling the function from CLI
yml_path = args[2] # 2nd argument, path to the yaml file containing the different options for the staining layout
staining_layout_version = args[3] #3rd argument, version of the staining layout

# session_id = "000012126003__2025-01-16T14_31_24-Measurement_1"
# yml_path = "dcp_helper_csaba/vignettes/staining_layouts.yml"
# staining_layout_version = "v2"

# Validate YAML file exists
if (!file.exists(yml_path)) {
  stop(paste("Error: YAML file not found at:", yml_path), call. = FALSE)
}

bucket_dir = "s3://ascstore/flatfield/Batch_000012128203"             # AWS S3 folder containing single-cell morphological measurements (or observations)
results_dir = "/home/ubuntu/data_tmp" # temporary local folder to pull single cell data

print(paste("Location of files in bucket: ", file.path(bucket_dir,session_id)))
print(paste("Location of files locally: ", file.path(results_dir,session_id)))

tic("Preparing data")
# Temporarily download analysis files locally for proecessing 
# TODO: is this really needed?
if ( dir.exists( file.path(results_dir,session_id) ) ) {
  print("start processing")
} else {
  print("needs to download")
  dir.create(file.path(results_dir,session_id))
  system( paste('aws s3 sync',
                file.path(bucket_dir, session_id),
                file.path(results_dir, session_id),
                '--exclude "*" --include "*mid24*.csv" --include "*resolution1*.csv" --force-glacier-transfer --no-progress'
                ) )
}
toc()


compute_measurement_checksum <- function(result_path, pattern = "*Cells.csv") {
  # list all the files containing the pattern in the indicated directory
  # i.e. lists all the feature tables for each image (measurement), there are 3 files per image
  measurement.list <- list.files(result_path, pattern = pattern, recursive = TRUE, full.names = TRUE)

  # reorder file.list so that it has all the csvs for ch2 (birghtfield, "resolution1") first and then all the ones for ch3_ch4 and ch5_ch6 ("mid24")
  # TODO: is this necessary?
  measurement.list <- c(measurement.list[grepl("resolution1", measurement.list)], measurement.list[grepl("mid24", measurement.list)])
  # print(result_path)
  # print(measurement.list)

  return(paste(tools::md5sum(measurement.list), collapse = '|'))
}

read_and_merge_measurements <- function(result_path, pattern = "*Cells.csv"){
  # list all the files containing the pattern in the indicated directory
  # i.e. lists all the feature tables for each image (measurement), there are 3 files per image
  measurement.list <- list.files(result_path, pattern = pattern, recursive = TRUE, full.names = TRUE)

  # reorder file.list so that it has all the csvs for ch2 (birghtfield, "resolution1") first and then all the ones for ch3_ch4 and ch5_ch6 ("mid24")
  # TODO: is this necessary?
  measurement.list <- c(measurement.list[grepl("resolution1", measurement.list)], measurement.list[grepl("mid24", measurement.list)])
  # print(result_path)
  # print(measurement.list)

  if (length(measurement.list)<1) {

    print(paste0("No measurements found for this session: ", result_path))
    return (NULL)

  } else {

    tbl_list <- c()
    i <- 1
    # feature.file will be each .csv containing features for a given image ("measurement") 
    # tbl_list is a list of each feature table
    for (feature.file in measurement.list) {
      tbl_list[[i]] <- read_csv(feature.file)
      i <- i + 1
    }

    # reduced.obesrvations is the merged table with all the features for each object ("observation") in all images ("measurements") across all channels
    # Includes 'ImageNumber', 'ObejctNumber' and then all the features for all channels (except ch1)
    reduced.observations <- Reduce(function(x, y) merge(x, y, all.x = TRUE),tbl_list) %>%
      select(-contains("Metadata"))
    colnames(reduced.observations) <- colnames(reduced.observations) %>%
      str_replace(.,"projection","")

    return(reduced.observations %>% janitor::clean_names()) #janitor removes all capitalization from the col names
  }
}

get_validated_channels <- function(yml_path, staining_layout_version) {
  # Read and parse YAML file
  yaml_data <- yaml.load_file(yml_path)
  
  # Convert to proper list structure if it's an atomic vector
  if (is.atomic(yaml_data)) {
    yaml_data <- list(yaml_data)
  }
  
  # Find the requested staining_layout_version
  layout <- NULL
  for (item in yaml_data) {
    if (is.list(item) && !is.null(item$staining_layout) && item$staining_layout == staining_layout_version) {
      layout <- item
      break
    }
  }
  
  if (is.null(layout)) {
    stop(paste("Staining layout version", staining_layout_version, "not found in YAML"), call. = FALSE)
  }
  
  # Required channels
  required_channels <- paste0("ch", 1:6)
  
  # Validate all channels exist and are non-null
  for (ch in required_channels) {
    if (!ch %in% names(layout) || is.null(layout[[ch]])) {
      stop(paste("Missing or NULL value for required channel:", ch), call. = FALSE)
    }
  }
  
  # Return as tibble
  tibble(
    ch1 = layout$ch1,
    ch2 = layout$ch2,
    ch3 = layout$ch3,
    ch4 = layout$ch4,
    ch5 = layout$ch5,
    ch6 = layout$ch6
  )
}
## ---------------------------------------------------------------------------------------------------------------------------------------------

# Establish connection to the RDS database using temporary token
RDS_HOST = "manuscript-202505-cluster.cluster-ro-c9k2hfiwt5mi.us-east-2.rds.amazonaws.com"
RDS_DB = "biosensor"
RDS_PORT = 5432
RDS_USER = "nrindtorff"
AWS_REGION = "us-east-2"

## use 'system2()' to execute commands as in the CLI
RDS_TOKEN <- system2("aws", 
              args = c("rds", "generate-db-auth-token", 
                    "--hostname", RDS_HOST,
                    "--port", RDS_PORT, 
                    "--username", RDS_USER,
                    "--region", AWS_REGION),
              stdout = TRUE)

con.manuscript202505 <- dbConnect(RPostgres::Postgres(),
                 dbname = RDS_DB,
                 host = RDS_HOST,
                 port = 5432,
                 user = RDS_USER,
                 password = RDS_TOKEN)

pool.manuscript202505 <- pool::dbPool(RPostgres::Postgres(),
                       dbname = RDS_DB,
                       host = RDS_HOST,
                       port = 5432,
                       user = RDS_USER,
                       password = RDS_TOKEN)

## ---------------------------------------------------------------------------------------------------------------------------------------------

# Append new imaging session to the session table 
tic("Adding new session to database")
new_session <- tibble(session_id = session_id)

new_session %>%
    dbWriteTable(pool.manuscript202505, "session", ., append = TRUE)
toc()

# Append new sample to the sample table 
tic("Adding new sample to database")
new_sample <- tibble(sample_id = session_id %>% str_extract(pattern = "0000\\d+"))

new_sample %>%
    dbWriteTable(pool.manuscript202505, "sample", ., append = TRUE)
toc()

## ---------------------------------------------------------------------------------------------------------------------------------------------
# Extract the highest staining_layout_id from the database to inform what id to start from when assigning to new wells
staining_layout <- tbl(pool.manuscript202505, "staining_layout")

max_id <- staining_layout %>%
  summarise(max_id = max(staining_layout_id, na.rm = TRUE)) %>%
  collect() %>%
  pull(max_id)

# results_dir is a list of absolute paths to the main results folder for each well (the one ending in "ch1")
# i.e: "/home/ubuntu/dcp_helper_csaba/data/results/000012126003__2025-01-16T14_31_24-Measurement_1/000012126003__2025-01-16T14_31_24-Measurement_1-sk1-C14-f01-ch1"
results_list <- dir(file.path(results_dir, session_id), pattern = "ch1", full.names = TRUE)

tic("Creating records for new measurements")
# measurement is the existing Measurement table within the RDS DB
measurement <- tbl(pool.manuscript202505, "measurement")

#measurement_id is the id of each image (field of view)
# i.e. "000012112403__2021-06-09T14_14_23-Measurement_1-sk1-A01-f01-ch1"
existing_measurement <- measurement %>% dplyr::select(measurement_id, measurement_checksum) %>% distinct() %>% collect()

new_measurement = tibble(sample_id = session_id %>% str_extract(pattern = "0000\\d+"),
         session_id = session_id,
         measurement_id = results_list %>% str_extract(pattern = "0000\\d+__\\d+-\\d\\d-\\d+T\\d+_\\d+_\\d+-Measurement_\\d-sk\\d+-...-f..-ch\\d")
         ) %>%
    separate(measurement_id, remove = FALSE, sep = "-", c("t1", "t2", "t3",  "measurement_descriptor", "timepoint_descriptor", "well", "field_descriptor", "channel")) %>%
    select(-(t1:t3)) %>% # drop t1, t2, t3
    select(-(channel)) %>% # drop channel
    mutate(measurement_descriptor = str_extract(measurement_descriptor, pattern = "\\d") %>% as.numeric(),
           timepoint_descriptor = str_extract(timepoint_descriptor, pattern = "\\d+") %>% as.numeric()) %>%
    mutate(local_path = results_list) %>% 
    #############################################
    # this is adding 1 to the id for each image, but i need to add for each well!!!
    mutate(staining_layout_id = max_id + dense_rank(well)) %>% # add sequentially incrementing numbers for each well, based on the last satining_layout_id listed in the DB
    rowwise() %>%
    mutate(measurement_checksum = compute_measurement_checksum(local_path %>% as.character()) %>% as.character()) %>%
    anti_join(existing_measurement)
toc()

# Extract channel list from the yml file, based on the version indicated whren calling this script
# TODO: CURRENT VERSION ASSUMES ONE STAINING LAYOUT PER PLATE (SESSION_ID), IT MIGHT NOT BE THIS WAY IN THE FUTURE!
staining_layout_channels <- staining_layout_channels <- get_validated_channels(yml_path, staining_layout_version = staining_layout_version)

new_staining_layout <- new_measurement %>%
  mutate(row = match(str_extract(well, "[A-Z]"), LETTERS),   # Convert row letter to number (A=1, B=2, ..., Z=26)
    col = as.numeric(str_extract(well, "\\d+"))) %>%         # Column is just the numeric part
  select(well,row,col,staining_layout_id) %>%
  distinct(well, .keep_all = TRUE) %>%
  mutate(
    staining_layout = staining_layout_version,
    ch1 = staining_layout_channels$ch1,
    ch2 = staining_layout_channels$ch2,
    ch3 = staining_layout_channels$ch3,
    ch4 = staining_layout_channels$ch4,
    ch5 = staining_layout_channels$ch5,
    ch6 = staining_layout_channels$ch6
    ) 

# Append new staining_layout to the staining_layout table 
tic("Adding new staining_layout to database")
new_staining_layout %>%
    dbWriteTable(pool.manuscript202505, "staining_layout", ., append = TRUE)
toc()


tic("Adding new measurements to database")
new_measurement %>%
    dbWriteTable(pool.manuscript202505, "measurement", ., append = TRUE)
toc()

## ---------------------------------------------------------------------------------------------------------------------------------------------

tic("Adding new observations to database")
## ------ This is not used for now -----------------
# merge both columns containing 'measurement', i.e. 'measurement_id', 'measurement_descriptor' and 'measurement_checksum' 
# into a new column called 'observation' (strings sepparated by "___")

# THIS IS NOT CORRECT, THIS IS NOT A DESCRIPTOR OF THE OBSERVATION (EACH OBJECT)!!!!
# new_measurement_2 <- new_measurement %>%
#   unite("observation", contains("measurement"), sep = "___")
## -------------------------------------------------

## parallelize the read_and_merge_measurements() function to all the measurements (as listed in the measurement_id of the new_measurements 
# table and located in the indicated local_path), with the addition of the corresponding 'measurement_id' as a primary key and then write to the DB.
furrr::future_map2( new_measurement$measurement_id,
                    new_measurement$local_path,
                    # ~ indicates the start of an anonymous lambda function to be parallelized following the pair of lists above 
                    ~ {read_and_merge_measurements(.y) %>% # .y is the second argument, i.e. 'new_measurement$local_path'
                          mutate(measurement_id = .x) %>% # .x is the first argument, i.e. 'new_measurement$measurement_id'
                          dbWriteTable(pool.manuscript202505, "observation", ., append = TRUE)
                    })
toc()






