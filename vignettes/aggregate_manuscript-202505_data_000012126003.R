


## ----setup, include=FALSE---------------------------------------------------------------------------------------------------------------------
knitr::opts_chunk$set(echo = TRUE)


## ----libraries--------------------------------------------------------------------------------------------------------------------------------
library(tidyverse)
library(pool)
library(tictoc)
library(DBI)
library(RPostgres)
library(digest)
library(furrr)



tic("Preparing data")

# session_id is the name of the specific 'reading' of the plate, i.e. "000012126003__2025-01-16T14_31_24-Measurement_1"
session_id = args = commandArgs(trailingOnly=TRUE) # takes the value from the arguments when calling the function from CLI
# session_id = 000012126003__2025-01-16T14_31_24-Measurement_1

bucket_dir = "s3://ascstore/flatfieldv2_test/"             # AWS S3 folder containing single-cell morphological measurements (or observations)
results_dir = "/home/ubuntu/dcp_helper_csaba/data/results" # temporary local folder to pull single cell data

# Temporarily download analysis files locally for proecessing 
# TODO: is this really needed?
if ( dir.exists( file.path(results_dir,session_id) ) ) {
  print("start processing")
} else {
  print("needs to download")
  dir.create(file.path(results_dir,session_id))
  system( paste('aws s3 sync',
                paste0(bucket_dir, session_id),
                paste0(results_dir, session_id),
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

# test functions
# observation.checksum <- compute_measurement_checksum(results_list[1])
# reduced.observation <- read_and_merge_measurements(results_list[1])


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
    rowwise() %>%
    mutate(measurement_checksum = compute_measurement_checksum(local_path %>% as.character()) %>% as.character()) %>%
    mutate(staining_layout_id = "placeholder")
    anti_join(existing_measurement)
toc()
## OK up to here!

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

furrr::future_map2(new_measurement$measurement_id,
                   new_measurement$local_path,
                     ~ { read_and_merge_measurements(.y) %>% # .y is the second argument, i.e. 'new_measurement$local_path'
                           mutate(measurement_id = .x) %>% # .x is the first argument, i.e. 'new_measurement$measurement_id'
                           #separate(observation, c("id_observation", "id_observation_checksum"), sep = "___" ) %>%. #these columns are no logner used in the current version of the DB
                           dbWriteTable(pool.manuscript202505, "observation", ., append = TRUE)
                         # print(paste0("appended ", .y))
                         # flush.console()
                       }
    )
toc()





