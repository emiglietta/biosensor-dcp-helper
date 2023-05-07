


## ----setup---------------------------------------------------------------------------------------------------------------------
knitr::opts_chunk$set(echo = TRUE)


## ----libraries--------------------------------------------------------------------------------------------------------------------------------
library(tidyverse)
library(pool)
library(tictoc)
library(DBI)
library(RPostgres)
library(digest)
library(furrr)
library(tryCatchLog)


## preparing data---------------------------------------------------------------------------------------

tic("Preparing data")
measurement_id = "000012116603__2023-01-24T14_33_15-Measurement_3"

bucket_dir = "s3://ascstore/flatfieldv2/"             # AWS S3 folder containing single-cell morphological measurements (or observations)
results_dir = "/home/ubuntu/dcp_helper/data/results/" # temporary local folder to pull single cell data

if ( dir.exists( file.path(results_dir,measurement_id) ) ) {
  print("Start processing")
} else {
  print("Needs to download")
  dir.create(file.path(results_dir,measurement_id))
  system( paste('aws s3 sync',
                paste0(bucket_dir, measurement_id),
                paste0(results_dir, measurement_id),
                '--exclude "*" --include "*.csv" --force-glacier-transfer --no-progress') )
  # system( paste('aws s3 sync',
  #               paste0(bucket_dir, measurement_id),
  #               paste0(results_dir, measurement_id),
  #               '--exclude "*" --include "*mid24*.csv" --include "*resolution1*.csv" --force-glacier-transfer --no-progress') )
}
toc()


## define functions-------------------------------------------------------------------------------------------------------------

compute_observations_checksum <- function(result_path, pattern = "*Cells.csv") {
  observation.list <- list.files(result_path, pattern = pattern, recursive = TRUE, full.names = TRUE)
  # print(result_path)
  # print(observation.list)

  return(paste(tools::md5sum(observation.list), collapse = '|'))
}

read_and_merge_observations <- function(result_path, pattern = "*Cells.csv"){

  observation.list <- list.files(result_path, pattern = pattern, recursive = TRUE, full.names = TRUE)
  # print(result_path)
  # print(observation.list)

  tbl_list <- c()

  i <- 1
  for (observation.file in observation.list) {
    tbl_list[[i]] <- read_csv(observation.file)
    i <- i + 1
  }

  reduced.observations <- Reduce(function(x, y) merge(x, y, all.x = TRUE),tbl_list) %>%
    select(-contains("Metadata"))
  colnames(reduced.observations) <- colnames(reduced.observations) %>%
    str_replace(.,"projection","")

  return(reduced.observations %>% janitor::clean_names())
}

read_observations <- function(result_path, pattern = "*Cells.csv", feature_group = NULL){

  if (is.null(feature_group)) {

    return( read_and_merge_observations(result_path = result_path, pattern = pattern) )

  } else if (feature_group=="areashape") {

    observation.list <- list.files(result_path, pattern = pattern, recursive = TRUE, full.names = TRUE)
    # print("areashape1")
    # print(observation.list)
    observation.list <- observation.list[grepl(pattern = feature_group, observation.list)]
    # print("areashape2")
    # print(observation.list)

    if (length(observation.list)<1) {

      print(paste0("No observation found for this measurement: ", result_path))
      return (NULL)

    } else {

      tbl <- read_csv(observation.list[[1]], show_col_types = FALSE)

      areashape_observations <- tbl %>%
        janitor::clean_names() %>%
        select(image_number, object_number, contains("area_shape"))

      return( areashape_observations )
    }

  } else {

    observation.list <- list.files(result_path, pattern = pattern, recursive = TRUE, full.names = TRUE)
    # print("fluorescence1")
    # print(observation.list)
    observation.list <- observation.list[grepl(pattern = feature_group, observation.list)]
    # print("fluorescence2")
    # print(observation.list)

    if (length(observation.list)<1) {

      print(paste0("No observation found for this measurement: ", result_path))
      return (NULL)

    } else {

      tbl_list <- c()
      i <- 1

      for (obs in observation.list) {
        obs_split <- str_split(obs, pattern = "/")[[1]]
        downsampling = obs_split[length(obs_split)-1]
        tbl_list[[i]] <- read_csv(obs, show_col_types = FALSE)
        tbl_list[[i]]$downsampling <- downsampling
        i <- i + 1
      }
      reduced.observations <- Reduce(function(x, y) merge(x, y, all.x = TRUE),tbl_list) %>%
        janitor::clean_names() %>%
        select(image_number, object_number, contains(feature_group), downsampling)

      colnames(reduced.observations) <- colnames(reduced.observations) %>%
        str_replace(.,"projection","")

      return( reduced.observations )

    }
  }

}

read_and_insert_observation <- function(combined_observation_id, result_path, pattern = "*Cells.csv", feature_group = NULL, conn=NULL){

  if (is.null(conn)) {
    return (0)
  }
  if (is.null(feature_group)) {

    return( 0 )

  } else if (feature_group=="areashape") {

    observation.list <- list.files(result_path, pattern = pattern, recursive = TRUE, full.names = TRUE)
    # print("areashape1")
    # print(observation.list)
    # observation.list <- observation.list[grepl(pattern = feature_group, observation.list)]
    # print("areashape2")
    # print(observation.list)

    if (length(observation.list)<1) {

      print(paste0("No observation found for this measurement: ", result_path))
      return (NULL)

    } else {

      tbl <- read_csv(observation.list[[1]], show_col_types = FALSE)

      areashape_observations <- tbl %>%
        janitor::clean_names() %>%
        select(image_number, object_number, contains("area_shape"))

      if (is.null(areashape_observations)){
        return (NULL)
      } else {
        areashape_observations %>%
          mutate(observation=combined_observation_id) %>%
          separate(observation, c("id_observation", "id_observation_checksum"), sep = "___" ) %>%
          dbWriteTable(conn, "observation_areashape", ., append = TRUE)
        return( 1 )
      }

    }

  } else {

    observation.list <- list.files(result_path, pattern = pattern, recursive = TRUE, full.names = TRUE)
    # print("fluorescence1")
    # print(observation.list)
    observation.list <- observation.list[grepl(pattern = feature_group, observation.list)]
    # print("fluorescence2")
    # print(observation.list)

    if (length(observation.list)<1) {

      print(paste0("No observation found for this measurement: ", result_path))
      return (NULL)

    } else {

      tbl_list <- c()
      i <- 1

      for (obs in observation.list) {
        obs_split <- str_split(obs, pattern = "/")[[1]]
        downsampling = obs_split[length(obs_split)-1]
        tbl_list[[i]] <- read_csv(obs, show_col_types = FALSE)
        tbl_list[[i]]$downsampling <- downsampling
        i <- i + 1
      }
      reduced.observations <- Reduce(function(x, y) merge(x, y, all.x = TRUE),tbl_list) %>%
        janitor::clean_names() %>%
        select(image_number, object_number, contains(feature_group), downsampling)

      colnames(reduced.observations) <- colnames(reduced.observations) %>%
        str_replace(.,"projection","")

      if (is.null(reduced.observations)){
        return (NULL)
      } else {
        reduced.observations %>%
          mutate(observation=combined_observation_id) %>%
          separate(observation, c("id_observation", "id_observation_checksum"), sep = "___" ) %>%
          dbWriteTable(conn, paste0("observation_", feature_group), ., append = TRUE)
        return( 1 )
      }

    }
  }

}

######
# %>% {
#   ifelse( is.null(.), .,
#           mutate(observation = .x) %>%
#             separate(observation, c("id_observation", "id_observation_checksum"), sep = "___" ) %>%
#             dbWriteTable(pool.v3, paste0("observation_",feature_group), ., append = TRUE)
#   )
# }
######


# test functions
# observation.checksum <- compute_observations_checksum(results_list[1])
# reduced.observation <- read_and_merge_observations(results_list[1])

## connecting to database ---------------------------------------------------------------------------------------------------------------------------------------------

# endpoint.v3 = "biosensorv3.cluster-c9k2hfiwt5mi.us-east-2.rds.amazonaws.com"
endpoint.v3 = "biosensorv3-downsampling-cluster.c9k2hfiwt5mi.us-east-2.rds.amazonaws.com"
dbname.v3 <- "biosensor"

con.v3 <- dbConnect(RPostgres::Postgres(),
                 dbname = dbname.v3,
                 host = endpoint.v3,
                 port = 5432,
                 user = "biosensor",
                 password = "biosensor")

pool.v3 <- pool::dbPool(RPostgres::Postgres(),
                       dbname = dbname.v3,
                       host = endpoint.v3,
                       port = 5432,
                       user = "biosensor",
                       password = "biosensor")


## adding measurements---------------------------------------------------------------------------------------------------------------------------------------------
# files <- list.files(file.path(results_dir, measurement_id), pattern = "ch1", full.names = TRUE) %>% paste0(., "/resolution1/", "measurement_ch2_Cells.csv")
results_list <- dir(file.path(results_dir, measurement_id), pattern = "ch1", full.names = TRUE)

tic("New measurement")
measurement <- tbl(pool.v3, "measurement")
existing_measurement <- measurement %>% dplyr::select(id_observation,id_observation_checksum) %>% distinct() %>% collect()

new_measurement = tibble(id_barcode = measurement_id %>% str_extract(pattern = "0000\\d+"),
         id_measurement = measurement_id,
         id_observation = results_list %>% str_extract(pattern = "0000\\d+__\\d+-\\d\\d-\\d+T\\d+_\\d+_\\d+-Measurement_\\d-sk\\d+-...-f..-ch\\d")
         ) %>%
    separate(id_observation, remove = FALSE, sep = "-", c("t1", "t2", "t3",  "measurement_no", "iteration_no", "well", "field", "channel")) %>%
  select(-(t1:t3)) %>%
    mutate(measurement_no = str_extract(measurement_no, pattern = "\\d") %>% as.numeric(),
           iteration_no = str_extract(iteration_no, pattern = "\\d") %>% as.numeric()) %>%
    mutate(full_path = results_list) %>% rowwise() %>%
    mutate(id_observation_checksum = compute_observations_checksum(full_path %>% as.character()) %>% as.character()) %>%
    anti_join(existing_measurement)
toc()

# new_measurement <- new_measurement %>% head(2000)
# new_measurement <- new_measurement %>%
#   filter(id_observation=="000012117103__2023-03-10T13_13_16-Measurement_1-sk2-N04-f01-ch1")

tic("Adding to database")
new_measurement %>%
    dbWriteTable(pool.v3, "measurement", ., append = TRUE)
toc()

## adding observations-------

tic("Adding observations")

new_measurement_2 <- new_measurement %>%
  unite("observation", contains("observation"), sep = "___")

# for (feature_group in c("areashape", "ch2", "ch3", "ch4", "ch5", "ch6")) {
#   tic()
#   print(feature_group)
#   furrr::future_map2(new_measurement_2$observation,
#                      new_measurement_2$full_path,
#                      ~ { read_observations(.y, feature_group = feature_group) %>%
#                          mutate(observation = .x) %>%
#                          separate(observation, c("id_observation", "id_observation_checksum"), sep = "___" ) %>%
#                          dbWriteTable(pool.v3, paste0("observation_",feature_group), ., append = TRUE)
#                        # print(paste0("appended ", .y))
#                        # flush.console()
#                      }
#   )
#   toc()
# }




# for (feature_group in c("areashape", "ch2", "ch3", "ch4", "ch5", "ch6")) {
#   tic()
#   print(feature_group)
#   furrr::future_map2(new_measurement_2$observation,
#                      new_measurement_2$full_path,
#                      ~ { read_observations(.y, feature_group = feature_group) %>% {
#                          ifelse( is.null(.), .,
#                             mutate(observation = .x) %>%
#                             separate(observation, c("id_observation", "id_observation_checksum"), sep = "___" ) %>%
#                             dbWriteTable(pool.v3, paste0("observation_",feature_group), ., append = TRUE)
#                          )
#                        }
#                      }
#   )
#   toc()
# }
#
# toc()

tic()

for (feature_group in c("areashape", "ch2", "ch3", "ch4", "ch5", "ch6")) {
  tic()
  print(paste("Adding observations for feature group", feature_group))
  furrr::future_map2(new_measurement_2$observation,
                     new_measurement_2$full_path,
                     ~ { read_and_insert_observation(.x, .y, feature_group = feature_group, conn=pool.v3) }
  )
  toc()
}

toc()

system(paste0("rm -r ", paste0(results_dir, measurement_id)))

#1-500: 457.342sec
#501-1000: 454.088 sec elapsed
#all: 8000 sec
