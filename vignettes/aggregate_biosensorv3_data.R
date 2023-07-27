


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



## ----pressure, echo=FALSE, message=FALSE, warning=FALSE---------------------------------------------------------------------------------------
tic("Preparing data")
# measurement_id = "000012117203__2023-03-17T15_05_10-Measurement_5"
measurement_id = args = commandArgs(trailingOnly=TRUE)

bucket_dir = "s3://ascstore/flatfieldv2/"             # AWS S3 folder containing single-cell morphological measurements (or observations)
results_dir = "/home/ubuntu/dcp_helper/data/results/" # temporary local folder to pull single cell data

if ( dir.exists( file.path(results_dir,measurement_id) ) ) {
  print("start processing")
} else {
  print("needs to download")
  dir.create(file.path(results_dir,measurement_id))
  system( paste('aws s3 sync',
                paste0(bucket_dir, measurement_id),
                paste0(results_dir, measurement_id),
                '--exclude "*" --include "*.csv" --force-glacier-transfer --no-progress') )
}
toc()


## ----eval=FALSE, include=FALSE----------------------------------------------------------------------------------------------------------------
## tic("Listing results")
## file_list <- list.files(file.path(results_dir, measurement_id), pattern = "*Cells.csv", recursive=TRUE, full.names = TRUE)
## # file_list <- Sys.glob(file.path(results_dir, measurement_id,"*","*Cells.csv"))
## toc()


## ----eval=FALSE, include=FALSE----------------------------------------------------------------------------------------------------------------
## tic("Listing results")
## results_list <- dir(file.path(results_dir, measurement_id), full.names = TRUE)
## for (result in results_list) {
##
## }
## toc()


## ----message=FALSE, warning=FALSE-------------------------------------------------------------------------------------------------------------

compute_observations_checksum <- function(result_path, pattern = "*Cells.csv") {
  observation.list <- list.files(result_path, pattern = pattern, recursive = TRUE, full.names = TRUE)

  # reading measurements only for all z-stack samplings (resolution1, mid24)
  observation.list <- c(observation.list[grepl("resolution1", observation.list)], observation.list[grepl("mid24", observation.list)])
  # print(result_path)
  # print(observation.list)

  return(paste(tools::md5sum(observation.list), collapse = '|'))
}

read_and_merge_observations <- function(result_path, pattern = "*Cells.csv"){

  observation.list <- list.files(result_path, pattern = pattern, recursive = TRUE, full.names = TRUE)

  observation.list <- c(observation.list[grepl("resolution1", observation.list)], observation.list[grepl("mid24", observation.list)])
  # print(result_path)
  # print(observation.list)

  if (length(observation.list)<1) {

    print(paste0("No observation found for this measurement: ", result_path))
    return (NULL)

  } else {

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
}

# test functions
# observation.checksum <- compute_observations_checksum(results_list[1])
# reduced.observation <- read_and_merge_observations(results_list[1])


## ---------------------------------------------------------------------------------------------------------------------------------------------

endpoint.v3 = "biosensorv3.cluster-c9k2hfiwt5mi.us-east-2.rds.amazonaws.com"
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


## ----eval=FALSE, include=FALSE----------------------------------------------------------------------------------------------------------------
## # dbRemoveTable(con.v3, "measurement")
## # dbCreateTable(con.v3, "measurement", new_measurement)
## # dbExecute(con.v3, "TRUNCATE TABLE measurement")
##
## table_list <- dbListTables(con.v3)
## for (table_name in table_list) {
##   print(table_name)
##   table <- tbl(pool.v3, table_name)
##   glimpse(table)
##   # print(colnames(table))
## }


## ---------------------------------------------------------------------------------------------------------------------------------------------
# files <- list.files(file.path(results_dir, measurement_id), pattern = "ch1", full.names = TRUE) %>% paste0(., "/resolution1/", "measurement_ch2_Cells.csv")
results_list <- dir(file.path(results_dir, measurement_id), pattern = "ch1", full.names = TRUE)

tic("Creating records for new measurements")
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

tic("Adding new measurements to database")
new_measurement %>%
    dbWriteTable(pool.v3, "measurement", ., append = TRUE)
toc()


## ---------------------------------------------------------------------------------------------------------------------------------------------
# dbCreateTable(con.v3, "observation", reduced.observation)
# dbRemoveTable(con.v3, "observation") # completely deletes table schema
# dbExecute(con.v3, "TRUNCATE TABLE observation") # drops all rows from the table

# observation <- tbl(pool.v3, "observation")
# glimpse(observation)


## ---------------------------------------------------------------------------------------------------------------------------------------------
# tic("adding observations")
#
# new_measurement[1:1000,] %>%
#     unite("observation", contains("observation"), sep = "___") %>%
#     mutate(data = furrr::future_map2(observation,
#                                      full_path,
#                                      ~ {read_and_merge_observations(.y) %>%
#                                        mutate(observation = .x) %>%
#                                        separate(observation, c("id_observation", "id_observation_checksum"), sep = "___", ) %>%
#                                        dbWriteTable(pool.v3, "observation", ., append = TRUE)
#                                     print(paste0("appended ", .y))
#                                     flush.console() }
#     ))
# toc()


## ----message=FALSE, warning=FALSE-------------------------------------------------------------------------------------------------------------

tic("Adding new observations to database")

new_measurement_2 <- new_measurement %>%
  unite("observation", contains("observation"), sep = "___")

furrr::future_map2(new_measurement_2$observation,
                   new_measurement_2$full_path,
                     ~ { read_and_merge_observations(.y) %>%
                           mutate(observation = .x) %>%
                           separate(observation, c("id_observation", "id_observation_checksum"), sep = "___" ) %>%
                           dbWriteTable(pool.v3, "observation", ., append = TRUE)
                         # print(paste0("appended ", .y))
                         # flush.console()
                       }
    )
toc()

#1-500: 457.342sec
#501-1000: 454.088 sec elapsed
#all: 8000 sec


## ---------------------------------------------------------------------------------------------------------------------------------------------
# for (row_num in 1:nrow(new_measurement)) {
# for (row_num in c(1,2)) {
#   row <- new_measurement[row_num,]
#   glimpse(row)
#   observation <- read_and_merge_observations( row[1,"full_path"] %>% as.character() )
#   observation$id_observation <- row[1,"id_observation"] %>% as.character()
#   observation$id_observation_checksum <- compute_observations_checksum( row[1,"full_path"] %>% as.character() )
#   glimpse(observation)
# }


## ----eval=FALSE, include=FALSE----------------------------------------------------------------------------------------------------------------
## # observation <- tbl(pool.v3, "observation") %>% collect()

