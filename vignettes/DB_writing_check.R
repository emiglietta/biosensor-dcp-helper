# Utility code to check the number of wells written to the manuscript-202505 DB after transfering data
# use together with parallel and the list of sessions to check all at once

suppressPackageStartupMessages({
  library(tidyverse)
  library(pool)
  library(RPostgres)
})

args = commandArgs(trailingOnly=TRUE)

if (length(args) < 1) {
  stop(paste0(length(args), " arguments were provided. Usage: Rscript DB_writing_check.R <session_id>"), call. = FALSE)
}

session_id = args[1]

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

pool.manuscript202505 <- pool::dbPool(RPostgres::Postgres(),
                       dbname = RDS_DB,
                       host = RDS_HOST,
                       port = 5432,
                       user = RDS_USER,
                       password = RDS_TOKEN)

# Check what measurements exist (this should be fast)
measurement_check <- tbl(pool.manuscript202505, "measurement") %>%
  filter(session_id == !!session_id) %>%
  select(measurement_id, well) %>%
  collect()

measurement_count <- nrow(measurement_check)

# Check what timepoints exist
timepoint_check <- tbl(pool.manuscript202505, "measurement") %>%
  filter(session_id == !!session_id) %>%
  select(measurement_id, timepoint_id, well) %>%
  collect()

timepoint_count <- nrow(timepoint_check)

# If measurements exist, then query observations using %in%
if(measurement_count > 0) {
  obs_counts <- tbl(pool.manuscript202505, "observation") %>%
    filter(measurement_id %in% !!measurement_check$measurement_id) %>%
    group_by(measurement_id) %>%
    summarise(
      unique_image_numbers = n_distinct(image_number),
      total_observations = n(),
      .groups = 'drop'
    ) %>%
    collect() %>%
    # Join with measurement info to get well names
    left_join(measurement_check, by = "measurement_id") %>%
    select(well, unique_image_numbers, total_observations) %>%
    arrange(well)
  
  obs_count <- nrow(obs_counts)
} else {
  obs_count <- 0
}


# Output results to console
cat(session_id, "measurement well count:", measurement_count, "||","timepoint count:", timepoint_count, sep = "  ", "observation well count:", obs_count, sep = "  ")
cat("\n")

# Close the connection pool
poolClose(pool.manuscript202505)
