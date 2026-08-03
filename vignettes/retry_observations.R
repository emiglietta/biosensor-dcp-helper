## retry_observations.R
##
## Purpose: (re)write "observation" rows for measurements that are already registered
## in the "measurement" table but have zero rows in "observation" -- e.g. because the
## first transfer attempt hit the missing area_shape_convex_area column error (see
## PATCH_read_and_merge_measurements.md in this same folder).
##
## Safe to run more than once: it only (re)writes observations for measurement_ids that
## currently have zero rows in "observation", so it won't duplicate rows for anything
## that already succeeded.
##
## Prerequisite: run this on the EC2 control node that still has the downloaded
## mid24/resolution1 CSVs under /home/ubuntu/data_tmp/<session_id>/... (the local_path
## values stored in the "measurement" table point there).
##
## Usage: Rscript retry_observations.R <session_id>
## Example: Rscript retry_observations.R 000012135503__2026-07-17T16_24_35-Measurement_1

suppressPackageStartupMessages({
  library(tidyverse)
  library(pool)
  library(tictoc)
  library(DBI)
  library(RPostgres)
  library(furrr)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript retry_observations.R <session_id>", call. = FALSE)
}
session_id <- args[1]

## Same fix as the patch for aggregate_manuscript-202505_data.R:
##  1. drop AreaShape_ConvexArea (not present in the `observation` table)
##  2. merge on ObjectNumber ALONE, not ImageNumber/ObjectNumber. result_path is already
##     scoped to one plate/timepoint/well/site, so that identity doesn't need to be
##     re-verified via a join key -- ImageNumber is just each CellProfiler execution's
##     own internal row-sequence number and isn't guaranteed to mean the same thing
##     across the independent ch2/ch3_ch4/ch5_ch6 runs. Also drops the duplicate
##     AreaShape/Location columns ch3_ch4/ch5_ch6 re-measure, which can differ from
##     ch2's copy by floating-point rounding and would otherwise leak into the join
##     (merge() without `by=` joins on every shared column name).
## See PATCH_read_and_merge_measurements.md in this folder for the full writeup.
read_and_merge_measurements <- function(result_path, pattern = "*Cells.csv") {
  measurement.list <- list.files(result_path, pattern = pattern, recursive = TRUE, full.names = TRUE)
  measurement.list <- c(measurement.list[grepl("resolution1", measurement.list)],
                         measurement.list[grepl("mid24", measurement.list)])

  if (length(measurement.list) < 1) {
    print(paste0("No measurements found for this session: ", result_path))
    return(NULL)
  }

  tbl_list <- list()
  for (i in seq_along(measurement.list)) {
    df <- read_csv(measurement.list[i], show_col_types = FALSE)

    n_images <- n_distinct(df$ImageNumber)
    if (n_images != 1) {
      stop(paste0(measurement.list[i], " has ", n_images,
                  " distinct ImageNumbers, expected exactly 1 -- merging by ObjectNumber alone is unsafe here."))
    }

    if (i > 1) {
      shared_non_key <- setdiff(intersect(names(df), names(tbl_list[[1]])), "ObjectNumber")
      df <- df %>% select(-any_of(shared_non_key))
    }
    tbl_list[[i]] <- df %>% select(-any_of("AreaShape_ConvexArea"))
  }

  reduced.observations <- Reduce(function(x, y) merge(x, y, by = "ObjectNumber", all.x = TRUE), tbl_list) %>%
    select(-contains("Metadata"))
  colnames(reduced.observations) <- colnames(reduced.observations) %>%
    str_replace(., "projection", "")

  reduced.observations %>% janitor::clean_names()
}

## ---------------------------------------------------------------------------------------------
RDS_HOST <- "manuscript-202505-cluster.cluster-ro-c9k2hfiwt5mi.us-east-2.rds.amazonaws.com"
RDS_DB <- "biosensor"
RDS_PORT <- 5432
RDS_USER <- "nrindtorff"
AWS_REGION <- "us-east-2"

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

## All measurement rows already registered for this session (these already succeeded --
## it's only the observation writes that are missing)
session_measurements <- tbl(pool.manuscript202505, "measurement") %>%
  filter(session_id == !!session_id) %>%
  select(measurement_id, local_path) %>%
  collect()

if (nrow(session_measurements) == 0) {
  poolClose(pool.manuscript202505)
  stop(paste0("No 'measurement' rows found for session ", session_id,
              ". Run aggregate_manuscript-202505_data.R for this session first."),
       call. = FALSE)
}

## Which of those measurement_ids currently have zero rows in "observation"?
existing_observation_counts <- tbl(pool.manuscript202505, "observation") %>%
  filter(measurement_id %in% !!session_measurements$measurement_id) %>%
  group_by(measurement_id) %>%
  summarise(n = n(), .groups = "drop") %>%
  collect()

to_retry <- session_measurements %>%
  anti_join(existing_observation_counts, by = "measurement_id")

print(paste0(nrow(to_retry), " of ", nrow(session_measurements),
             " measurements for ", session_id, " are missing observations."))

if (nrow(to_retry) == 0) {
  print("Nothing to do -- every measurement for this session already has observations.")
} else {
  tic("Writing missing observations")
  furrr::future_map2(
    to_retry$measurement_id, to_retry$local_path,
    ~ {
      result <- read_and_merge_measurements(.y)
      if (!is.null(result)) {
        result %>%
          mutate(measurement_id = .x) %>%
          dbWriteTable(pool.manuscript202505, "observation", ., append = TRUE)
        print(paste0("Wrote observations for ", .x))
      }
    }
  )
  toc()
}

poolClose(pool.manuscript202505)
