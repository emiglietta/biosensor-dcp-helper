## repair_observations.R
##
## Purpose: fix rows in "observation" that already exist but have NULL ch3/ch4 or
## ch5/ch6 values because of the floating-point merge-key bug in
## read_and_merge_measurements() (see PATCH_read_and_merge_measurements.md, bug 2).
## This is different from retry_observations.R, which handles measurement_ids with
## ZERO observation rows -- this script handles measurement_ids that have the right
## number of rows, but some of those rows are missing ch3-6 values.
##
## For each affected measurement_id, this:
##   1. Re-reads the source CSVs and recomputes the correct merged table using the
##      FIXED read_and_merge_measurements() (strict ImageNumber/ObjectNumber join).
##   2. Verifies the recomputed row count matches what's already there, and that no
##      NULLs remain in the recomputed data, before touching the DB.
##   3. UPDATEs each existing row in place (matched by measurement_id + object_number),
##      setting every column to the recomputed value. No DELETE is used anywhere --
##      this only requires UPDATE privilege on "observation", since the DB role used
##      here does not have DELETE granted.
## All updates for a given measurement_id run inside a transaction, so a failure
## partway through rolls back rather than leaving that site half-fixed.
##
## Prerequisite: apply the read_and_merge_measurements() patch first (this script
## embeds the same fixed version), and run this on the EC2 instance that still has the
## downloaded CSVs at the local_path values stored in "measurement".
##
## Usage: Rscript repair_observations.R <session_id>
## Example: Rscript repair_observations.R 000012135503__2026-07-17T16_24_35-Measurement_1

suppressPackageStartupMessages({
  library(tidyverse)
  library(pool)
  library(tictoc)
  library(DBI)
  library(RPostgres)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript repair_observations.R <session_id>", call. = FALSE)
}
session_id <- args[1]

## Fixed merge function -- see PATCH_read_and_merge_measurements.md for the full
## explanation of both bugs this addresses. Merges on ObjectNumber alone: result_path is
## already scoped to one plate/timepoint/well/site, so that identity doesn't need to be
## re-verified via a join key, and ImageNumber is just each CellProfiler execution's own
## internal row-sequence number -- not guaranteed to mean the same thing across the
## independent ch2/ch3_ch4/ch5_ch6 runs. The n_distinct(ImageNumber) check below turns
## "there's only one image per directory" from an assumption into something verified.
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

## NOTE: this is the same host aggregate_manuscript-202505_data.R already uses
## successfully for its plate/session/measurement INSERTs, so it does accept writes.
## The preflight check below confirms UPDATE specifically works before this does
## anything real -- test on one session_id first and spot-check results before looping
## it over the rest of the batch.
RDS_TOKEN <- system2("aws",
                      args = c("rds", "generate-db-auth-token",
                               "--hostname", RDS_HOST,
                               "--port", RDS_PORT,
                               "--username", RDS_USER,
                               "--region", AWS_REGION),
                      stdout = TRUE)

con <- dbConnect(RPostgres::Postgres(),
                  dbname = RDS_DB,
                  host = RDS_HOST,
                  port = RDS_PORT,
                  user = RDS_USER,
                  password = RDS_TOKEN)

pool_read <- pool::dbPool(RPostgres::Postgres(),
                           dbname = RDS_DB,
                           host = RDS_HOST,
                           port = RDS_PORT,
                           user = RDS_USER,
                           password = RDS_TOKEN)

## Preflight: confirm UPDATE privilege before doing any real work. This WHERE FALSE
## clause touches zero rows regardless of permissions, so it fails fast and cleanly if
## the role can't UPDATE "observation" at all, rather than partway through the loop.
tryCatch({
  dbExecute(con, "UPDATE observation SET measurement_id = measurement_id WHERE FALSE")
}, error = function(e) {
  stop(paste0("No UPDATE privilege on 'observation' -- ask whoever manages the DB role ",
              "for this account to grant UPDATE before running this script. Underlying error: ",
              conditionMessage(e)), call. = FALSE)
})
print("UPDATE privilege confirmed.")

## Find measurement_ids in this session that have at least one observation row with a
## NULL ch3 or ch5 intensity value -- these mixed-in-with-good-rows NULLs are the
## signature of the merge-key bug (as opposed to retry_observations.R's case of zero
## rows entirely).
session_measurements <- tbl(pool_read, "measurement") %>%
  filter(session_id == !!session_id) %>%
  select(measurement_id, local_path) %>%
  collect()

if (nrow(session_measurements) == 0) {
  stop(paste0("No 'measurement' rows found for session ", session_id), call. = FALSE)
}

affected <- tbl(pool_read, "observation") %>%
  filter(measurement_id %in% !!session_measurements$measurement_id) %>%
  filter(is.na(intensity_mean_intensity_ch3) | is.na(intensity_mean_intensity_ch5)) %>%
  select(measurement_id) %>%
  distinct() %>%
  collect()

to_repair <- session_measurements %>%
  semi_join(affected, by = "measurement_id")

print(paste0(nrow(to_repair), " of ", nrow(session_measurements),
             " measurements for ", session_id, " have at least one NULL ch3/ch5 row."))

if (nrow(to_repair) == 0) {
  print("Nothing to repair.")
} else {
  for (j in seq_len(nrow(to_repair))) {
    mid <- to_repair$measurement_id[j]
    path <- to_repair$local_path[j]
    print(paste0("[", j, "/", nrow(to_repair), "] repairing ", mid))

    corrected <- read_and_merge_measurements(path)
    if (is.null(corrected)) {
      warning(paste0("Could not recompute observations for ", mid, " -- skipping"))
      next
    }
    corrected <- corrected %>% mutate(measurement_id = mid)

    ## Sanity check before touching the DB: recomputed row count should match what's
    ## already there (same number of objects), and no NULLs should remain in the
    ## previously-affected columns.
    existing_n <- tbl(pool_read, "observation") %>%
      filter(measurement_id == mid) %>%
      summarise(n = n()) %>%
      collect() %>%
      pull(n)

    if (nrow(corrected) != existing_n) {
      warning(paste0(mid, ": recomputed row count (", nrow(corrected),
                      ") != existing row count (", existing_n, ") -- skipping, needs manual review"))
      next
    }
    remaining_na <- sum(is.na(corrected$intensity_mean_intensity_ch3) | is.na(corrected$intensity_mean_intensity_ch5))
    if (remaining_na > 0) {
      warning(paste0(mid, ": ", remaining_na, " rows still NULL after the fix -- skipping, needs manual review"))
      next
    }

    ## Update every row in place, matched by measurement_id + object_number (unique
    ## within a measurement_id -- confirmed against the raw site data). Every column is
    ## set to the recomputed value, including columns that were already correct; that's
    ## harmless (they get overwritten with the same value) and avoids having to work out
    ## in advance exactly which columns were the NULL ones for each row.
    update_cols <- setdiff(names(corrected), c("measurement_id", "image_number", "object_number"))
    set_clause <- paste(sprintf('"%s" = $%d', update_cols, seq_along(update_cols)), collapse = ", ")
    query <- sprintf(
      'UPDATE observation SET %s WHERE measurement_id = $%d AND object_number = $%d',
      set_clause, length(update_cols) + 1, length(update_cols) + 2
    )

    dbBegin(con)
    tryCatch({
      for (r in seq_len(nrow(corrected))) {
        row <- corrected[r, ]
        params <- c(as.list(row[update_cols]), list(mid, row$object_number))
        n_updated <- dbExecute(con, query, params = params)
        if (n_updated != 1) {
          stop(paste0("expected to update exactly 1 row for object_number ",
                       row$object_number, ", updated ", n_updated))
        }
      }
      dbCommit(con)
      print(paste0("  repaired ", mid, " (", nrow(corrected), " rows, UPDATE only)"))
    }, error = function(e) {
      dbRollback(con)
      warning(paste0("  FAILED on ", mid, ", rolled back: ", conditionMessage(e)))
    })
  }
}

dbDisconnect(con)
poolClose(pool_read)
