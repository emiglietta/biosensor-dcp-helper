#' Fill a JSON template with a specific metadata path and metadata groupings
#'
#' @param metadata_split_path Table that contains metadata and file information for CellProfiler pipeline using LoadData module
#' @param json_template
#' @param json_path
#' @param path_base Destination directory of job files
#' @param flatfield_dir Directory for intemediate and final results
#' @param path_to_metadata Directory containing metadata files
#'
#' @return
#' @export
#' @import jsonlite
#' @import parallel
#'
#' @examples
#'

link_data_file <- function(metadata_split_path,
                           json_template,
                           json_path,
                           path_base,
                           flatfield_dir = "flatfield",
                           path_to_metadata = 'dcp_helper/metadata'){

  json_new <- json_template #probably not needed

  json_new$data_file <- metadata_split_path %>% #str_sub(21, -1) #10, -1
    str_extract(pattern = file.path(path_to_metadata, "\\d+__\\d+-\\d\\d-\\d+T\\d+_\\d+_\\d+-Measurement_\\d+/.*"))

  json_new$output <- file.path(flatfield_dir,
                            str_extract(pattern = "\\d+__\\d+-\\d\\d-\\d+T\\d+_\\d+_\\d+-Measurement_\\d+", string = metadata_split_path))

  # I create the path for the grouping data that was previously generated using a python script. #TODO - incorporate the python script's function into R
  grouping_path <- metadata_split_path %>% str_sub(.,1, -5) %>% paste0(., "batch.txt")
  # I import the grouping text after trimming the last comma
  grouping <- readLines(grouping_path) %>% paste(. , collapse = " ") %>% str_sub(., 1, -3) %>% paste("[", ., "]") %>% jsonlite::fromJSON()

  # Now I insert the grouping into the JSON
  json_new$groups <- grouping

  # Finally I write my results
  name_metadata <- metadata_split_path %>% str_split(pattern = "/") %>% unlist() %>% tail(1) %>% str_sub(1, -5)
  name_json <- json_path %>% str_split(pattern = "/") %>% unlist() %>% tail(1) %>% str_sub(1, -15)

  jsonlite::write_json(json_new, pretty = TRUE, path = file.path(path_base, paste0(name_json, "_", name_metadata, ".json")), auto_unbox = TRUE)
  print(paste0("created ", file.path(path_base, paste0(name_json, "_", name_metadata, ".json"))))

}
