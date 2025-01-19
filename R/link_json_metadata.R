#' Linking JSON job file to metadata object
#'
#' @param metadata_split_path Table that contains metadata and file information for CellProfiler pipeline using LoadData module
#' @param path_base Destination directory of job files
#' @param flatfield_dir Directory for intermediate and final results
#' @param path_to_metadata Directory containing metadata files
#'
#'
#' @return
#' @export
#' @import jsonlite
#' @import parallel
#'
#' @examples
link_json_metadata <- function(json_path,
                               metadata_split_path,
                               path_base,
                               flatfield_dir = "flatfield",
                               path_to_metadata = "dcp_helper/metadata"){
  #loading json file
  json <- jsonlite::read_json(path = json_path)

  json_metadata <- parallel::mclapply(metadata_split_path,
                                      link_data_file,
                                      json_template = json,
                                      json_path = json_path,
                                      path_base = path_base,
                                      flatfield_dir = flatfield_dir,
                                      path_to_metadata = path_to_metadata)
}




