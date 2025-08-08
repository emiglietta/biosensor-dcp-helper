#' List all elements in a directory and store a list of them in the metadata directory. If a filelist has already been created, it will not list all files again
#'
#' @param path Directory of raw files and Index.idx.xml file
#' @param force
#' @param path_base Destination directory of metadata
#'
#' @return A dataframe
#' @export
#' @import readr
#' @import dplyr
#' @import magrittr
#' @import reticulate
#'
#' @examples
build_filelist <- function(path, force, path_base, path_yml){

  # parent <- path %>% str_split(pattern = "/") %>% unlist %>% .[length(.)-1]

  if (file.exists(file.path(path_base, "loaddata_output.csv")) & force == FALSE){
    print("Found file list")
  } else {
    # TODO: re-add code if Index.idx.xml does not exist
    print("Initiating file list")
    system(paste("python3.8 -m pe2loaddata",
                "--index-directory", path,
                "--index-file", file.path(path, "Index.xml"),
                path_yml,
                file.path(path_base, "loaddata_output.csv"),     
                "--search-subdirectories"))
  }

  print("Reading file list")
  dir_content <- readr::read_csv(file.path(path_base, "loaddata_output.csv"), col_types = cols()) %>%
    dplyr::mutate(file_path = paste(path))

  return(dir_content)
}
