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
<<<<<<< Updated upstream
build_filelist <- function(path, force, path_base, path_yml="~/mcsaba/biosensor/src/dcp_helper/python/pe2loaddata_config.yml"){
  # path mus be with trailing backslash

  # parent <- path %>% str_split(pattern = "/") %>% unlist() %>% .[length(.)-1]
=======
build_filelist <- function(path, force, path_base, path_yml){

#  path = "/home/ubuntu/bucket/inbox_mit/000012128303__2025-06-17T10_53_56-Measurement_1/Images/"
#  force = TRUE
#  path_base = "~/output.csv"
#  path_yml = "~/dcp_helper_csaba/python/pe2loaddata_config_000012128303.yml"
>>>>>>> Stashed changes

  if (file.exists(file.path(path_base, "loaddata_output.csv")) & force == FALSE){
    print("Found file list")
  } else {
    # TODO: re-add code if Index.idx.xml does not exist
    print("Initiating file list")

<<<<<<< Updated upstream
    system(paste("python2",
                 "~/projects/biosensor/src/biosensor-dcp-helper/python/pe2loaddata.py",
                 paste0("--index-directory=", path),
                 path_yml,
                 file.path(path_base, "loaddata_output.csv") ) )
=======
#### The new version of the Operetta index file (June 2025) requires the use of an updated version of pe2loaddata, which requires python 3.8 and has a different call

   # system(paste("python2",
   #              "~/dcp_helper_csaba/python/pe2loaddata.py",
   #              paste0("--index-directory=", path),
   #              path_yml,
   #              file.path(path_base, "loaddata_output.csv") ) )
   system(paste("python3.8",
                "-m",
		"pe2loaddata",
                paste0("--index-directory=", path),
                paste0("--index-file=", paste(path, "Index.xml", sep="/")),
                path_yml,
                file.path(path_base, "loaddata_output.csv"),
                "--search-subdirectories"))
>>>>>>> Stashed changes
  }

  print("Reading file list")
  dir_content <- readr::read_csv(file.path(path_base, "loaddata_output.csv"), col_types = cols()) %>%
    dplyr::mutate(file_path = paste(path))

  return(dir_content)
}

