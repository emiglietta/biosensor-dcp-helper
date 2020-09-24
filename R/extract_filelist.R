#' Return tibble describing each image file with well, field etc. in a given directory
#'
#' @param path the path of the directory to screen. The path is not checked recursively
#' @param force
#' @param path_base
#' @return a tibble with columns: file_name, n_zst, well, col, fld, zst, l_row, channel, file_path, name, ext, parent, timepoint
#' @export
#'
#' @examples
#'
extract_filelist <- function(path = getwd(), force, path_base, path_yml="~/mcsaba/biosensor/src/dcp_helper/python/pe2loaddata_config.yml"){
   file <- build_filelist(path, force, path_base, path_yml) %>%
      #I keep only tiff files
      separate(Image_FileName, c("file_name", "type"), sep = "\\.") %>%
      mutate(is_image = grepl(pattern = "tiff", x = type)) %>% filter(is_image == TRUE) %>%
      #I start formatting files
      mutate(channel = paste0('ch', Metadata_ChannelID)) %>%
      rename(row = Metadata_Row, col = Metadata_Col, fld = Metadata_FieldID, n_zst = Metadata_PlaneID, well = Metadata_Well) %>%
      mutate(zst = sprintf("%02d", n_zst)) %>%
      mutate(fld = sprintf("%02d", fld)) %>%
      rename(timepoint = Metadata_TimepointID) %>% mutate(timepoint = paste0("sk",timepoint+1)) %>%
      rename(abstime = Metadata_AbsTime) %>%
      rename(ext = type) %>%
      mutate(name = paste0(Image_PathName, file_name)) %>%
      select(-contains("Metadata_")) %>%
      mutate(parent = path %>% str_split(pattern = "/") %>% unlist %>% .[length(.)-2])
  return(file)
}
