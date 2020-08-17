#' Title
#'
#' @param path
#' @param channel
#' @param interactive
#' @param interactive_path
#' @param name
#'
#' @return
#' @export
#'
#' @examples
create_flatfield_metadata_split <- function(path = paste0(getwd(), "/"),
                                      channel_of_interest,
                                      name = "bf",
                                      path_base = "~/bucket/metadata/",
                                      flatfield_dir = "flatfield",
                                      force = FALSE,
                                      include_brightfield_proj = FALSE,
                                      include_additional_proj = FALSE){
  file <- extract_filelist(path, force, path_base)

  #filtering the channel of interest
  print("filtering channel")
  file_f <- file %>%
    dplyr::filter(channel == channel_of_interest)

  #I reformat the table
  print("formatting data")
  file_ff <- file_f %>% reformat_filelist(metadata_tag = "original")

  if(include_brightfield_proj == TRUE | include_additional_proj == TRUE){
    file_ff <- add_proj(file_ff, channel_n = "ch2", flatfield_dir = flatfield_dir, projection_tag = "varprojection.tiff", metadata_tag = "varprojection")
    # file_ff <- add_brightfield_proj(file_ff, flatfield_dir = flatfield_dir, projection_tag = "varprojection.tiff", metadata_tag = "varprojection")
    # file_ff <- add_brightfield_proj(file_ff, flatfield_dir = flatfield_dir, projection_tag = "maxprojection.tiff", metadata_tag = "maxprojection")
    # file_ff <- add_brightfield_proj(file_ff, flatfield_dir = flatfield_dir, projection_tag = "minprojection.tiff", metadata_tag = "minprojection")
    # file_ff <- add_brightfield_proj(file_ff, flatfield_dir = flatfield_dir, projection_tag = "averageprojection.tiff", , metadata_tag = "averageprojection")
    # file_ff <- add_brightfield_proj(file_ff, flatfield_dir = flatfield_dir, projection_tag = "brightfieldprojection.tiff", , metadata_tag = "brightfieldprojection")
    print("adding brightfield metadata")
  }

  # uncoupled the two if statements
  if(include_additional_proj == TRUE){
      # file_ff <- add_flourescent_proj(file_ff, flatfield_dir = flatfield_dir)
      file_ff <- add_proj(file_ff, channel_n = "ch3", flatfield_dir = flatfield_dir, projection_tag = "maxprojection.tiff", metadata_tag = "maxprojection")
      file_ff <- add_proj(file_ff, channel_n = "ch4", flatfield_dir = flatfield_dir, projection_tag = "maxprojection.tiff", metadata_tag = "maxprojection")
      print("adding additional metadata")
  }

  metadata_split_path <- write_metadata_split(file_ff, name = name, path_base = path_base)

  return(metadata_split_path)

}
