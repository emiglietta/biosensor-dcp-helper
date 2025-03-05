#' format_output_structure
#'
#' @param metadata_tags List of metadata values
#' @return Formatted string for output structure
#' @export

format_output_structure <- function(metadata_tags){
  paste0(metadata_tags, collapse = "-")
}
