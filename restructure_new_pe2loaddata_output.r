# Load required libraries
library(tidyverse)
library(readr)

# Function to restructure the CSV
restructure_csv <- function(input_file, output_file = NULL) {
  
  # Read the CSV file
  df <- read_csv(input_file)
  
  # Get all FileName columns
  filename_cols <- grep("^FileName_", names(df), value = TRUE)
  
  # Get all PathName columns  
  pathname_cols <- grep("^PathName_", names(df), value = TRUE)
  
  # Get metadata columns (excluding the ones we're restructuring)
  metadata_cols <- names(df)[!grepl("^(FileName_|PathName_)", names(df))]
  
  # Create a list to store all restructured data
  restructured_list <- list()
  
  # Process each FileName column
  for (i in seq_along(filename_cols)) {
    filename_col <- filename_cols[i]
    
    # Extract channel name from column name (everything after "FileName_")
    channel_name <- sub("^FileName_", "", filename_col)
    
    # Find corresponding PathName column
    pathname_col <- paste0("PathName_", channel_name)
    
    # Check if corresponding PathName column exists
    if (pathname_col %in% pathname_cols) {
      
      # Create temporary dataframe for this channel
      temp_df <- df %>%
        select(all_of(metadata_cols), all_of(filename_col), all_of(pathname_col)) %>%
        # Remove rows where both filename and pathname are NA
        filter(!is.na(!!sym(filename_col)) | !is.na(!!sym(pathname_col))) %>%
        # Rename columns
        rename(
          Image_FileName = !!sym(filename_col),
          Image_PathName = !!sym(pathname_col)
        ) %>%
        # Add channel information
        mutate(
          Metadata_ChannelCPName = channel_name
        )
      
      # Add to list
      restructured_list[[i]] <- temp_df
    }
  }
  
  # Combine all dataframes
  final_df <- bind_rows(restructured_list)
  
  # Reorder columns to put new columns first
  final_df <- final_df %>%
    select(Image_FileName, Image_PathName, Metadata_ChannelCPName, everything())

  
  # Write output file if specified
  if (!is.null(output_file)) {
    write_csv(final_df, output_file)
    cat("Restructured data written to:", output_file, "\n")
  }
  
  # Print summary
  cat("Original data dimensions:", nrow(df), "rows x", ncol(df), "columns\n")
  cat("Restructured data dimensions:", nrow(final_df), "rows x", ncol(final_df), "columns\n")
  cat("Number of channels found:", length(unique(final_df$Metadata_ChannelCPName)), "\n")
  cat("Channels:", paste(unique(final_df$Metadata_ChannelCPName), collapse = ", "), "\n")
  
  return(final_df)
}

# Usage example:
# Uncomment and modify the following lines to use the function

# # Basic usage - replace "your_file.csv" with your actual file path
# result <- restructure_csv("your_file.csv")
# 
# # To save the output to a new file
# result <- restructure_csv("your_file.csv", "restructured_data.csv")
# 
# # View the first few rows of the result
# head(result)



# input = '/Users/emigliet/Downloads/loaddata_output (2).csv'
# output = '/Users/emigliet/Downloads/loaddata_output_restructured.csv'

# result <- restructure_csv(input, output)
# head(result)