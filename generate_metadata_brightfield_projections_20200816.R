################ Important Inputs
## Define directory names - you will have to change this manually!
## Make sure the directories contain no spaces!
## The pipeline assumes there is a directory in the "inbox" directory that has the following name:
## Call this script with: Rscript generate_metadata_"your-platename(s)-here".R

library(tidyverse)
library(dcphelper)
library(tictoc)

format_output_structure <- function(metadata_tags){
  paste0(metadata_tags, collapse = "-")
}

plate_name = args = commandArgs(trailingOnly=TRUE)
# for debugging only
#plate_name = "000012095203__2019-12-09T17_58_26-Measurement_1"
print(paste0("Processing plate ", plate_name))

################ Define paths

flatfield_dir = "flatfield"

new_path_base = paste0("~/dcp_helper/metadata/", plate_name,"/") #relative path acceptable
inbox_path_base= paste0("/home/ubuntu/bucket/inbox/", plate_name,"/Images/") #absolute path with /home/ubuntu/ required
flatfield_path_base= paste0("~/bucket/", flatfield_dir, "/", plate_name,"/") #deprecated: only used for result collection

################ Creating target dir

lapply(new_path_base, dir.create, recursive=TRUE) # Do not execute this from a local machine if you expect other AWS services to access the directory later on

################ Set JSON template paths

new_json_path_brightfield_flatfield = "~/dcp_helper/python/job_brightfield_flatfield_template.json" #brightfield FFC
new_json_path_fluorescent_flatfield = "~/dcp_helper/python/job_fluorescent_flatfield_template.json" #fluorescent FFC

# new_json_path_brightfield_projection = "~/dcp_helper/python/job_brightfield_projection_template.json" #fluorescent projection
new_json_path_fluorescent_projection = "~/dcp_helper/python/job_fluorescent_projection_template.json" #fluorescent projection

new_json_path_segmentation = "~/dcp_helper/python/job_segmentation_template.json"
new_json_path_featureextraction = "~/dcp_helper/python/job_featureextraction_template.json"

################ This is where the execution starts

#==================================================#
#         PHASE 1: flatfield correction            #
#==================================================#

## Name of channels
channel_ffc_v <- c("ch2", "ch3", "ch4")
channel_ffc_n <- c("ffc_brightfield", "ffc_ch3", "ffc_ch4")
json_ffc_templates <- c(new_json_path_brightfield_flatfield, new_json_path_fluorescent_flatfield, new_json_path_fluorescent_flatfield)

################ Creating flatfield correction metadata (-> *.csv)

print("Creating flatfield correction metadata")
tic()
for(i in 1:length(channel_ffc_n)){
  file <- extract_filelist(path = inbox_path_base, force=FALSE, new_path_base)
  file_f <- file %>%
    dplyr::filter(channel == channel_ffc_v[i])
  file_ff <- file_f %>% reformat_filelist() %>%
    rename(Image_PathName_original = Image_PathName_brightfield,
           Image_FileName_original = Image_FileName_brightfield)
  metadata_split_path <- write_metadata_split(file_ff, name = channel_ffc_n[i], path_base = new_path_base)
}
toc()

################ Grouping feature extraction / measurements metadata (-> *create_group.sh, *.batch.txt)
################ Aggregating information and executable file

tic()
print("Creating shell script for grouping")
path <- c()
path <- generate_group(plate_name,
                       c(channel_ffc_n),
                       new_path_base,
                       group_tag = "ffc")
print(path)
print("Grouping data using python script")
system(path)
toc()

################  (-> job*.json)

print("Generating job files with grouping")

tic()
for (i in 1:length(channel_ffc_v)){
  print(paste0(i, ": Generating job files: ", channel_ffc_n))
  link_json_metadata(metadata_split_path = list.files(new_path_base, pattern = "metadata_", full.names = TRUE) %>%
                       stringr::str_subset(pattern = ".csv") %>%
                       stringr::str_subset(pattern = channel_ffc_n[i]),
                     json_path = json_ffc_templates[i],
                     path_base = new_path_base)
}
toc()

################ Grouping final job files (-> quick*.sh)

tic()
for(i in 1:length(channel_ffc_n)){
  group_jobs_bash(path_base = new_path_base,
                  name = channel_ffc_n[i],
                  letter_row_interval = c(1:16),
                  number_col_interval = c(1:24))
}
toc()

#==================================================#
#              PHASE 2: projections                #
#==================================================#

# ## Name of channels
channel_projection_v <- c("ch3", "ch4")
channel_projection_n <- c("maximumprojection_ch3", "maximumprojection_ch4")
json_projection_templates <- c(new_json_path_fluorescent_projection, new_json_path_fluorescent_projection)
#
# ################ Creating fluorescence projection metadata (-> *.csv)

tic()
print("Creating fluorescence projection metadata")
for(i in 1:length(channel_projection_v)){
  file <- extract_filelist(path = inbox_path_base, force=FALSE, new_path_base)
  file_ff <- file %>%
    dplyr::filter(channel == channel_projection_v[i]) %>%
    reformat_filelist() %>%
    rowwise() %>%
    mutate(Image_FileName_original = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, Metadata_zst, channel_projection_v[i])), "_flatfield_corrected.tiff"),
           Image_PathName_original = Image_PathName_brightfield %>%
              stringr::str_split(pattern = "/") %>%
              unlist() %>% .[c(1:4)] %>%
              append(flatfield_dir) %>%
              append(Metadata_parent) %>%
              append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, channel_projection_v[i]))) %>%
              paste(collapse = "/") ) %>%
              select(-Image_PathName_brightfield, -Image_FileName_brightfield) %>% ungroup()
  metadata_split_path <- write_metadata_split(file_ff, name = channel_projection_n[i], path_base = new_path_base)
}
toc()

################ Grouping feature extraction / measurements metadata (-> *create_group.sh, *.batch.txt)
################ Aggregating information and executable file

tic()
print("Creating shell script for grouping")
path <- c()
path <- generate_group(plate_name,
                       c(channel_projection_n),
                       new_path_base,
                       group_tag = "projection")
print(path)
print("Grouping data using python script")
system(path)
toc()

# ################ (-> job*.json)

print("Generating job files with grouping")

tic()
for (i in 1:length(channel_projection_v)){
  link_json_metadata(metadata_split_path = list.files(new_path_base, pattern = "metadata_", full.names = TRUE) %>%
                       stringr::str_subset(pattern = ".csv") %>%
                       stringr::str_subset(pattern = channel_projection_n[i]),
                     json_path = json_projection_templates[i],
                     path_base = new_path_base)
}
toc()

################ Grouping projection final job files

tic()
for(i in 1:length(channel_projection_n)){
  group_jobs_bash(path_base = new_path_base,
                  name = channel_projection_n[i],
                  letter_row_interval = c(1:16),
                  number_col_interval = c(1:24))
}
toc()

#==================================================#
#             PHASE 3: segmentation                #
#==================================================#


# Name of channels
channel_v <- c("ch1")
channel_segmentation_n <- c("segmentation_ch1")
json_segmentation_templates = c(new_json_path_segmentation)

# ################ Creating segmentation metadata (-> *.csv)

tic()
print("Creating segmentation metadata")
for(i in 1:length(channel_segmentation_n)){
  file <- extract_filelist(path = inbox_path_base, force=FALSE, new_path_base)
  file_ff <- file %>%
    dplyr::filter(channel == channel_v[i]) %>%
    reformat_filelist() %>%
    rowwise() %>%
    mutate(Image_FileName_original = Image_FileName_brightfield,
           Image_PathName_original = Image_PathName_brightfield ) %>%
    select(-Image_PathName_brightfield, -Image_FileName_brightfield) %>% ungroup()
  metadata_split_path <- write_metadata_split(file_ff, name = channel_segmentation_n[i], path_base = new_path_base)
}
toc()

################ Grouping feature extraction / measurements metadata (-> *create_group.sh, *.batch.txt)
################ Aggregating information and executable file

tic()
print("Creating shell script for grouping")
path <- c()
path <- generate_group(plate_name,
                       c(channel_segmentation_n),
                       new_path_base,
                       group_tag = "segmentation")
print(path)
print("Grouping data using python script")
system(path)
toc()

################# (-> job*.json)

tic()
print("Generating job files with grouping")

for (i in 1:length(channel_segmentation_n)){
  link_json_metadata(metadata_split_path = list.files(new_path_base, pattern = "metadata_", full.names = TRUE) %>%
                       stringr::str_subset(pattern = ".csv") %>%
                       stringr::str_subset(pattern = channel_segmentation_n[i]),
                     json_path = json_segmentation_templates[i],
                     path_base = new_path_base)
}
toc()

################ Grouping final job files

tic()
for(i in 1:length(channel_segmentation_n)){
  group_jobs_bash(path_base = new_path_base,
                  name = channel_segmentation_n[i],
                  letter_row_interval = c(1:16),
                  number_col_interval = c(1:24))
}
toc()

#==================================================#
#   PHASE 4: feature extraction / measurements     #
#==================================================#

## Name of channels
channel_v <- c("ch1")
channel_measurement_n <- c("measurement_ch3")

################ Creating feature extraction / measurements metadata (-> *.csv)

# NOTE: only ch3 added

tic()
print("Creating feature extraction / measurements metadata")
for(i in 1:length(channel_v)){
  file <- extract_filelist(path = inbox_path_base, force=FALSE, new_path_base)
  file_ff <- file %>%
    dplyr::filter(channel == channel_v[i]) %>%
    reformat_filelist() %>%
    rowwise() %>%
    mutate(Image_ObjectsFileName_Cells = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch1")), "_segmentation.tiff"),
           Image_ObjectsPathName_Cells = Image_PathName_brightfield %>%
              stringr::str_split(pattern = "/") %>%
              unlist() %>% .[c(1:4)] %>%
              append(flatfield_dir) %>%
              append(Metadata_parent) %>%
              append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, channel_v[i]))) %>%
              paste(collapse = "/"),
           Image_FileName_ch3 = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch3")), "_maxprojection.tiff") ,
           Image_PathName_ch3 = Image_PathName_brightfield %>%
             stringr::str_split(pattern = "/") %>%
             unlist() %>% .[c(1:4)] %>%
             append(flatfield_dir) %>%
             append(Metadata_parent) %>%
             append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch3"))) %>%
             paste(collapse = "/")) %>%
              select(-Image_PathName_brightfield, -Image_FileName_brightfield) %>% ungroup()
  # glimpse(file_ff)
  metadata_split_path <- write_metadata_split(file_ff, name = channel_measurement_n[i], path_base = new_path_base)
}
toc()

################ Grouping feature extraction / measurements metadata (-> *create_group.sh, *.batch.txt)
################ Aggregating information and executable file

tic()
print("Creating shell script for grouping")
path <- c()
path <- generate_group(plate_name,
                       c(channel_measurement_n),
                       new_path_base,
                       group_tag = "featureextraction")
print(path)
print("Grouping data using python script")
system(path)
toc()

# ################# Job files

tic()
print("Generating job files with grouping")
for (i in 1:length(channel_measurement_n)){
  link_json_metadata(metadata_split_path = list.files(new_path_base, pattern = "metadata_", full.names = TRUE) %>%
                       stringr::str_subset(pattern = ".csv") %>%
                       stringr::str_subset(pattern = channel_measurement_n[i]),
                     json_path = new_json_path_featureextraction,
                     path_base = new_path_base)
}
toc()

# ################ Grouping final job files

tic()
for(i in 1:length(channel_measurement_n)){
  group_jobs_bash(path_base = new_path_base,
                  name = channel_measurement_n[i],
                  letter_row_interval = c(1:16),
                  number_col_interval = c(1:24))
}
toc()

################ Pushing metadata to S3 bucket
system("./sync_metadata_to_bucket.sh")
