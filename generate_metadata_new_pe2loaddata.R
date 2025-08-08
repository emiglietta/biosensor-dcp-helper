################ Important Inputs
## Define directory names - you will have to change this manually!
## Make sure the directories contain no spaces!
## The pipeline assumes there is a directory in the "inbox" directory that has the following name:
## Call this script with: Rscript generate_metadata_alternative_setup_20220929.R <measurement_id>

library(tidyverse)
library(dcphelper)
library(tictoc)

format_output_structure <- function(metadata_tags){
  paste0(metadata_tags, collapse = "-")
}

plate_name = args = commandArgs(trailingOnly=TRUE)

# for debugging only
#plate_name = "000012128303__2025-06-17T10_53_56-Measurement_1"

print(paste0("Processing plate ", plate_name))

################ Define paths

bucket_mount_dir = "/home/ubuntu/bucket"

inbox_dir = "inbox_mit"
flatfield_dir = "flatfield"
metadata_dir = "dcp_helper_csaba/metadata"

new_path_base = normalizePath(paste("~", metadata_dir, plate_name, sep="/")) #relative path acceptable
inbox_path_base= paste(bucket_mount_dir, inbox_dir, plate_name, "Images/", sep="/") #absolute path with /home/ubuntu/ required. Trailing / required
flatfield_path_base= normalizePath(paste(bucket_mount_dir, flatfield_dir, plate_name, sep="/"))

dcp_helper_config_dir = "~/dcp_helper_csaba/python"

path_yml = file.path(dcp_helper_config_dir, "pe2loaddata_config_000012128303.yml") # should match with Index.idx.xml metadata

# To list unique channel names you can run the following command
# `grep 'Entry ChannelID' -A1 Index.xml  #grep the line of the string and 1 line below it`

################ Creating target dir

lapply(new_path_base, dir.create, recursive=TRUE) # Do not execute this from a local machine if you expect other AWS services to access the directory later on

################ Set JSON template paths

# All json templates are available in fork csmolnar/dcp_helper

# READY
new_json_path_brightfield_flatfield =  file.path(dcp_helper_config_dir,"job_brightfield_flatfield_template.json") #brightfield FFC
new_json_path_fluorescent_flatfield =  file.path(dcp_helper_config_dir,"job_fluorescent_flatfield_template.json") #fluorescent FFC

# TO REPRODUCE
new_json_path_brightfield_projection =  file.path(dcp_helper_config_dir, "job_brightfield_projection_template.json") #fluorescent projection
new_json_path_fluorescent_projection =  file.path(dcp_helper_config_dir, "job_fluorescent_projection_downsampling_template.json") #fluorescent projection, downsampling

# READY
new_json_path_segmentation = "~/dcp_helper/python/job_segmentation_template.json"

# TO REPRODUCE
new_json_path_featureextraction_ch2 =  file.path(dcp_helper_config_dir, "job_featureextraction_ch2_template.json")
new_json_path_featureextraction_ch3_ch4 =  file.path(dcp_helper_config_dir, "job_featureextraction_ch3_ch4_downsampled_template.json")
new_json_path_featureextraction_ch5_ch6 =  file.path(dcp_helper_config_dir, "job_featureextraction_ch5_ch6_downsampled_template.json")

python_call_submitjob = "python ~/DCP2.0/run.py submitJob "

################ This is where the execution starts

fileConn <- file(file.path(new_path_base, paste0("metadata_generation_", plate_name,".log")))
start.time <- Sys.time()
writeLines(c("Start time: ", start.time), fileConn)

#==================================================#
#       PHASE 0: initiate metadata dataframe       #
#==================================================#

loaddata_output <- build_filelist(path = inbox_path_base, force=FALSE, new_path_base, path_yml ) %>%
   separate(Metadata_ChannelCPName, c("Metadata_ChannelCPName", "Metadata_PlaneID"), '_') %>%
   transform(., Metadata_PlaneID = as.numeric(Metadata_PlaneID)) %>%
   replace_na(list(Metadate_PlaneID = 0)) %>%
   mutate(Metadata_ChannelID = case_when(
     Metadata_ChannelCPName == "PhaseContrast" ~ 1,
     Metadata_ChannelCPName == "Brightfield" ~ 2,
     Metadata_ChannelCPName == "Ch3" ~ 3,
     Metadata_ChannelCPName == "Ch4" ~ 4,
     Metadata_ChannelCPName == "Ch5" ~ 5,
     Metadata_ChannelCPName == "Ch6" ~ 6
   )) %>%
   mutate(channel = paste0('ch', Metadata_ChannelID)) %>%
   separate(Image_FileName, c("file_name", "type"), sep = "\\.") %>%
   mutate(is_image = grepl(pattern = "tiff", x = type)) %>% filter(is_image == TRUE) %>%
   rename(row = Metadata_Row, col = Metadata_Col, fld = Metadata_FieldID, n_zst = Metadata_PlaneID, well = Metadata_Well) %>%
   mutate(zst = sprintf("%02d", n_zst+2)) %>%
   mutate(fld = sprintf("%02d", fld)) %>%
   rename(timepoint = Metadata_TimepointID) %>% mutate(timepoint = paste0("sk",as.numeric(timepoint)+1)) %>%
   rename(abstime = Metadata_AbsTime) %>%
   rename(ext = type) %>%
   mutate(name = file.path(Image_PathName, file_name)) %>%
   select(-contains("Metadata_")) %>%
   mutate(parent = inbox_path_base %>% str_split(pattern = "/") %>% unlist %>% .[length(.)-1])

loaddata.finish.time <- Sys.time()
# writeLines(c("Loaddata finished:", loaddata.finish.time), fileConn)

# plane_format = "%02d"
# # brightfield_projection_subsets <- list(
# #   resolution1 = lapply(seq(0,8,by=1), sprintf, fmt=plane_format),
# #   resolution2 = lapply(seq(0,8,by=2), sprintf, fmt=plane_format)
# # )
#
# fluorescent_projection_subsets <- list(
#   # resolution1 = lapply(seq(1,14,by=1), sprintf, fmt=plane_format)#,
#   # resolution2 = lapply(seq(1,14,by=2), sprintf, fmt=plane_format)#,
#   # resolution3 = lapply(seq(1,14,by=3), sprintf, fmt=plane_format),
#   # resolution4 = lapply(seq(1,14,by=4), sprintf, fmt=plane_format),
#   # middle3 = lapply(seq(5,7,by=1), sprintf, fmt=plane_format),
#   # middle4 = lapply(seq(5,8,by=1), sprintf, fmt=plane_format),
#   # middle5 = lapply(seq(4,8,by=1), sprintf, fmt=plane_format),
#   # middle6 = lapply(seq(4,9,by=1), sprintf, fmt=plane_format)
#   # mid123 = lapply(seq(1,3,by=1), sprintf, fmt=plane_format),
#   # mid234 = lapply(seq(2,4,by=1), sprintf, fmt=plane_format),
#   # mid345 = lapply(seq(3,5,by=1), sprintf, fmt=plane_format),
#   # mid456 = lapply(seq(4,6,by=1), sprintf, fmt=plane_format),
#   # mid246 = lapply(seq(2,6,by=2), sprintf, fmt=plane_format),
#   mid24 = lapply(seq(2,4,by=2), sprintf, fmt=plane_format)
# )

brightfield_planes = as.list(loaddata_output %>% filter(channel=="ch2") %>% .$zst %>% unique())
fluorescent_planes = as.list(loaddata_output %>% filter(channel=="ch3") %>% .$zst %>% unique())

brightfield_projection_subsets <- list(
  resolution1 = brightfield_planes
  # resolution2 = brightfield_planes[seq(1,length(brightfield_planes),by=2)]
)

fluorescent_projection_subsets <- list(
  mid24 = fluorescent_planes
  # mid2 = fluorescent_planes[1],
  # mid4 = fluorescent_planes[2]
)

#==================================================#
#         PHASE 1: flatfield correction            # # DONE #
#==================================================#

# if (FALSE){

# Name of channels
channel_ffc_v <- c("ch2", "ch3", "ch4", "ch5", "ch6")
channel_ffc_n <- c("ffc_brightfield", "ffc_ch3", "ffc_ch4", "ffc_ch5", "ffc_ch6")
json_ffc_templates <- c(new_json_path_brightfield_flatfield,
                        new_json_path_fluorescent_flatfield,
                        new_json_path_fluorescent_flatfield,
                        new_json_path_fluorescent_flatfield,
                        new_json_path_fluorescent_flatfield)

################ Creating flatfield correction metadata (-> *.csv)

print("Creating flatfield correction metadata")
tic()
for(i in 1:length(channel_ffc_n)){
  file_ff <- loaddata_output %>%
    dplyr::filter(channel == channel_ffc_v[i]) %>%
    reformat_filelist() %>%
    rename(Image_PathName_original = Image_PathName_brightfield,
           Image_FileName_original = Image_FileName_brightfield)
  metadata_split_path <- write_metadata_split(file_ff, name = channel_ffc_n[i], path_base = new_path_base)
}
toc()

################ Grouping feature extraction / measurements metadata (-> *create_group.sh, *.batch.txt)
################ Aggregating information and executable file

tic()
print("Creating shell script for grouping fluorescent ffc")
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
  print(paste0(i, ": Generating job files: ", channel_ffc_n[i]))
  link_json_metadata(metadata_split_path = list.files(new_path_base, pattern = "metadata_", full.names = TRUE) %>%
                       stringr::str_subset(pattern = ".csv") %>%
                       stringr::str_subset(pattern = channel_ffc_n[i]),
                     json_path = json_ffc_templates[i],
                     path_base = new_path_base,
                     flatfield_dir = flatfield_dir,
                     path_to_metadata = metadata_dir)
}
toc()

################ Grouping final job files (-> quick*.sh)

tic()
for(i in 1:length(channel_ffc_n)){
  group_jobs_bash(path_base = new_path_base,
                  name = channel_ffc_n[i],
                  letter_row_interval = c(1:16),
                  number_col_interval = c(1:24),
                  python_call = python_call_submitjob)
}
toc()
# end ffc section

ffc.finish.time <- Sys.time()
# writeLines(c("FFC finished:", ffc.finish.time), fileConn)

#==================================================#
#        PHASE 2.1: fluorescent projections        #
#==================================================#

## Name of channels
channel_projection_v <- c("ch3", "ch4", "ch5", "ch6")
channel_projection_n <- c("maximumprojection_ch3", "maximumprojection_ch4", "maximumprojection_ch5", "maximumprojection_ch6")
json_projection_templates <- c(new_json_path_fluorescent_projection,
                               new_json_path_fluorescent_projection,
                               new_json_path_fluorescent_projection,
                               new_json_path_fluorescent_projection)

# Plane subsets
plane_format = "%02d"


# ################ Creating fluorescence projection metadata (-> *.csv)

tic()

for (subset_name in names(fluorescent_projection_subsets)){
  planes <- fluorescent_projection_subsets[[subset_name]]

  print(paste0("Creating fluorescence projection metadata with ", subset_name," planes"))
  for(i in 1:length(channel_projection_v)){
    # print(colnames(loaddata_output))
    file_ff <- loaddata_output %>%
      dplyr::filter(channel == channel_projection_v[i]) %>%
      dplyr::filter(zst %in% planes) %>%
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
      mutate(Metadata_planesampling = subset_name) %>%
      select(-Image_PathName_brightfield, -Image_FileName_brightfield) %>% ungroup()
    # print(colnames(file_ff))
    metadata_split_path <- write_metadata_split(file_ff,
                                                name = paste0(channel_projection_n[i], "_", subset_name),
                                                path_base = new_path_base)
  }
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
                       group_tag = "projection",
                       group_template_file="group_template.txt")
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
                     path_base = new_path_base,
                     flatfield_dir = flatfield_dir,
                     path_to_metadata = metadata_dir)
}
toc()

################ Grouping projection final job files

tic()
for(i in 1:length(channel_projection_n)){
  group_jobs_bash(path_base = new_path_base,
                  name = channel_projection_n[i],
                  letter_row_interval = c(1:16),
                  number_col_interval = c(1:24),
                  python_call = python_call_submitjob)
}
toc()

#==================================================#
#        PHASE 2.2: brightfield projections        #
#==================================================#

## Name of channels
channel_projection_v <- c("ch2")
channel_projection_n <- c("projection_ch2")
json_projection_templates <- c(new_json_path_brightfield_projection)

# # ################ Creating brightfield projection metadata (-> *.csv)

# Plane subsets
# plane_format = "%02d"
# projection_subsets <- list(
#   resolution1 = lapply(seq(0,8,by=1), sprintf, fmt=plane_format), # 999999990000 only
#   resolution2 = lapply(seq(0,8,by=2), sprintf, fmt=plane_format)#, 999999990000 only
#   # resolution1 = lapply(seq(1,14,by=1), sprintf, fmt=plane_format)#,
#   # resolution2 = lapply(seq(1,10,by=2), sprintf, fmt=plane_format)#,
#   # resolution3 = lapply(seq(1,14,by=3), sprintf, fmt=plane_format),
#   # resolution4 = lapply(seq(1,14,by=4), sprintf, fmt=plane_format),
#   # middle3 = lapply(seq(5,7,by=1), sprintf, fmt=plane_format),
#   # middle4 = lapply(seq(5,8,by=1), sprintf, fmt=plane_format),
#   # middle5 = lapply(seq(4,8,by=1), sprintf, fmt=plane_format),
#   # middle6 = lapply(seq(4,9,by=1), sprintf, fmt=plane_format)
# )

for (subset_name in names(brightfield_projection_subsets)){
  planes <- brightfield_projection_subsets[[subset_name]]

  print(paste0("Creating brightfield projection metadata with ", subset_name," planes"))
  for(i in 1:length(channel_projection_v)){
    file_ff <- loaddata_output %>%
      dplyr::filter(channel == channel_projection_v[i]) %>%
      dplyr::filter(zst %in% planes) %>%
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
      mutate(Metadata_planesampling = subset_name) %>% #TODO handle different subsets in pipeline or with bash script
      select(-Image_PathName_brightfield, -Image_FileName_brightfield) %>% ungroup()
    metadata_split_path <- write_metadata_split(file_ff,
                                                name = paste0(channel_projection_n[i], "_", subset_name),
                                                path_base = new_path_base)
  }
}

################ Grouping feature extraction / measurements metadata (-> *create_group.sh, *.batch.txt)
################ Aggregating information and executable file


tic()
print("Creating shell script for grouping")
path <- c()
path <- generate_group(plate_name,
                       c(channel_projection_n),
                       new_path_base,
                       group_tag = "brightfieldprojection",
                       group_template_file="group_template.txt")
print(path)
print("Grouping data using python script")
system(path)
toc()

################# (-> job*.json)

tic()
print("Generating job files with grouping")

for (name in names(brightfield_projection_subsets)){
  planes <- brightfield_projection_subsets[[name]]
  for (i in 1:length(channel_projection_n)){
      link_json_metadata(metadata_split_path = list.files(new_path_base, pattern = "metadata_", full.names = TRUE) %>%
                           stringr::str_subset(pattern = ".csv") %>%
                           stringr::str_subset(pattern = channel_projection_n[i]),
                         json_path = json_projection_templates[i],
                         path_base = new_path_base,
                         flatfield_dir = flatfield_dir,
                         path_to_metadata = metadata_dir)
  }
}
toc()

# ################ Grouping projection final job files
#
tic()
for(i in 1:length(channel_projection_n)){
  group_jobs_bash(path_base = new_path_base,
                  name = channel_projection_n[i],
                  letter_row_interval = c(1:16),
                  number_col_interval = c(1:24),
                  python_call = python_call_submitjob)
}
toc()

projection.finish.time <- Sys.time()
# writeLines(c("Projection finished:", projection.finish.time), fileConn)

#==================================================#
#             PHASE 3: segmentation                #
#==================================================#

#if (FALSE){

# Name of channels
channel_v <- c("ch1")
channel_segmentation_n <- c("segmentation_ch1")
json_segmentation_templates = c(new_json_path_segmentation)

# ################ Creating segmentation metadata (-> *.csv)

tic()
print("Creating segmentation metadata")
for(i in 1:length(channel_segmentation_n)){
  file_ff <- loaddata_output %>%
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
                     path_base = new_path_base,
                     flatfield_dir = flatfield_dir,
                     path_to_metadata = metadata_dir)
}
toc()

################ Grouping final job files

tic()
for(i in 1:length(channel_segmentation_n)){
  group_jobs_bash(path_base = new_path_base,
                  name = channel_segmentation_n[i],
                  letter_row_interval = c(1:16),
                  number_col_interval = c(1:24),
                  python_call = python_call_submitjob)
}
toc()

#} # end segmentation section

segmentation.finish.time <- Sys.time()
# writeLines(c("Segmentation finished:", segmentation.finish.time), fileConn)


#================================================================#
#   PHASE 4.1: fluorescent feature extraction / measurements     #
#================================================================#

# Plane subsets
# plane_format = "%02d"
# projection_subsets <- list(
#   # mid123 = lapply(seq(1,3,by=1), sprintf, fmt=plane_format),
#   # mid234 = lapply(seq(2,4,by=1), sprintf, fmt=plane_format),
#   # mid345 = lapply(seq(3,5,by=1), sprintf, fmt=plane_format),
#   # mid456 = lapply(seq(4,6,by=1), sprintf, fmt=plane_format),
#   mid246 = lapply(seq(2,6,by=2), sprintf, fmt=plane_format)
# )

# Name of channels
channel_v <- c("ch1")
channel_measurement_n <- c("measurement_ch3_ch4")
json_featureextraction_templates <- c(new_json_path_featureextraction_ch3_ch4)


################ Creating feature extraction / measurements metadata (-> *.csv)

# NOTE: handles only ch3, ch4

tic()

for (subset_name in names(fluorescent_projection_subsets)){
  planes <- fluorescent_projection_subsets[[subset_name]]

  print(paste0("Creating flurescence feature extraction metadata with ", subset_name," planes"))

  for(i in 1:length(channel_v)){
    file_ff <- loaddata_output %>%
      dplyr::filter(channel == channel_v[i]) %>%
      reformat_filelist() %>%
      rowwise() %>%
      mutate(Metadata_planesampling = subset_name) %>%
      mutate(Image_ObjectsFileName_Cells = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch1")), "_segmentation.tiff"),
             Image_ObjectsPathName_Cells = Image_PathName_brightfield %>%
                stringr::str_split(pattern = "/") %>%
                unlist() %>% .[c(1:4)] %>%
                append(flatfield_dir) %>%
                append(Metadata_parent) %>%
                append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch1"))) %>%
                paste(collapse = "/"),
             Image_FileName_ch3 = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch3", Metadata_planesampling)), "_maxprojection.tiff") ,
             Image_PathName_ch3 = Image_PathName_brightfield %>%
               stringr::str_split(pattern = "/") %>%
               unlist() %>% .[c(1:4)] %>%
               append(flatfield_dir) %>%
               append(Metadata_parent) %>%
               append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch3"))) %>%
               paste(collapse = "/"),
             Image_FileName_ch4 = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch4", Metadata_planesampling)), "_maxprojection.tiff") ,
             Image_PathName_ch4 = Image_PathName_brightfield %>%
               stringr::str_split(pattern = "/") %>%
               unlist() %>% .[c(1:4)] %>%
               append(flatfield_dir) %>%
               append(Metadata_parent) %>%
               append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch4"))) %>%
               paste(collapse = "/")) %>%
        select(-Image_PathName_brightfield, -Image_FileName_brightfield) %>% ungroup()
    metadata_split_path <- write_metadata_split(file_ff,
                                                name = paste0(channel_measurement_n[i], "_", subset_name),
                                                path_base = new_path_base)
  }
}
toc()

################ Grouping feature extraction / measurements metadata (-> *create_group.sh, *batch.txt)
################ Aggregating information and executable file

tic()
print("Creating shell script for grouping")
path <- c()
path <- generate_group(plate_name,
                       c(channel_measurement_n),
                       new_path_base,
                       group_tag = "featureextraction_ch3_ch4")
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
                     json_path = json_featureextraction_templates[i],
                     path_base = new_path_base,
                     flatfield_dir = flatfield_dir,
                     path_to_metadata = metadata_dir)
}
toc()

# ################ Grouping final job files

tic()
for(i in 1:length(channel_measurement_n)){
  group_jobs_bash(path_base = new_path_base,
                  name = channel_measurement_n[i],
                  letter_row_interval = c(1:16),
                  number_col_interval = c(1:24),
                  python_call = python_call_submitjob)
}
toc()


# Name of channels
channel_v <- c("ch1")
channel_measurement_n <- c("measurement_ch5_ch6")
json_featureextraction_templates <- c(new_json_path_featureextraction_ch5_ch6)

################ Creating feature extraction / measurements metadata (-> *.csv)

# NOTE: handles only ch5 and ch6

tic()

for (subset_name in names(fluorescent_projection_subsets)){
  planes <- fluorescent_projection_subsets[[subset_name]]

  print(paste0("Creating flurescence feature extraction metadata with ", subset_name," planes"))

  for(i in 1:length(channel_v)){
    file_ff <- loaddata_output %>%
      dplyr::filter(channel == channel_v[i]) %>%
      reformat_filelist() %>%
      rowwise() %>%
      mutate(Metadata_planesampling = subset_name) %>%
      mutate(Image_ObjectsFileName_Cells = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch1")), "_segmentation.tiff"),
             Image_ObjectsPathName_Cells = Image_PathName_brightfield %>%
               stringr::str_split(pattern = "/") %>%
               unlist() %>% .[c(1:4)] %>%
               append(flatfield_dir) %>%
               append(Metadata_parent) %>%
               append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch1"))) %>%
               paste(collapse = "/"),
             Image_FileName_ch5 = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch5", Metadata_planesampling)), "_maxprojection.tiff") ,
             Image_PathName_ch5 = Image_PathName_brightfield %>%
               stringr::str_split(pattern = "/") %>%
               unlist() %>% .[c(1:4)] %>%
               append(flatfield_dir) %>%
               append(Metadata_parent) %>%
               append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch5"))) %>%
               paste(collapse = "/"),
             Image_FileName_ch6 = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch6", Metadata_planesampling)), "_maxprojection.tiff") ,
             Image_PathName_ch6 = Image_PathName_brightfield %>%
               stringr::str_split(pattern = "/") %>%
               unlist() %>% .[c(1:4)] %>%
               append(flatfield_dir) %>%
               append(Metadata_parent) %>%
               append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch6"))) %>%
               paste(collapse = "/")) %>%
      select(-Image_PathName_brightfield, -Image_FileName_brightfield) %>% ungroup()
    metadata_split_path <- write_metadata_split(file_ff,
                                                name = paste0(channel_measurement_n[i], "_", subset_name),
                                                path_base = new_path_base)
  }
}
toc()

################ Grouping feature extraction / measurements metadata (-> *create_group.sh, *batch.txt)
################ Aggregating information and executable file

tic()
print("Creating shell script for grouping")
path <- c()
path <- generate_group(plate_name,
                       c(channel_measurement_n),
                       new_path_base,
                       group_tag = "featureextraction_ch5_ch6")
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
                     json_path = json_featureextraction_templates[i],
                     path_base = new_path_base,
                     flatfield_dir = flatfield_dir,
                     path_to_metadata = metadata_dir)
}
toc()

# ################ Grouping final job files

tic()
for(i in 1:length(channel_measurement_n)){
  group_jobs_bash(path_base = new_path_base,
                  name = channel_measurement_n[i],
                  letter_row_interval = c(1:16),
                  number_col_interval = c(1:24),
                  python_call = python_call_submitjob)
}
toc()

#================================================================#
#   PHASE 4.2: brightfield feature extraction / measurements     #
#================================================================#

# Name of channels
channel_measurement_v <- c("ch1")
channel_measurement_n <- c("measurement_ch2")
json_featureextraction_templates <- c(new_json_path_featureextraction_ch2)


################ Creating feature extraction / measurements metadata (-> *.csv)

for (subset_name in names(brightfield_projection_subsets)){
  planes <- brightfield_projection_subsets[[subset_name]]

  print(paste0("Creating brightfield feature extraction metadata with ", subset_name," planes"))
  for(i in 1:length(channel_measurement_v)){
    file_ff <- loaddata_output %>%
      dplyr::filter(channel == channel_measurement_v[i]) %>%
      reformat_filelist() %>%
      rowwise() %>%
      mutate(Metadata_planesampling = subset_name) %>% #TODO handle diffent subsets in pipeline or with bash script
      mutate(Image_ObjectsFileName_Cells = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch1")), "_segmentation.tiff"),
             Image_ObjectsPathName_Cells = Image_PathName_brightfield %>%
               stringr::str_split(pattern = "/") %>%
               unlist() %>% .[c(1:4)] %>%
               append(flatfield_dir) %>%
               append(Metadata_parent) %>%
               append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch1"))) %>%
               paste(collapse = "/"),
             Image_FileName_ch2_minimumprojection = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch2", Metadata_planesampling)), "_minimumprojection.tiff") ,
             Image_PathName_ch2_minimumprojection = Image_PathName_brightfield %>%
               stringr::str_split(pattern = "/") %>%
               unlist() %>% .[c(1:4)] %>%
               append(flatfield_dir) %>%
               append(Metadata_parent) %>%
               append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch2"))) %>%
               paste(collapse = "/"),
             Image_FileName_ch2_maximumprojection = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch2", Metadata_planesampling)), "_maximumprojection.tiff") ,
             Image_PathName_ch2_maximumprojection = Image_PathName_brightfield %>%
               stringr::str_split(pattern = "/") %>%
               unlist() %>% .[c(1:4)] %>%
               append(flatfield_dir) %>%
               append(Metadata_parent) %>%
               append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch2"))) %>%
               paste(collapse = "/"),
             Image_FileName_ch2_averageprojection = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch2", Metadata_planesampling)), "_averageprojection.tiff") ,
             Image_PathName_ch2_averageprojection = Image_PathName_brightfield %>%
               stringr::str_split(pattern = "/") %>%
               unlist() %>% .[c(1:4)] %>%
               append(flatfield_dir) %>%
               append(Metadata_parent) %>%
               append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch2"))) %>%
               paste(collapse = "/"),
             Image_FileName_ch2_varianceprojection = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch2", Metadata_planesampling)), "_varianceprojection.tiff") ,
             Image_PathName_ch2_varianceprojection = Image_PathName_brightfield %>%
               stringr::str_split(pattern = "/") %>%
               unlist() %>% .[c(1:4)] %>%
               append(flatfield_dir) %>%
               append(Metadata_parent) %>%
               append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch2"))) %>%
               paste(collapse = "/"),
             Image_FileName_ch2_brightfieldprojection = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch2", Metadata_planesampling)), "_brightfieldprojection.tiff") ,
             Image_PathName_ch2_brightfieldprojection = Image_PathName_brightfield %>%
               stringr::str_split(pattern = "/") %>%
               unlist() %>% .[c(1:4)] %>%
               append(flatfield_dir) %>%
               append(Metadata_parent) %>%
               append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch2"))) %>%
               paste(collapse = "/")) %>%
      select(-Image_PathName_brightfield, -Image_FileName_brightfield) %>% ungroup()
    metadata_split_path <- write_metadata_split(file_ff,
                                                name = paste0(channel_measurement_n[i], "_", subset_name),
                                                path_base = new_path_base)
  }
}

################ Grouping feature extraction / measurements metadata (-> *create_group.sh, *batch.txt)
################ Aggregating information and executable file

tic()
print("Creating shell script for grouping")
path <- c()
path <- generate_group(plate_name,
                       c(channel_measurement_n),
                       new_path_base,
                       group_tag = "featureextraction_ch2")
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
                     json_path = json_featureextraction_templates[i],
                     path_base = new_path_base,
                     flatfield_dir = flatfield_dir,
                     path_to_metadata = metadata_dir)
}
toc()

# ################ Grouping final job files

tic()
for(i in 1:length(channel_measurement_n)){
  group_jobs_bash(path_base = new_path_base,
                  name = channel_measurement_n[i],
                  letter_row_interval = c(1:16),
                  number_col_interval = c(1:24),
                  python_call = python_call_submitjob)
}
toc()

phase1bash.file.conn <- file(file.path(new_path_base, "submit_phase1.sh"))
writeLines(c("#!/bin/sh", "",
             "./quick_group_jobs_bash_segmentation_ch1_.sh",
             "./quick_group_jobs_bash_ffc_brightfield_.sh",
             "./quick_group_jobs_bash_ffc_ch3_.sh",
             "./quick_group_jobs_bash_ffc_ch4_.sh",
             "./quick_group_jobs_bash_ffc_ch5_.sh",
             "./quick_group_jobs_bash_ffc_ch6_.sh"),
           con=phase1bash.file.conn)
close(phase1bash.file.conn)
system(paste0("sudo chmod +x ", file.path(new_path_base, "submit_phase1.sh")))

phase2bash.file.conn <- file(file.path(new_path_base, "submit_phase2.sh"))
writeLines(c("#!/bin/sh", "",
             "./quick_group_jobs_bash_projection_ch2_.sh",
             "./quick_group_jobs_bash_maximumprojection_ch3_.sh",
             "./quick_group_jobs_bash_maximumprojection_ch4_.sh",
             "./quick_group_jobs_bash_maximumprojection_ch5_.sh",
             "./quick_group_jobs_bash_maximumprojection_ch6_.sh"),
           con=phase2bash.file.conn)
close(phase2bash.file.conn)
system(paste0("sudo chmod +x ", file.path(new_path_base, "submit_phase2.sh")))

phase3bash.file.conn <- file(file.path(new_path_base, "submit_phase3.sh"))
writeLines(c("#!/bin/sh", "",
             "./quick_group_jobs_bash_measurement_ch2_.sh",
             "./quick_group_jobs_bash_measurement_ch3_ch4_.sh",
             "./quick_group_jobs_bash_measurement_ch5_ch6_.sh"),
           con=phase3bash.file.conn)
close(phase3bash.file.conn)
system(paste0("sudo chmod +x ", file.path(new_path_base, "submit_phase3.sh")))

measurement.finish.time <- Sys.time()
# writeLines(c("Measurement finished:", measurement.finish.time), fileConn)

################ Pushing metadata to S3 bucket
# system("./sync_metadata_to_bucket.sh") # TODO: prepare sync metadata script for the new plates only
system(paste0("aws s3 sync ~/", metadata_dir, " s3://ascstore/", metadata_dir, " --exclude \".git*\""))

upload.finish.time <- Sys.time()
# writeLines(c("Upload finished:", upload.finish.time), fileConn)
writeLines(c("Start time: ", start.time,
             "Loaddata finished:", loaddata.finish.time,
             "FFC finished:", ffc.finish.time,
             "Projection finished:", projection.finish.time,
             "Segmentation finished:", segmentation.finish.time,
             "Measurement finished:", measurement.finish.time,
             "Upload finished:", upload.finish.time),
           fileConn)

close(fileConn)
