################ Important Inputs
## Define directory names - you will have to change this manually!
## Make sure the directories contain no spaces!
## The pipeline assumes there is a directory in the "inbox" directory that has the following name:
## Call this script with: Rscript generate_metadata_"your-platename(s)-here".R

library(tidyverse)
library(dcphelper)
library(tictoc)

plate_name = args = commandArgs(trailingOnly=TRUE)
# for debugging only
#plate_name = "000012095203__2019-12-09T17_58_26-Measurement_1"
print(paste0("Processing plate ", plate_name))

################ Define paths

flatfield_dir = "flatfield"

new_path_base = paste0("~/dcp_helper/metadata/", plate_name,"/") #relative path acceptable
inbox_path_base= paste0("/home/ubuntu/bucket/inbox/", plate_name,"/Images/") #absolute path with /home/ubuntu/ required
flatfield_path_base= paste0("~/bucket/", flatfield_dir, "/", plate_name,"/") #deprecated: only used for result collection

path_yml = "~/mcsaba/biosensor/src/dcp_helper/python/pe2loaddata_config_000012107103.yml"
# path_yml = "~/mcsaba/biosensor/src/dcp_helper/python/pe2loaddata_config_000012106803.yml"

################ Creating target dir

lapply(new_path_base, dir.create, recursive=TRUE) # Do not execute this from a local machine if you expect other AWS services to access the directory later on

################ Set JSON template paths

# All json templates are available in fork csmolnar/dcp_helper
new_json_path_brightfield_flatfield = "~/dcp_helper/python/job_brightfield_flatfield_template.json" #brightfield FFC
new_json_path_fluorescent_flatfield = "~/dcp_helper/python/job_fluorescent_flatfield_template.json" #fluorescent FFC

new_json_path_brightfield_projection = "~/dcp_helper/python/job_brightfield_projection_template.json" #fluorescent projection
new_json_path_fluorescent_projection = "~/dcp_helper/python/job_fluorescent_projection_template.json" #fluorescent projection

new_json_path_segmentation = "~/dcp_helper/python/job_segmentation_template.json"
new_json_path_featureextraction_ch2 = "~/dcp_helper/python/job_featureextraction_ch2_template.json"
new_json_path_featureextraction_ch3_ch4 = "~/dcp_helper/python/job_featureextraction_ch3_ch4_template.json"
new_json_path_featureextraction_ch5_ch6 = "~/dcp_helper/python/job_featureextraction_ch5_ch6_template.json"

################ This is where the execution starts

fileConn <- file("metadata_generation.log")
# writeLines( c("Hello","World"), fileConn )
start.time <- Sys.time()
# writeLines(c("Start time: ", start.time), fileConn)

#==================================================#
#       PHASE 0: initiate metadata dataframe       #
#==================================================#

loaddata_output <- extract_filelist(path = inbox_path_base, force=FALSE, new_path_base, path_yml )
loaddata.finish.time <- Sys.time()
# writeLines(c("Loaddata finished:", loaddata.finish.time), fileConn)

#==================================================#
#         PHASE 1: flatfield correction            #
#==================================================#


# Name of channels
channel_ffc_v <- c("ch2", "ch3", "ch4", "ch5", "ch6")
channel_ffc_n <- c("ffc_brightfield", "ffc_ch3", "ffc_ch4", "ffc_ch5", "ffc_ch6")
json_ffc_templates <- c(new_json_path_brightfield_flatfield, new_json_path_fluorescent_flatfield, new_json_path_fluorescent_flatfield, new_json_path_fluorescent_flatfield, new_json_path_fluorescent_flatfield)

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

ffc.finish.time <- Sys.time()
# writeLines(c("FFC finished:", ffc.finish.time), fileConn)

#==================================================#
#        PHASE 2.1: fluorescent projections        #
#==================================================#

## Name of channels
channel_projection_v <- c("ch3", "ch4", "ch5", "ch6")
channel_projection_n <- c("maximumprojection_ch3", "maximumprojection_ch4", "maximumprojection_ch5", "maximumprojection_ch6")
json_projection_templates <- c(new_json_path_fluorescent_projection, new_json_path_fluorescent_projection, new_json_path_fluorescent_projection, new_json_path_fluorescent_projection)

# ################ Creating fluorescence projection metadata (-> *.csv)

tic()
print("Creating fluorescence projection metadata")
for(i in 1:length(channel_projection_v)){
  file_ff <- loaddata_output %>%
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
#        PHASE 2.2: brightfield projections        #
#==================================================#

## Name of channels
channel_projection_v <- c("ch2")
channel_projection_n <- c("projection_ch2")
json_projection_templates <- c(new_json_path_brightfield_projection)

# # ################ Creating brightfield projection metadata (-> *.csv)

# Plane subsets
plane_format = "%02d"
projection_subsets <- list(
  resolution1 = lapply(seq(1,14,by=1), sprintf, fmt=plane_format)#,
  # resolution2 = lapply(seq(1,14,by=2), sprintf, fmt=plane_format),
  # resolution3 = lapply(seq(1,14,by=3), sprintf, fmt=plane_format),
  # resolution4 = lapply(seq(1,14,by=4), sprintf, fmt=plane_format),
  # middle3 = lapply(seq(5,7,by=1), sprintf, fmt=plane_format),
  # middle4 = lapply(seq(5,8,by=1), sprintf, fmt=plane_format),
  # middle5 = lapply(seq(4,8,by=1), sprintf, fmt=plane_format),
  # middle6 = lapply(seq(4,9,by=1), sprintf, fmt=plane_format)
)

for (subset_name in names(projection_subsets)){
  planes <- projection_subsets[[subset_name]]

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
      mutate(Metadata_planesampling = subset_name) %>% #TODO handle diffent subsets in pipeline or with bash script
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
                       group_template_file="group_template_subsampling.txt")
print(path)
print("Grouping data using python script")
system(path)
toc()

################# (-> job*.json)

tic()
print("Generating job files with grouping")

for (name in names(projection_subsets)){
  planes <- projection_subsets[[name]]
  for (i in 1:length(channel_projection_n)){
      link_json_metadata(metadata_split_path = list.files(new_path_base, pattern = "metadata_", full.names = TRUE) %>%
                           stringr::str_subset(pattern = ".csv") %>%
                           stringr::str_subset(pattern = channel_projection_n[i]),
                         json_path = json_projection_templates[i],
                         path_base = new_path_base)
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
                  number_col_interval = c(1:24))
}
toc()

projection.finish.time <- Sys.time()
# writeLines(c("Projection finished:", projection.finish.time), fileConn)

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


segmentation.finish.time <- Sys.time()
# writeLines(c("Segmentation finished:", segmentation.finish.time), fileConn)

#================================================================#
#   PHASE 4.1: fluorescent feature extraction / measurements     #
#================================================================#

# Name of channels
channel_v <- c("ch1")
channel_measurement_n <- c("measurement_ch3_ch4")
json_featureextraction_templates <- c(new_json_path_featureextraction_ch3_ch4)

################ Creating feature extraction / measurements metadata (-> *.csv)

# NOTE: only ch3, ch4

tic()
print("Creating feature extraction / measurements metadata")
for(i in 1:length(channel_v)){
  file_ff <- loaddata_output %>%
    dplyr::filter(channel == channel_v[i]) %>%
    reformat_filelist() %>%
    rowwise() %>%
    mutate(Image_ObjectsFileName_Cells = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch1")), "_segmentation.tiff"),
           Image_ObjectsPathName_Cells = Image_PathName_brightfield %>%
              stringr::str_split(pattern = "/") %>%
              unlist() %>% .[c(1:4)] %>%
              append(flatfield_dir) %>%
              append(Metadata_parent) %>%
              append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch1"))) %>%
              paste(collapse = "/"),
           Image_FileName_ch3 = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch3")), "_maxprojection.tiff") ,
           Image_PathName_ch3 = Image_PathName_brightfield %>%
             stringr::str_split(pattern = "/") %>%
             unlist() %>% .[c(1:4)] %>%
             append(flatfield_dir) %>%
             append(Metadata_parent) %>%
             append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch3"))) %>%
             paste(collapse = "/"),
           Image_FileName_ch4 = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch4")), "_maxprojection.tiff") ,
           Image_PathName_ch4 = Image_PathName_brightfield %>%
             stringr::str_split(pattern = "/") %>%
             unlist() %>% .[c(1:4)] %>%
             append(flatfield_dir) %>%
             append(Metadata_parent) %>%
             append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch4"))) %>%
             paste(collapse = "/")) %>%
      select(-Image_PathName_brightfield, -Image_FileName_brightfield) %>% ungroup()
  metadata_split_path <- write_metadata_split(file_ff, name = channel_measurement_n[i], path_base = new_path_base)
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


# Name of channels
channel_v <- c("ch1")
channel_measurement_n <- c("measurement_ch5_ch6")
json_featureextraction_templates <- c(new_json_path_featureextraction_ch5_ch6)

################ Creating feature extraction / measurements metadata (-> *.csv)

# NOTE: only ch5 and ch6 added

tic()
print("Creating feature extraction / measurements metadata")
for(i in 1:length(channel_v)){
  file_ff <- loaddata_output %>%
    dplyr::filter(channel == channel_v[i]) %>%
    reformat_filelist() %>%
    rowwise() %>%
    mutate(Image_ObjectsFileName_Cells = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch1")), "_segmentation.tiff"),
           Image_ObjectsPathName_Cells = Image_PathName_brightfield %>%
             stringr::str_split(pattern = "/") %>%
             unlist() %>% .[c(1:4)] %>%
             append(flatfield_dir) %>%
             append(Metadata_parent) %>%
             append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch1"))) %>%
             paste(collapse = "/"),
           Image_FileName_ch5 = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch5")), "_maxprojection.tiff") ,
           Image_PathName_ch5 = Image_PathName_brightfield %>%
             stringr::str_split(pattern = "/") %>%
             unlist() %>% .[c(1:4)] %>%
             append(flatfield_dir) %>%
             append(Metadata_parent) %>%
             append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch5"))) %>%
             paste(collapse = "/"),
           Image_FileName_ch6 = paste0(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch6")), "_maxprojection.tiff") ,
           Image_PathName_ch6 = Image_PathName_brightfield %>%
             stringr::str_split(pattern = "/") %>%
             unlist() %>% .[c(1:4)] %>%
             append(flatfield_dir) %>%
             append(Metadata_parent) %>%
             append(format_output_structure(c(Metadata_parent, Metadata_timepoint, Metadata_well, Metadata_fld, "ch6"))) %>%
             paste(collapse = "/")) %>%
    select(-Image_PathName_brightfield, -Image_FileName_brightfield) %>% ungroup()
  metadata_split_path <- write_metadata_split(file_ff, name = channel_measurement_n[i], path_base = new_path_base)
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
                       group_tag = "measurement_ch5_ch6")
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

#================================================================#
#   PHASE 4.2: brightfield feature extraction / measurements     #
#================================================================#

# Name of channels
channel_measurement_v <- c("ch1")
channel_measurement_n <- c("measurement_ch2")
json_featureextraction_templates <- c(new_json_path_featureextraction_ch2)

################ Creating feature extraction / measurements metadata (-> *.csv)

for (subset_name in names(projection_subsets)){
  planes <- projection_subsets[[subset_name]]

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
                       group_tag = "featureextraction_ch2",
                       group_template_file="group_template_subsampling.txt")
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

measurement.finish.time <- Sys.time()
# writeLines(c("Measurement finished:", measurement.finish.time), fileConn)

################ Pushing metadata to S3 bucket
system("./sync_metadata_to_bucket.sh") # TODO: prepare sync metadata script for the new plates only


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
