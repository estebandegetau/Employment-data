# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
pacman::p_load(targets, tarchetypes, tidyverse, here)
# library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
  packages = c(
    "tibble",
    "here",
    "haven",
    "janitor",
    "arrow",
    "readxl",
    "lubridate",
    "stringr",
    "dplyr"
  ) # Packages that your targets need for their tasks.
  # format = "qs", # Optionally set the default storage format. qs is fast.
  #
  # Pipelines that take a long time to run may benefit from
  # optional distributed computing. To use this capability
  # in tar_make(), supply a {crew} controller
  # as discussed at https://books.ropensci.org/targets/crew.html.
  # Choose a controller that suits your needs. For example, the following
  # sets a controller that scales up to a maximum of two workers
  # which run as local R processes. Each worker launches when there is work
  # to do and exits if 60 seconds pass with no tasks to run.
  #
  #   controller = crew::crew_controller_local(workers = 2, seconds_idle = 60)
  #
  # Alternatively, if you want workers to run on a high-performance computing
  # cluster, select a controller from the {crew.cluster} package.
  # For the cloud, see plugin packages like {crew.aws.batch}.
  # The following example is a controller for Sun Grid Engine (SGE).
  # 
  #   controller = crew.cluster::crew_controller_sge(
  #     # Number of workers that the pipeline can scale up to:
  #     workers = 10,
  #     # It is recommended to set an idle time so workers can shut themselves
  #     # down if they are not running tasks.
  #     seconds_idle = 120,
  #     # Many clusters install R as an environment module, and you can load it
  #     # with the script_lines argument. To select a specific verison of R,
  #     # you may need to include a version string, e.g. "module load R/4.3.2".
  #     # Check with your system administrator if you are unsure.
  #     script_lines = "module load R"
  #   )
  #
  # Set other options as needed.

)
tar_option_set(error = "trim")
# Run the R scripts in the R/ folder with your custom functions:
tar_source()
# tar_source("other_functions.R") # Source other scripts as needed.
# targets::tar_make(callr_function = NULL, use_crew = F, as_job = F)
# Replace the target list below with your own:
list(
  tar_target(
    name = raw_data_path,
    command = here("data/raw"),
    format = "file"
  ),
  tar_target(
    name = historical_sheets_data,
    command = read_sheets(raw_data_path),
   # cue = tar_cue(mode = "never"),
    format = "feather"
  ),

  # tar_target(
  #   name = sheets_data,
  #   command = here("data/sheets"),
  #   format = "file"
  # ),
  tar_target(
    name = fresh_data,
    command = pull_fresh_data(),
    cue = tar_cue("always")
    
  ),
  tar_target(
    name = current_month,
    command = {
      if (day(today()) >= 5) {
        (today() - months(1)) |>
          floor_date(unit = "month")
      } else {
        (today() - months(2)) |>
          floor_date(unit = "month")
      }
    },
    cue = tar_cue("always")
  ),
  tar_target(
    name = dictionary_path,
    command = here::here("data/dictionary.xlsx"),
    format = "file"
  ),
  tar_target(
    name = dictionary,
    command = dictionary_path |>
      readxl::read_excel(sheet = 1, col_types = "text") |>
      mutate(
        sheet_name_code = case_when(
          nchar(sheet_name_code) > 5 ~
            sheet_name_code |>
              as.numeric() |>
              round(digits = 1) |>
              as.character(),
          T ~ sheet_name_code
        )
      )
  ),
  tar_target(
    name = fresh_data_cleaned,
    command = clean_fresh_data(fresh_data, current_month, dictionary)
  ),
  tar_target(
    name = new_sheets,
    command = make_new_sheets(fresh_data_cleaned, dictionary)
  ),
  tar_target(
    name = updated_sheets,
    command = update_sheets(new_sheets)
  ),
    tar_target(
    name = saved_sheets_data,
    command = save_updated_sheets(updated_sheets)
  )
)

# targets::tar_make(callr_function = NULL, use_crew = F, as_job = F)