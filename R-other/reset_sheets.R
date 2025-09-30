rm(list = ls())
gc()

pacman::p_load(targets, here, tidyverse, arrow)

tar_source()

historic_sheets <- tar_read(historical_sheets_data)

historic_sheets |>
  save_updated_sheets()
