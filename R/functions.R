
pull_fresh_data <- function() {
  ruta <- "sftp://sasca_dom:Dir_cpe_*2025*@10.250.8.99/sasdata/F/ctirss/sasdatos/Esteban/employment_counts"

  path <- paste0(
    ruta,
    ".sas7bdat"
  )

  i <- 0
  while(i < 5) {
    # Try to read the file
    tryCatch({
      a <- haven::read_sas(path)
      break  # If successful, exit the loop
    }, error = function(e) {
      i <<- i + 1
      if (i >= 5) {  # Limit the number of retries
        stop("Failed to read the file after multiple attempts.")
      }
      Sys.sleep(5)  # Wait for 5 seconds before retrying
    })
  }

  n_rows <- nrow(a)
  stopifnot(n_rows > 0)

  return(a)
}


clean_fresh_data <- function(fresh_data, current_month, dictionary) {
  fresh_data |>
    dplyr::rename(
      clave_delegacion = cve_del_final,
      clave_subdelegacion = cve_subdel_final,
    ) |>
    dplyr::mutate(date = current_month) |>
    dplyr::mutate(
      cve_modalidad = str_c(mod, ".", tipotrc),
      cve_modalidad = case_when(
        mod == "32" ~ "32.1",
        mod == "33" ~ "33.1",
        mod == "14" ~ "14.1",
        T ~ cve_modalidad
      ),
      size_cierre = dplyr::case_when(
        size_cierre != "NA" ~ stringr::str_to_lower(size_cierre)
      ),
    ) 
 
}


make_new_sheets <- function(fresh_data_cleaned, dictionary) {
  clean_new_sheet <- function(x) {
    x |>
      dplyr::left_join(dictionary) |>
      select(!sheet_name_code)
  }

  

  # Asegurados modalidad
  aseg_mod <- fresh_data_cleaned |>
    summarise(
      .by = c(date, clave_delegacion, clave_subdelegacion, cve_modalidad),
      value = sum(asegurados)
    ) |>
    mutate(
      series = "Asegurados",
      disaggregation = "Modalidad"
    ) |>
    rename(sheet_name_code = cve_modalidad) |>
    clean_new_sheet()

  # Asegurados sector
  aseg_sector <- fresh_data_cleaned |>
    drop_na(div_final) |>
    filter(ta == 1) |>
    summarise(
      .by = c(date, clave_delegacion, clave_subdelegacion, div_final),
      value = sum(asegurados)
    ) |>
    mutate(
      series = "Asegurados",
      disaggregation = "Sector económico"
    ) |>
    rename(sheet_name_code = div_final) |>
    mutate(sheet_name_code = as.character(sheet_name_code)) |>
    clean_new_sheet()

  # Patrones modalidad
  patrones_mod <- fresh_data_cleaned |>
    filter(ta == 1) |>
    mutate(
      cve_modalidad = str_c(mod, ".1")
    ) |>
    summarise(
      .by = c(date, clave_delegacion, clave_subdelegacion, cve_modalidad),
      value = sum(patrones, na.rm = T)
    ) |>
    mutate(
      series = "Patrones",
      disaggregation = "Modalidad"
    ) |>
    rename(sheet_name_code = cve_modalidad) |>
    clean_new_sheet()

  # Patrones rango
  patrones_rango <- fresh_data_cleaned |>
    filter(ta == 1) |>
    summarise(
      .by = c(date, clave_delegacion, clave_subdelegacion, size_cierre),
      value = sum(patrones, na.rm = T)
    ) |>
    mutate(
      series = "Patrones",
      disaggregation = "Rango de trabajadores"
    ) |>
    rename(sheet_name_code = size_cierre) |>
    clean_new_sheet()

  # Salario modalidad
  salario_mod <- fresh_data_cleaned |>
    filter(ta == 1) |>
    summarise(
      .by = c(date, clave_delegacion, clave_subdelegacion, cve_modalidad),
      value = weighted.mean(salario, asegurados, na.rm = T)
    ) |>
    mutate(
      series = "Salario",
      disaggregation = "Modalidad"
    ) |>
    rename(sheet_name_code = cve_modalidad) |>
    clean_new_sheet()

  # Trabajadores modalidad
  aseg_mod <- fresh_data_cleaned |>
    filter(ta == 1) |>
    summarise(
      .by = c(date, clave_delegacion, clave_subdelegacion, cve_modalidad),
      value = sum(asegurados)
    ) |>
    mutate(
      series = "Asegurados",
      disaggregation = "Modalidad"
    ) |>
    rename(sheet_name_code = cve_modalidad) |>
    clean_new_sheet()

  # This works:
  # salario_mod |>
  #   summarise(
  #     salario = weighted.mean(value, trabajadores, na.rm = T)
  #   ) |>
  #   pull(salario)

  sheets_new_subdel <- bind_rows(
    aseg_mod,
    aseg_sector,
    patrones_mod,
    patrones_rango,
    salario_mod
  ) |>
    drop_na(sheet_name) |>
    mutate(
      level = "Subdelegación"
    )
  
  # Delegacion level  
  sheets_new_del <- sheets_new_subdel |>
    filter(series != "Salario") |>
    summarise(
      .by = c(
        date,
        clave_delegacion,
        series,
        disaggregation,
        sheet_name,
        book_name
      ),
      value = sum(value, na.rm = TRUE)
    ) |>
    mutate(
      level = "Delegación"
    )
  
  salario_del <- fresh_data_cleaned |>
    filter(ta == 1) |>
    summarise(
      .by = c(date, clave_delegacion, cve_modalidad),
      value = weighted.mean(salario, asegurados, na.rm = T)
    ) |>
    mutate(
      series = "Salario",
      disaggregation = "Modalidad"
    ) |>
    rename(sheet_name_code = cve_modalidad) |>
    clean_new_sheet() |>
    mutate(
      level = "Delegación"
    )
  
  sheets_new <- bind_rows(
    sheets_new_subdel,
    sheets_new_del,
    salario_del
  ) |>
    drop_na(sheet_name, value) 

  # National totals
  totals_labs <- tibble(
    level = c("Delegación", "Subdelegación"),
    clave_delegacion = c(99, 99),
    clave_subdelegacion = c(NA, 99),
  )


  sheets_new_nat <- sheets_new |>
    filter(series != "Salario") |>
    summarise(
      .by = c(date, series, disaggregation, sheet_name, book_name, level),
      value = sum(value, na.rm = TRUE)
    ) |>
    left_join(totals_labs)

  salario_nat <- fresh_data_cleaned |>
    filter(ta == 1) |>
    summarise(
      .by = c(date, cve_modalidad),
      value = weighted.mean(salario, asegurados, na.rm = T)
    ) |>
    mutate(
      series = "Salario",
      disaggregation = "Modalidad"
    ) |>
    rename(sheet_name_code = cve_modalidad) |>
    clean_new_sheet() |>
    drop_na(sheet_name) |>
    cross_join(totals_labs) 
  


  delegaciones_dictionary <- read_delegaciones_dictionary()

  sheets_new <- bind_rows(
    sheets_new,
    sheets_new_nat,
    salario_nat
  ) |>
    left_join(
      delegaciones_dictionary |>
        distinct(clave_delegacion, clave_subdelegacion, subdelegacion) 
    ) |>
    left_join(
      delegaciones_dictionary |>
        distinct(clave_delegacion, delegacion)
    )

    glimpse(sheets_new)


  sheets_new |>
    distinct(series, disaggregation, level) |>
    arrange(series, disaggregation, level) 


  return(sheets_new)
}


update_sheets <- function(new_sheets) {
  browser()
  my_schema <- schema(
    book_id = int32(),
    sheet_id = int32(),
    book_name = string(),
    sheet_name = string(),
    level = string(),
    date = date32(),
    clave_delegacion = int32(),
    clave_subdelegacion = int32(),
    delegacion = string(),
    subdelegacion = string(),
    series = string(),
    disaggregation = string(),
    value = float64()
  )
  sheets_old <- here::here("data/sheets/sheets_data.parquet") |>
    arrow::open_dataset(
      format = "parquet",
      schema = my_schema
    )

  
  ids <- sheets_old |>
    distinct(book_id, sheet_id, book_name, sheet_name, level) |>
    collect()


  new_sheets <- new_sheets |>
    left_join(ids)

  # Validate 
  # months_in_common <- intersect(
  #   unique(sheets_old |> pull(date)) ,
  #   unique(new_sheets$date)
  # )


  # validate <- new_sheets |>
  #   right_join(
  #     sheets_old,
  #     by = c(
  #       "book_id",
  #       "sheet_id",
  #       "date",
  #       "clave_delegacion",
  #       "clave_subdelegacion",
  #       "delegacion",
  #       "subdelegacion",
  #       "series",
  #       "disaggregation",
  #       "sheet_name",
  #       "book_name",
  #       "level"
  #     ),
  #     copy = T
  #   )
  
  # errors <- validate |>
  #   mutate(diff = value.x - value.y) |>
  #   filter(abs(diff) > 1 | is.na(value.y))

  # stopifnot(nrow(errors) == 0)

  month_to_update <- unique(new_sheets$date)
  last_month_in_old <- max(sheets_old$date)

  stopifnot(last_month_in_old + months(1) == month_to_update)

  updated_sheets <- bind_rows(
    new_sheets,
    sheets_old
  ) |>
    arrange(book_id, sheet_id, date, clave_delegacion, clave_subdelegacion) 

  return(updated_sheets)
}

save_updated_sheets <- function(updated_sheets) {
  updated_sheets |>
    arrow::write_dataset(
      path = here::here("data/sheets/sheets_data.parquet"),
      format = "parquet",
      partitioning = c("book_id", "sheet_id"),
      existing_data_behavior = "overwrite"
    )
  return(1)
}