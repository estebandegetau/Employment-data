
#---- Setup---------------------------------------------------------------------

rm(list = ls())
gc()

pacman::p_load(
  tidyverse,
  here,
  readxl,
  arrow
)

#---- Functions ----------------------------------------------------------------

content_col <- function(x) {

  any(str_detect(x, "Hoja|Contenido"), na.rm = T) 

}


read_content <- function(path) {
  path |>
    readxl::read_excel(
      sheet = "Contenido",
    ) |>
    select(
      where(content_col)
    ) |>
    drop_na() |>
    mutate(across(everything(), ~ str_squish(.x))) |>
    rename(Hoja = 1, Contenido = 2) |>
    mutate(
      Hoja = str_extract(Hoja, "\\d+") |>
        as.numeric(),
      Contenido = Contenido |>
        str_squish()
    ) |>
    drop_na() |>
    filter(!str_detect(Contenido, "Resumen|resumen"))
}


pull_poistions <- function(path) {
  sheets <- path |>
    readxl::excel_sheets()

  positions <- tibble(
    sheet_name = sheets,
  ) |>
    rowid_to_column() |>
    rename(position = rowid) |>
  #  filter(!str_detect(sheet, "[:alpha:]")) |>
    mutate(Hoja = str_extract(sheet_name, "\\d+") |> as.numeric()) |>
    filter(!str_detect(sheet_name, "[:alpha:]|\\,|\\.|\\("))
}

read_sheet <- function(path, position) {
  a <- path |>
    read_excel(
      sheet = position,
      skip = 3
    ) |>
    pivot_longer(
      matches("\\d"),
      names_to = "date",
      values_transform = as.numeric
    ) |>
    mutate(
      # Clean excel dates
      date = as.Date(as.numeric(date), origin = "1899-12-30") |>
        as_date()
    ) |>
    janitor::clean_names()

  return(a)
}

#---- Load ---------------------------------------------------------------------

dirs <- "//11.254.31.147/dasr" |>
  list.files(
    pattern = "ASEGURADOS|PATRONES|SALARIO|MASA|TRABSAL",
    recursive = T,
    full.names = T
  )



books <- tibble(
  path = dirs
) |>
  filter(
    !str_detect(path, "\\$")
  ) |>
  mutate(
    content = map(path, read_content),
    positions = map(path, pull_poistions),
    name = map_chr(path, ~ str_extract(.x, "([^/]+)\\.xlsx$")) |>
      str_remove("\\.xlsx"),
    level = case_when(
      str_detect(name, "Delegaciones") ~ "Delegación",
      str_detect(name, "Subdelegaciones") ~ "Subdelegación"
    ),
    name = name |>
      str_remove("_\\w+") |>
      str_squish() |>
      str_remove("\\.") |>
      str_to_lower(),
    series = case_when(
      str_detect(name, "asegurados") ~ "Asegurados Trabajadores",
      str_detect(name, "patrones") ~ "Patrones",
      str_detect(name, "masa") ~ "Masa salarial",
      str_detect(name, "salario") ~ "Salario",
      str_detect(name, "trabsal") ~ "Trabsal"
    ),
    disaggregation = case_when(
      str_detect(name, "modalidad") ~ "Modalidad",
      str_detect(name, "sector") ~ "Sector económico",
      str_detect(name, "rango") ~ "Rango de trabajadores",
      T ~ "Modalidad"
    )
  )


books_indexed <- books |>
  mutate(book_id = row_number(), .before = 1) |>
  mutate(
    
    index = map2(
      content, positions,
      ~ left_join(.x, .y, by = "Hoja", relationship = "one-to-one")
    ),
    conflicts = map_dbl(index, ~ .x |>
      filter(.by = Hoja, n_distinct(sheet_name) > 1 | sum(is.na(position) > 0)) |>
      nrow()
    )
  )

f <- books_indexed |>
filter(conflicts > 0)

stopifnot(nrow(f) == 0)



#---- Read sheets --------------------------------------------------------------------

sheets_raw <- books_indexed |>
  unnest(index) |>
  mutate(sheet_id = row_number(), .after = book_id) |>
  mutate(
    data = map2(path, position, read_sheet)
  )


fix_dates <- sheets_raw |>
  mutate(dates_errors = map_dbl(data, ~ .x |>
    filter(is.na(date)) |>
    nrow())) |>
  filter(dates_errors > 0) |>
  mutate(
    data = map(data, ~ .x |>
      mutate(
        date_c = seq(first(date), along.with = date, by = "month"),
        date = date_c
      ) |>
      select(!date_c))
  )

sheets_clean <- sheets_raw |>
  filter(!sheet_id %in% fix_dates$sheet_id) |>
  bind_rows(fix_dates) |>
  arrange(book_id, sheet_id) |>
  select(-sheet_name) |>
  select(
    book_id,
    sheet_id,
    book_name = name,
    sheet_name = Contenido,
    level,
    series,
    disaggregation,
    data
  ) |>
  mutate(
    across(matches("_name"), str_to_sentence),
    data = map(data,
     ~ .x |>
        rename_with( ~ .x |> 
          str_replace("clave_del$", "clave_delegacion")
          )
    )
    ) 

sheets_raw$data[[118]]
sheets_raw |> slice(118) |> glimpse()
glimpse(sheets_clean)

#---- Save -----------------------------------------------------------

sheets <- sheets_clean

save(sheets, file = here("data/sheets.RData"))


series <- sheets |> 
  unnest(data) |>
  relocate(clave_delegacion, delegacion, clave_subdelegacion, subdelegacion, date, value, .after = disaggregation)

series |>
  group_by(book_name) |>
  arrow::write_dataset(
    path = here("data/sheets"),
    format = "parquet"
    )

# Display file sizes inside data/sheets
here("data/sheets") |>
  list.files(full.names = T, recursive = T) |>
  map_dbl(fs::file_size) |>
  fs::fs_bytes() |>
  print()
