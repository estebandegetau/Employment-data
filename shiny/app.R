# app.R
pacman::p_load(
  shiny, shinyWidgets, here, tidyverse, plotly,
  duckdb, arrow, DT, writexl, lubridate
)

# ------------ data ------------
data_file <- here::here("data/sheets/sheets_data.parquet")
if (!file.exists(data_file)) {
  stop("Data file not found at: ", data_file)
}
my_schema <- schema(
  series              = string(),
  level               = string(),
  clave_delegacion    = int32(),
  delegacion         = string(),
  clave_subdelegacion = int32(),
  subdelegacion      = string(),
  disaggregation     = string(),
  sheet_name         = string(),
  date               = date32(),
  value              = float64()
)
series <- arrow::open_dataset(
  data_file, format = "parquet",
  schema = my_schema
        ) 

series_choices <- series |>
  distinct(series) |>
  collect() |>
  pull(series) |>
  sort()

# ------------ module UI ------------
seriesModuleUI <- function(id, series_name) {
  ns <- NS(id)
  sidebarLayout(
    sidebarPanel(
      h4(series_name),

      # Geographic level
      selectInput(ns("level"), "Desagregación geográfica:", choices = NULL),

      # Members (multi with Select All)
      pickerInput(
        inputId = ns("members"),
        label   = "Selecciona entidades:",
        choices = NULL, multiple = TRUE,
        options = pickerOptions(
          actionsBox = TRUE, liveSearch = TRUE,
          selectedTextFormat = "count > 3",
          countSelectedText = "{0} seleccionados"
        )
      ),

      # Conceptual disaggregation: SINGLE-SELECT
      selectInput(ns("concept"), "Desagregación conceptual:", choices = NULL),

      # Specific time series
      selectInput(ns("sheet"), "Serie de tiempo:", choices = NULL),

      # Presentation / export controls
      checkboxInput(ns("aggregate"), "Agregar selección (mostrar Total)", value = FALSE),

      radioButtons(ns("shape"), "Forma de salida:",
        choices = c("Ancha (fechas en columnas)" = "wide",
                    "Larga (fecha, valor)"       = "long"),
        inline = TRUE
      ),

      dateRangeInput(ns("dates"), "Rango de fechas:",
                     start = Sys.Date() - years(6), end = Sys.Date()),

      downloadButton(ns("download_csv"),  "Descargar CSV"),
      downloadButton(ns("download_xlsx"), "Descargar XLSX"),
      width = 4
    ),
    mainPanel(
      plotlyOutput(ns("plot")),
      DTOutput(ns("table")),
      width = 8
    )
  )
}

# ------------ module server ------------
seriesModule <- function(id, series_df, series_name) {
  moduleServer(id, function(input, output, session) {

    # 1) Populate geographic level
    observe({
      lvls <- series_df |>
        filter(series == series_name) |>
        distinct(level) |>
        collect() |>
        arrange(level) |>
        pull(level)
      updateSelectInput(session, "level", choices = lvls,
                        selected = if (length(lvls)) lvls[1] else NULL)
    })

    # 2) Populate members once level chosen
    observeEvent(input$level, {
      req(input$level)
      members <- series_df |>
        filter(series == series_name, level == input$level) |>
        distinct(clave_delegacion, delegacion,
                 clave_subdelegacion, subdelegacion) |>
        collect() |>
        mutate(
          label = if_else(
            !is.na(subdelegacion) & subdelegacion != "",
            paste0(sprintf("%02d", clave_delegacion), "-", sprintf("%03d", clave_subdelegacion),
                   " | ", delegacion, " - ", subdelegacion),
            paste0(sprintf("%02d", clave_delegacion), " | ", delegacion)
          ),
          key = paste(clave_delegacion, clave_subdelegacion, sep = "_")
        ) |>
        arrange(label)

      updatePickerInput(session, "members",
                        choices  = stats::setNames(members$key, members$label),
                        selected = members$key)  # select all by default
    }, ignoreInit = TRUE)

    # 3) Conceptual disaggregation (single select)
    observeEvent(input$level, {
      req(input$level)
      concepts <- series_df |>
        filter(series == series_name, level == input$level) |>
        distinct(disaggregation) |>
        collect() |>
        arrange(disaggregation) |>
        pull(disaggregation)
      updateSelectInput(session, "concept",
                        choices = concepts, selected = if (length(concepts)) concepts[1] else NULL)
    }, ignoreInit = TRUE)

    # 4) Sheet names based on level + concept (single)
    observeEvent(list(input$level, input$concept), {
      req(input$level, input$concept)
      sheets <- series_df |>
        filter(series == series_name,
               level == input$level,
               disaggregation == input$concept) |>
        distinct(sheet_name) |>
        collect() |>
        arrange(sheet_name) |>
        pull(sheet_name)
      updateSelectInput(session, "sheet",
                        choices = sheets,
                        selected = if (length(sheets)) sheets[1] else NULL)
    }, ignoreInit = TRUE)

    # 5) Date range to min/max for the slice
    observeEvent(input$sheet, {
      req(input$level, input$concept, input$sheet)
      rng <- series_df |>
        filter(series == series_name,
               level == input$level,
               disaggregation == input$concept,
               sheet_name == input$sheet) |>
        collect() |>
        summarise(min_date = min(date, na.rm = TRUE),
                  max_date = max(date, na.rm = TRUE))
      if (is.finite(rng$min_date) && is.finite(rng$max_date)) {
        updateDateRangeInput(session, "dates",
          start = rng$min_date, end = rng$max_date,
          min   = rng$min_date, max = rng$max_date
        )
      }
    }, ignoreInit = TRUE)

    # ---- Canonical reactive used by plot/table/downloads ----
    prepared_data <- reactive({
      lvl   <- req(input$level)
      conc  <- req(input$concept)  # single value now
      sh    <- req(input$sheet)
      ts    <- req(input$dates)
      membs <- req(input$members)

      # parse "del_subdel" keys to integers
      keys <- tidyr::separate(
        tibble(key = membs), key, into = c("cd","cs"), sep = "_",
        remove = TRUE, fill = "right"
      ) |>
        mutate(
          clave_delegacion    = as.integer(cd),
          clave_subdelegacion = suppressWarnings(as.integer(cs))
        )

      df <- series_df |>
        inner_join(keys, by = c("clave_delegacion","clave_subdelegacion")) |>
        filter(series == series_name,
               level == lvl,
               disaggregation == conc,       # single-select
               sheet_name == sh,
               date >= ts[1], date <= ts[2]) |>
        select(clave_delegacion, delegacion,
               clave_subdelegacion, subdelegacion,
               date, value) |>
        collect()

      if (isTRUE(input$aggregate)) {
        df |>
          group_by(date) |>
          summarise(value = sum(value, na.rm = TRUE), .groups = "drop") |>
          collect() |>
          mutate(grupo = "Total seleccionado")
      } else {
        df |>
          collect() |>
          mutate(grupo = if_else(
            !is.na(subdelegacion) & subdelegacion != "",
            paste0(sprintf("%02d", clave_delegacion), "-", sprintf("%03d", clave_subdelegacion),
                   " | ", delegacion, " - ", subdelegacion),
            paste0(sprintf("%02d", clave_delegacion), " | ", delegacion)
          ))
      }
    })

    # ---- Plot ----
    output$plot <- renderPlotly({
      df <- prepared_data(); req(nrow(df) > 0)
      df <- arrange(df, grupo, date)
      plot_ly(df, x = ~date, y = ~value,
              split = ~grupo, type = "scatter", mode = "lines",
              hovertemplate = "%{customdata}<br>%{x|%Y-%m}: %{y:,.0f}<extra></extra>",
              customdata = ~grupo) |>
        layout(title = series_name, xaxis = list(title = ""),
               yaxis = list(title = ""), hovermode = "x unified") |>
        config(displaylogo = FALSE)
    })

    # ---- Table ----
    output$table <- renderDT({
      df <- prepared_data(); req(nrow(df) > 0)
      out <- if (identical(input$shape, "wide")) {
        df |> tidyr::pivot_wider(names_from = date, values_from = value)
      } else df
      datatable(out, options = list(pageLength = 10, scrollX = TRUE))
    })

    # ---- Downloads ----
    make_export <- reactive({
      df <- prepared_data()
      if (identical(input$shape, "wide")) {
        df |> tidyr::pivot_wider(names_from = date, values_from = value)
      } else df
    })

    output$download_csv <- downloadHandler(
      filename = function() paste0("time_series_", series_name, "_", Sys.Date(), ".csv"),
      content  = function(file) readr::write_csv(make_export(), file, na = "")
    )
    output$download_xlsx <- downloadHandler(
      filename = function() paste0("time_series_", series_name, "_", Sys.Date(), ".xlsx"),
      content  = function(file) writexl::write_xlsx(make_export(), path = file)
    )
  })
}

# ------------ app UI ------------
ui <- fluidPage(
  titlePanel("Series de tiempo DIR"),
  # splice tabPanel list with do.call()
  do.call(tabsetPanel, c(
    list(id = "series_tab"),
    lapply(series_choices, function(s) tabPanel(title = s, seriesModuleUI(paste0("mod_", s), s)))
  ))
)

# ------------ app server ------------
server <- function(input, output, session) {
  # IMPORTANT: use lapply (or local/force) so each module captures its own 's'
  lapply(series_choices, function(s) {
    seriesModule(id = paste0("mod_", s), series_df = series, series_name = s)
  })
}

shinyApp(ui, server)
