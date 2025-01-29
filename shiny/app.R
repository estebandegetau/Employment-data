pacman::p_load(shiny, here, tidyverse, plotly, duckdb, arrow, DT, writexl)


series <- here("data/sheets") |>
    open_dataset()

ui <- fluidPage(
    titlePanel("Time Series Exporter"),
    sidebarLayout(
        sidebarPanel(
            selectInput("level", "Select Level:", choices = NULL),
            selectInput("series", "Select Series:", choices = NULL),
            selectInput("disaggregation", "Select Disaggregation:", choices = NULL),
            selectInput("sheet_name", "Select Sheet Name:", choices = NULL),
            dateRangeInput("time_span", "Select Time Span:"),
            downloadButton("download_csv", "Download CSV"),
            downloadButton("download_xlsx", "Download XLSX")
        ),
        mainPanel(
            plotlyOutput("time_series_plot"),
            DTOutput("data_table")
        )
    )
)

server <- function(input, output, session) {
    # Update selectInput choices based on dataset
    observe({
        levels_i <- series |> distinct(level) |> pull(level, as_vector = TRUE)
        series_i <- series |> distinct(series) |> pull(series, as_vector = TRUE)
        updateSelectInput(session, "level", choices = levels_i)
        updateSelectInput(session, "series", choices = series_i)
    })

    observeEvent(input$series, {
        disaggregation_i <- series %>%
            filter(series == input$series) %>%
            distinct(disaggregation) %>%
            pull(disaggregation, as_vector = TRUE)
        updateSelectInput(session, "disaggregation", choices = disaggregation_i)
    })

    observeEvent(input$disaggregation, {
        sheet_names_i <- series %>%
            filter(series == input$series, disaggregation == input$disaggregation) %>%
            distinct(sheet_name) %>%
            pull(sheet_name, as_vector = TRUE)
        updateSelectInput(session, "sheet_name", choices = sheet_names_i)
    })

    filtered_data <- reactive({
        data <- series %>%
            filter(level == input$level,
                   series == input$series,
                   disaggregation == input$disaggregation,
                   sheet_name == input$sheet_name,
                   date >= input$time_span[1],
                   date <= input$time_span[2]) |>
          select(clave_delegacion, delegacion, clave_subdelegacion, subdelegacion, date, value) |>
          collect() 
        data
    })

    output$time_series_plot <- renderPlotly({
        data <- filtered_data()
        plot_ly(data, x = ~date, y = ~value, type = 'scatter', mode = 'lines') %>%
            layout(title = paste("Time Series for", input$series))
    })

    output$data_table <- renderDT({
        data <- filtered_data()
        data <- data |>
          pivot_wider(
            names_from = date,
            values_from = value
          ) 
        datatable(data)
    })

    output$download_csv <- downloadHandler(
        filename = function() {
            paste("time_series_", Sys.Date(), ".csv", sep = "")
        },
        content = function(file) {
            data <- filtered_data()
        data <- data |>
          pivot_wider(
            names_from = date,
            values_from = value
          ) 
            write.csv(data, file)
        }
    )

    output$download_xlsx <- downloadHandler(
        filename = function() {
            paste("time_series_", Sys.Date(), ".xlsx", sep = "")
        },
        content = function(file) {
            data <- filtered_data()
        data <- data |>
          pivot_wider(
            names_from = date,
            values_from = value
          ) 
            write_xlsx(data, file)
        }
    )
}

shinyApp(ui, server)
# shiny::runApp()
