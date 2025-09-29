library(testthat)
library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)

# =================================================================
# MOCK DATA SETUP
# =================================================================

# This function simulates the raw output from csodata::cso_get_data("RSM08") or
# ("RSM07")
# The column names, casing, and date format are typical of CSO output.
make_mock_cso_data <- function(has_nace = TRUE) {
  df <- data.frame(
    Statistic = c("Sales Index", "Sales Index", "Sales Index"),
    "NACE.Group" = c(
      "Total Retail Trade", "Food, Beverages", "Electrical Goods"
    ),
    "2023M01" = c(100.0, 105.0, 95.0),
    "2023M02" = c(102.5, 107.5, 97.5),
    check.names = FALSE
  )
  if (!has_nace) {
    # Simulate a structural change where NACE.Group is missing
    df <- dplyr::select(df, -dplyr::all_of("NACE.Group"))
  }
  return(df)
}

# =================================================================
# TESTS FOR clean_cso_data
# =================================================================

test_that("clean_cso_data transforms structure and renames correctly", {
  raw_data <- make_mock_cso_data(has_nace = TRUE)
  clean_data <- clean_cso_data(raw_data, "sales_index")

  # Check final structure (columns and number of rows)
  expect_s3_class(clean_data, "data.frame")
  expect_true(all(c("date", "statistic", "nace_group", "sales_index") %in%
                    names(clean_data)))
  expect_equal(nrow(clean_data), 6) # 3 NACE groups * 2 months

  # Check data types
  expect_s3_class(clean_data$date, "Date")
  expect_type(clean_data$statistic, "character")
  expect_type(clean_data$sales_index, "double")

  # Check that column cleaning and date conversion worked
  expect_equal(min(clean_data$date), ymd("2023-01-01"))
  expect_equal(max(clean_data$date), ymd("2023-02-01"))
  expect_equal(clean_data$sales_index[1], 100.0)
})

test_that("clean_cso_data throws error on missing ID column", {
  # Simulates CSO dropping the NACE.Group column (or changing its name)
  raw_data_missing_id <- make_mock_cso_data(has_nace = FALSE)

  # Run the pipeline and expect an error due to a missing 'nace_group'
  expect_error(
    clean_cso_data(raw_data_missing_id, "value_col"),
    regexp = paste0("Structural change detected in CSO table. The following ",
                    "critical columns are missing.*nace_group")
  )
})

test_that("clean_cso_data throws error on insufficient columns", {
  # Simulates receiving a data frame with only two columns (IDs, no values)
  raw_data_too_few <- data.frame(A = 1:5, B = 1:5)

  # Expect the stop() from the initial check
  expect_error(
    clean_cso_data(raw_data_too_few, "value_col"),
    regexp = "Input data must be a data frame with at least 3 columns"
  )
})

# =================================================================
# TESTS FOR get_combined_retail_sales_with_online
# =================================================================

# NOTE: Since get_retail_sales_index and get_online_turnover call
# csodata::cso_get_data,
# we must use mocking or skipping for CRAN. Here we use `with_mock` for
# demonstration.

test_that("get_retail_sales_with_online correctly joins data", {
  # Mock the underlying data retrieval functions (critical for package testing)
  # This avoids hitting the CSO API and relies on the mock data
  with_mock(
    # Mock csodata::cso_get_data to return mock data for each table code
    `csodata::cso_get_data` = function(table_code) {
      if (table_code == "RSM08") {
        # Sales Index data
        make_mock_cso_data()
      } else if (table_code == "RSM07") {
        # Online Turnover data (use different values)
        df <- make_mock_cso_data()
        df[, "2023M01"] <- df[, "2023M01"] / 10
        df[, "2023M02"] <- df[, "2023M02"] / 10
        df
      } else {
        stop("Unexpected table code")
      }
    },
    {
      combined_data <- get_retail_sales_with_online()

      # Check final structure
      expect_s3_class(combined_data, "data.frame")
      expect_true(
        all(
          c(
            "date", "statistic", "nace_group",
            "observation", "online_turnover_pct"
          ) %in% names(combined_data)
        )
      )
      expect_equal(nrow(combined_data), 6) # All 6 rows should join successfully

      # Check that the join was performed correctly and values are present
      expect_equal(combined_data$observation[1], 100.0)
      expect_equal(combined_data$online_turnover_pct[1], 100.0 / 10)
    }
  )
})
