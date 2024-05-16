
user_staging_dir <- "s3://alpha-everyone/Rdbtools/test/"

test_reconnection <- rstudioapi::showPrompt("Test reconnection",
                                            "Do you want to wait 15 mins to test the reconnection function? (y/N)",
                                            "N")

ath_con <- connect_athena(staging_dir = user_staging_dir,
                          session_duration = 900)

df <- data.frame(a = c("a", "b", "c"),
                 b = round(runif(3)*100))



test_that("Test writing and reading", {


  dbWriteTable(ath_con,
               "__temp__.testthat",
               df,
               overwrite = TRUE)

  df_return <- dbGetQuery(ath_con, "SELECT * FROM __temp__.testthat")

  expect_equal(df_return, df, ignore_attr = TRUE)

})


test_that("Test existing and listing before removing", {

  expect_equal(dbExistsTable(ath_con, "__temp__.testthat"), TRUE)
  expect_equal("testthat" %in% (dbGetTables(ath_con, "__temp__") %>% .[,"TableName"] %>% as.list()), TRUE)
  expect_equal("testthat" %in% dbListTables(ath_con, "__temp__"), TRUE)

  expect_setequal(dbListFields(ath_con, "__temp__.testthat"), names(df))

})

dbRemoveTable(ath_con, "__temp__.testthat", delete_data = TRUE, confirm = TRUE)

test_that("Test existing and listing after removing", {

  expect_equal(dbExistsTable(ath_con, "__temp__.testthat"), FALSE)
  expect_equal("testthat" %in% (dbGetTables(ath_con, "__temp__") %>% .[,"TableName"] %>% as.list()), FALSE)
  expect_equal("testthat" %in% dbListTables(ath_con, "__temp__"), FALSE)

})



test_that("Test queries after timeout", {

  skip_if(tolower(test_reconnection) != "y")

  # This block handle waiting the right amount of time, and prints progress
  start_bar <- Sys.time()
  expiry_t <- ath_con@MoJdetails$authentication_expiry %>% lubridate::ymd_hms()
  expiry_t_string <- format(expiry_t, format = "%H:%M")
  cat('\n')
  while (TRUE) {
    now <- Sys.time()
    elapsed <- difftime(now, start_bar, units = "secs") %>% as.numeric()
    time_left <- max(0, difftime(expiry_t, now, units = "secs") %>% as.numeric())
    extra <- nchar('Wait to  ||100%') + nchar(expiry_t_string)
    width <- options()$width
    step <- round(elapsed / (elapsed + time_left) * (width - extra))
    text <- sprintf('%s|%s%s|% 3s%%',
                    paste0("Wait to ", expiry_t_string, " "),
                    strrep('=', step),
                    strrep(' ', width - step - extra), round(elapsed / (elapsed + time_left) * 100))
    cat(text)
    Sys.sleep(2)
    if (time_left == 0) {
      cat('\n')
      break()
    } else cat('\r')
  }

  # should be an error because we've passed the authentication expiry
  expect_error(dbExistsTable(ath_con, "__temp__.testthat"))

  # refresh and then it should work
  refresh_athena_connection(ath_con)
  expect_equal(dbExistsTable(ath_con, "__temp__.testthat"), FALSE)

})





# cleanup
dbDisconnect(ath_con)
