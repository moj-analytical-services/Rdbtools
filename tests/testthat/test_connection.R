ath_con <- connect_athena()

df <- data.frame(a = c(1,2,3),
           b = c("a", "b", "c"))

if (dbExistsTable(ath_con, "__temp__.testthat")) {
  dbRemoveTable(ath_con, "__temp__.testthat", delete_data = FALSE, confirm = TRUE)
}


test_that("Check a table doesn't exist", {
  expect_equal(dbExistsTable(ath_con, "__temp__.testthat"), FALSE)
})

dbExecute(ath_con,
          sqlCreateTable(ath_con, "__temp__.testthat", df))

test_that("Check that the table was created", {
  expect_equal(dbExistsTable(ath_con, "__temp__.testthat"), TRUE)
})

dbDisconnect(ath_con)
