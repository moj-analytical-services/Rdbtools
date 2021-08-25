context("Test connection and basic read")

ath_con <- connect_athena()

df <- data.frame(a = c(1,2,3),
           b = c("a", "b", "c"))

dbExecute(ath_con,
          sqlCreateTable(ath_con, "__temp__.testthat", df))


test_that("Something", {
 # test here
})



dbDisconnect(ath_con)
