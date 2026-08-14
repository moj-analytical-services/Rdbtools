
# This adapts the same function in R/dplyr_integration.R so that it is
# compatible with dbplyr v2.6.0. Need this for testing, and if it works then
# should contribute to noctua upstream.
# I am not sure why I need to declare it in dbplyr's namespace, but if I don't
# then Noctua's broken version for AthenaConnection still gets used
#' @exportS3Method dbplyr::sql_query_fields MoJAthenaConnection
sql_query_fields.MoJAthenaConnection <- function(con, sql, ...) {
  # pass ident class to dbGetQuery to continue same functionality as dbplyr v1 api.
  if (inherits(sql, "ident")) {
    return(sql)
  } else {
    # None ident class uses dbplyr:::sql_query_fields.DBIConnection method
    sql_query_select <- noctua:::pkg_method("sql_query_select", "dbplyr")
    sql_query_wrap <- noctua:::pkg_method("sql_query_wrap", "dbplyr")
    dplyr_sql <- noctua:::pkg_method("sql", "dplyr")

    return(sql_query_select(
      con,
      dplyr_sql("*"),
      sql_query_wrap(con, sql),
      where = dplyr_sql("0 = 1")
    ))
  }
}
