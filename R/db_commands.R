

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Define the methods to use on MoJAthenaConnection objects so that the
# __temp__ database gets replaced with the appropriate string
# before calling the appropriate AthenaConnection method from noctua
# This may not be an exhaustive list of methods, but any other
# method from the DBI/noctua packages will still work due to class inheritance,
# but may not replace the __temp__ word (unless it calls dbExecute itself)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#' dbGetQuery
#'
#' @rdname dbGetQuery
#' @export
setMethod("dbGetQuery", c("MoJAthenaConnection","character"),
          function(conn, statement, statistics = FALSE, ...) {
            # prepare the statement
            statement <- prepare_statement(conn, statement)
            # run the query using the noctua function
            getMethod("dbGetQuery", c("AthenaConnection", "character"), asNamespace("noctua"))(conn, statement, statistics, ...)
          }
)


#' dbExecute
#'
#' @rdname dbExecute
#' @export
setMethod("dbExecute", c("MoJAthenaConnection","character"),
          function(conn, statement, ...) {
            # prepare the statement
            statement <- prepare_statement(conn, statement)
            # run the query using the noctua function
            getMethod("dbExecute", c("AthenaConnection", "character"), asNamespace("noctua"))(conn, statement, ...)
          }
)

#' dbGetTables
#'
#' @rdname dbGetTables
#' @export
setMethod("dbGetTables", "MoJAthenaConnection",
          function(conn, schema = NULL, ...) {
            # prepare the statement
            if (isTRUE(schema == "__temp__")) schema <- conn@MoJdetails$temp_db_name
            # run the query using the noctua function
            getMethod("dbGetTables", "AthenaConnection", asNamespace("noctua"))(conn, schema, ...)
          }
)

#' dbListTables
#'
#' @rdname dbListTables
#' @export
setMethod("dbListTables", "MoJAthenaConnection",
          function(conn, schema = NULL, ...) {
            # prepare the statement
            if (isTRUE(schema == "__temp__")) schema <- conn@MoJdetails$temp_db_name
            # run the query using the noctua function
            getMethod("dbListTables", "AthenaConnection", asNamespace("noctua"))(conn, schema, ...)
          }
)

#' dbExistsTable
#'
#' @rdname dbExistsTable
#' @export
setMethod("dbExistsTable", c("MoJAthenaConnection","character"),
          function(conn, name, ...) {
            # prepare the statement
            name <- prepare_name(conn, name)
            # run the query using the noctua function
            getMethod("dbExistsTable", c("AthenaConnection","character"), asNamespace("noctua"))(conn, name, ...)
          }
)

#' dbListFields
#'
#' @rdname dbListFields
#' @export
setMethod("dbListFields", c("MoJAthenaConnection","character"),
          function(conn, name, ...) {
            # prepare the statement
            name <- prepare_name(conn, name)
            # run the query using the noctua function
            getMethod("dbListFields", c("AthenaConnection","character"), asNamespace("noctua"))(conn, name, ...)
          }
)

#' dbRemoveTable
#'
#' @rdname dbRemoveTable
#' @export
setMethod("dbRemoveTable", c("MoJAthenaConnection","character"),
          function(conn, name, delete_data = TRUE, confirm = FALSE, ...) {
            # prepare the statement
            name <- prepare_name(conn, name)
            # run the query using the noctua function
            getMethod("dbRemoveTable", c("AthenaConnection","character"), asNamespace("noctua"))(conn, name, delete_data, confirm, ...)
          }
)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## Convenience functions to match dbtools
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#' @export
read_sql <- function(sql_query,
                     return_df_as = "tibble") {

  if(return_df_as == 'tibble') noctua_options(file_parser = "vroom")
  else if(return_df_as == 'data.table') noctua_options(file_parser = "data.table")
  else stop("input var return_df_as must be one of the following 'tibble' or 'data.table'")


  con = connect_athena(rstudio_conn_tab = FALSE)

  data <- dbGetQuery(con, sql_query)

  #disconnect athena
  dbDisconnect(con)

  return(data)

}

#' @export
create_temp_table <- function(sql,
                              table_name) {

  con = connect_athena(rstudio_conn_tab = FALSE)

  drop_table_query = paste0("DROP TABLE IF EXISTS __temp__.", table_name)
  resp <- dbExecute(con, drop_table_query)

  sql_query <- paste0("CREATE TABLE __temp__.",
                      table_name,
                      " as ",
                      sql)


  # run the query
  resp <- dbExecute(con, sql_query)

  #disconnect
  dbDisconnect(con)

  return(resp)

}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## Other methods we could define - note that Athena doesn't like using
## the INSERT INTO by VALUES way of doing it:
## https://docs.aws.amazon.com/athena/latest/ug/insert-into.html
## In practise it is probably better just to introduce new data
## in R once data is read in?

####### Can't get this one to work - might be a permissions issue

#' dbWriteTable
#'
#' @rdname dbWriteTable
#' @export
setMethod("dbWriteTable", c("MoJAthenaConnection", "character", "data.frame"),
          function(conn, name, value, overwrite=FALSE, append=FALSE,
                   row.names = NA, field.types = NULL,
                   partition = NULL, s3.location = NULL, file.type = c("tsv", "csv", "parquet", "json"),
                   compress = FALSE, max.batch = Inf, ...) {
            # prepare the statement
            name <- prepare_statement(conn, name)
            # run the query using the noctua function
            getMethod("dbWriteTable", c("AthenaConnection", "character", "data.frame"), asNamespace("noctua"))(conn,
                                                                                                               name,
                                                                                                               value,
                                                                                                               overwrite,
                                                                                                               append,
                                                                                                               row.names,
                                                                                                               field.types,
                                                                                                               partition,
                                                                                                               s3.location,
                                                                                                               file.type,
                                                                                                               compress,
                                                                                                               max.batch,
                                                                                                               ...)
          }
)

# library(tidyverse)
# test_table <- tibble(col1 = c(1,2,3,4), col2 = c("a","b","c","d"))
#
# con <- connect_athena()
#
# dbWriteTable(con, "__temp__.tab_test", test_table)

## I don't think this is needed, since in practise the output will be used with dbExecute anyway which
## picks up the __temp__ word
#' #' sqlCreateTable
#' #' @rdname sqlCreateTable
#' #' @export
#' setMethod("sqlCreateTable", "MoJAthenaConnection",
#'           function(con, table, fields,
#'                    field.types = NULL, partition = NULL, s3.location = NULL,
#'                    file.type = c("tsv", "csv", "parquet", "json"),
#'                    compress = FALSE, ...) {
#'             # prepare the statement
#'             table <- prepare_name(con, table)
#'             # run the query using the noctua function
#'             getMethod("sqlCreateTable", "AthenaConnection", asNamespace("noctua"))(con,
#'                                                                                    table,
#'                                                                                    fields,
#'                                                                                    field.types = field.types,
#'                                                                                    partition = partition,
#'                                                                                    s3.location = s3.location,
#'                                                                                    file.type = file.type,
#'                                                                                    compress = compress,
#'                                                                                    ...)
#'           }
#' )
#'

