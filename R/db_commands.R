

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
#' See [noctua::dbGetQuery()]
#'
#' @inheritParams noctua::dbGetQuery
#' @param conn A DBIConnection object, as returned by `connect_athena()`
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
#' See [noctua::Query]
#'
#' @inheritParams noctua::Query
#' @param conn A DBIConnection object, as returned by `connect_athena()`
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
#' See [noctua::dbGetTables()]
#'
#' @inheritParams noctua::dbGetTables
#' @param conn A DBIConnection object, as returned by `connect_athena()`
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
#' See [noctua::dbListTables()]
#'
#' @inheritParams noctua::dbListTables
#' @param conn A DBIConnection object, as returned by `connect_athena()`
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
#' See [noctua::dbExistsTable()]
#'
#' @inheritParams noctua::dbExistsTable
#' @param conn A DBIConnection object, as returned by `connect_athena()`
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
#' See [noctua::dbListFields()]
#'
#' @inheritParams noctua::dbListFields
#' @param conn A DBIConnection object, as returned by `connect_athena()`
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
#' See [noctua::dbRemoveTable()]
#'
#' @inheritParams noctua::dbRemoveTable
#' @param conn A DBIConnection object, as returned by `connect_athena()`
#' @rdname dbRemoveTable
#' @export
#' @md
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

#' read_sql (deprecated)
#'
#' A convenience function to match dbtools which reads a table into a tibble or dataframe.
#' Please now use [dbGetQuery()] directly instead.
#'
#' @param sql_query A sql command which is passed to [dbGetQuery()]
#' @param return_df_as must be 'tibble' or 'data.table'.
#' @param conn (optional) A DBIConnection object, as returned by `connect_athena()`. If unused then the query will create its own connection and close it subsequently. If reading a table created by a previous connection then the same connection must be supplied, otherwise you may get permission errors.
#' @export
read_sql <- function(sql_query,
                     return_df_as = "tibble",
                     conn = NULL) {

  if(return_df_as == 'tibble') noctua_options(file_parser = "vroom")
  else if(return_df_as == 'data.table') noctua_options(file_parser = "data.table")
  else stop("input var return_df_as must be one of the following 'tibble' or 'data.table'")


  if (is.null(conn)) {
    conn <- connect_athena(rstudio_conn_tab = FALSE)
    do_disconnect <- TRUE
  } else {
    do_disconnect <- FALSE
  }

  data <- dbGetQuery(conn, sql_query)

  #disconnect athena
  if (do_disconnect) dbDisconnect(conn)

  return(data)

}

#' create_temp_table (deprecated)
#'
#' A convenience function to match dbtools which creates a table in the user's temporary database.
#' Please now use [dbExecute()] directly instead, e.g. with variables `conn`, `table_name` and `sql` then run: `dbExecute(conn, glue("CREATE TABLE __temp__.{table_name} as {sql}")`)
#'
#' @param sql A sql command which generates a table to be created in the temporary location.
#' @param table_name The name of the table to be created in the user's temporary database.
#' @param conn (optional) A DBIConnection object, as returned by `connect_athena()`. If unused then the query will create its own connection and close it subsequently.  Note that if you choose not to supply this argument then you may find you cannot access the table later with a different connection.
#' @export
create_temp_table <- function(sql,
                              table_name,
                              conn = NULL) {

  if (is.null(conn)) {
    conn <- connect_athena(rstudio_conn_tab = FALSE)
    do_disconnect <- TRUE
  } else {
    do_disconnect <- FALSE
  }

  drop_table_query = paste0("DROP TABLE IF EXISTS __temp__.", table_name)
  resp <- dbExecute(conn, drop_table_query)

  sql_query <- paste0("CREATE TABLE __temp__.",
                      table_name,
                      " as ",
                      sql)


  # run the query
  resp <- dbExecute(conn, sql_query)

  #disconnect
  if (do_disconnect) dbDisconnect(conn)

  return(resp)

}

#' write_small_temp_table
#'
#' I can't get dbWriteTable to work, so this is a hacky way to create a table
#' and run INSERT INTO to append values to it.
#' Probably only works with numbers and strings
#' Won't work with a query larger than the maximum SQL size (262144 bytes)
#' Will only write to tempdb, but don't put it in the table_name parameter.
#' NOTE the encryptions issue here is problematic: https://docs.aws.amazon.com/athena/latest/ug/insert-into.html
#'
#' @export
write_small_temp_table <- function(con,
                                   table_name,
                                   table_to_write,
                                   overwrite=FALSE,
                                   append=FALSE,
                                   row.names = NA) {

  # create the string used to add the data at the end first
  # so we can check it isn't too long
  # 262144 is the maximum number of bytes that AWS allows for a single query
  # also keeps all the hacky bits at the top
  table_to_write_sql <- rbind(table_to_write[1,], table_to_write) # I don't know why it ignores the first line, so have to add a fake one
  table_to_write_sql <- table_to_write_sql %>% mutate(across(where(is.character), ~dbQuoteString(con, .x))) # have manually quote strings...
  sql_add_data <- DBI::sqlAppendTable(con, table_name, table_to_write_sql, row.names = row.names)
  if (enc2utf8(sql_add_data) %>% charToRaw() %>% length() > 262144) stop("data too large to write with this method - consider breaking up into smaller chunks")

  table_exist <- dbExistsTable(con, paste0("__temp__.", table_name))
  if (overwrite & append) stop("overwrite and append cannot both be TRUE")
  if (table_exist & !append & !overwrite) stop("Table already exists, set overwrite or append to TRUE to proceed")


  if (table_exist & overwrite) remove_table <- TRUE else remove_table <- FALSE

  if (remove_table) dbRemoveTable(con, paste0("__temp__.", table_name), confirm = TRUE)

  con@info$dbms.name <- con@MoJdetails$temp_db_name # the following won't work unless it thinks it is working in the right database

  # creates a new table if not appending
  if (!append) dbExecute(con, noctua::sqlCreateTable(con, table_name, table_to_write))

  invisible(dbExecute(con, sql_add_data))

}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## Other methods we could define, but I can't get to work
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#' #' dbWriteTable
#' #'
#' #' @rdname dbWriteTable
#' #' @export
#' setMethod("dbWriteTable", c("MoJAthenaConnection", "character", "data.frame"),
#'           function(conn, name, value, overwrite=FALSE, append=FALSE,
#'                    row.names = NA, field.types = NULL,
#'                    partition = NULL, s3.location = NULL, file.type = c("tsv", "csv", "parquet", "json"),
#'                    compress = FALSE, max.batch = Inf, ...) {
#'             # prepare the statement
#'             name <- prepare_statement(conn, name)
#'             # run the query using the noctua function
#'             getMethod("dbWriteTable", c("AthenaConnection", "character", "data.frame"), asNamespace("noctua"))(conn,
#'                                                                                                                name,
#'                                                                                                                value,
#'                                                                                                                overwrite,
#'                                                                                                                append,
#'                                                                                                                row.names,
#'                                                                                                                field.types,
#'                                                                                                                partition,
#'                                                                                                                s3.location,
#'                                                                                                                file.type,
#'                                                                                                                compress,
#'                                                                                                                max.batch,
#'                                                                                                                ...)
#'           }
#' )




