
#' @importFrom noctua dbDisconnect
#' @export
noctua::dbDisconnect

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
#' See [noctua::dbGetQuery()]. This function calls `noctua::dbGetQuery()`, after replacing any references to `__temp__`
#' in the statement with your temporary database in Athena. Your temporary database will be created
#' if you do not already have one.
#'
#' @inheritParams noctua::dbGetQuery
#' @param conn A DBIConnection object, as returned by `connect_athena()`
#' @rdname dbGetQuery
#' @export
setMethod("dbGetQuery", c("MoJAthenaConnection","character"),
          function(conn, statement, statistics = FALSE, ...) {
            if (!is_auth_within_expiry(conn, window = 0)) stop("Authentication has expired.")
            # prepare the statement
            statement <- prepare_statement(conn, statement)
            # run the query using the noctua function
            getMethod("dbGetQuery", c("AthenaConnection", "character"), asNamespace("noctua"))(conn, statement, statistics, ...)
          }
)


#' dbExecute
#'
#' See [noctua::Query]. This function calls `noctua::dbExecute()`, after replacing any references to `__temp__`
#' in the statement with your temporary database in Athena. Your temporary database will be created
#' if you do not already have one.
#'
#' @inheritParams noctua::Query
#' @param conn A DBIConnection object, as returned by `connect_athena()`
#' @rdname dbExecute
#' @export
setMethod("dbExecute", c("MoJAthenaConnection","character"),
          function(conn, statement, ...) {
            if (!is_auth_within_expiry(conn, window = 0)) stop("Authentication has expired.")
            # prepare the statement
            statement <- prepare_statement(conn, statement)
            # run the query using the noctua function
            getMethod("dbExecute", c("AthenaConnection", "character"), asNamespace("noctua"))(conn, statement, ...)
          }
)

#' dbGetTables
#'
#' See [noctua::dbGetTables()]. This function calls `noctua::dbGetTables()` but if
#' the schema argument is `__temp__` then it looks at your temporary database in Athena.
#'
#' @inheritParams noctua::dbGetTables
#' @param conn A DBIConnection object, as returned by `connect_athena()`
#' @rdname dbGetTables
#' @export
setMethod("dbGetTables", "MoJAthenaConnection",
          function(conn, schema = NULL, ...) {
            if (!is_auth_within_expiry(conn, window = 0)) stop("Authentication has expired.")
            # prepare the statement
            if (isTRUE(schema == "__temp__")) schema <- conn@MoJdetails$temp_db_name
            # run the query using the noctua function
            getMethod("dbGetTables", "AthenaConnection", asNamespace("noctua"))(conn, schema, ...)
          }
)

#' dbListTables
#'
#' See [noctua::dbListTables()]. This function calls `noctua::dbListTables()` but if
#' the schema argument is `__temp__` then it looks at your temporary database in Athena.
#'
#' @inheritParams noctua::dbListTables
#' @param conn A DBIConnection object, as returned by `connect_athena()`
#' @rdname dbListTables
#' @export
setMethod("dbListTables", "MoJAthenaConnection",
          function(conn, schema = NULL, ...) {
            if (!is_auth_within_expiry(conn, window = 0)) stop("Authentication has expired.")
            # prepare the statement
            if (isTRUE(schema == "__temp__")) schema <- conn@MoJdetails$temp_db_name
            # run the query using the noctua function
            getMethod("dbListTables", "AthenaConnection", asNamespace("noctua"))(conn, schema, ...)
          }
)

#' dbExistsTable
#'
#' See [noctua::dbExistsTable()]. This function calls noctua::dbExistsTable(), after replacing any references to `__temp__`
#' in the name argument with your temporary database in Athena.
#'
#' @inheritParams noctua::dbExistsTable
#' @param conn A DBIConnection object, as returned by `connect_athena()`
#' @rdname dbExistsTable
#' @export
setMethod("dbExistsTable", c("MoJAthenaConnection","character"),
          function(conn, name, ...) {
            if (!is_auth_within_expiry(conn, window = 0)) stop("Authentication has expired.")
            # prepare the statement
            name <- prepare_name(conn, name)
            # run the query using the noctua function
            # however, the error returned from AWS has changed, but we cannot
            # update noctua to a version which deals with this because of a
            # permission conflict
            # So reduce the number of retries to 1
            actual_retry_setting <- noctua:::athena_option_env$retry
            noctua_options(retry = 1)
            # capture.output avoids printing an error which we are handling anyway
            cnd <- capture.output(
              # this tries the notcua function, and handles the error
              resp <- rlang::try_fetch(getMethod("dbExistsTable", c("AthenaConnection","character"), asNamespace("noctua"))(conn, name, ...),
                                       error = function(cnd) {
                                         # This is the error which indicates the table doesn't exist
                                         if (grepl("EntityNotFoundException", cnd$message)) return(FALSE)
                                         # all other errors are returned (cannot abort here because the capture.output kills it)
                                         else return(cnd)
                                       }
              ),
              type = "message")

            # put the retries back
            noctua_options(retry = actual_retry_setting)
                    
            # abort if the above has returned an error
            if (inherits(resp, "error")) rlang::abort("Error in dbExistsTable response.", parent = resp)
            return(resp)
          }
)

#' dbListFields
#'
#' See [noctua::dbListFields()]. This function calls `noctua::dbListFields()`, after replacing any references to `__temp__`
#' in the name argument with your temporary database in Athena.
#'
#' @inheritParams noctua::dbListFields
#' @param conn A DBIConnection object, as returned by `connect_athena()`
#' @rdname dbListFields
#' @export
setMethod("dbListFields", c("MoJAthenaConnection","character"),
          function(conn, name, ...) {
            if (!is_auth_within_expiry(conn, window = 0)) stop("Authentication has expired.")
            # prepare the statement
            name <- prepare_name(conn, name)
            # run the query using the noctua function
            getMethod("dbListFields", c("AthenaConnection","character"), asNamespace("noctua"))(conn, name, ...)
          }
)

#' dbRemoveTable
#'
#' See [noctua::dbRemoveTable()]. This function calls `noctua::dbRemoveTable()`, after replacing any references to `__temp__`
#' in the names argument with your temporary database in Athena.
#'
#' @inheritParams noctua::dbRemoveTable
#' @param conn A DBIConnection object, as returned by `connect_athena()`
#' @rdname dbRemoveTable
#' @export
#' @md
setMethod("dbRemoveTable", c("MoJAthenaConnection","character"),
          function(conn, name, delete_data = TRUE, confirm = FALSE, ...) {
            if (!is_auth_within_expiry(conn, window = 0)) stop("Authentication has expired.")
            # prepare the statement
            name <- prepare_name(conn, name)
            # run the query using the noctua function
            getMethod("dbRemoveTable", c("AthenaConnection","character"), asNamespace("noctua"))(conn, name, delete_data, confirm, ...)
          }
)


#' dbWriteTable
#'
#' See [`noctua::dbWriteTable()`][noctua::AthenaWriteTables]. Note that you must have write permission to the s3 directory where the data is stored.
#' In general you will not have this permission for the automatically generated directory generated by `connect_athena()`
#' so you must specify an s3 directory where you do have write permission.
#' You can do this either as an argument to `connect_athena` (which will affect all your Athena transactions), or
#' specifically to the `dbWriteTable` call using the `s3.location` argument.
#' This function calls `noctua::dbWriteTable()`, after replacing any references to `__temp__`
#' in the statement with your temporary database in Athena. Your temporary database will be created
#' if you do not already have one.
#'
#' @inheritParams noctua::AthenaWriteTables
#' @param conn A DBIConnection object, as returned by `connect_athena()`
#' @examples
#' # Either specify the location to dbWriteTable itself
#' con <- connect_athena()
#' dbWriteTable(con, "__temp__.table_name", dataframe, s3.location = "s3://bucket_you_have_write_permission/dir")
#'
#' # Or to the connection object
#' con <- connect_athena(staging_dir = "s3://bucket_you_have_write_permission/dir")
#' dbWriteTable(con, "__temp__.table_name", dataframe)
#' @rdname dbWriteTable
#' @export
setMethod("dbWriteTable", c("MoJAthenaConnection", "character", "data.frame"),
          function(conn, name, value, overwrite=FALSE, append=FALSE,
                   row.names = NA, field.types = NULL,
                   partition = NULL, s3.location = NULL, file.type = c("tsv", "csv", "parquet", "json"),
                   compress = FALSE, max.batch = Inf, ...) {
            if (!is_auth_within_expiry(conn, window = 0)) stop("Authentication has expired.")
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

  if (!is_auth_within_expiry(conn, window = 0)) stop("Authentication has expired.")

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

  if (!is_auth_within_expiry(conn, window = 0)) stop("Authentication has expired.")

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

