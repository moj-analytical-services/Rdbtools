
create_temp_database <- function(conn) {

  create_db_query <- paste0("CREATE DATABASE IF NOT EXISTS ", conn@MoJdetails$temp_db_name)
  resp <- noctua::dbExecute(conn, create_db_query)
  cat("Created __temp__ database\n")
  conn@MoJdetails$temp_db_exists <- TRUE # set to true since we just created it

  return(resp)
}


#' dbGetQuery
#'
#' @rdname dbGetQuery
#' @export
setMethod("dbGetQuery", c("MoJAthenaConnection","character"),
          function(conn, statement, statistics = FALSE, ...) {
            # prepare the statement
            statement <- prepare_statement(conn, statement)
            # run the query (as AthenaConnection to avoid recursion)
            data <- dbGetQuery(as(conn, "AthenaConnection"),
                               statement,
                               statistics, ...)
            return(data)
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
            # run the query (as AthenaConnection to avoid recursion)
            resp <- dbExecute(as(conn, "AthenaConnection"),
                              statement,
                              ...)
            return(resp)
          }
)

####### Can't get this one to work - might be a permissions issue
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
#'             name <- stringr::str_replace_all(name, "__temp__", conn@MoJdetails$temp_db_name)
#'             # run the query (as AthenaConnection to avoid recursion)
#'             resp <- dbWriteTable(as(conn, "AthenaConnection"),
#'                                  name, value, overwrite, append,
#'                                  row.names, field.types,
#'                                  partition, s3.location, file.type,
#'                                  compress, max.batch, ...)
#'             return(resp)
#'           }
#' )

#' @rdname sqlCreateTable
#' @export
setMethod("sqlCreateTable", "MoJAthenaConnection",
          function(con, table, fields, field.types = NULL, partition = NULL, s3.location = NULL, file.type = c("tsv", "csv", "parquet", "json"),
                   compress = FALSE, ...) {
            # prepare the statement
            table <- stringr::str_replace_all(table, "__temp__", con@MoJdetails$temp_db_name)
            # run the query (as AthenaConnection to avoid recursion)
            resp <- sqlCreateTable(as(con, "AthenaConnection"),
                                   table, fields, field.types = field.types, partition = partition, s3.location = s3.location, file.type = file.type,
                                   compress = compress, ...)
            return(resp)
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
            # run the query (as AthenaConnection to avoid recursion)
            resp <- dbGetTables(as(conn, "AthenaConnection"),
                                schema,
                                ...)
            return(resp)
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
            # run the query (as AthenaConnection to avoid recursion)
            resp <- dbListTables(as(conn, "AthenaConnection"),
                                 schema,
                                 ...)
            return(resp)
          }
)

#' dbExistsTable
#'
#' @rdname dbExistsTable
#' @export
setMethod("dbExistsTable", c("MoJAthenaConnection","character"),
          function(conn, name, ...) {
            # prepare the statement
            name <- stringr::str_replace_all(name, "__temp__", conn@MoJdetails$temp_db_name)
            # run the query (as AthenaConnection to avoid recursion)
            resp <- dbExistsTable(as(conn, "AthenaConnection"),
                                  name,
                                  ...)
            return(resp)
          }
)

#' dbListFields
#'
#' @rdname dbListFields
#' @export
setMethod("dbListFields", c("MoJAthenaConnection","character"),
          function(conn, name, ...) {
            # prepare the statement
            name <- stringr::str_replace_all(name, "__temp__", conn@MoJdetails$temp_db_name)
            # run the query (as AthenaConnection to avoid recursion)
            resp <- dbListFields(as(conn, "AthenaConnection"),
                                 name,
                                 ...)
            return(resp)
          }
)

#' dbRemoveTable
#'
#' @rdname dbRemoveTable
#' @export
setMethod("dbRemoveTable", c("MoJAthenaConnection","character"),
          function(conn, name, delete_data = TRUE, confirm = FALSE, ...) {
            # prepare the statement
            name <- stringr::str_replace_all(name, "__temp__", conn@MoJdetails$temp_db_name)
            # run the query (as AthenaConnection to avoid recursion)
            resp <- dbRemoveTable(as(conn, "AthenaConnection"),
                                  name,
                                  delete_data,
                                  confirm,
                                  ...)
            return(resp)
          }
)



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
