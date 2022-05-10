
#' athena_temp_db
#'
#' Returns a string containing the temporary database name for an Athena Connection.
#' Will also check if that temporary database exists and if not will create it, unless `check_exists = FALSE`.
#'
#' @param conn This is a connection object returned by `connect_athena()`.
#' @param check_exists This is `TRUE` by default.  If set to `FALSE` then it will not check if the temporary database exists and will not create it if it does not exist.
#'
#' @examples
#'  con <- connect_athena() # creates a connection with sensible defaults
#'  athena_temp_db(con)
#'  > [1] "mojap_de_temp_alpha_user_Rdbtools_User"
#'
#' @export
athena_temp_db <- function(conn, check_exists = TRUE) {
  if(!isFALSE(check_exists)) {
    # check if the conn object already knows the db exists
    if (!isTRUE(conn@MoJdetails$temp_db_exists)) {
      # get all schemas and create the temp database if the temp db is not in that list
      all_schemas <- dbGetQuery(conn, "show schemas")
      exists <- conn@MoJdetails$temp_db_name %in% all_schemas[,1]
      if (isFALSE(exists)) {
        create_temp_database(conn)
      } else {
        conn@MoJdetails$temp_db_exists <- TRUE # set this to avoid all this checking next time
      }
    }
  }
  conn@MoJdetails$temp_db_name
}

create_temp_database <- function(conn) {

  create_db_query <- paste0("CREATE DATABASE IF NOT EXISTS ", conn@MoJdetails$temp_db_name)
  resp <- dbExecute(conn, create_db_query)
  cat("Created __temp__ database\n")
  conn@MoJdetails$temp_db_exists <- TRUE # set to true since we just created it

  return(resp)
}


# returns same output as equivalent function from dbtools
get_database_name_from_userid <- function(user_id) {

  end_str <- user_id %>%
    stringr::str_remove("^.*:") %>% # remove everything before the colon
    stringr::str_remove("_[0-9]*[.][0-9]*$") %>% #remove the trailing decimal number
    stringr::str_replace_all("-", "_") %>%
    stringr::str_replace_all("\\.", "_")

  return(paste0("mojap_de_temp_", end_str))
}


get_staging_dir_from_userid <- function(user_id) {
  paste("s3://mojap-athena-query-dump", user_id, sep = "/")
}


# This prepares the sql statement ready for execution, by replacing the
# __temp__ string with the temporary db name. It also creates the temporary
# database if it does not already exist.
prepare_statement <- function(conn, statement) {
  # check if the special string is present
  if (stringr::str_detect(statement, "__temp__")) {
    # replace __temp__ with the temp db name
    statement <- stringr::str_replace_all(statement, "__temp__", athena_temp_db(conn))
  }
  return(statement)
}

# as above, but for names we don't need to create the database if it already exists
prepare_name <- function(conn, name) {
  stringr::str_replace_all(name, "__temp__", athena_temp_db(conn, check_exists = FALSE))
}


