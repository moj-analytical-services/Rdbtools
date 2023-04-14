
#' @include Rdbtools.R

# This extends the AthenaConnection S4 class defined by noctua to
# be a class we can use in MoJ. This has two effects, one is to add
# a slot with extra details (MoJdetails) and the other is that it
# lets us define a new set of methods for the MoJAthenaConnection
# objects - these new methods can be calls to the AthenaConnection
# methods (which work by inheritance) but with some pre-processing.
setClass(
  "MoJAthenaConnection",
  contains="AthenaConnection",
  slots=c(MoJdetails="environment")
)

#' connect_athena
#'
#' Creates a connection object which permits the user to interact with the
#' Athena databases that are hosted on the MoJ's Analytical Platform.
#' It uses the [noctua package][https://dyfanjones.github.io/noctua/], with the MoJ's authentication.
#' This returns an object with class MoJAthenaConnection, which inherits
#' methods from noctua's AthenaConnection class, which in turn are DBI
#' methods.
#' In general the expected usage is to run the function with no arguments to
#' get a standard database connection, which should work for most purposes.
#'
#' @param aws_region This is the region where the database is held. If unset or NULL then will default to the AP's region.
#' @param staging_dir This the s3 location where outputs of queries can be held. If unset or NULL then will default to a session specific temporary dir.
#' @param rstudio_conn_tab Set this to true to show this connection in you RStudio connections frame (warning: this takes a long time to load because of the number of databases in the AP's Athena)
#' @param session_duration The number of seconds which the session should last before needing new authentication. Minimum of 900.
#' @param role_session_name This is a parameter for authentication, and should be left to NULL in normal operation.
#' @param schema_name This is the default database that tables not specifying a database will be looked in. If this is set to the string "__temp__" then it will use (and create if required) the temporary database based on your username - this is useful for using dbplyr which does not understand the __temp__ keyword, alongside the DBI commands.
#'
#' @examples
#'  con <- connect_athena() # creates a connection with sensible defaults
#'  data <- dbGetQuery(con, "SELECT * FROM database.table") # queries and puts data in R environment
#'  dbDisconnect(con) # disconnects the connection
#'
#' @seealso See also noctua's documentation for connecting to a database [noctua::dbConnect,AthenaDriver-method]
#' @export
connect_athena <- function(aws_region = NULL,
                           staging_dir = NULL,
                           rstudio_conn_tab = FALSE,
                           session_duration = 3600,
                           role_session_name = NULL,
                           schema_name = "default",
                           ...
) {

  if (is.null(aws_region)) aws_region <- get_region()

  aws_role_arn <- Sys.getenv('AWS_ROLE_ARN')
  aws_web_identity_token_file <- Sys.getenv('AWS_WEB_IDENTITY_TOKEN_FILE')

  if (nchar(aws_role_arn) > 0 & nchar(aws_web_identity_token_file) > 0) {

    # Obtain the WebIdentity credentials
    # ref: https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html
    # Set the arbitrary session name to user plus timestamp
    user <- stringr::str_split(aws_role_arn, '/')[[1]][2]
    if (is.null(role_session_name)) role_session_name <- glue::glue("{user}_{as.numeric(Sys.time())}")
    query = glue::glue(
      "https://sts.amazonaws.com/",
      "?Action=AssumeRoleWithWebIdentity",
      "&DurationSeconds={session_duration}",
      "&RoleSessionName={role_session_name}",
      "&RoleArn={aws_role_arn}",
      "&WebIdentityToken={readr::read_file(aws_web_identity_token_file)}",
      "&Version=2011-06-15"
    )
    response <- httr::POST(query)

    if (!is.null(httr::content(response)$Error$Message)) rlang::abort(c("Something went wrong getting temporary credentials",
                                                                        "*" = "The message from https://sts.amazonaws.com/ is:",
                                                                        "i" = httr::content(response)$Error$Message))

    credentials <- httr::content(response)$AssumeRoleWithWebIdentityResponse$AssumeRoleWithWebIdentityResult$Credentials
    #temporary_authentication <- TRUE
    authentication_expiry <- as.POSIXct(credentials$Expiration, origin = "1970-01-01", tz="UTC")


    # Use the WebIdentity credentials to access AWS services
    sts_svc <- paws::sts(
      config = list(
        credentials = list(
          creds = list(
            access_key_id = credentials$AccessKeyId,
            secret_access_key = credentials$SecretAccessKey,
            session_token = credentials$SessionToken
          )
        ),
        region = aws_region
      )
    )
    user_id <- sts_svc$get_caller_identity()$UserId

    # work out what your staging dir should be on the AP if unset
    if (is.null(staging_dir)) {
      staging_dir = get_staging_dir_from_userid(user_id)
    }

    # this works out the temp db name from the user id
    temp_db_name <- get_database_name_from_userid(user_id)

    if (schema_name == "__temp__") {
      schema_name_set <- temp_db_name
    } else {
      schema_name_set <- schema_name
    }

    # connect to athena
    # returns an AthenaConnection object, see noctua docs for details
    con <- dbConnect(noctua::athena(),
                     region_name = aws_region,
                     s3_staging_dir = staging_dir,
                     rstudio_conn_tab = rstudio_conn_tab,
                     aws_access_key_id = credentials$AccessKeyId,
                     aws_secret_access_key = credentials$SecretAccessKey,
                     aws_session_token = credentials$SessionToken,
                     schema_name = schema_name_set,
                     ...)
  } else {

    # get the athena user id, needed for staging dir and temp db name
    svc <- paws::sts(config=list(region=aws_region))
    user_id <- svc$get_caller_identity()$UserId
    #temporary_authentication <- FALSE
    authentication_expiry <- NULL
    role_session_name <- NULL

    # work out what your staging dir should be on the AP if unset
    if (is.null(staging_dir)) {
      staging_dir = get_staging_dir_from_userid(user_id)
    }

    # this works out the temp db name from the user id
    temp_db_name <- get_database_name_from_userid(user_id)

    if (schema_name == "__temp__") {
      schema_name_set <- temp_db_name
    } else {
      schema_name_set <- schema_name
    }

    # connect to athena
    # returns an AthenaConnection object, see noctua docs for details
    con <- dbConnect(noctua::athena(),
                     region_name = aws_region,
                     s3_staging_dir = staging_dir,
                     rstudio_conn_tab = rstudio_conn_tab,
                     schema_name = schema_name,
                     ...)

  }

  # coerce the AthenaConnection object to be a MoJAthenaConnection object
  # this just adds the slot MoJdetails, as defined in setClass above
  con <- as(con,"MoJAthenaConnection")
  # then we can set the extra details we need in MoJ in the new slot
  con@MoJdetails$user_id <- user_id
  con@MoJdetails$role_session_name <- role_session_name
  con@MoJdetails$aws_region <- aws_region
  con@MoJdetails$staging_dir <- staging_dir
  con@MoJdetails$authentication_expiry <- authentication_expiry
  con@MoJdetails$session_duration_set <- session_duration
  con@MoJdetails$temp_db_name <- temp_db_name
  con@MoJdetails$temp_db_exists <- NA # Don't know if the temp db exists yet

  # this checks that the temp database exists if it is set as the default db
  if (schema_name == "__temp__") {
    result <- athena_temp_db(con, check_exists = TRUE)
  }

  return(con)

}


#' refresh_athena_connection
#'
#' Refreshes an athena connection to the AP (e.g. if the credentials have expired).
#'
#' @param conn This is the connection which will be refreshed.
#'
#' @examples
#'  con <- connect_athena() # creates a connection with sensible defaults
#'  data <- dbGetQuery(con, "SELECT * FROM database.table") # queries and puts data in R environment
#'  # Some time later...
#'  refresh_athena_connection(con) # refresh the connection for any further queries on the same session
#'  dbDisconnect(con) # disconnects the connection
#' @export
refresh_athena_connection <- function(conn) {

  role_session_name <- conn@MoJdetails$role_session_name
  aws_region <- conn@MoJdetails$aws_region
  staging_dir <- conn@MoJdetails$staging_dir
  session_duration <- conn@MoJdetails$session_duration_set

  conn_refreshed <- connect_athena(aws_region = aws_region,
                                   staging_dir = staging_dir,
                                   session_duration = session_duration,
                                   role_session_name = role_session_name)

  # updates the conn slots which are environments to the new refreshed versions
  slotNames(conn) %>%
    purrr::walk(function(name_of_slot) {
      slot_old <- slot(conn, name_of_slot)
      slot_new <- slot(conn_refreshed, name_of_slot)
      if(class(slot_old) == "environment") {
        #for(n in ls(slot_old, all.names=TRUE)) rm(n, envir = slot_old)
        for(n in ls(slot_new, all.names=TRUE)) assign(n, get(n, slot_new), slot_old)
      }
    })

  invisible(conn_refreshed)
}

#' refresh_if_expired
#'
#' Refreshes an athena connection to the AP only if the credentials have expired.
#'
#' @param conn This is the connection which has expired, but will be refreshed.
#' @param window The number of seconds in advance of expiry that a refresh will still happen (default 5 mins).
#'
#' @export
refresh_if_expired <- function(conn, window = 5 * 60) {
  if (!is_auth_within_expiry(conn, window)) {
    refresh_athena_connection(conn)
    message("Refreshed credentials")
  }
}
