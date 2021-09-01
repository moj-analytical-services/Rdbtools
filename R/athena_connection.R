
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

#' @export
connect_athena <- function(aws_region = "eu-west-1",
                           staging_dir = NULL,
                           rstudio_conn_tab = FALSE
) {

  # this removes expired credentials
  check_credentials()
  # get the athena user id, needed for staging dir and temp db name
  user_id <- athena_user_id()

  # work out what your staging dir should be on the AP if unset
  if (is.null(staging_dir)) {
    staging_dir = paste("s3://mojap-athena-query-dump", user_id, sep = "/")
  }

  # connect to athena
  # returns an AthenaConnection object, see noctua docs for details
  con <- dbConnect(noctua::athena(),
                   region_name = aws_region,
                   s3_staging_dir = staging_dir,
                   rstudio_conn_tab = rstudio_conn_tab)

  # this works out the temp db name from the user id
  temp_db_name <- get_database_name_from_userid(user_id)

  # coerce the AthenaConnection object to be a MoJAthenaConnection object
  # this just adds the slot MoJdetails, as defined in setClass above
  con <- as(con,"MoJAthenaConnection")
  # then we can set the extra details we need in MoJ in the new slot
  con@MoJdetails$user_id <- user_id
  con@MoJdetails$temp_db_name <- temp_db_name
  con@MoJdetails$temp_db_exists <- NA # Don't know if the temp db exists yet

  return(con)

}


