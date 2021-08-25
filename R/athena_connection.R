
#' @include Rdbtools.R

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

  # work out what your staging dir should be on the AP if unset
  if (is.null(staging_dir)) {
    user_id <- athena_user_id()
    staging_dir = paste("s3://mojap-athena-query-dump", user_id, sep = "/")
  }

  # connect to athena
  con <- DBI::dbConnect(noctua::athena(),
                        region_name = aws_region,
                        s3_staging_dir = staging_dir,
                        rstudio_conn_tab = rstudio_conn_tab)

  user_id <- athena_user_id()
  temp_db_name <- get_database_name_from_userid(user_id)


  con <- as(con,"MoJAthenaConnection")
  con@MoJdetails$user_id <- user_id
  con@MoJdetails$temp_db_name <- temp_db_name
  con@MoJdetails$temp_db_exists <- NA # Don't know if the temp db exists yet

  return(con)

}


