
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

  # Obtain the WebIdentity credentials
  # ref: https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html
  aws_role_arn <- Sys.getenv('AWS_ROLE_ARN')
  # Set the arbitrary session name to user plus timestamp
  user <- stringr::str_split(aws_role_arn, '/') %>% unlist() %>% tail(1)
  role_session_name = stringr::str_glue("{user}_{as.numeric(Sys.time())}")
  query = stringr::str_glue(
    "https://sts.amazonaws.com/",
    "?Action=AssumeRoleWithWebIdentity",
    "&RoleSessionName={role_session_name}",
    "&RoleArn={aws_role_arn}",
    "&WebIdentityToken={readr::read_file(Sys.getenv('AWS_WEB_IDENTITY_TOKEN_FILE'))}",
    "&Version=2011-06-15"
  )
  response <- httr::POST(query)
  credentials <- httr::content(response)$AssumeRoleWithWebIdentityResponse$AssumeRoleWithWebIdentityResult$Credentials

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
    staging_dir = paste("s3://mojap-athena-query-dump", user_id, sep = "/")
  }

  # connect to athena
  # returns an AthenaConnection object, see noctua docs for details
  con <- dbConnect(noctua::athena(),
                   region_name = aws_region,
                   s3_staging_dir = staging_dir,
                   rstudio_conn_tab = rstudio_conn_tab,
                   aws_access_key_id = credentials$AccessKeyId,
                   aws_secret_access_key = credentials$SecretAccessKey,
                   aws_session_token = credentials$SessionToken)

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


