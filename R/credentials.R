
#' get_aws_credentials
#'
#' Gets temporary credentials for an AWS service.
#'
#' @param aws_region Default is NULL, which will look for relevant environment variables and if not found set this to be the relevant region for most Analytical Platform users.
#' @param session_duration The number of seconds which the session should last before needing new authentication. Minimum of 900.
#' @param role_session_name This is a parameter for authentication, and should be left to NULL in normal operation.
#' @param ... Further arguments for the `paws` function `assume_role_with_web_identity`
#'
#' @export
get_aws_credentials <- function(aws_region = NULL,
                                session_duration = 3600L,
                                role_session_name = NULL,
                                ...) {

  if (is.null(aws_region)) aws_region <- get_region()

  # Obtain the WebIdentity credentials
  # ref: https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html

  aws_role_arn <- Sys.getenv('AWS_ROLE_ARN')
  if (!(nchar(aws_role_arn) > 0)) rlang::abort(c("Could not find your AWS ARN", "i" = "The env variable is missing"))
  aws_web_identity_token_file <- Sys.getenv('AWS_WEB_IDENTITY_TOKEN_FILE')
  if (!(nchar(aws_web_identity_token_file) > 0)) rlang::abort(c("Could not find your token file path", "*" = "The env variable is missing"))

  # Set the arbitrary session name to user plus timestamp
  user <- stringr::str_split(aws_role_arn, '/')[[1]][2]
  if (is.null(role_session_name)) role_session_name <- glue::glue("{user}_{as.numeric(Sys.time())}")

  tryCatch({
    # https://paws-r.github.io/docs/sts/
    svc <- paws::sts(config = list(credentials = list(anonymous = TRUE)),
                     region = get_region())

    creds <- svc$assume_role_with_web_identity(
      DurationSeconds = session_duration,
      RoleArn = aws_role_arn,
      RoleSessionName = role_session_name,
      WebIdentityToken = readr::read_file(aws_web_identity_token_file),
      ...
    )},
    error = function(e){
      rlang::abort(c("Something went wrong getting temporary credentials",
                     "*" = paste(e)))
    }
  )

  return(creds)
}

is_auth_within_expiry <- function(con, window = 5 * 60) {
  expiry_t <- con@MoJdetails$authentication_expiry
  ifelse(
    is.null(expiry_t),
    TRUE,
    as.POSIXct(Sys.time(), tz='UTC') + window < expiry_t
  )
}

# Check for region in environment variables based on the following order.
# AWS_ATHENA_QUERY_REGION:
#   An environment variable for specifying the region when the region
#   where the query will be run is different from the default region
#   from underlying running environment.
# AWS_DEFAULT_REGION and AWS_REGION:
#   The default region. Usually the 2 variables will be setup by the
#   underlying running environment e.g. cluster, and they cannot be amended
# othewise use 'eu-west-1' as the default
get_region <- function() {
  if (nchar(Sys.getenv("AWS_ATHENA_QUERY_REGION")) > 0) {
    return(Sys.getenv("AWS_ATHENA_QUERY_REGION"))
  } else if (nchar(Sys.getenv("AWS_DEFAULT_REGION")) > 0) {
    return(Sys.getenv("AWS_DEFAULT_REGION"))
  } else if (nchar(Sys.getenv("AWS_REGION")) > 0) {
    return(Sys.getenv("AWS_REGION"))
  } else {
    return("eu-west-1")
  }
}
