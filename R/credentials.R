

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Sometimes if using s3tools as well then we can end up with expired AWS credentials
# in our evironment. These functions remove them if they are expired.
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


#' @export
wipe_aws_credentials <- function() {
  Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  Sys.unsetenv("AWS_ACCESS_KEY_ID")
  Sys.unsetenv("AWS_SESSION_TOKEN")
  Sys.unsetenv("AWS_CREDENTIAL_EXPIRATION")
}

check_credentials <- function() {
  expiry <- Sys.getenv("AWS_CREDENTIAL_EXPIRATION")

  if (expiry != "") {
    if (expiry %>% ymd_hms() < now()) {
      wipe_aws_credentials()
      cat("Expired enivronment credentials removed")
    }
  }
}
