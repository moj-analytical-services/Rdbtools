
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
