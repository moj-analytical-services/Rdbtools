



athena_user_id <- function(aws_region = "eu-west-1") {
  svc <- paws::sts(config=list(region=aws_region))
  user_id <- svc$get_caller_identity()$UserId
  return(user_id)
}


get_database_name_from_userid <- function(user_id) {

  end_str <- user_id %>%
    stringr::str_split(":") %>%
    unlist() %>% tail(1) %>%
    stringr::str_split("-", n = 2) %>%
    unlist() %>% tail(1)%>%
    stringr::str_replace_all("-", "_")

  return(paste0("mojap_de_temp_", end_str))
}



prepare_statement <- function(conn, statement) {
  if (stringr::str_detect(statement, "__temp__")) {
    if (!isTRUE(conn@MoJdetails$temp_db_exists)) create_temp_database(conn)
    statement <- stringr::str_replace_all(statement, "__temp__", conn@MoJdetails$temp_db_name)
  }
  return(statement)
}

