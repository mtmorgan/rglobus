## OAUTH

OAUTH2 <- "https://auth.globus.org/v2/oauth2"

#' @importFrom httr2 oauth_client
client <-
    function()
{
    oauth_client(
        id = "66ab38b0-4eb9-4751-a474-21f463b9881d",
        token_url = paste0(OAUTH2, "/token"),
        name = "HuBMAPR"
    )
}

#' @importFrom httr2 req_oauth_auth_code
oauth <-
    function(.req, scope, cache_disk = TRUE)
{
    redirect_uri <- paste0("http://localhost:", httpuv::randomPort())
    req_oauth_auth_code(
        .req,
        client = client(),
        auth_url = paste0(OAUTH2, "/authorize"),
        scope = scope,
        redirect_uri = redirect_uri,
        cache_disk = cache_disk
    )
}

## TRANSFER

TRANSFER <- "https://transfer.api.globus.org/v0.10"

TRANSFER_SCOPE <- paste(
    "urn:globus:auth:scope:transfer.api.globus.org:all",
    "offline_access"
)

HUBMAP_COLLECTION <-
    ## from the web interface, looking for details on 'HuBMAP Public'
    ## collection.  Are all datasets under 'HuBMAP Public' ??
    "af603d86-eab9-4eec-bb1d-9d26556741bb"

## Utilities & workflows

#' @importFrom dplyr pull
pull_id <-
    function(.data, id_column)
{
    stopifnot(
        `'.data' must have exactly 1 row` = NROW(.data) == 1L,
        `'.data' must have a column 'id'` = id_column %in% colnames(.data)
    )
    pull(.data, id_column)
}

#' @importFrom httr2 resp_body_string
#'
#' @importFrom rjsoncons j_query
error_body <-
    function(resp)
{
    body <- resp_body_string(resp)
    code <- j_query(body, "code", as = "R")
    message <- j_query(body, "message", as = "R")
    extra <-
        if (identical(code, "ConsentRequired")) {
            paste0("\n  ", j_query(body, "required_scopes", as = "R"))
        }
    paste0(
        code, ":\n  ",
        message,
        extra
    )
}

#' @importFrom httr2 request req_url_query req_body_raw req_error
#'     req_perform
req_resp <-
    function(uri, ..., .body = NULL)
{
    req <- request(uri)
    req <- req_url_query(req, ...)
    if (!is.null(.body))
        req <- req_body_raw(req, .body)
    req <- oauth(req, TRANSFER_SCOPE)
    req <- req_error(req, body = error_body)
    req_perform(req)
}

## clean 'list' columns to vectors, if possible
tibble_column_unlist_maybe <-
    function(.x)
{
    ## requirement for unlisting -- 0 or 1 elements in each list
    lengths <- lengths(.x)
    if (!all(lengths) < 2L)
        return(.x)

    result <- rep(NA, length(.x))
    result[lengths == 1] <- unlist(.x)
    result
}

#' @importFrom httr2 resp_body_string
#'
#' @importFrom dplyr bind_rows mutate select everything across where
#'
#' @importFrom rjsoncons j_pivot
resp_as_tibble <-
    function(resp, data_type, all_fields, tbl0)
{
    required_fields <- colnames(tbl0)
    body <- resp_body_string(resp)
    stopifnot(
        `unexpected 'DATA_TYPE' in response` =
            identical(j_query(body, "DATA_TYPE"), data_type)
    )
    tbl <- j_pivot(body, "DATA", as = "tibble")
    if (!NROW(tbl))
        tbl <- tbl0
    tbl <- select(tbl, required_fields, if (all_fields) everything())

    mutate(tbl, across(where(is.list), tibble_column_unlist_maybe))
}
