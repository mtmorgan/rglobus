#' @importFrom dplyr tibble
collections_tbl <-
    function(resp, all_fields)
{
    tbl0 <- tibble(
        display_name = character(),
        id = character()
    )
    resp_as_tibble(resp, "endpoint_list", all_fields, tbl0)
}

#' @rdname collections
#'
#' @title Globus collections, directories, and files
#'
#' @examples
#' ##
#' ## collections
#' ##
#'
#' collections <- collections("HuBMAP Public")  # '&' words or elements
#' collections
#'
#' hubmap <-
#'     collections |>
#'     dplyr::filter(display_name == "HuBMAP Public")
#'
#' @export
collections <-
    function(
        filter_fulltext,
        filter_scope = "all",
        ...,
        all_fields = FALSE)
{
    filter_scopes <- c(
        "all", "my-endpoints", "my-gcp-endpoints",
        "recently-used", "in-use", "shared-by-me", "shared-with-me",
        "administered-by-me"
    )
    stopifnot(
        is.character(filter_fulltext),
        length(filter_fulltext) > 0L,
        !anyNA(filter_fulltext),
        .is_scalar_logical(all_fields),
        filter_scope %in% filter_scopes
    )

    uri <- paste0(TRANSFER, "/endpoint_search")
    resp <- req_resp(
        uri,
        filter_scope = filter_scope,
        filter_fulltext = paste(filter_fulltext, collapse = " "),
        ...,
        limit = 100
    )

    collections_tbl(resp, all_fields)
}

#' @rdname collections
#'
#' @examples
#' my_collection <- my_collections()
#'
#' @export
my_collections <-
    function(filter_fulltext = "", ...)
{
    collections(filter_fulltext, ..., filter_scope = "my-endpoints")
}

#' @rdname collections
#'
#' @examples
#' ##
#' ## Directories and files
#' ##
#'
#' globus_ls(hubmap)    # same as `hubmap |> globus_ls()`
#'
#' ## hierarchical traversal
#' hubmap |>
#'     globus_ls("0008a49ac06f4afd886be81491a5a926/sprm_outputs")
#'
#' ## filter files with name ending in 'csv', and with size < 1000
#' hubmap |>
#'     globus_ls(
#'         "0008a49ac06f4afd886be81491a5a926/sprm_outputs",
#'         filters = "name:~*.csv/size:<1000"
#'     )
#'
#' ## filter files ending in 'json', or with size > 100000000
#' hubmap |>
#'     globus_ls(
#'         "0008a49ac06f4afd886be81491a5a926/sprm_outputs",
#'         filters = c("name:~*.json", "size:>100000000")
#'     )
#'
#' @export
globus_ls <-
    function(
        .data,
        path = "", show_hidden = FALSE, filters = character(),
        all_fields = FALSE)
{
    collection_id <- pull_id(.data, "id")
    stopifnot(
        .is_scalar_character(path, zchar_ok = TRUE),
        .is_scalar_logical(show_hidden),
        is.character(filters), !anyNA(filters), all(nzchar(filters)),
        .is_scalar_logical(all_fields)
    )

    uri <- paste0(TRANSFER, "/operation/endpoint/", collection_id, "/ls")
    resp <- req_resp(
        uri,
        path = path, show_hidden = tolower(show_hidden),
        filter = if (length(filters)) filters,
        .multi = "explode"
    )

    tbl0 <- tibble(
        name = character(),
        last_modified = character(),
        size = integer(),
        type = character()
    )
    resp_as_tibble(resp, "file_list", all_fields, tbl0)
}

#' @rdname collections
#'
#' @examples
#' \dontrun{
#' mkdir(my_collection, "tmp/HuBMAP/test")
#' }
#'
#' @export
mkdir <-
    function(.data, path)
{
    collection_id <- pull_id(.data, "id")
    stopifnot(
        .is_scalar_character(path, zchar_ok = TRUE)
    )

    uri <- paste0(TRANSFER, "/operation/endpoint/", collection_id, "/mkdir")
    body <- json_template("mkdir", path = path)
    resp <- req_resp(uri, .body = body)

    body <- resp_body_string(resp)
    stopifnot(identical(j_query(body, "DATA_TYPE"), "mkdir_result"))

    ls(.data, dirname(path))
}
