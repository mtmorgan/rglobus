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
#' @title Collections, directories, and files
#'
#' @description `collections()` returns collections available to the
#'     user.
#'
#' @param filter_fulltext `character()` filter applied to fields
#'     associated with collections, including display name,
#'     description, user, etc. `filter_fulltext` is required when
#'     `filter_scope = "all"`.
#'
#' @param filter_scope `character(1)` scope of search, e.g., `"all"`
#'     or `"my-endpoints"`. See details.
#'
#' @param ... additional, less-common parameters influencing
#'     search. See details.
#'
#' @param all_fields `logical(1)` indicating whether abbreviated or
#'     full listing should be returned.
#'
#' @details
#'
#' `filter_fulltext` follows rules defined at
#'
#' <https://docs.globus.org/api/transfer/endpoint_and_collection_search/#endpoint_search>
#'
#' Approximately, each word is treated as a separate term and matched
#' to the prefix of search fields. Thus `"Public HuBMAP"` matches
#' collection mentioning both `"Public"` AND `"HuBMAP"`, appearing in
#' any order, e.g., `"HuBMAP Public"`, `"HuBMAP Dev Public"`, etc.
#'
#' `filter_scope` can take on values described in
#'
#' <https://docs.globus.org/api/transfer/endpoint_and_collection_search/#search_scope>
#'
#' `...` arguments to `collections()` and `my_collections()` are
#' described in
#'
#' <https://docs.globus.org/api/transfer/endpoint_and_collection_search/#query_parameters>
#'
#' 'offset' and 'limit' are used internally.
#'
#' @return
#'
#' `collections()` and `my_collections()` return a `tibble` containing
#' columns `display_name` and (collection / endpoint) `id`. Additional
#' fields returned when `all_fields = TRUE` are described at
#'
#' <https://docs.globus.org/api/transfer/endpoints_and_collections/#endpoint_or_collection_fields>.
#'
#' @examples
#' ##
#' ## collections
#' ##
#'
#' collections <- collections("HuBMAP Public")  # 'AND' words or elements
#' collections
#'
#' hubmap <-
#'     collections |>
#'     dplyr::filter(display_name == "HuBMAP Public")
#'
#' @export
collections <-
    function(
        filter_fulltext = "",
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
        is.character(filter_fulltext), !anyNA(filter_fulltext),
        filter_scope %in% filter_scopes,
        .is_scalar_logical(all_fields)
    )
    if (filter_scope == "all") {
        stopifnot(
            `'filter_fulltext' is required when 'filter_scope' == "all"` =
                length(filter_fulltext) > 0L
        )
    }

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
#' @description `my_collections()` is a convenience function to
#'     identify collections belonging to the current user.
#'
#' @examples
#' my_collection <- my_collections()
#' my_collection
#'
#' @export
my_collections <-
    function(filter_fulltext = "", ...)
{
    collections(filter_fulltext, ..., filter_scope = "my-endpoints")
}

#' @rdname collections
#'
#' @description `globus_ls()` lists the content of a collection.
#'
#' @param .data a `tibble` with a single row, containing a column `id`
#'     identifying the collection, as returned by `collections()` or
#'     `my_collections()`.
#'
#' @param path `character(1)` location in the collection for listing
#'     or directory creation.
#'
#' @param show_hidden `logical(1)` controls inclusion of 'hidden'
#'     (starting with '.') files in the return value of `globus_ls()`.
#'
#' @param filters `character()` of filter terms. See details.
#'
#' @details
#'
#' When used with 'Globus Connect Personal' (used to enable transfer
#' to and from a local computer), `path` is either the full path on
#' the user system (starting with `/`, e.g., `/Users/mtmorgan/HuBMAP`)
#' or relative to the user home directory (starting *without* a `/`,
#' e.g., '~/HuBMAP').
#'
#' `filters` for `globus_ls()` are described at
#'
#' <https://docs.globus.org/api/transfer/file_operations/#dir_listing_filtering>
#'
#' Elements of `filters` are treated as 'OR' operations, thus `filters
#' = "name:~*.csv/size:<200"` finds `csv` files smaller than 200 bytes,
#' whereas `filters = #' c("name:~*.csv", "size:<200")` finds `csv`
#' files of any size, or any file smaller than 200 bytes.
#'
#' @returns
#'
#' `globus_ls()` returns a `tibble` with the name, last modified, size,
#' and type (directory or file) of each entry. Additional fields are
#' returned when `all_fields = TRUE`, as described in
#'
#' <https://docs.globus.org/api/transfer/file_operations/#file_document>
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
#' ## filter files with name ending in 'csv', and with size < 200
#' hubmap |>
#'     globus_ls(
#'         "0008a49ac06f4afd886be81491a5a926/sprm_outputs",
#'         filters = "name:~*.csv/size:<200"
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
        path = "",
        show_hidden = FALSE,
        filters = character(),
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
#' @description `mkdir()` creates a directory in a collection.
#'
#' @details
#'
#' `mkdir()` creates directory elements recursively.
#'
#' @returns
#'
#' `mkdir()` returns a `tibble` of the directory listing of the
#' enclosing directory.
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

    globus_ls(.data, dirname(path))
}
