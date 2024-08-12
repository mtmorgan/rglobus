## transfer/task_submit <https://docs.globus.org/api/transfer/task_submit>

.is_sync_level <-
    function(sync_level)
{
    is.null(sync_level) ||
        (.is_scalar_numeric(sync_level) && sync_level %in% 0:3)
}

submission_id <-
    function()
{
    uri <- paste0(TRANSFER, "/submission_id")
    resp <- req_resp(uri)

    body <- resp_body_string(resp)
    stopifnot(identical(j_query(body, "DATA_TYPE"), "submission_id"))
    j_query(body, "value", as = "R")
}

#' @rdname transfer
#'
#' @title Globus file and directory transfer
#'
#' @description `copy()` copies files or directories (perhaps
#'     recursively) from one collection to another.
#'
#' @param source a tibble containing a single row with column `id`,
#'     the unique identifier for the source collection.
#'
#' @param destination a tibble containing a single row with column
#'     `id`, the unique identifier of the destination collection.
#'
#' @param source_path `character(1)` path from the source collection
#'     root to the directory or file to be copied.
#'
#' @param destination_path `character(1)` path from the destination
#'     collection root to the location of the directory or file to be
#'     copied.
#'
#' @param recursive `logical(1)` when `TRUE` and `source_path` is a
#'     directory, copy directory content recursively to
#'     `destination_path`. `recursive = TRUE` is an error when the
#'     source is a file.
#'
#' @param ... additional arguments passed from `copy()` to
#'     `transfer()`.
#'
#' @details
#'
#' `copy()` implements the common task of copying files or
#' folders between collections. `transfer()` allows
#' specification of multiple transfer operations in a single task.
#'
#' `copy()` and `transfer()` submit a *task* to initiate
#' a transaction, but it is necessary to check on the stataus of the
#' task with `task_status()`. It is also necessary to manually
#' cancel tasks that fail with `task_cancel()`.
#'
#' In the Globus documentation, 
#'
#' - <https://docs.globus.org/api/transfer/task_submit/#document_types>
#'   sections 2.2, 2.3, and 2.4 describe options relevant to copying
#'   directories and files.
#'
#' - <https://docs.globus.org/api/transfer/task_submit/#errors>
#'   describes errors during task submission.
#'
#' @return
#'
#' `copy()` and `transfer()` return a tibble with
#' columns `submission_id`, `task_id` and `code`. `code` is one of
#' 'Accepted' (the task is queued for execution) or `Duplicate` (the
#' task is a re-submission of an existing task). Use the return value
#' to check or cancel the task with `task_status()` or
#' `task_cancel()`.
#' 
#'
#' @examples
#' my_collection <- my_collections()
#' hubmap <-
#'     collections("HuBMAP Public") |>
#'     dplyr::filter(display_name == "HuBMAP Public")
#'
#' ##
#' ## File and directory copying
#' ##
#'
#' ## HubMAP dataset id, from HuBMAPR::datasets()
#' dataset_id <- "d1dcab2df80590d8cd8770948abaf976"
#' ls(hubmap, dataset_id)
#'
#' ## copy file from HuBMAP dataset to `my_collection`
#' source_path <- paste0(dataset_id, "/metadata.json")
#' destination_path <- paste0("tmp/HuBMAP/", source_path)
#' task <- copy(
#'     hubmap, my_collection,
#'     source_path, destination_path
#' )
#' task
#'
#' @export
copy <-
    function(
        source, destination,
        source_path = "", destination_path = "",
        recursive = FALSE,
        ...)
{
    ## construct 'transfer_item'
    transfer_item <-
        transfer_item(source_path, destination_path, recursive)
    transfer(source, destination, transfer_item, ...)
}

## transfer implementation

#' @rdname transfer
#'
#' @description `transfer()` and `transfer_item()` are
#'     lower-level functions that allow for one or more files or
#'     directories, including symbolic links, to be transfered as a
#'     single task.
#'
#' @param transfer_items `character()` of `transfer_item()` or
#'     `symlink_item()` objects.
#'
#' @param label `character(1)` identifier for the task.
#'
#' @param notify_on_succeeded `logical(1)`, see
#'     <https://docs.globus.org/api/transfer/task_submit/#transfer_specific_fields>.
#' @param notify_on_failed `logical(1)`.
#' @param encrypt_data `logical(1)`.
#' @param verify_checksum `logical(1)`.
#' @param preserve_timestamp `logical(1)`.
#' @param delete_destination_extra `logical(1)`.
#' @param skip_source_errors `logical(1)`.
#' @param fail_on_quota_errors `logical(1)`
#'
#' @param filter_rules not implemented.
#' @param sync_level not implemented.
#'
#' @export
transfer <-
    function(
        source,
        destination,
        transfer_items,
        ## less-common options
        label = transfer_label(),
        notify_on_succeeded = TRUE,
        notify_on_failed = TRUE,
        encrypt_data = FALSE,
        verify_checksum = FALSE,
        preserve_timestamp = FALSE,
        delete_destination_extra = FALSE,
        skip_source_errors = FALSE,
        fail_on_quota_errors = FALSE,
        filter_rules = NULL,
        sync_level = NULL)
{
    source_collection <- pull_id(source, "id")
    destination_collection <- pull_id(destination, "id")
    stopifnot(
        is.character(transfer_items),
        !anyNA(transfer_items),
        all(nzchar(transfer_items)),
        .is_scalar_character(label),
        .is_scalar_logical(notify_on_succeeded),
        .is_scalar_logical(notify_on_failed),
        .is_scalar_logical(encrypt_data),
        .is_scalar_logical(verify_checksum),
        .is_scalar_logical(preserve_timestamp),
        .is_scalar_logical(delete_destination_extra),
        .is_scalar_logical(skip_source_errors),
        .is_scalar_logical(fail_on_quota_errors)
    )
    stopifnot(
        ## FIXME: not yet supported
        `'filter_rules' not yet supported` = is.null(filter_rules),
        `'sync_level' not yet supported` = is.null(sync_level)
    )

    ## collapse 'transfer_items' to a JSON list-of-items
    transfer_items <- paste(transfer_items, collapse = ", ")
    transfer_items <- paste0("[", transfer_items, "]")

    ## construct 'transfer'
    submission_id <- submission_id()
    body <- json_template(
        "transfer",
        submission_id = submission_id,
        label = label,
        source_endpoint = source_collection,
        destination_endpoint = destination_collection,
        DATA = transfer_items,
        notify_on_succeeded = tolower(notify_on_succeeded),
        notify_on_failed = tolower(notify_on_failed),
        encrypt_data = tolower(encrypt_data),
        verify_checksum = tolower(verify_checksum),
        preserve_timestamp = tolower(preserve_timestamp),
        delete_destination_extra = tolower(delete_destination_extra),
        skip_source_errors = tolower(skip_source_errors),
        fail_on_quota_errors = tolower(fail_on_quota_errors)
    )

    uri <- paste0(TRANSFER, "/transfer")
    resp <- req_resp(uri, .body = body)

    body <- resp_body_string(resp)
    stopifnot(identical(j_query(body, "DATA_TYPE"), "transfer_result"))
    j_pivot(body, as = "tibble") |>
        select("submission_id", "task_id", "code")
}

#' @rdname transfer
#'
#' @description `transfer_item()` constructs the JSON
#'     description of a single transfer, e.g., of a source file to a
#'     destination file. It is used as input to `transfer()`.
#'
#' @export
transfer_item <-
    function(source_path, destination_path, recursive)
{
    stopifnot(
        .is_scalar_character(source_path, zchar_ok = TRUE),
        .is_scalar_character(destination_path, zchar_ok = TRUE),
        .is_scalar_logical(recursive)
    )

    json_template(
        "transfer_item",
        source_path = source_path,
        destination_path = destination_path,
        recursive = tolower(recursive)
    )
}

#' @rdname transfer
#'
#' @description `transfer_label()` is a helper function to
#'     provide an identifying label for each task, visible for
#'     instance in the Globus web application.
#'
#' @return
#'
#' `transfer_label()` returns a `character(1)` label including
#' date, time, and user information, with suffix 'r-globus-transfer'.
#'
#' @examples
#' ## default task label
#' transfer_label()
#'
#' @export
transfer_label <-
    function()
{
    paste(
        format(Sys.time(), "%Y-%m-%d %H:%M:%S%z"),
        Sys.info()[["user"]],
        "r-globus-transfer"
    )
}

## tasks

#' @rdname transfer
#'
#' @description `task_status()` retrieves the status of a task
#'     started by `copy()` or `transfer()`.
#'
#' @param .data a tibble with column `task_id`, as returned by
#'     `copy()` or `transfer()`.
#'
#' @param all_fields `logical(1)` indicating whether abbreviated or
#'     detailed information about the task status should be returned
#'
#' @return
#'
#' `task_status()` returns a tibble summarizing the status of
#' the task. The meaning of each column is described in the Globus
#' documentation at
#'
#' - https://docs.globus.org/api/transfer/task/#task_document
#'
#' With the default value `all_fields = FALSE`, the tibble
#' contains three columns
#'
#' - `status`: one of 'ACTIVE', 'INACTIVE', 'SUCCEEDED' or 'FAILED'
#'
#' - `nice_status`: either NULL or, for a failing task, an indication
#'   of the reason for failure, e.g., 'PERMISSION_DENIED'.
#'
#' - `task_id`: the id of the task.
#'
#' @examples
#' ## check transfer task status
#' task_status(task)
#'
#' @export
task_status <-
    function(.data, all_fields = FALSE)
{
    task_id <- pull_id(.data, "task_id")
    uri <- paste0(TRANSFER, "/task/", task_id)
    resp <- req_resp(uri)

    body <- resp_body_string(resp)
    stopifnot(identical(j_query(body, "DATA_TYPE"), "task"))
    tbl <- j_pivot(body, as = "tibble")
    select(
        tbl,
        "status", "nice_status", "task_id",
        if (all_fields) everything())
}

#' @rdname transfer
#'
#' @description `task_cancel()` cancels a task started with
#'     `copy()` or `transfer()`.
#'
#' @details
#'
#' `task_cancel()` prints a message indicating successful
#' submission of the task cancelation operation.
#'
#' @return
#'
#' `task_cancel()` returns its input argument, invisibly.
#'
#' @examples
#' ## tasks that are failing need to be cancelled
#' \dontrun{
#' task_cancel(task)
#' }
#' @export
task_cancel <-
    function(.data)
{
    task_id <- pull_id(.data, "task_id")
    uri <- paste0(TRANSFER, "/task/", task_id, "/cancel")
    resp <- req_resp(uri, .body = '')

    body <- resp_body_string(resp)
    stopifnot(identical(j_query(body, "DATA_TYPE"), "result"))
    message(
        j_query(body, "code"), ": ", j_query(body, "message")
    )

    invisible(.data)
}
