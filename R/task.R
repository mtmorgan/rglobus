#' @rdname task
#'
#' @title Task management
#'
#' @description `task_status()` retrieves the status of a task
#'     started by `copy()` or `transfer()`.
#'
#' @param .data a `tibble` with column `task_id`, as returned by
#'     `copy()` or `transfer()`.
#'
#' @param all_fields `logical(1)` indicating whether abbreviated or
#'     detailed information about the task status should be returned
#'
#' @details
#'
#' The input to `task_status()` is usually the return value from
#' `copy()` or `transfer()`.
#'
#' @return
#'
#' `task_status()` and `task_list()` return a `tibble` summarizing the
#' status of the task(s). The meaning of each column is described in the
#' Globus documentation at
#'
#' <https://docs.globus.org/api/transfer/task/#task_document>
#'
#' With the default value `all_fields = FALSE`, the `tibble`)
#' contains columns
#'
#' - `task_id`: the id of the task.
#'
#' - `type`: one of `TRANSFER` or `DELETE`.
#'
#' - `status`: one of `ACTIVE`, `INACTIVE`, `SUCCEEDED` or `FAILED`.
#'
#' - `nice_status`: `NULL` for a completed task; `OK` or `Queued`
#'   tasks proceeding normally; or an indication of the reason for
#'   `ACTIVE` or `INACTIVE` errors. Examples of `nice_status`
#'   indicating transient failure include: `PERMISSION_DENIED` if the
#'   user does not currently have access to the source or destination
#'   resource; `CONNECT_FAILED` if the Globus Connect Personal client
#'   is not currently on-line; `GC_PAUSED` if the client is paused.
#'
#' @examples
#' if (exists("task")) {
#'     ## check transfer task status
#'     task_status <- task_status(task)
#'     print(task_status)
#'
#'     ## tasks that are failing need to be cancelled
#'     status <- pull(task_status, "status")
#'     nice_status <- pull(task_status, "nice_status")
#'     cancel <-
#'         ## task has failed, or...
#'         identical(status, "FAILED") ||
#'         ## ...something's gone wrong
#'         (!is.null(nice_status) && !nice_status %in% c("OK", "Queued"))
#'     if (cancel)
#'         task_cancel(task)
#' }
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
    tbl <- select(
        tbl,
        "task_id", "type", "status", "nice_status", "label",
        if (all_fields) everything()
    )
    mutate(tbl, across(where(is.list), tibble_column_unlist_maybe))
}

#' @rdname task
#'
#' @description `task_cancel()` cancels a task started with
#'     `copy()` or `transfer()`.
#'
#' @details
#'
#' `task_cancel()` prints a message indicating successful
#' submission of the task cancellation operation.
#'
#' @return
#'
#' `task_cancel()` returns its input argument, invisibly.
#'
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

#' @rdname task
#'
#' @description `task_list()` retrieves information on all tasks
#'     submitted by the user.
#'
#' @param filter character(1) query parameter restricting which tasks
#'     are returned by `task_list()`. See details.
#'
#' @param orderby character(1) task list ordering; the default places
#'     the most recently requested task at the top.
#'
#' @details
#'
#' For `task_list()`, the `filter` and `orderby` syntax is described at
#'
#' <https://docs.globus.org/api/transfer/task/#filter_and_order_by_options>
#'
#' A simple task list filter is to return all 'ACTIVE' tasks, with `filter =
#' "status:ACTIVE"`.
#'
#' @examples
#' task_list()
#'
#' @export
task_list <-
    function(filter = "", orderby = "request_time DESC", all_fields = FALSE)
{
    stopifnot(
        .is_scalar_character(filter, zchar_ok = TRUE),
        .is_scalar_character(orderby),
        .is_scalar_logical(all_fields)
    )

    uri <- paste0(TRANSFER, "/task_list")
    resp <- req_resp(
        uri,
        filter = if (nzchar(filter)) filter,
        orderby = orderby,
        limit = 1000
    )

    tbl0 <- tibble(
        task_id = character(),
        type = character(),
        status = character(),
        nice_status = list(),
        label = list()
    )
    resp_as_tibble(resp, "task_list", all_fields, tbl0)
}
