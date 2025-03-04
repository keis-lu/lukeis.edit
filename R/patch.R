#' Patch BFSNR Values for PV Installations
#'
#' @description Updates the BFSNR (municipality code) values for specific
#'   addresses in the photovoltaic installations database table:
#'   `strom_produktionsanlagen`.
#'
#' @param con A valid database connection object.
#' @param name A string specifying the table name to update.
#' @param bfsnr_to A numeric scalar value of the new BFSNR code to apply.
#' @param mapping A data.frame specifying the street names and number whose
#'   BFSNR needs to be updated. `NULL` by default.
#'
#' @return The database connection, invisibly.
#'
#' @details This function updates the municipality code (BFSNR) for a predefined
#'   set of addresses in the photovoltaic installations table. It executes each
#'   update within a transaction for safety, rolling back changes if errors
#'   occur.
#'
#'   The function handles the special case of Thorenbergstrasse which has no
#'   street number by looking for rows where the street number is either NULL or
#'   empty.
#' @export
patch_pv_bfsnr_values <- function(con, bfsnr_to, mapping = NULL) {
  stopifnot(
    DBI::dbIsValid(con),
    rlang::is_string(name),
    rlang::is_scalar_double(bfsnr_to),
    DBI::dbExistsTable(con, "strom_produktionsanlagen"),
    is.null(mapping) || is.data.frame(mapping),
    all(
      c("standort_str_name", "standort_str_nummer", "bfsnr_new") %in%
        DBI::dbListFields(con, "strom_produktionsanlagen")
    )
  )

  if (is.null(mapping)) {
    mapping <- tibble::tribble(
      ~standort_str_name, ~standort_str_nummer, ~bfsnr_new,
      "Reusseggstrasse", "7", bfsnr_to,
      "Gasshofstrasse", "2", bfsnr_to,
      "Ritterstrasse", "57", bfsnr_to,
      "Horwerstrasse", "91", bfsnr_to,
      "Grossmatte", "19", bfsnr_to,
      "Obermatt", "2a", bfsnr_to,
      "Reusseggstrasse", "2", bfsnr_to,
      "Udelbodenstrasse", "28", bfsnr_to,
      "Thorenbergstrasse", NA_character_, bfsnr_to
    )
  }

  if (!is.null(mapping)) {
    stopifnot(
      setequal(
        c("standort_str_name", "standort_str_nummer", "bfsnr_new"),
        colnames(mapping)
      )
    )
  }

  build_query <- function(number, street, new_bfsnr) {
    if (is.na(number)) {
      q <- paste0(
        "UPDATE strom_produktionsanlagen ",
        "SET bfsnr = ", new_bfsnr, " ",
        "WHERE standort_str_name = '", street, "' ",
        "AND (standort_str_nummer IS NULL OR standort_str_nummer = '')"
      )
    } else {
      q <- paste0(
        "UPDATE strom_produktionsanlagen ",
        "SET bfsnr = ", new_bfsnr, " ",
        "WHERE standort_str_name = '", street, "' ",
        "AND standort_str_nummer = '", number, "'"
      )
    }
    q
  }

  apply_update <- function(con, query) {

    DBI::dbBegin(con)

    rows <- try(DBI::dbExecute(con, query, immediate = TRUE), silent = TRUE)

    if (inherits(rows, "try-error")) {
      DBI::dbRollback(con)
      warning("Transaction rolled back!")
      return(invisible(con))
    }

    DBI::dbCommit(con)
  }

  queries <- purrr::pmap(
    mapping$standort_str_name,
    mapping$standort_str_nummer,
    mapping$bfsnr_new,
    function(x, y, z) build_query(x, y, z)
  )

  purrr::walk(queries, function(con, query) apply_update(con, query))

  invisible(con)

}
