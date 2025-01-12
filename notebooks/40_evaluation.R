# fdiff_mnths <- function(x, y) {
#   strdate <- ymd(paste0(x, "01"))
#   enddate <- ymd(paste0(y, "01"))
#   lubridate::interval(strdate, enddate) %/% months(1)
# }

#' Compute Differences Between ACT and FCT Values
#'
#' This function takes a data.table (\code{tab}) containing columns
#' \code{VTYPE}, \code{SALESORG}, \code{PLANT}, \code{MATERIAL},
#' \code{CALMONTH}, \code{FTYPE}, \code{STEP}, and \code{Q}.
#'
#' It splits the data into ACT (where \code{VTYPE = "010"}) and FCT (where \code{VTYPE = "060"}),
#' merges them, replaces missing values with zero, and computes several derived
#' columns, including errors and absolute percentage errors.
#'
#' @param tab A data.table containing at least the columns:
#'   \itemize{
#'     \item \code{VTYPE} (character)
#'     \item \code{SALESORG}, \code{PLANT}, \code{MATERIAL} (character or factor)
#'     \item \code{CALMONTH} (character or factor representing YYYYMM)
#'     \item \code{FTYPE} (numeric or factor)
#'     \item \code{STEP} (numeric) -- used only for the FCT subset
#'     \item \code{Q} (numeric) -- quantity
#'   }
#'
#' @return A data.table with columns:
#' \itemize{
#'   \item \code{SALESORG, PLANT, MATERIAL, CALMONTH, FTYPE, STEP}
#'   \item \code{ACT, FCT, E, E2, AE, APE, EPE}
#' }
#'
#' @details
#' \describe{
#'   \item{ACT subset}{\code{VTYPE = "010"} \eqn{\rightarrow} \code{ACT = Q}}
#'   \item{FCT subset}{\code{VTYPE = "060"} \eqn{\rightarrow} \code{FCT = Q}}
#'   \item{\code{merge()}}{A full outer join on \code{SALESORG, PLANT, MATERIAL, CALMONTH, FTYPE}}
#'   \item{NAs to 0}{Any missing \code{ACT} or \code{FCT} becomes 0}
#'   \item{Derived columns}{
#'     \code{E  = ACT - FCT} \\
#'     \code{E2 = E^2} \\
#'     \code{AE = abs(E)} \\
#'     \code{APE = 100 * (1 - AE / ACT)} \\
#'     \code{EPE = 100 * (1 - AE / FCT)}
#'   }
#' }
#'
#' @import data.table
#' @keywords internal
.calc_accuracy <- function(
  tab, act_ftype = 1, fct_ftype = 2) {
  
  # Ensure `tab` is a data.table
  stopifnot(data.table::is.data.table(tab))
  
  # --- 1) Subset for ACT (where VTYPE = "010") ---
  ACT <- tab[
    VTYPE == "010" & FTYPE == act_ftype,
    .(SALESORG, PLANT, MATERIAL, CALMONTH, FTYPE, ACT = Q)
  ]
  
  # --- 2) Subset for FCT (where VTYPE = "060") ---
  FCT <- tab[
    VTYPE == "060" & FTYPE == fct_ftype,
    .(SALESORG, PLANT, MATERIAL, CALMONTH, FTYPE, STEP, FCT = Q)
  ]
  
  # --- 3) Merge both (full outer join) ---
  out <- merge(
    x   = ACT,
    y   = FCT,
    by  = c("SALESORG", "PLANT", "MATERIAL", "CALMONTH"), # , "FTYPE"
    all = TRUE  # full outer join
  )
  
  # --- 4) Replace NA with 0 for ACT & FCT ---
  out[, `:=`(
    ACT = data.table::fifelse(is.na(ACT), 0, ACT),
    FCT = data.table::fifelse(is.na(FCT), 0, FCT)
  )]
  
  # --- 5) Compute derived columns (E, E2, AE, APE, EPE) ---
  #     We do this inside a j-expression so we can return them as new columns.
  out <- out[, {
    E   = ACT - FCT
    E2  = E ^ 2
    AE  = abs(E)
    APE = 100 * (AE / ifelse(ACT == 0, NA, ACT)) # Avoid division by zero
    APA = 100 - APE
    EPE = 100 * (AE / ifelse(FCT == 0, NA, FCT))
    EPA = 100 - EPE
    .(SALESORG, PLANT, MATERIAL, CALMONTH, STEP, # FTYPE,
      ACT, FCT, E, E2, AE, APE, APA, EPE, EPA)
  }]
  
  
  # MAE  = mean(abs(ACT - FCT), na.rm = TRUE),
  # RMSE = sqrt(mean((ACT - FCT)^2, na.rm = TRUE))
  # MAPE = mean(abs(ACT - FCT) / ifelse(ACT == 0, NA, ACT), na.rm = TRUE) * 100
  
  # Return the final data.table
  out
}
