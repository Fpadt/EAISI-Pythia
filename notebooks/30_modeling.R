fdiff_mnths <- function(x, y) {
  strdate <- ymd(paste0(x, "01"))
  enddate <- ymd(paste0(y, "01"))
  lubridate::interval(strdate, enddate) %/% months(1)
}