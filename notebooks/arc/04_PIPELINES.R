
PA <- 
  fread(file = file.path(PS01, SYS, "B4", "B4_PIPELINE.csv")) %>%
  .[, SRC:= "P"] %>% .[, PYTHIA:=NULL]

PO <- PIPELINES %>% .[, SRC:= "O"] %>% .[, PYTHIA:=NULL]

PL <- 
  rbind(
    PO,
    PA
  ) %>%
  dcast.data.table(
    ... ~ SRC,
    fun.aggregate = length,
    value.var = "SRC"
  ) %>%
  .[, TOT:= O + P] %>%
  .[TOT < 2 & P ==1] %T>%
  setorderv(c("OHDEST", "POSIT", "O", "P")) %>%
  .[, .(PYTHIA = 'C', OHDEST, POSIT, DATATYPE, FLDNM_IN, FIELDTP, TRNSFRM, FLDNM_OUT)]

View(PL)

View(
  rbind(PL, PIPELINES) %T>%
    setorder(PYTHIA, OHDEST, POSIT) %>%
    .[, .SD[1], by = .(OHDEST, POSIT)]
)


fwrite(
  x    = PL,
  file = file.path(PS01, SYS, "B4", "B4_PIPELINE_MOD.csv")
)
