##
## Title: ETL Functions for ANZHFR Mortality Data
## Author: Xingzhong (Jason) Jin
## Purpose: To collect functions for the ETL processes
## Last updated: 16 Apr 2025
##


# Data extraction ----

# Extract latest ANZHFR data from datalake
get_anzhfr_data <- function(datalake_dir) {
  require(dplyr)
  coltype <- list(
    start_date = col_datetime(format = "%d/%m/%Y %H:%M"),
    report_id = col_character(),
    id = col_character(),
    area = col_character(),
    age = col_double(),
    sex = col_double(),
    indig = col_double(),
    ethnic = col_double(),
    a_pcode = col_character(),
    ptype = col_double(),
    uresidence = col_double(),
    ahos_code = col_character(),
    e_dadmit = col_double(),
    athoscode = col_character(),
    tarrdatetime = col_datetime(format = "%d/%m/%Y %H:%M"),
    arrdatetime = col_datetime(format = "%d/%m/%Y %H:%M"),
    depdatetime = col_datetime(format = "%d/%m/%Y %H:%M"),
    admdatetimeop = col_datetime(format = "%d/%m/%Y %H:%M"),
    painassess = col_double(),
    painmanage = col_double(),
    ward = col_double(),
    tfanalges = col_double(),
    walk = col_double(),
    amts = col_double(),
    cogassess = col_double(),
    cogstat = col_double(),
    bonemed = col_double(),
    passess = col_double(),
    side = col_double(),
    afracture = col_double(),
    ftype = col_double(),
    surg = col_double(),
    asa = col_double(),
    frailty = col_double(),
    addelassess = col_double(),
    sdatetime =col_datetime(format = "%d/%m/%Y %H:%M"),
    delay = col_double(),
    anaesth = col_double(),
    analges = col_double(),
    consult = col_double(),
    optype = col_double(),
    inter_op_fracture = col_double(),
    wbear = col_double(),
    mobil = col_double(),
    pulcers = col_double(),
    gerimed = col_double(),
    gdate = col_date(format = "%d/%m/%Y"),
    fassess = col_double(),
    dbonemed1 = col_double(),
    delassess = col_double(),
    malnutrition = col_double(),
    mobil2 = col_double(),
    ons = col_double(),
    wdisch = col_date(format = "%d/%m/%Y"),
    wdest = col_double(),
    hdisch = col_date(format = "%d/%m/%Y"),
    olos = col_double(),
    tlos = col_double(),
    dresidence = col_double(),
    fdate1 = col_date(format = "%d/%m/%Y"),
    date30 = col_date(format = "%d/%m/%Y"),
    fsurvive1 = col_double(),
    fresidence1 = col_double(),
    weight_bear30 = col_double(),
    fwalk1 = col_double(),
    fbonemed1 = col_double(),
    fop1 = col_double(),
    fdate2 = col_date(format = "%d/%m/%Y"),
    date120 = col_date(format = "%d/%m/%Y"),
    fsurvive2 = col_double(),
    fresidence2 = col_double(),
    weight_bear120 = col_double(),
    fwalk2 = col_double(),
    fbonemed2 = col_double(),
    fop2 = col_double(),
    predod = col_date(format = "%d/%m/%Y"),
    findod = col_date(format = "%d/%m/%Y")
  )
  
  # list all files csv files
  csv_files <- list.files(csv_datalake, pattern = ".csv")
  # get the latest csv files
  latest_files <- max(substr(list.files(csv_datalake), 0, 6)) 
  import_files <- csv_files[str_detect(csv_files, latest_files)]
  
  # import into memory 
  dat_mort <- lapply(paste0(csv_datalake, import_files), readr::read_csv, col_type = coltype, show_col_types = FALSE, 
                na = "NULL", name_repair = janitor::make_clean_names) %>% 
    set_names(import_files) %>% 
    bind_rows(.id = "ds") 
  
  return(dat_mort)
}

# Create a patient journey dataset (ie. long-form of all datetime variables)
pt_journey <- function(data) {
  journey_data <- data %>% 
    select(id, where(is.POSIXct), where(is.Date), e_dadmit, h_name, -predod) %>% 
    pivot_longer(cols = where(is.POSIXct)|where(is.Date), names_to = "event", values_to = "datetime") %>% 
    mutate(datetime = na_if(datetime, as.POSIXlt("1900-01-01 00:00:00", tz = "UTC"))) %>% 
    mutate(event = factor(event, 
                          levels = c(
                            "start_date",      
                            "tarrdatetime",  
                            "arrdatetime",   
                            "depdatetime",   
                            "admdatetimeop",
                            "sdatetime",       
                            "gdate",               
                            "wdisch",              
                            "hdisch",             
                            "fdate1",              
                            "fdate2",              
                            "date30",              
                            "date120",           
                            "findod"              
                          ), 
                          labels = c(
                            "start_date", 
                            "arrive_transfer_hospital",
                            "arrive_operating_hospital", 
                            "depart_from_ED", 
                            "in_patient_fracture", 
                            "surgery", 
                            "geriatric_assessment", 
                            "discharge_from_ward", 
                            "discharge_from_hospital", 
                            "follow_up1", 
                            "follow_up2",
                            "discharge_from_system", 
                            "discharge_from_system", 
                            "death"
                          ))) %>% 
    filter(!is.na(datetime)) %>% 
    arrange(id, datetime, event) 
  
  return(journey_data)
}

# Clean up patient journey's datetime typo
clean_datetime <- function(data) {
  tmpdat <- data %>%  
    filter(event %in% c("start_date",      
                        "arrive_transfer_hospital",
                        "arrive_operating_hospital", 
                        "depart_from_ED", 
                        "in_patient_fracture", 
                        "surgery", 
                        "geriatric_assessment", 
                        "discharge_from_ward")) %>% 
    group_by(id) %>% 
    mutate(median_datetime = median(datetime)) %>% # get the mid-point of journey
    ungroup() %>% 
    mutate(normal_datetime_range = interval(median_datetime %m-% months(3), median_datetime %m+% months(3))) %>% # allow 6 month journey
    mutate(out_of_range = !(datetime %within% normal_datetime_range)) 
  
  # change typo in year entry
  # only change when the year differs from mid-point journey and the new date falls within the journey after the change
  editdat <- tmpdat %>% 
    mutate(year_from_median = year(datetime) - year(median_datetime)) %>% 
    mutate(same_year = year_from_median == 0) %>% 
    mutate(datetime_proposed = as.POSIXct(ifelse(out_of_range == TRUE & same_year == FALSE, update(datetime, year = year(median_datetime)), datetime))) %>% 
    mutate(proposed_in_range = (datetime_proposed %within% normal_datetime_range)) %>% 
    mutate(datetime_edit = as.POSIXct(case_when(abs(year_from_median) > 1 & proposed_in_range == FALSE ~ NA,  # invalidate datetime over 3 years from median and not likely to be typo. 
                                                proposed_in_range == TRUE ~ datetime_proposed, 
                                                .default = datetime))) %>% 
    mutate(change = (datetime != datetime_edit) | is.na(datetime_edit))
  
  newdata <-
    editdat %>% 
    mutate(datetime = datetime_edit) %>% 
    filter(!is.na(datetime)) %>% 
    select(colnames(data))
  
  return(newdata)
}

# create a admission pattern variable for deciding date of hip fracture diagnosis 

get_tedis <- function(data) {
  
  tmp_dat <- data %>% 
    clean_datetime() %>% 
    filter(event %in% c("start_date", 
                        "arrive_transfer_hospital",
                        "arrive_operating_hospital", 
                        "depart_from_ED", 
                        "in_patient_fracture", 
                        "surgery")) %>% 
    mutate(event = factor(event, 
                          levels = c(
                            "start_date",
                            "arrive_transfer_hospital",
                            "arrive_operating_hospital", 
                            "depart_from_ED", 
                            "in_patient_fracture", 
                            "surgery"
                          ), 
                          labels = c(
                            "start_date",
                            "T",
                            "E", 
                            "D", 
                            "I", 
                            "S" 
                          ))) %>% 
    arrange(id, date(datetime), event) %>% # as we only concern about dates in mortality calculation 
    group_by(id) %>% 
    mutate(report_year = ifelse(event == "start_date", year(datetime), NA)) %>% 
    fill(report_year, .direction = "downup") %>% 
    filter(!(n() > 1 & event == "start_date")) %>% #keep start_date only when it's the only record
    mutate(event_index = row_number() - 1) %>% 
    mutate(lag_datetime = lag(datetime)) %>% 
    ungroup() %>% 
    mutate(duration = as.duration(interval(lag_datetime, datetime))) 
  
  event_wide <- 
    tmp_dat %>% 
    select(id, report_year, event_index, event) %>% 
    pivot_wider(id_cols = c(id, report_year), 
                names_from = event_index, names_prefix = "event",
                values_from = event) %>% 
    unite(admittype, contains("event"), sep = "", na.rm = TRUE)
  
  duration_wide <- tmp_dat %>% 
    select(id, event_index, duration) %>% 
    pivot_wider(id_cols = id, 
                names_from = event_index, names_prefix = "duration",
                values_from = duration) %>% 
    select(-duration0)
  
  admit_dat <- left_join(event_wide, duration_wide, by = "id") %>% 
    # check TEDIS position in event sequence 
    mutate(loc_t = str_locate(admittype, "T")[, 1]) %>% 
    mutate(loc_e = str_locate(admittype, "E")[, 1]) %>% 
    mutate(loc_d = str_locate(admittype, "D")[, 1]) %>% 
    mutate(loc_i = str_locate(admittype, "I")[, 1]) %>% 
    mutate(loc_s = str_locate(admittype, "S")[, 1]) %>% 
    # check if I (in-patient fall) is next to D (ED discharge) -> patient fell in ED (in-ED fracture)
    mutate(i_next_to_d = abs(loc_i - loc_d) == 1) %>% 
    mutate(loc_i = ifelse(i_next_to_d & !is.na(i_next_to_d), loc_d, loc_i)) %>% 
    # check if pattern falls in the TEDIS order
    unite("tedis_order", contains("loc"), sep = "", na.rm = TRUE, remove = FALSE) %>% 
    mutate(tedis_in_order = sapply(str_split(tedis_order, pattern = ""), 
                                   function(x) all(diff(as.numeric(x)) >=0))) %>%  
    mutate(dx_event = case_when(str_detect(admittype, "I") ~ "I", 
                                str_detect(admittype, "T") ~ "T", 
                                str_detect(admittype, "E") ~ "E", 
                                str_detect(admittype, "D") ~ "D", 
                                .default =  NA)) %>% 
    mutate(valid_tedis = ifelse(tedis_in_order == TRUE & admittype != "start_date", TRUE, FALSE)) %>% 
    # get corresponding hip fracture diagnosis event as the diagnosis date
    left_join(tmp_dat %>% select(id, event, datetime), by = c("id" = "id", "dx_event" = "event")) %>% 
    mutate(dx_date = date(datetime))
  
  return(admit_dat)
}


# create time to death based on the date of hip fracture diagnosis 

get_mortality <- function(clean_data, tedis_info) {
  clean_data_with_mort <- left_join(clean_data, tedis_info %>% select(id, admittype, valid_tedis, dx_date), by = "id") %>% 
    mutate(days_to_death = as.numeric(difftime(findod, dx_date, units = "days"))) %>% 
    mutate(mort30d = factor(case_when(
      days_to_death < 0 ~ NA, 
      days_to_death >=0 & days_to_death <= 30 ~ 1, 
      days_to_death > 30 ~ 0,
      .default = 0
    ), levels = c(0, 1), labels = c("Alive", "Deceased"))) %>% 
    mutate(mort120d = factor(case_when(
      days_to_death < 0 ~ NA, 
      days_to_death >=0 & days_to_death <= 120 ~ 1, 
      days_to_death > 120 ~ 0,
      .default = 0
    ), levels = c(0, 1), labels = c("Alive", "Deceased"))) %>% 
    mutate(mort365d = factor(case_when(
      days_to_death < 0 ~ NA, 
      days_to_death >=0 & days_to_death <= 365 ~ 1, 
      days_to_death > 365 ~ 0,
      .default = 0
    ), levels = c(0, 1), labels = c("Alive", "Deceased")))
  
  return(clean_data_with_mort)
}




# Match hospital codes with hospital names
label_hoscode <- function(data, hoscode_dat) {
  labled_data <- dplyr::left_join(data, hoscode_dat, by = c("ahos_code" == "ahoscode"))
  return(labled_data)
}


# Data cleaning ----



# deduplicate records
deduplicate_data <- function(data) {
  data$n_miss <- naniar::n_miss_row(data)
  
  # all duplicate ids (incl. different sides)
  dup_ids <- data %>% 
    group_by(id) %>% 
    tally() %>% 
    filter(n > 1) %>% 
    pull(id)
  
  # duplicate ids (on same side)
  dup_ids_ss <- data %>% 
    group_by(id, side) %>% 
    tally() %>% 
    filter(n > 1) %>% 
    pull(id)
  
  dat_dd <- data %>% 
    # correct NZ hospital codes to make them unique identifiers. 
    mutate(ahos_code = ifelse(country == "nz", stringr::str_replace(ahos_code, "AU", "NZ"), ahos_code)) %>% 
    # for duplicate ids that record different sides, add "sequence number" at the end of id 
    group_by(id) %>% 
    arrange(start_date) %>% 
    mutate(id = ifelse((id %in% dup_ids) & !(id %in% dup_ids_ss), paste0(id, "_", row_number()), id)) %>% 
    ungroup() %>% 
    # for duplicate ids that record same side, keep the record with least missing values. 
    group_by(id) %>% 
    filter(n_miss == min(n_miss, na.rm = TRUE)) %>% 
    ungroup()  
  
  return(dat_dd)
}

# clean up data errors and missing values
clean_data <- function(data){
  
  dat_dd <- data %>% 
    mutate(country = ifelse(str_detect(ds, "NZ"), "nz", "au")) %>% 
    mutate(id = paste0(country, id)) %>% 
    deduplicate_data()
  
  dat_clean <- dat_dd %>% 
    mutate(report_year = year(start_date)) %>% 
    # # create date of death based on both AIHW-report and self-report dod
    # mutate(dod = case_when(
    #   !is.na(findod) ~ findod,
    #   !is.na(predod) ~ predod,
    #   .default = findod
    # )) %>%
    # # recode sex as numeric
    # mutate(sex = ifelse(str_detect(str_to_lower(sex), "f*male"), 2, sex)) %>% 
    # # confirm other numeric predictor variables
    # mutate(across(c(age, sex, asa, walk, ftype, uresidence, surg, afracture), ~ as.numeric(.x))) %>% 
    # confirm surgical indicator
    mutate(surg = ifelse((!is.na(sdatetime) | !is.na(optype)), 2, surg)) %>% # confirm surgical repair from other variables
    mutate(surg = ifelse(is.na(surg), 
                         ifelse(year(start_date) < 2021, 1, 5), surg)) %>% 
    
    # transform data for mortality report
    # replace unknown values as NA
    mutate(asa = na_if(asa, 0)) %>% 
    mutate(asa = na_if(asa, 9)) %>% 
    mutate(sex_2l = na_if(sex, 3)) %>% 
    mutate(sex = na_if(sex, 9)) %>% 
    mutate(walk = na_if(walk, 9)) %>% 
    mutate(ftype = na_if(ftype, 9)) %>% 
    mutate(uresidence = na_if(uresidence, 3)) %>% 
    mutate(uresidence = na_if(uresidence, 4)) %>% 
    
    mutate(age_cat5 = cut(age, c(seq(50, 100, 5), 110), right = FALSE, include.lowest = TRUE)) %>% # age groups of 5-year interval
    mutate(sex_2l = factor(sex, 
                           levels = c(1, 2), 
                           labels = c("Male", "Female"))) %>% 
    mutate(surg_yn = factor(surg == 2, 
                            levels = c(TRUE, FALSE), 
                            labels = c("Surgical", "Non-surgical"))) %>% 
    mutate(asa_nhfd = cut(asa, right = FALSE, 
                          breaks = c(1, 3, 4, 6), 
                          labels = c("1-2", "3", "4-5"))) %>% 
    mutate(uresidence_nhfd = cut(uresidence, right = FALSE, 
                                 breaks = c(1, 2, 3), 
                                 labels = c("Own home", "Residential care"))) %>% 
    mutate(walk_nhfd = cut(walk, right = FALSE, 
                           breaks = c(1, 2, 4, 5), 
                           labels = c("walked without aids", 
                                      "walked 1 aid 2 aids or frame", 
                                      "wheelchair bed bound"))) %>% 
    mutate(ftype_nhfd = cut(ftype, right = FALSE, 
                            breaks = c(1, 3, 5), 
                            labels = c("Introcapular", "Extracapsular"))) %>% 
    mutate(afracture = factor(afracture, levels = 0:2, 
                              labels = c("Not a pathological or atypical fracture", 
                                         "Pathological fracture", 
                                         "Atypical fracture")))
  return(dat_clean)
}


# label variables
lab_vars <- function(data) {
  
  labbed_data <- set_variable_labels(.data = data, 
                                     age = "Age", 
                                     sex_2l = "Sex (2 levels)", 
                                     asa_nhfd = "ASA group (NHFD definition)",
                                     walk_nhfd = "Walk score (NHFD definition)", 
                                     ftype_nhfd = "Fracture type (NHFD definition)", 
                                     afracture = "Atypical fracture type",
                                     uresidence_nhfd = "Residence type (NHFD definition)", 
                                     surg_yn = "Surgical repair",
                                     mort30d = "30-day mortality")
  return(labbed_data)
}

# function to exclude data due to linkage issues - This function is superseded by directly excluding dataset of NoMatch
exclude_linkage_issue <- function(data) {
  selected_data <- data %>% 
    filter(!(ahos_code == "AU10082" & report_year %in% 2017:2020)) %>% 
    filter(!(ahos_code == "AU10076" & report_year == 2017:2019))
  
  return(selected_data)
}


# function to select a cohort for outcome modelling

select_data <- function(data) {
  selected_data <- data %>% 
  # criteria 1 - age between 50-110
    filter(age >= 50 & age <= 110) %>% 
    
    # criteria 2 - remove overseas patients
    filter(a_pcode != 9998) %>% 
    # filter(!(ptype == 3 & (a_pcode == 0 | a_pcode == 8888 | a_pcode == 9999 | is.na(is.numeric(a_pcode))))) %>% 
    filter(!(ptype == 3 & (a_pcode == 0 | a_pcode == 8888 | a_pcode == 9999 ))) %>% # Need to revisit - retain records if postcode is missing.  
    
    # criteria 3 - date of death not before hip fracture diagnosis date
    filter(days_to_death >= 0 | is.na(days_to_death)) %>% 
    
    # criteria 4 - have a valid TEDIS pattern
    filter(valid_tedis == TRUE) %>% 
    
    # criteria 5 - had surgery for the hip fracture
    filter(surg_yn == "Surgical") 
  
  return(selected_data)
}


