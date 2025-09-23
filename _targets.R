# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline


# Preambles ----
# Load packages required to define the pipeline:
library(targets)
library(tarchetypes) 
library(qs2)
library(crew)
library(here)
library(tidyverse)
library(labelled)

# Set target options:
tar_option_set(
  packages = c("tidyverse", "haven", "readxl", "janitor", "Hmisc", "mice", "scales", "ggpubr", "quarto"), # Packages that your targets need for their tasks.
  format = "qs", # Optionally set the default storage format. qs is fast.
  garbage_collection = 1,
  controller = crew_controller_local(workers = 4)
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source("R/etl_functions.R")
tar_source("R/mod_functions.R")

# Set Datas Source Directory
csv_datalake <- "/Volumes/fipg/2. ANZHFR projects/10. Jason/anzhfr_mortality_report/Data/csv_datalake/"

## Analysis Parameters ----
### Universal logistic model formula ----
form <- ~ age_cat5 + sex_2l + asa_nhfd + walk_nhfd + ftype_nhfd + uresidence_nhfd 
form_mort30d <- update(form, mort30d ~ .)
form_mort120d <- update(form, mort120d ~ .)
form_mort365d <- update(form, mort365d ~ .)

### Set reportable years ----
reportable_years <- 2019:2025

# Analysis pipeline ---- 
tar_plan(
  ## Data ETL ----
  tar_target(latest_data, paste0(csv_datalake, list.files(csv_datalake, pattern = ".csv") %>% keep(str_detect(., max(str_sub(., 1, 6)))))), 
  tar_target(hoscodefile, here::here("data/hoscodes.csv"), format = "file"),
  tar_target(hoscode_data, readr::read_csv(hoscodefile, show_col_types = FALSE)),
  tar_target(raw_data, get_anzhfr_data(latest_data) %>% anzhfr_var_labels() %>% anzhfr_value_labels()),
  tar_target(tidy_data, 
             raw_data %>% 
               mutate(country = ifelse(str_detect(ds, "_NZ_"), "nz", "au")) %>%
               mutate(id = paste0(country, id)) %>%
               deduplicate_data() %>%
               clean_datetime() %>% 
               clean_data() %>%  
               transform_data() %>%
               left_join(hoscode_data %>% select(ahoscode, h_name), by = join_by(ahos_code == ahoscode))),
  tar_target(patient_journey, pt_journey(tidy_data)),
  tar_target(tedis_info, get_tedis(patient_journey)),
  
  tar_target(tidy_data_tedis, get_mortality(tidy_data, tedis_info)), 
  

  
  
  ## Multiple imputations ----
  # Customise the analysis data target below if needed
  tar_target(analysis_data, tidy_data_tedis %>% 
               filter(!str_detect(ds, "NoMatch")) %>% 
               filter(surg_yn == "Surgical") %>% 
               filter(report_year %in% 2016:(max(reportable_years)-1))), 
  
  tar_target(mi_mids, mice_nhfd_pmm(labelled::remove_labels(analysis_data, keep_var_label = TRUE))), 
  tar_target(mi_mod_data, 
             complete(mi_mids, "all") %>% 
               map(~.x %>% select(-age, -starts_with("mort"))) %>% 
               map(~.x %>% left_join(analysis_data %>% select(-names(.x)[names(.x) != "id"]), by = "id")) %>% 
               map(~.x %>% select_data())), 
  tar_target(hosp_to_report, 
             get_report_hosp(mi_mod_data %>% pluck(1), years = reportable_years)), # already checked removing data won't result in the number of reported hospitals over the year.
  
  ### Rolling year model ----
  tar_map(values = tidyr::expand_grid(reportable_year = reportable_years, 
                                      reportable_country = c("au", "nz")),
          tar_target(reportable_hosp,  
                     purrr::pluck(hosp_to_report, as.character(reportable_year)) %>% 
                       filter(reportable == TRUE & country == reportable_country)), 
          tar_target(mi_rolldata, 
                     mi_mod_data %>% 
                       map(~right_join(.x, reportable_hosp, by = c("country", "h_name", "ahos_code", "report_year")))), 
          
          tar_map(values = tibble::tibble(name = c("mort30d", "mort365d"), 
                                          form = c(form_mort30d, form_mort365d)), 
                  names = name,
                  tar_target(mi_rollmod, 
                             mi_rolldata %>% 
                               map(~glm(form, data = .x, family = "binomial"))), 
                  tar_target(mi_rollpreds, 
                             pool.mice.scalar(mi_rollmod)), 
                  tar_target(mi_rollamr, 
                             summort_by_group(mi_rollpreds, "h_name", all.vars(form)[1])), 
                  tar_target(mi_funnel, 
                             fun_funnel_hosp(mi_rollamr, 
                                             paste0("Funnel plot of standarised mortality by ", toupper(reportable_country), " hospitals (", reportable_year - 3, " - ", reportable_year - 1, ")"))),
                  tar_target(mi_smort_ctpl, 
                             fun_smort_ctpl_hosp(mi_rollamr, 
                                                 paste0("Catepillar plot of standarised mortality by ", toupper(reportable_country), " hospitals (", reportable_year - 3, " - ", reportable_year - 1, ")"))), 
                  tar_target(mi_smr_ctpl, 
                             fun_smr_ctpl_hosp(mi_rollamr, 
                                               paste0("Catepillar plot of SMR by ", toupper(reportable_country), " hospitals (", reportable_year - 3, " - ", reportable_year - 1, ")")))
                  
          )
  ), 
  
  ### Longitudinal model ----
  tar_map(
    values = tidyr::expand_grid(reportable_country = c("au", "nz")),
    tar_target(mi_fulldata,
               mi_mod_data %>%
                 map(~.x %>% filter(country == reportable_country))),
    
    
    tar_target(mi_fullmod_mort30d,
               mi_fulldata %>% map(~glm(form_mort30d, data = .x, family = "binomial"))),
    tar_target(mi_fullpreds_mort30d,
               pool.mice.scalar(mi_fullmod_mort30d)),
    tar_target(mi_fullamr_yearly_mort30d,
               summort_by_group(mi_fullpreds_mort30d, "report_year", all.vars(form_mort30d)[1])),
    tar_target(mi_fullamr_area_yearly_mort30d,
               summort_by_group(mi_fullpreds_mort30d, c("report_year", "area"), all.vars(form_mort30d)[1])),
    
    tar_target(mi_fullmod_mort365d,
               mi_fulldata %>% map(~glm(form_mort365d, data = .x %>% filter(report_year < max(reportable_years) - 1), family = "binomial"))),
    tar_target(mi_fullpreds_mort365d,
               pool.mice.scalar(mi_fullmod_mort365d)),
    tar_target(mi_fullamr_yearly_mort365d,
               summort_by_group(mi_fullpreds_mort365d, "report_year", all.vars(form_mort365d)[1])),
    tar_target(mi_fullamr_area_yearly_mort365d,
               summort_by_group(mi_fullpreds_mort365d, c("report_year", "area"), all.vars(form_mort365d)[1]))
  ),
  
  tar_target(mi_trend_combo30d, 
             fun_annual_trend(mi_fullamr_yearly_mort30d_au, mi_fullamr_area_yearly_mort30d_au, mi_fullamr_yearly_mort30d_nz, y_lab = "Standardised mortality rate within 30 days")),
  tar_target(mi_trend_combo365d, 
             fun_annual_trend(mi_fullamr_yearly_mort365d_au, 
                              mi_fullamr_area_yearly_mort365d_au, 
                              mi_fullamr_yearly_mort365d_nz, 
                              y_lab = "Standardised mortality rate within 365 days")),
  
  ## Reporting ----
  # ### Hospital code report 
  # tar_quarto(hoscode_report, path = here::here("R/hoscode_report.qmd"), quiet = FALSE),
  
  ### Mortality report 
  tar_quarto(mortality_report, path = here::here("R/mortality_report.qmd"),
             quiet = FALSE)
)



