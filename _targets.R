##
## Title: ANZHFR Mortality Analysis Pipeline
## Author: Dr Xingzhong (Jason) Jin
## Purpose: To establish a SOP for analyzing ANZFHR data for mortality reporting
## Last updated: 3 Dec 2025
##

# 1. Load Packages & Options ---------------------------------------------------
library(targets)
library(tarchetypes)
library(qs2)
library(crew)
library(here)
library(tidyverse)
library(labelled)
library(quarto)

# Set target options:
# strict=TRUE ensures global variables are tracked correctly
tar_option_set(
  packages = c(
    "tidyverse",
    "haven",
    "readxl",
    "janitor",
    "labelled",
    "Hmisc",
    "mice",
    "scales",
    "ggpubr",
    "quarto",
    "kableExtra"
  ),
  format = "qs",
  garbage_collection = 1,
  controller = crew_controller_local(
    workers = parallel::detectCores(),
    crashes_max = 10
  )
)

# 2. Source Custom Functions ---------------------------------------------------
tar_source("R/etl_functions.R")
tar_source("R/mod_functions.R")
tar_source("R/tedis_review.R")

# 3. Define Global Parameters --------------------------------------------------
# Define Data Lake Paths
csv_datalake <- "/Volumes/fipg/2. ANZHFR projects/10. Jason/anzhfr_mortality_report/Data/csv_datalake/"

# Report Settings
reportable_years <- 2018:2025

# Model Formulas
base_form <- ~ age_cat5 +
  sex_2l +
  asa_nhfd +
  walk_nhfd +
  ftype_nhfd +
  uresidence_nhfd
form_mort30d <- update(base_form, mort30d ~ .)
form_mort120d <- update(base_form, mort120d ~ .)
form_mort365d <- update(base_form, mort365d ~ .)

# 4. Define Target Lists -------------------------------------------------------

## Data ETL Targets ------------------------------------------------------------
list_etl <- tar_plan(
  # Detect file changes
  tar_target(
    name = datalake_path,
    command = csv_datalake,
    format = "file"
  ),

  # Identify latest file
  tar_target(
    latest_data,
    tibble(
      files = list.files(datalake_path, pattern = "\\.csv$", full.names = T)
    ) |>
      mutate(filename = basename(files)) |>
      mutate(date_str = str_extract(filename, "^\\d{6}")) |>
      mutate(file_date = ymd(date_str)) |>
      filter(file_date == max(file_date, na.rm = T)) |>
      pull(files)
  ),

  # Load Hospital Codes
  tar_target(hoscodefile, here::here("data/hoscodes.csv"), format = "file"),
  tar_target(
    hoscode_data,
    readr::read_csv(hoscodefile, show_col_types = FALSE)
  ),

  # Load & Clean Clinical Data
  tar_target(
    raw_data,
    get_anzhfr_data(latest_data) |>
      anzhfr_var_labels() |>
      anzhfr_value_labels()
  ),

  tar_target(
    tidy_data,
    raw_data |>
      deduplicate() |>
      tedis_review() |>
      clean_datetime() |>
      clean_data()
  ),

  # Patient Journey & Linkage
  tar_target(patient_journey, pt_journey(tidy_data)),
  tar_target(tedis_info, get_tedis(patient_journey)),
  tar_target(
    tidy_data_tedis,
    get_mortality(tidy_data, tedis_info)
  )
)

## Imputation Targets ----------------------------------------------------------
list_imputation <- tar_plan(
  tar_target(
    analysis_data,
    tidy_data_tedis |>
      transform_data() |>
      left_join(
        hoscode_data,
        by = join_by(ahoscode == ahoscode)
      ) |>
      filter(surg_yn == "Surgical") |>
      filter(report_year %in% 2016:(max(reportable_years))) |>
      filter(quality_flag == "L") |> # Include only hospitals with linked data
      filter(!is.na(mort30d)) # Exclude patients with missing 30-day mortality for modeling
  ),
  tar_target(
    mi_mids,
    mice_nhfd_pmm(labelled::remove_labels(analysis_data, keep_var_label = TRUE))
  ),
  tar_target(
    mi_mod_data,
    process_imputed_data(
      mids_object = mi_mids,
      reference_data = analysis_data
    )
  ),
  tar_target(
    hosp_to_report,
    get_report_hosp(mi_mod_data |> pluck(1), years = reportable_years)
  )
)

## Modeling: Rolling Year ------------------------------------------------------
list_rolling_model <- tar_map(
  values = tidyr::expand_grid(
    reportable_year = reportable_years,
    reportable_country = c("au", "nz")
  ),
  tar_target(
    reportable_hosp,
    purrr::pluck(hosp_to_report, as.character(reportable_year)) |>
      filter(reportable == TRUE & country == reportable_country)
  ),
  tar_target(
    mi_rolldata,
    mi_mod_data |>
      map(
        ~ right_join(
          .x,
          reportable_hosp,
          by = c("country", "h_name", "ahoscode", "report_id", "report_year")
        )
      )
  ),

  # Inner map for mortality windows
  tar_map(
    values = tibble::tibble(
      name = c("mort30d", "mort365d"),
      form = c(form_mort30d, form_mort365d)
    ),
    names = name,
    tar_target(
      mi_rollmod,
      mi_rolldata |> map(~ glm(form, data = .x, family = "binomial"))
    ),
    tar_target(mi_rollpreds, pool.mice.scalar(mi_rollmod)),
    tar_target(
      mi_rollamr,
      summort_by_group(mi_rollpreds, "report_id", all.vars(form)[1])
    ),

    # Plots
    tar_target(
      mi_funnel,
      fun_funnel_hosp(
        mi_rollamr,
        paste0(
          "Funnel plot of standarised mortality by ",
          toupper(reportable_country),
          " hospitals (",
          reportable_year - 2,
          " - ",
          reportable_year,
          ")"
        )
      )
    ),
    tar_target(
      mi_smort_ctpl,
      fun_smort_ctpl_hosp(
        mi_rollamr,
        paste0(
          "Catepillar plot of standarised mortality by ",
          toupper(reportable_country),
          " hospitals (",
          reportable_year - 2,
          " - ",
          reportable_year,
          ")"
        )
      )
    ),
    tar_target(
      mi_smr_ctpl,
      fun_smr_ctpl_hosp(
        mi_rollamr,
        paste0(
          "Catepillar plot of SMR by ",
          toupper(reportable_country),
          " hospitals (",
          reportable_year - 2,
          " - ",
          reportable_year,
          ")"
        )
      )
    )
  )
)

## Modeling: Longitudinal ------------------------------------------------------
list_longitudinal_model <- tar_map(
  values = tidyr::expand_grid(reportable_country = c("au", "nz")),
  tar_target(
    mi_fulldata,
    mi_mod_data |> map(~ .x |> filter(country == reportable_country))
  ),

  # 30 Day Models
  tar_target(
    mi_fullmod_mort30d,
    mi_fulldata |> map(~ glm(form_mort30d, data = .x, family = "binomial"))
  ),
  tar_target(mi_fullpreds_mort30d, pool.mice.scalar(mi_fullmod_mort30d)),
  tar_target(
    mi_fullamr_yearly_mort30d,
    summort_by_group(
      mi_fullpreds_mort30d,
      "report_year",
      all.vars(form_mort30d)[1]
    )
  ),
  tar_target(
    mi_fullamr_area_yearly_mort30d,
    summort_by_group(
      mi_fullpreds_mort30d,
      c("report_year", "area"),
      all.vars(form_mort30d)[1]
    )
  ),

  # 365 Day Models
  tar_target(
    mi_fullmod_mort365d,
    mi_fulldata |>
      map(
        ~ glm(
          form_mort365d,
          data = .x |> filter(report_year < max(reportable_years)),
          family = "binomial"
        )
      )
  ),
  tar_target(mi_fullpreds_mort365d, pool.mice.scalar(mi_fullmod_mort365d)),
  tar_target(
    mi_fullamr_yearly_mort365d,
    summort_by_group(
      mi_fullpreds_mort365d,
      "report_year",
      all.vars(form_mort365d)[1]
    )
  ),
  tar_target(
    mi_fullamr_area_yearly_mort365d,
    summort_by_group(
      mi_fullpreds_mort365d,
      c("report_year", "area"),
      all.vars(form_mort365d)[1]
    )
  )
)

## Combined Trends & Reporting -------------------------------------------------
list_reporting <- tar_plan(
  # Trend Combinations
  tar_target(
    mi_trend_combo30d,
    fun_annual_trend(
      mi_fullamr_yearly_mort30d_au,
      mi_fullamr_area_yearly_mort30d_au,
      mi_fullamr_yearly_mort30d_nz,
      y_lab = "Standardised mortality rate within 30 days"
    )
  ),
  tar_target(
    mi_trend_combo365d,
    fun_annual_trend(
      mi_fullamr_yearly_mort365d_au,
      mi_fullamr_area_yearly_mort365d_au,
      mi_fullamr_yearly_mort365d_nz,
      y_lab = "Standardised mortality rate within 365 days"
    )
  ),

  # Quarto Reports
  ## Mortality Report
  tar_quarto(
    mortality_report,
    path = "R/mortality_report.qmd",
    quiet = FALSE
  ),

  ## Site-specific Reports
  # One tar_quarto() target per site. Rendering R/site_report.qmd writes
  # fixed-name intermediate files (e.g. site_report.knit.md) next to the
  # source, so if two sites render concurrently on different crew workers,
  # one worker's cleanup deletes/overwrites files the other is still reading,
  # producing intermittent "cannot open file 'site_report.qmd'" or "failed to
  # move" errors. deployment = "main" forces these targets to run one at a
  # time in the main process instead of being farmed out to parallel workers.
  purrr::map(
    # hosp_to_report is a target, not an object in this script's environment,
    # so its value must be pulled from the targets store (tar_read()) rather
    # than referenced directly. On a completely fresh store (before
    # hosp_to_report has ever been built), fall back to an empty vector of
    # site ids so the pipeline can still be constructed; re-run tar_make()
    # once hosp_to_report exists to generate the real per-site targets.
    bind_rows(tar_read(hosp_to_report)) |>
      filter(reportable == TRUE) |>
      filter(country == "au") |>
      distinct(report_id) |>
      pull(report_id),
    ~ tarchetypes::tar_quarto_raw(
      name = paste0("site_report_", .x),
      path = "R/site_report.qmd",
      quiet = FALSE,
      deployment = "main",
      # execute_params must be an unevaluated language object (per
      # tar_quarto_raw()'s assertions), so splice the site id in as a literal
      # rather than passing an evaluated list().
      execute_params = bquote(list(site_id = .(.x))),
      # Quarto's project mode (_quarto.yml has execute-dir: project) requires
      # output_file to be a bare filename, not a path; the directory is
      # governed by _quarto.yml's output-dir instead.
      output_file = paste0("site_report_", .x, ".html")
    )
  )
)

# 5. Output Pipeline -----------------------------------------------------------
list(
  list_etl,
  list_imputation,
  list_rolling_model,
  list_longitudinal_model,
  list_reporting
)
