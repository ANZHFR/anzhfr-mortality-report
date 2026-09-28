##
## Title: ETL Functions for ANZHFR Mortality Data
## Author: Dr Xingzhong (Jason) Jin
## Purpose: To collect functions for the ETL processes
## Last updated: 3 Dec 2025
##

# YAML-driven labelling --------------------------------------------------------
#' Reads the YAML file and extracts column metadata for a specified table.
#'
#' @param dict_path Path to the YAML data dictionary file.
#' @param table     Name of the table to extract (default `"anzhfr_data"`).
#'
#' @return A list of column metadata, each element containing:
#'   \describe{
#'     \item{name}{Column name exactly as stored in the YAML.}
#'     \item{label}{Variable label (falls back to the first sentence of `description`
#'                  when `label` is absent).}
#'     \item{values}{Named numeric/character vector of coded values, or `NULL`.}
#'   }
#' @keywords internal
parse_data_dict <- function(dict_path, table = "anzhfr_data") {
  if (!file.exists(dict_path)) {
    stop("Data dictionary file not found: ", dict_path, call. = FALSE)
  }

  raw <- yaml::read_yaml(dict_path)

  tables <- raw[["tables"]]
  if (is.null(tables)) {
    stop("No 'tables' key found in the data dictionary.", call. = FALSE)
  }

  table_names <- vapply(tables, `[[`, character(1), "name")
  idx <- which(table_names == table)
  if (length(idx) == 0) {
    stop(
      "Table '",
      table,
      "' not found in the data dictionary. ",
      "Available tables: ",
      paste(table_names, collapse = ", "),
      call. = FALSE
    )
  }

  columns <- tables[[idx]]$columns
  if (is.null(columns)) {
    stop("Table '", table, "' has no 'columns' defined.", call. = FALSE)
  }

  lapply(columns, function(col) {
    # Label: prefer explicit 'label', fall back to first sentence of 'description'
    lbl <- col[["label"]]
    if (is.null(lbl) || nchar(trimws(lbl)) == 0) {
      desc <- col[["description"]]
      if (!is.null(desc)) {
        lbl <- trimws(sub("\\..*$", "", desc))
      }
    }
    lbl <- if (!is.null(lbl)) as.character(lbl) else NA_character_

    # Values: build named vector, coerce keys to numeric where possible
    vals <- col[["values"]]
    if (!is.null(vals) && length(vals) > 0) {
      codes <- names(vals)
      labels <- unname(unlist(vals))
      num_codes <- suppressWarnings(as.numeric(codes))
      if (any(is.na(num_codes)) && !all(is.na(num_codes))) {
        warning(
          "Column '",
          col[["name"]],
          "' has mixed numeric/character value keys — ",
          "keeping as character.",
          call. = FALSE
        )
        final_codes <- codes
      } else if (all(!is.na(num_codes))) {
        final_codes <- num_codes
      } else {
        final_codes <- codes
      }
      vals_out <- setNames(final_codes, labels)
    } else {
      vals_out <- NULL
    }

    list(
      name = col[["name"]],
      label = lbl,
      values = vals_out
    )
  })
}


#' Match data frame column names to dictionary column names
#'
#' Attempts to map each column in a data frame to the closest matching
#' entry in the data dictionary. Matching proceeds in three passes:
#' (1) exact case-sensitive, (2) case-insensitive, and optionally
#' (3) fuzzy via edit distance.
#'
#' @param data_cols      Character vector of column names from the data frame.
#' @param dict_meta      Output of \code{\link{parse_data_dict}}.
#' @param use_fuzzy      Logical; enable fuzzy matching as a third pass.
#' @param fuzzy_threshold Minimum similarity score (0-1) for a fuzzy match.
#'
#' @return A named character vector of the form
#'   `c(data_col = dict_col, ...)` for every successfully matched column.
#'   Unmatched columns are omitted (with a warning per unmatched column).
#' @keywords internal
match_to_dict <- function(
  data_cols,
  dict_meta,
  use_fuzzy = FALSE,
  fuzzy_threshold = 0.85
) {
  dict_names <- vapply(dict_meta, `[[`, character(1), "name")

  result <- character(0)

  for (col in data_cols) {
    # Pass 1: exact case-sensitive
    m <- which(dict_names == col)

    # Pass 2: case-insensitive
    if (length(m) == 0) {
      m <- which(tolower(dict_names) == tolower(col))
    }

    # Pass 3: optional fuzzy
    if (length(m) == 0 && use_fuzzy) {
      dists <- adist(col, dict_names, ignore.case = TRUE)
      min_dist <- min(dists)
      lens <- pmax(nchar(col), nchar(dict_names))
      sim <- 1 - min_dist / lens
      best <- which(sim == max(sim) & sim >= fuzzy_threshold)
      if (length(best) > 0) {
        m <- best
      }
    }

    if (length(m) == 0) {
      # silent: unmatched columns are expected (e.g. derived/computed columns)
      next
    }

    if (length(m) > 1) {
      warning(
        "Column '",
        col,
        "' matches multiple dictionary entries: ",
        paste(dict_names[m], collapse = ", "),
        " — using the first match.",
        call. = FALSE
      )
      m <- m[1]
    }

    result[col] <- dict_names[m]
  }

  result
}


#' Apply variable and value labels from a YAML data dictionary
#'
#' Reads the ANZHFR YAML data dictionary and automatically attaches
#' variable labels and value labels to a data frame.
#' Missing-value code handling is intentionally out of scope.
#'
#' @param data            A data frame (typically imported from \code{.xlsx}).
#' @param dict_path       Path to the YAML data dictionary file.
#' @param table           Name of the table to use from the dictionary.
#' @param use_fuzzy_match Logical; enable fuzzy column-name matching.
#' @param fuzzy_threshold Minimum similarity score (0–1) for fuzzy matching.
#'
#' @return The input data frame with \pkg{labelled} variable and value labels
#'   attached, returned invisibly.
#'
#' @examples
#' \dontrun{
#' df <- readxl::read_xlsx("data/anzhfr.xlsx")
#' df_labelled <- apply_labels_from_yaml(df)
#' }
#'
#' @export
apply_labels_from_yaml <- function(
  data,
  dict_path = "documentation/anzhfr-data-dictionary.yaml",
  table = "anzhfr_data",
  use_fuzzy_match = FALSE,
  fuzzy_threshold = 0.85
) {
  dict_meta <- parse_data_dict(dict_path, table)
  col_map <- match_to_dict(
    names(data),
    dict_meta,
    use_fuzzy = use_fuzzy_match,
    fuzzy_threshold = fuzzy_threshold
  )

  if (length(col_map) == 0) {
    warning(
      "No data columns could be matched to the dictionary. ",
      "No labels applied.",
      call. = FALSE
    )
    return(invisible(data))
  }

  # Index dict_meta by dict name for fast lookup
  dict_by_name <- setNames(
    dict_meta,
    vapply(dict_meta, `[[`, character(1), "name")
  )

  # Apply labels column-by-column to avoid dataframe-level type issues.
  for (data_col in names(col_map)) {
    dict_col <- col_map[[data_col]]
    meta <- dict_by_name[[dict_col]]

    if (!is.na(meta$label)) {
      labelled::var_label(data[[data_col]]) <- meta$label
    }

    if (!is.null(meta$values)) {
      if (is.numeric(data[[data_col]]) || is.integer(data[[data_col]])) {
        labelled::val_labels(data[[data_col]]) <- meta$values
      } else {
        warning(
          "Value labels skipped for non-numeric column: ",
          data_col,
          call. = FALSE
        )
      }
    }
  }

  invisible(data)
}


# Data extraction --------------------------------------------------------------

# A helper functions to determine if the database columns are from the SQL backend or from the data dictionary.
classify_column_names_base <- function(
  df,
  sql_cols,
  dict_vars,
  threshold = 0.8
) {
  # Extract current column names
  current_names <- colnames(df)

  # Normalize reference lists to lowercase
  sql_lower <- tolower(sql_cols)
  dict_lower <- tolower(dict_vars)
  current_lower <- tolower(current_names)

  # Prepare a result data frame
  results <- data.frame(
    original_name = current_names,
    normalized_name = current_lower,
    source = NA_character_,
    match_score = NA_real_,
    matched_reference = NA_character_,
    stringsAsFactors = FALSE
  )

  for (i in seq_along(current_lower)) {
    name <- current_lower[i]
    best_source <- NA
    best_score <- -Inf
    best_ref <- NA

    # 1. Check Exact Matches (Score = 1.0)
    if (name %in% sql_lower) {
      best_source <- "SQL"
      best_score <- 1.0
      best_ref <- sql_cols[which(sql_lower == name)]
    } else if (name %in% dict_lower) {
      best_source <- "Dictionary"
      best_score <- 1.0
      best_ref <- dict_vars[which(dict_lower == name)]
    } else {
      # 2. Check Partial Matches using adist()
      # adist returns the Levenshtein distance (number of edits)

      # Calculate distances to all SQL names
      # ignore.case is set to FALSE because we already normalized to lowercase
      sql_dists <- adist(name, sql_lower)
      min_sql_dist <- min(sql_dists)
      closest_sql_idx <- which(sql_dists == min_sql_dist)

      # Calculate distances to all Dict names
      dict_dists <- adist(name, dict_lower)
      min_dict_dist <- min(dict_dists)
      closest_dict_idx <- which(dict_dists == min_dict_dist)

      # Convert distance to similarity score (0 to 1)
      # Formula: 1 - (distance / max_length)
      len_name <- nchar(name)
      len_sql <- nchar(sql_lower[closest_sql_idx])
      len_dict <- nchar(dict_lower[closest_dict_idx])

      max_len_sql <- max(len_name, len_sql)
      max_len_dict <- max(len_name, len_dict)

      # Avoid division by zero
      if (max_len_sql == 0) {
        sim_sql <- 1
      } else {
        sim_sql <- 1 - (min_sql_dist / max_len_sql)
      }
      if (max_len_dict == 0) {
        sim_dict <- 1
      } else {
        sim_dict <- 1 - (min_dict_dist / max_len_dict)
      }

      # Decide based on similarity threshold
      if (sim_sql >= threshold && sim_sql >= sim_dict) {
        best_source <- "SQL"
        best_score <- sim_sql
        best_ref <- sql_cols[closest_sql_idx]
      } else if (sim_dict >= threshold && sim_dict > sim_sql) {
        best_source <- "Dictionary"
        best_score <- sim_dict
        best_ref <- dict_vars[closest_dict_idx]
      }
    }

    # Fallback for no match found
    if (is.na(best_source)) {
      best_source <- "Unknown"
      best_score <- 0
      best_ref <- NA
    }

    # Store results
    results$source[i] <- best_source
    results$match_score[i] <- round(best_score, 3)
    results$matched_reference[i] <- best_ref
  }

  return(results)
}


#' Extract latest ANZHFR data from datalake
#'
#' @param latest_data A list of CSV files extracted from ANZHFR database
#' @return A R dataframe
get_anzhfr_data <- function(latest_data) {
  mortality_vars <- c(
    "id",
    "area",
    "ahoscode",
    "start_date",
    "age",
    "sex",
    "side",
    "tarrdatetime",
    "arrdatetime",
    "depdatetime",
    "admdatetimeop",
    "sdatetime",
    "gdate",
    "wdisch",
    "findod",
    "surg",
    "optype",
    "walk",
    "uresidence",
    "ftype",
    "asa",
    "afracture",
    "date120",
    "quality_flag"
  )

  # A list of variable types in the ANZHFR dataset for future reference
  config_coltype <- list(
    start_date = readr::col_datetime(format = "%d/%m/%Y %H:%M"),
    hosp_report_id = readr::col_character(),
    id = readr::col_character(),
    area = readr::col_character(),
    age = readr::col_double(),
    sex = readr::col_double(),
    indig = readr::col_double(),
    ethnic = readr::col_double(),
    a_pcode = readr::col_character(),
    medicare = readr::col_character(),
    ptype = readr::col_double(),
    uresidence = readr::col_double(),
    ahoscode = readr::col_character(),
    e_dadmit = readr::col_double(),
    athoscode = readr::col_character(),
    tarrdatetime = readr::col_datetime(format = "%d/%m/%Y %H:%M"),
    arrdatetime = readr::col_datetime(format = "%d/%m/%Y %H:%M"),
    depdatetime = readr::col_datetime(format = "%d/%m/%Y %H:%M"),
    admdatetimeop = readr::col_datetime(format = "%d/%m/%Y %H:%M"),
    painassess = readr::col_double(),
    painmanage = readr::col_double(),
    ward = readr::col_double(),
    tfanalges = readr::col_double(),
    walk = readr::col_double(),
    amts = readr::col_double(),
    cogassess = readr::col_double(),
    cogstat = readr::col_double(),
    bonemed = readr::col_double(),
    passess = readr::col_double(),
    side = readr::col_double(),
    afracture = readr::col_double(),
    ftype = readr::col_double(),
    surg = readr::col_double(),
    asa = readr::col_double(),
    frailty = readr::col_double(),
    addelassess = readr::col_double(),
    sdatetime = readr::col_datetime(format = "%d/%m/%Y %H:%M"),
    delay = readr::col_double(),
    delay_txt = readr::col_character(),
    anaesth = readr::col_double(),
    analges = readr::col_double(),
    consult = readr::col_double(),
    optype = readr::col_double(),
    inter_op_fracture = readr::col_double(),
    wbear = readr::col_double(),
    mobil = readr::col_double(),
    pulcers = readr::col_double(),
    gerimed = readr::col_double(),
    gdate = readr::col_date(format = "%d/%m/%Y"),
    fassess = readr::col_double(),
    dbonemed1 = readr::col_double(),
    delassess = readr::col_double(),
    malnutrition = readr::col_double(),
    mobil2 = readr::col_double(),
    ons = readr::col_double(),
    wdisch = readr::col_date(format = "%d/%m/%Y"),
    wdest = readr::col_double(),
    hdisch = readr::col_date(format = "%d/%m/%Y"),
    olos = readr::col_double(),
    tlos = readr::col_double(),
    dresidence = readr::col_double(),
    fdate1 = readr::col_date(format = "%d/%m/%Y"),
    date30 = readr::col_date(format = "%d/%m/%Y"),
    fsurvive1 = readr::col_double(),
    fresidence1 = readr::col_double(),
    weight_bear30 = readr::col_double(),
    fwalk1 = readr::col_double(),
    fbonemed1 = readr::col_double(),
    fop1 = readr::col_double(),
    fdate2 = readr::col_date(format = "%d/%m/%Y"),
    date120 = readr::col_date(format = "%d/%m/%Y"),
    fsurvive2 = readr::col_double(),
    fresidence2 = readr::col_double(),
    weight_bear120 = readr::col_double(),
    fwalk2 = readr::col_double(),
    fbonemed2 = readr::col_double(),
    fop2 = readr::col_double(),
    predod = readr::col_date(format = "%d/%m/%Y"),
    findod = readr::col_date(format = "%d/%m/%Y"),
    eq5dmob = readr::col_double(),
    eq5dcare = readr::col_double(),
    eq5dact = readr::col_double(),
    eq5dpain = readr::col_double(),
    eq5danx = readr::col_double(),
    eq5dhealth = readr::col_double(),
    .default = readr::col_character()
  )

  read_csv_data <- function(csv_file) {
    csv_data <- readr::read_csv(
      csv_file,
      show_col_types = FALSE,
      na = c("", "NA", "NULL")
    ) |>
      janitor::clean_names() |>
      dplyr::mutate(
        country = if_else(stringr::str_detect(csv_file, "_NZ_"), "nz", "au")
      ) |>
      readr::type_convert(col_types = config_coltype)

    return(csv_data)
  }

  # import into memory
  imported_data <-
    lapply(
      latest_data,
      read_csv_data
    ) |>
    bind_rows() |>
    mutate(
      id = paste0(country, str_pad(id, width = 6, side = "left", pad = "0"))
    ) |>
    # correct NZ hospital codes to make them unique identifiers.
    mutate(across(
      any_of("ahoscode"),
      ~ if_else(country == "nz", stringr::str_replace(.x, "AU", "NZ"), .x)
    )) |>
    # NZ patients are 100% linked
    mutate(quality_flag = if_else(country == "nz", "L", quality_flag)) |>
    select(any_of(c("country", mortality_vars)))

  return(imported_data)
}

#' Attach ANZHFR variable labels
#'
#' @param data A R dataframe
#' @return A labelled R dataframe
anzhfr_var_labels <- function(data) {
  # Variable labels
  config_varlabs <- list(
    start_datetime = "Start DateTime",
    start_date = "Start Date",
    start_time = "Start Time",
    dx_datetime = "Episode Start DateTime",
    dx_date = "Episode Start Date",
    dx_time = "Episode Start Time",
    hosp_report_id = "Hospital ID for Reporting",
    id = "Record Unique Identifier",
    area = "Australian and New Zealand Jurisdiction",
    age = "Age",
    sex = "Sex",
    indig = "Australian Indigenous Status",
    ethnic = "New Zealand Ethnic Status",
    a_pcode = "Postcode",
    medicare = "Medicare Number",
    ptype = "Patient Type",
    uresidence = "Usual Place of Residence",
    ahoscode = "Identifier of Operating Hospital",
    e_dadmit = "ED Admission of Operating Hospital",
    athoscode = "Identifier of Transfer Hospital",
    tarrdatetime = "Transfer Hospital Arrival DateTime",
    tarrdate = "Transfer Hospital Arrival Date",
    tarrtime = "Transfer Hospital Arrival Time",
    arrdatetime = "Operating Hospital Arrival DateTime",
    arrdate = "Operating Hospital Arrival Date",
    arrtime = "Operating Hospital Arrival Time",
    depdatetime = "Operating Hospital Departure DateTime",
    depdate = "Operating Hospital Departure Date",
    deptime = "Operating Hospital Departure Time",
    admdatetimeop = "In-patient Fracture DateTime",
    admdateop = "In-patient Fracture Date",
    admtimeop = "In-patient Fracture Date",
    painassess = "Pain Assessment",
    painmanage = "Pain Management",
    ward = "Ward Type",
    tfanalges = "Nerve Block Before Transfer",
    walk = "Pre-admission Walking Ability",
    amts = "Abbreviated Mental Test Score (AMTS)",
    cogassess = "Pre-admission Cognitive Assessment",
    cogstat = "Pre-admission Cognitive Status",
    bonemed = "Bone Protection Medication At Admission",
    passess = "Pre-operative Medical Assessment",
    side = "Side of Hip Fracture",
    afracture = "Atypical Fracture",
    ftype = "Type of Fracture",
    surg = "Surgical Repair",
    asa = "ASA Grade",
    frailty = "Clinical Frailty Scale",
    addelassess = "Delirium Assessment Prior To Surgery",
    sdatetime = "Hip Fracture Surgery DateTime",
    sdate = "Hip Fracture Surgery Date",
    stime = "Hip Fracture Surgery Time",
    delay = "Surgery Delay",
    delay_txt = "Other Surgery Delay Reason",
    anaesth = "Type of Anaesthesia",
    analges = "Analgesia - Nerve Block",
    consult = "Consultant Surgeon Present",
    optype = "Type of Operation Performed",
    inter_op_fracture = "Intraoperative fracture",
    wbear = "Postoperative Weight Bearing Status",
    mobil = "First Day Mobilisation", # Variable added 1 Jan 2020 [retired 31 December 2023]
    pulcers = "New Skin Pressure Injuries",
    gerimed = "Assessed By Geriatric Medicine",
    gdate = "Geriatric Medicine Assessment Date",
    fassess = "Specialist Falls Assessment",
    dbonemed1 = "Bone Protection Medication At Hospital Discharge",
    delassess = "Post-operative Delirium Assessment",
    malnutrition = "Clinical Malnutrition Assessment",
    mobil2 = "First Day Walking",
    ons = "Oral Nutritional Supplements",
    wdisch = "Discharge Date From Acute Ward",
    wdest = "Discharge Destination From Acute Ward",
    hdisch = "Discharge Date From Hospital",
    olos = "Opearting Hospital Length of Stay",
    tlos = "Health System Length of Stay",
    dresidence = "Discharge Place of Residence",
    fdate1 = "30-day Follow-up Date",
    date30 = "Health System Discharge Date at 30-day Follow-up",
    fsurvive1 = "Survival at 30-day Post-surgery",
    fresidence1 = "Place of Residence at 30-day Follow-up",
    weight_bear30 = "Full Weight Bear at 30-day Follow- up",
    fwalk1 = "Walking Ability at 30-day Follow-up",
    fbonemed1 = "Bone Protection Medication at 30-day Follow-up",
    fop1 = "Re-operation within 30-day Follow-up",
    fdate2 = "120-day Follow-up Date",
    date120 = "Health System Discharge Date at 120-day Follow-up",
    fsurvive2 = "Survival at 120-day Post-surgery",
    fresidence2 = "Place of Residence at 120-day Follow-up",
    weight_bear120 = "Full Weight Bear at 120-day Follow- up",
    fwalk2 = "Walking Ability at 120-day Follow-up",
    fbonemed2 = "Bone Protection Medication at 120-day Follow-up",
    fop2 = "Re-operation within 120-day Follow-up",
    predod = "Preliminary Date of Death",
    findod = "Final Date of Death",
    eq5dmob = "EQ-5D-5L Mobility",
    eq5dcare = "EQ-5D-5L Self Care",
    eq5d_usualactivity = "EQ-5D-5L Usual Activities",
    eq5dpain = "EQ-5D-5L Pain/Discomfort",
    eq5danx = "EQ-5D-5L Anxiety/Depression",
    eq5dhealth = "EQ-5D-5L Health Status"
  )

  dat_lbled <- data |>
    set_variable_labels(.labels = config_varlabs, .strict = FALSE)

  return(dat_lbled)
}

#' Attach ANZHFR variable value labels and values indicating missingness
#'
#' @param data A R dataframe
#' @return A labelled R dataframe

anzhfr_value_labels <- function(data) {
  ## Value labels ----
  config_valuelabs <- list(
    # area = c(
    #   "New South Wales" = 1,
    #   "Victoria" = 2,
    #   "Queensland" = 3,
    #   "South Australia" = 4,
    #   "Western Australia" = 5,
    #   "Tasmania" = 6,
    #   "Northern Territory" = 7,
    #   "Australian Capital Territory" = 8,
    #   "Other Territories (Cocos Keeling Islands, Christmas Island and Jervis Bay Territory)" = 9,
    #   "New Zealand" = 10
    # ),
    sex = c(
      "Male" = 1,
      "Female" = 2,
      "Intersex or indetermine" = 3,
      "Not stated/inadequately described" = 9
    ),
    indig = c(
      "Aboriginal but not Torres Strait Islander origin" = 1,
      "Torres Strait Islander but not Aboriginal origin" = 2,
      "Both Aboriginal and Torres Strait Islander origin" = 3,
      "Neither Aboriginal or Torres Strait Islander origin" = 4,
      "Not stated / inadequately described" = 9
    ),
    ethnic = c(
      "European" = 1,
      "Māori" = 2,
      "Pacific Peoples" = 3,
      "Asian" = 4,
      "Middle Eastern/Latin American/African" = 5,
      "Other ethnicity" = 6,
      "Not elsewhere included" = 9,
      "European" = 10,
      "New Zealand European" = 11,
      "Other European" = 12,
      "Māori" = 21,
      "Pacific peoples not further defined" = 30,
      "Samoan" = 31,
      "Cook Islands Māori" = 32,
      "Tongan" = 33,
      "Niuean" = 34,
      "Tokelauan" = 35,
      "Fijian" = 36,
      "Other Pacific Peoples" = 37,
      "Asian not further defined" = 40,
      "Southeast Asian" = 41,
      "Chinese" = 42,
      "Indian" = 43,
      "Other Asian" = 44,
      "Middle Eastern" = 51,
      "Latin American" = 52,
      "African" = 53,
      "Other ethnicity" = 61,
      "Don’t know" = 94,
      "Refused to answer" = 95,
      "Response unidentifiable" = 98,
      "Not stated" = 99
    ),
    ptype = c(
      "Public" = 1,
      "Private" = 2,
      "Overseas" = 3,
      "Not known" = 9
    ),
    uresidence = c(
      "Private residence" = 1,
      "Residential aged care facility" = 2,
      "Other" = 3,
      "Not known" = 4
    ),
    e_dadmit = c(
      "Yes" = 1,
      "No - transferred from another hospital (via ED)" = 2,
      "No - in-patient fall" = 3,
      "No - transferred from another hospital (direct to ward)" = 4,
      "Other / not known" = 9
    ),
    painassess = c(
      "Within 30 minutes of ED presentation" = 1,
      "Greater than 30 minutes of ED presentation" = 2,
      "Pain assessment not documented or not done" = 3,
      "Not known" = 9
    ),
    painmanage = c(
      "Given within 30 minutes of ED presentation" = 1,
      "Given more than 30 minutes after ED presentation" = 2,
      "Not required - already provided by paramedics" = 3,
      "Not required - no pain documented on assessment" = 4,
      "Not known" = 9
    ),
    ward = c(
      "Hip fracture unit/Orthopaedic ward/ preferred ward" = 1,
      "Outlying ward" = 2,
      "HDU / ICU / CCU" = 3,
      "Other/ not known" = 9
    ),
    tfanalges = c(
      "No" = 1,
      "Yes" = 2,
      "Not known" = 9
    ),
    walk = c(
      "Walks without walking aids" = 1,
      "Walks with either a stick or crutch" = 2,
      "Walks with two aids or frame" = 3,
      "Uses a wheelchair / bed bound" = 4,
      "Not known" = 9
    ),
    cogassess = c(
      "Not assessed" = 1,
      "Assessed (and normal [from 2018-01-01])" = 2,
      "Assessed and abnormal or impaired" = 3,
      "Not known" = 9
    ),
    cogstat = c(
      "Normal cognition" = 1,
      "Impaired cognition or known dementia" = 2,
      "Not assessed" = 8,
      "Not known" = 9
    ),
    bonemed = c(
      "No bone protection medication" = 0,
      "Calcium and/or vitamin D only" = 1,
      "Bisphosphonates, denosumab, romosozumab, teriparitide, raloxifene or HRT" = 2,
      "Not known" = 9
    ),
    passess = c(
      "No assessment conducted" = 0,
      "Geriatrician / Geriatric Team" = 1,
      "Physician / Physician Team" = 2,
      "GP" = 3,
      "Specialist nurse" = 4,
      "Not known" = 9
    ),
    side = c(
      "Left" = 1,
      "Right" = 2
    ),
    afracture = c(
      "Not a pathological or atypical fracture" = 0,
      "Pathological fracture" = 1,
      "Atypical fracture" = 2
    ),
    ftype = c(
      "Intracapsular undisplaced/impacted displaced" = 1,
      "Intracapsular displaced" = 2,
      "Per/intertrochanteric" = 3,
      "Subtrochanteric" = 4
    ),
    surg = c(
      "No" = 1,
      "Yes" = 2,
      "No - surgical fixation not clinically indicated" = 3,
      "No - patient for palliation" = 4,
      "No - other reason" = 5
    ),
    asa = c(
      "Healthy individual with no systemic disease" = 1,
      "Mild systemic disease not limiting activity" = 2,
      "Severe systemic disease that limits activity but is not incapacitating" = 3,
      "Incapacitating systemic disease which is constantly life threatening" = 4,
      "Moribund not expected to survive 24 hours with or without surgery" = 5,
      "Not known" = 9
    ),
    frailty = c(
      "Very Fit" = 1,
      "Well" = 2,
      "Well, with treated comorbid disease" = 3,
      "Vulnerable" = 4,
      "Mildly frail" = 5,
      "Moderately frail" = 6,
      "Severely frail" = 7,
      "Very severely frail" = 8,
      "Terminally ill" = 9,
      "Frailty assessment using other validated tool" = 10,
      "Not known" = 99
    ),
    addelassess = c(
      "Not assessed" = 1,
      "Assessed and not identified" = 2,
      "Assessed and identified" = 3,
      "Not known" = 9
    ),
    delay = c(
      "No delay, surgery completed <48 hours" = 1,
      "Delay due to patient deemed medically unfit" = 2,
      "Delay due to issues with anticoagulation" = 3,
      "Delay due to theatre availability" = 4,
      "Delay due to surgeon availability" = 5,
      "Delay due to delayed diagnosis of hip fracture" = 6,
      "Other type of delay (state reason)" = 7,
      "Not known" = 9
    ),
    anaesth = c(
      "General anaesthesia" = 1,
      "Spinal anaesthesia" = 2,
      "General and spinal anaesthesia" = 3,
      "Spinal / regional anaesthesia" = 5,
      "General and spinal/regional anaesthesia" = 6,
      "Other" = 97,
      "Not known" = 99
    ),
    analges = c(
      "Nerve block administered before arriving in OT" = 1,
      "Nerve block administered in OT" = 2,
      "Both" = 3,
      "Neither" = 4,
      "Not known" = 99
    ),
    consult = c(
      "No" = 0,
      "Yes" = 1,
      "Not known" = 9
    ),
    optype = c(
      "Cannulated screws (e.g. multiple screws)" = 1,
      "Sliding hip screw" = 2,
      "Intramedullary nail short" = 3,
      "Intramedullary nail long" = 4,
      "Hemiarthroplasty stem cemented" = 5,
      "Hemiarthroplasty stem uncemented" = 6,
      "Total hip replacement stem cemented" = 7,
      "Total hip replacement stem uncemented" = 8,
      "Femoral neck system (FNS)" = 9,
      "Other" = 97,
      "Not known" = 99
    ),
    inter_op_fracture = c(
      "No" = 0,
      "Yes" = 1,
      "No operation" = 8,
      "Not known" = 9
    ),
    wbear = c(
      "Unrestricted weight bearing" = 0,
      "Restricted / non weight bearing" = 1,
      "Not known" = 9
    ),
    mobil = c(
      "Patient out of bed and given opportunity to start mobilising day 1 post surgery" = 0,
      "Patient not given opportunity to start mobilising day 1 post surgery" = 1,
      "Not known" = 9
    ),
    pulcers = c(
      "No" = 0,
      "Yes" = 1,
      "Not known" = 9
    ),
    gerimed = c(
      "No" = 0,
      "Yes" = 1,
      "No geriatric medicine service available" = 8,
      "Not known" = 9
    ),
    fassess = c(
      "No" = 0,
      "Performed during admission" = 1,
      "Awaits falls clinic assessment" = 2,
      "Further intervention not appropriate" = 3,
      "Not relevant" = 8,
      "Not known" = 9
    ),
    dbonemed1 = c(
      "No bone protection medication" = 0,
      "Yes - Calcium and/or vitamin D only" = 1,
      "Yes - Bisphosphonates, denosumab, romosozumab, teriparatide, raloxifene or HRT" = 2,
      "No but received prescription at separation from hospital" = 3,
      "Not known" = 9
    ),
    delassess = c(
      "Not assessed" = 1,
      "Assessed and not identified" = 2,
      "Assessed and identified" = 3,
      "Not known" = 9
    ),
    malnutrition = c(
      "Not done" = 0,
      "Malnourished" = 1,
      "Not malnourished" = 2,
      "Not known" = 9
    ),
    mobil2 = c(
      "No" = 0,
      "Yes" = 1,
      "No - Stood without stepping/walking" = 2,
      "No - Sat on the edge of the bed" = 3,
      "No - Sat out of bed via hoist" = 4,
      "No - Did not attempt to get out of bed on day one" = 5,
      "Not known" = 9
    ),
    ons = c(
      "No" = 0,
      "Yes" = 1,
      "Not known" = 9
    ),
    wdest = c(
      "Private residence" = 1,
      "Residential aged care facility" = 2,
      "Rehabilitation unit public" = 3,
      "Rehabilitation unit private" = 4,
      "Other hospital / ward / specialty" = 5,
      "Deceased" = 6,
      "Short term care in residential care facility (New Zealand only)" = 7,
      "Other" = 97,
      "Not known" = 99
    ),
    dresidence = c(
      "Private residence" = 1,
      "Residential aged care facility" = 2,
      "Deceased" = 3,
      "Other" = 7,
      "Not known" = 9
    ),
    fsurvive1 = c(
      "No" = 0,
      "Yes" = 1,
      "Not known" = 9
    ),
    fresidence1 = c(
      "Private residence" = 1,
      "Residential aged care facility" = 2,
      "Rehabilitation unit public" = 3,
      "Rehabilitation unit private" = 4,
      "Other hospital / ward / specialty" = 5,
      "Deceased" = 6,
      "Short term care in residential care facility (New Zealand only)" = 7,
      "Other" = 97,
      "Not known" = 99
    ),
    weight_bear30 = c(
      "Unrestricted weight bearing" = 0,
      "Restricted / non weight bearing" = 1,
      "Not known" = 9
    ),
    fwalk1 = c(
      "Walks without walking aids" = 1,
      "Walks with either a stick or crutch" = 2,
      "Walks with two aids or frame" = 3,
      "Uses a wheelchair / bed bound" = 4,
      "Not relevant" = 8,
      "Not known" = 9
    ),
    fbonemed1 = c(
      "No bone protection medication" = 0,
      "Calcium and/or vitamin D only" = 1,
      "Bisphosphonates, denosumab, romosozumab, teriparatide, raloxifene or HRT" = 2,
      "Not known" = 9
    ),
    fop1 = c(
      "No reoperation" = 0,
      "Reduction of dislocated prosthesis" = 1,
      "Washout or debridement" = 2,
      "Implant removal" = 3,
      "Revision of internal fixation" = 4,
      "Conversion to hemiarthroplasty" = 5,
      "Conversion to total hip replacement" = 6,
      "Excision arthroplasty" = 7,
      "Periprosthetic fracture" = 8,
      "Revision arthroplasty" = 9,
      "Not relevant" = 88,
      "Not known" = 99
    ),
    fsurvive2 = c(
      "No" = 0,
      "Yes" = 1,
      "Not known" = 9
    ),
    fresidence2 = c(
      "Private residence" = 1,
      "Residential aged care facility" = 2,
      "Rehabilitation unit public" = 3,
      "Rehabilitation unit private" = 4,
      "Other hospital / ward / specialty" = 5,
      "Deceased" = 6,
      "Short term care in residential care facility (New Zealand only)" = 7,
      "Other" = 97,
      "Not known" = 99
    ),
    weight_bear120 = c(
      "Unrestricted weight bearing" = 0,
      "Restricted / non weight bearing" = 1,
      "Not known" = 9
    ),
    fwalk2 = c(
      "Walks without walking aids" = 1,
      "Walks with either a stick or crutch" = 2,
      "Walks with two aids or frame" = 3,
      "Uses a wheelchair / bed bound" = 4,
      "Not relevant" = 8,
      "Not known" = 9
    ),
    fbonemed2 = c(
      "No bone protection medication" = 0,
      "Calcium and/or vitamin D only" = 1,
      "Bisphosphonates, denosumab, romosozumab, teriparatide, raloxifene or HRT" = 2,
      "Not known" = 9
    ),
    fop2 = c(
      "No reoperation" = 0,
      "Reduction of dislocated prosthesis" = 1,
      "Washout or debridement" = 2,
      "Implant removal" = 3,
      "Revision of internal fixation" = 4,
      "Conversion to hemiarthroplasty" = 5,
      "Conversion to total hip replacement" = 6,
      "Excision arthroplasty" = 7,
      "Periprosthetic fracture" = 8,
      "Revision arthroplasty" = 9,
      "Not relevant" = 88,
      "Not known" = 99
    ),
    eq5dmob = c(
      "No problems" = 1,
      "Slight problems" = 2,
      "Moderate problems" = 3,
      "Severe problems" = 4,
      "Unable to" = 5
    ),
    eq5dcare = c(
      "No problems" = 1,
      "Slight problems" = 2,
      "Moderate problems" = 3,
      "Severe problems" = 4,
      "Unable to" = 5
    ),
    eq5dact = c(
      "No problems" = 1,
      "Slight problems" = 2,
      "Moderate problems" = 3,
      "Severe problems" = 4,
      "Unable to" = 5
    ),
    eq5dpain = c(
      "No pain" = 1,
      "Slight pain" = 2,
      "Moderate pain" = 3,
      "Severe pain" = 4,
      "Exteme pain" = 5
    ),
    eq5dnx = c(
      "Not anxious" = 1,
      "Slightly anxious" = 2,
      "Moderately anxious" = 3,
      "Severely anxious" = 4,
      "Extemely anxious" = 5
    )
  )

  ## Missing value labels ----
  config_nalabs <- list(
    sex = 9,
    indig = 9,
    ethnic = c(94, 95, 98, 99),
    ptype = 9,
    uresidence = 4,
    e_dadmit = 9,
    painassess = 9,
    painmanage = 9,
    ward = 9,
    tfanalges = 9,
    walk = 9,
    cogassess = 9,
    cogstat = c(8, 9),
    bonemed = 9,
    passess = 9,
    asa = 9,
    frailty = 99,
    addelassess = 9,
    delay = 9,
    anaesth = 99,
    analges = 99,
    consult = 9,
    optype = 99,
    inter_op_fracture = 9,
    wbear = 9,
    mobil = 9,
    pulcers = 9,
    gerimed = 9,
    fassess = 9,
    dbonemed1 = 9,
    delassess = 9,
    malnutrition = 9,
    mobil2 = 9,
    ons = 9,
    wdest = 99,
    dresidence = 9,
    fsurvive1 = 9,
    fresidence1 = 99,
    weight_bear30 = 9,
    fwalk1 = 9,
    fbonemed1 = 9,
    fop1 = 99,
    fsurvive2 = 9,
    fresidence2 = 99,
    weight_bear120 = 9,
    fwalk2 = 9,
    fbonemed2 = 9,
    fop2 = 99
  )

  dat_lbled <- data |>
    set_value_labels(.labels = config_valuelabs, .strict = FALSE) |>
    nolabel_to_na() |>
    set_na_values(.values = config_nalabs, .strict = FALSE)

  return(dat_lbled)
}

#' Standardise ID
#'
#' @param data A dataframe with ID
#' @return A dataframe with standardised ID

std_id <- function(data) {
  std_id_data <- data |>
    separate(
      id,
      into = c("letters", "digits"),
      sep = "(?<=[a-zA-Z])(?=[0-9])"
    ) |>
    mutate(digits = str_pad(digits, width = 6, side = "left", pad = "0")) |>
    unite(id, letters, digits, sep = "")

  return(std_id_data)
}


#' Create a patient journey dataset (ie. event and datetime)
#'
#' @param data A dataframe with datetime in wide format
#' @return A dataframe with events and dates in long format
pt_journey <- function(data) {
  long_date <- data |>
    select(id, where(is.Date), side) |>
    pivot_longer(
      cols = where(is.Date),
      names_to = "event",
      values_to = "date"
    )

  long_time <- data |>
    select(id, where(hms::is_hms), side) |>
    pivot_longer(
      cols = where(hms::is_hms),
      names_to = "event",
      values_to = "time"
    ) |>
    mutate(event = str_replace(event, "time", "date"))

  journey_data <- left_join(
    long_date,
    long_time,
    by = c("id", "event", "side")
  ) |>
    mutate(
      event = factor(
        event,
        levels = c(
          "start_date",
          "tarrdate",
          "arrdate",
          "depdate",
          "admdateop",
          "sdate",
          "gdate",
          "wdisch",
          "hdisch",
          "fdate1",
          "fdate2",
          "date30",
          "date120",
          "predod",
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
          "discharge_from_system1",
          "discharge_from_system2",
          "preliminary death",
          "death"
        )
      )
    ) |>
    filter(!is.na(date) | !is.na(time)) |>
    arrange(id, date, time)

  return(journey_data)
}


#' Create a admission pattern variable for deciding date of hip fracture diagnosis
#'
#' @param data Patient journey data
#' @return A dataframe with TEDIS admission patterns
get_tedis <- function(data) {
  tmp_dat <- data |>
    filter(
      event %in%
        c(
          "start_date",
          "arrive_transfer_hospital",
          "arrive_operating_hospital",
          "depart_from_ED",
          "in_patient_fracture",
          "surgery"
        )
    ) |>
    mutate(
      event = factor(
        event,
        levels = c(
          "start_date",
          "arrive_transfer_hospital",
          "arrive_operating_hospital",
          "depart_from_ED",
          "in_patient_fracture",
          "surgery"
        ),
        labels = c("start_date", "T", "E", "D", "I", "S")
      )
    ) |>
    # as we only concern about dates in mortality calculation
    arrange(id, date, event) |>
    group_by(id) |>
    mutate(report_year = if_else(event == "start_date", year(date), NA)) |>
    fill(report_year, .direction = "downup") |>
    # keep start_date only when it's the only record
    filter(!(n() > 1 & event == "start_date")) |>
    mutate(event_index = row_number() - 1) |>
    mutate(lag_date = lag(date)) |>
    ungroup() |>
    mutate(duration = as.duration(interval(lag_date, date)))

  event_wide <- tmp_dat |>
    select(id, report_year, event_index, event) |>
    pivot_wider(
      id_cols = c(id, report_year),
      names_from = event_index,
      names_prefix = "event",
      values_from = event
    ) |>
    unite(admittype, contains("event"), sep = "", na.rm = TRUE)

  duration_wide <- tmp_dat |>
    select(id, event_index, duration) |>
    pivot_wider(
      id_cols = id,
      names_from = event_index,
      names_prefix = "duration",
      values_from = duration
    ) |>
    select(-duration0)

  admit_dat <- left_join(event_wide, duration_wide, by = "id") |>
    # check TEDIS position in event sequence
    mutate(loc_t = str_locate(admittype, "T")[, 1]) |>
    mutate(loc_e = str_locate(admittype, "E")[, 1]) |>
    mutate(loc_d = str_locate(admittype, "D")[, 1]) |>
    mutate(loc_i = str_locate(admittype, "I")[, 1]) |>
    mutate(loc_s = str_locate(admittype, "S")[, 1]) |>
    # check if I (in-patient fall) is next to D (ED discharge)
    # -> patient fell in ED (in-ED fracture)
    mutate(i_next_to_d = abs(loc_i - loc_d) == 1) |>
    mutate(loc_i = if_else(i_next_to_d & !is.na(i_next_to_d), loc_d, loc_i)) |>
    # check if pattern falls in the TEDIS order
    unite(
      "tedis_order",
      contains("loc"),
      sep = "",
      na.rm = TRUE,
      remove = FALSE
    ) |>
    mutate(
      tedis_in_order = sapply(
        str_split(tedis_order, pattern = ""),
        function(x) all(diff(as.numeric(x)) >= 0)
      )
    ) |>
    mutate(
      dx_event = case_when(
        str_detect(admittype, "I") ~ "I",
        str_detect(admittype, "T") ~ "T",
        str_detect(admittype, "E") ~ "E",
        str_detect(admittype, "D") ~ "D",
        str_detect(admittype, "S") ~ "S",
        .default = NA
      )
    ) |>
    mutate(
      valid_tedis = if_else(
        tedis_in_order == TRUE & admittype != "start_date",
        TRUE,
        FALSE
      )
    ) |>
    # get corresponding hip fracture diagnosis event as the diagnosis date
    left_join(
      tmp_dat |> select(id, event, date, time),
      by = c("id" = "id", "dx_event" = "event")
    ) |>
    mutate(dx_date = date) |>
    mutate(dx_time = time)

  return(admit_dat)
}


#' Calculate mortality status within different timeframes
#'
#' @param clean_data Cleaned data
#' @param tedis_info TEDIS information generated from patient journey
#' @return A dataframe with mortality indicators for 30, 90, 120 and 365-day
get_mortality <- function(clean_data, tedis_info) {
  clean_data_with_mort <- left_join(
    clean_data,
    tedis_info |> select(id, admittype, valid_tedis, dx_date, dx_time),
    by = "id"
  ) |>
    mutate(
      days_to_death = as.numeric(difftime(findod, dx_date, units = "days"))
    ) |>
    mutate(
      mort30d = factor(
        case_when(
          days_to_death < 0 ~ NA,
          days_to_death >= 0 & days_to_death <= 30 ~ 1,
          days_to_death > 30 ~ 0,
          .default = 0
        ),
        levels = c(0, 1),
        labels = c("Alive", "Deceased")
      )
    ) |>

    mutate(
      mort90d = factor(
        case_when(
          days_to_death < 0 ~ NA,
          days_to_death >= 0 & days_to_death <= 90 ~ 1,
          days_to_death > 90 ~ 0,
          .default = 0
        ),
        levels = c(0, 1),
        labels = c("Alive", "Deceased")
      )
    ) |>
    mutate(
      mort120d = factor(
        case_when(
          days_to_death < 0 ~ NA,
          days_to_death >= 0 & days_to_death <= 120 ~ 1,
          days_to_death > 120 ~ 0,
          .default = 0
        ),
        levels = c(0, 1),
        labels = c("Alive", "Deceased")
      )
    ) |>
    mutate(
      mort365d = factor(
        case_when(
          days_to_death < 0 ~ NA,
          days_to_death >= 0 & days_to_death <= 365 ~ 1,
          days_to_death > 365 ~ 0,
          .default = 0
        ),
        levels = c(0, 1),
        labels = c("Alive", "Deceased")
      )
    ) |>
    labelled::set_variable_labels(
      mort30d = "30-day mortality",
      mort90d = "90-day mortality",
    )

  return(clean_data_with_mort)
}


# Data cleaning ----------------------------------------------------------------

#' Match hospital codes with hospital names
#'
#' @param data A R dataframe
#' @param hoscode_dat Dataframe that links hospital codes to hospital names
#' @return A labelled R dataframe
label_hoscode <- function(data, hoscode_dat) {
  labled_data <- dplyr::left_join(
    data,
    hoscode_dat,
    by = "ahoscode"
  )
  return(labled_data)
}


#' Deduplicate records
#'
#' @param data Raw ANZHFR data
#' @return Deduplicated dataset
deduplicate <- function(raw_data) {
  dedup_data <- raw_data |>
    group_by(id) |>
    mutate(n_records = n()) |>
    ungroup() |>
    filter(n_records == 1) |>
    select(-n_records)

  # # Identify and separate duplicates for subsequent processes
  # dup_data <- raw_data |>
  #   group_by(id) |>
  #   mutate(n = n()) |>
  #   filter(n > 1) |>
  #   ungroup()

  # print("Max number of duplicates:")
  # print(max(dup_data$n))

  # # The following can only handle number of duplicates == 2

  # tmpdat <- dup_data |>
  #   mutate(n_miss = rowSums(is.na(dup_data))) |>
  #   group_by(id) |>
  #   arrange(start_date) |>
  #   mutate(distance = adist(start_date[1], start_date[2])) |> # number of typo in start_date
  #   mutate(same_side = (sum(side) / 2) %% 1 == 0) |> # same fracture side?
  #   mutate(same_sex = (sum(sex) / 2) %% 1 == 0) |> # same sex?
  #   mutate(age_diff = age[2] - age[1]) |> # age difference?
  #   mutate(year_diff = year(start_date[2]) - year(start_date[1])) |> # date difference in years?
  #   mutate(date_diff = interval(start_date[1], start_date[2]) %/% days(1)) |> # date difference in days?
  #   mutate(age_year_same = age_diff == year_diff) |> # age difference == date difference in years?
  #   ungroup()

  # # Scenario 1 - same sex, start_date and age match in year and same fracture side, 1 typo in start_date
  # # (i.e., true duplicates due to typo)
  # tmpdat1 <- tmpdat |>
  #   filter(
  #     age_year_same == TRUE &
  #       same_sex == TRUE &
  #       same_side == TRUE &
  #       distance == 1
  #   ) |>
  #   group_by(id) |>
  #   arrange(start_date) |>
  #   fill(everything(), .direction = "updown") |> # fill up missing values using latest record as reference
  #   filter(row_number() == n()) |> # select the latest record
  #   ungroup()

  # # Scenario 2 - same sex, start_date and age match in year and same fracture side, with start_date difference < 30 days
  # # (i.e., same person with same records that are likely overwritten)
  # tmpdat2 <- tmpdat |>
  #   filter(
  #     age_year_same == TRUE &
  #       same_sex == TRUE &
  #       same_side == TRUE &
  #       (date_diff < 30)
  #   ) |>
  #   group_by(id) |>
  #   arrange(start_date) |>
  #   fill(everything(), .direction = "updown") |> # fill up missing values using latest record as reference
  #   filter(row_number() == n()) |> # select the latest record
  #   ungroup()

  # # Scenario 3 - same sex, start_date and age match in year, but not included in scenario 1 & 2
  # # (i.e., same person with different records of likely contralateral fractures)
  # tmpdat3 <-
  #   tmpdat |>
  #   filter(
  #     age_year_same == TRUE &
  #       same_sex == TRUE &
  #       !(id %in% c(tmpdat1$id, tmpdat2$id))
  #   ) |>
  #   group_by(id) |>
  #   arrange(start_date) |>
  #   mutate(side = if_else(row_number() > 1, 3 - lag(side), side)) |> # change the second record's fracture side based on first record
  #   mutate(id = paste0(id, "_", row_number())) |>
  #   ungroup()

  # # Scenario 4 - different sex or different start_date and age in year
  # # (i.e., different person)

  # tmpdat4 <- tmpdat |>
  #   filter(
  #     (age_year_same == FALSE | same_sex == FALSE)
  #   ) |>
  #   group_by(id) |>
  #   mutate(id = paste0(id, letters[row_number()])) |>
  #   ungroup()

  # # Combine all tmpdats
  # dup_data_edit <-
  #   bind_rows(tmpdat1, tmpdat2, tmpdat3, tmpdat4) |>
  #   select(colnames(raw_data))

  # print("The following IDs have not been proccessed:")
  # print(dup_data$id[!(dup_data$id %in% str_sub(dup_data_edit$id, 1, 8))])

  # # Merge back to original data
  # dedup_data <- raw_data |>
  #   filter(!(id %in% dup_data$id)) |>
  #   bind_rows(dup_data_edit)

  return(dedup_data)
}

#' Clean up invalid datetime and typos
#'
#' @param data A R dataframe
#' @return A R dataframe
clean_datetime <- function(data) {
  # Mortality-focused datetime cleaning process
  newdata <- data |>
    # rename start_date to start_datetime to keep consistent naming
    rename(start_datetime = start_date) |>
    # extract date from datetime
    mutate(
      across(
        where(is.POSIXct),
        date,
        .names = "{.col}_tmp"
      )
    ) |>
    rename_with(
      .cols = where(is.Date),
      ~ str_remove(str_remove(.x, "time"), "_tmp")
    ) |>
    # extract time from datetime
    mutate(
      across(
        where(is.POSIXct),
        hms::as_hms,
        .names = "{.col}_tmp"
      )
    ) |>
    rename_with(
      .cols = where(hms::is_hms),
      ~ str_remove(str_remove(.x, "date"), "_tmp")
    ) |>
    # Change incorrect dates and times to NA
    mutate(
      across(
        where(is.Date),
        ~ na_if(.x, ymd("1900-01-01"))
      )
    ) |>
    mutate(
      across(
        where(hms::is_hms),
        ~ na_if(.x, hms::as_hms("00:00:00"))
      )
    ) |>
    mutate(
      across(
        where(hms::is_hms),
        ~ na_if(.x, hms::as_hms("00:00:01"))
      )
    ) |>
    # Auto-correct potential typo based on acute-care anchor dates
    rowwise() |>
    mutate(
      median_date = median(
        c(
          tarrdate,
          arrdate,
          depdate,
          admdateop,
          sdate,
          gdate,
          wdisch
        ),
        na.rm = TRUE
      )
    ) |>
    ungroup() |>
    filter(!is.na(median_date)) |>
    mutate(
      across(
        c(
          tarrdate,
          arrdate,
          depdate,
          admdateop,
          sdate
        ),
        ~ if_else(
          .x %within%
            interval(median_date %m-% months(3), median_date %m+% months(3)),
          .x,
          if_else(
            update(.x, year = year(median_date)) %within%
              interval(median_date %m-% months(1), median_date %m+% months(1)),
            update(.x, year = year(median_date)),
            .x
          )
        )
      )
    ) |>
    select(-starts_with("median"), -where(is.POSIXct))

  return(newdata)
}


#' Clean up data errors and missing values
#'
#' @param raw_data A R dataframe
#' @return A R dataframe
clean_data <- function(raw_data) {
  new_data <- raw_data |>
    mutate(report_year = year(start_date)) |>
    # confirm surgical indicator from available surgery timing fields
    mutate(
      surg = if_else(!is.na(sdate) | !is.na(optype), 2, surg)
    ) |>
    mutate(
      surg = case_when(
        !is.na(surg) ~ surg,
        year(start_date) < 2021 ~ 1,
        TRUE ~ 5
      )
    ) |>
    # `surg` new coding frame was introduced on 01-Jan-2021
    mutate(surg = if_else(surg %in% 3:5 & year(start_date) < 2021, 1, surg))

  return(new_data)
}

#' Clean up reason for surgery delay
#'
#' @param data A R dataframe (data with cleaned datetime and TEDIS info)
#' @return A R dataframe of records with cleaned-up reason for surgery delay
clean_delay <- function(data) {
  clean_data <-
    data |>
    mutate(
      tts = as.duration(as.interval(
        (ymd(dx_date) + hms(dx_time)),
        (ymd(sdate) + hms(stime))
      ))
    ) |>
    mutate(
      delay_yn = if_else(
        year(start_date) < 2024,
        tts > hours(48),
        tts > hours(36)
      )
    ) |>
    mutate(
      delay = case_when(
        delay_yn == FALSE ~ 1,
        delay_yn == TRUE & delay == 1 ~ NA,
        .default = delay
      )
    )

  return(clean_data)
}


#' Create analysis variables that match NHFR classification
#'
#' @param raw_data A R dataframe (clean data)
#' @return A R dataframe
transform_data <- function(raw_data) {
  new_data <- raw_data |>
    # age groups of 5-year interval
    mutate(
      age_cat5 = cut(
        age,
        c(seq(50, 100, 5), 110),
        right = FALSE,
        include.lowest = TRUE
      )
    ) |>
    # age groups of 5-year interval
    mutate(
      sex_2l = factor(
        sex,
        levels = c(1, 2),
        labels = c("Male", "Female")
      )
    ) |>
    mutate(
      surg_yn = factor(
        surg == 2,
        levels = c(TRUE, FALSE),
        labels = c("Surgical", "Non-surgical")
      )
    ) |>
    mutate(
      asa_nhfd = cut(
        asa,
        right = FALSE,
        breaks = c(1, 3, 4, 6),
        labels = c("1-2", "3", "4-5")
      )
    ) |>
    mutate(
      uresidence_nhfd = cut(
        uresidence,
        right = FALSE,
        breaks = c(1, 2, 3),
        labels = c("Own home", "Residential care")
      )
    ) |>
    mutate(
      walk_nhfd = cut(
        walk,
        right = FALSE,
        breaks = c(1, 2, 4, 5),
        labels = c(
          "walked without aids",
          "walked 1 aid 2 aids or frame",
          "wheelchair bed bound"
        )
      )
    ) |>
    mutate(
      ftype_nhfd = cut(
        ftype,
        right = FALSE,
        breaks = c(1, 3, 5),
        labels = c("Intracapular", "Extracapsular")
      )
    ) |>
    # set variable labels
    set_variable_labels(
      age_cat5 = "5-year age group",
      sex_2l = "Sex",
      asa_nhfd = "ASA group",
      walk_nhfd = "Walk score",
      ftype_nhfd = "Fracture type",
      uresidence_nhfd = "Residence type",
      surg_yn = "Surgical repair"
    )

  return(new_data)
}


#' Create analysis cohort for modelling stage
#'
#' @param data A R dataframe (clean data)
#' @return A R dataframe of records that meet the eligibility criteria
select_data <- function(data) {
  selected_data <- data |>
    # criteria 1 - age between 50-110
    filter(age >= 50 & age <= 110) |>
    # # criteria 2 - remove overseas patients - No postcode in the latest data
    # filter(a_pcode != 9998) |>
    # # Need to revisit - retain records if postcode is missing.
    # filter(
    #   !(ptype == 3 & (a_pcode == 0 | a_pcode == 8888 | a_pcode == 9999))
    # ) |>
    # criteria 3 - date of death not before hip fracture diagnosis date
    filter(days_to_death >= 0 | is.na(days_to_death)) |>
    # criteria 4 - have a valid TEDIS pattern
    filter(valid_tedis == TRUE) |>
    # criteria 5 - had surgery for the hip fracture
    filter(surg_yn == "Surgical")

  return(selected_data)
}
