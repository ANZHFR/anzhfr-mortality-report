##
## Title: Modelling Functions for ANZHFR Mortality Data
## Author: Dr Xingzhong (Jason) Jin
## Purpose: To collect functions for various adjusted mortality models
## Last updated: 3 Dec 2025
##

# Multiple Imputations ---------------------------------------------------------

#' Multiple imputation on missing values related to mortality modelling
#'
#' @param data Analysis dataset
#' @return A mids object containing multiple imputed datasets
mice_nhfd_pmm <- function(data) {
  # Set multiple imputations parameters
  rand_seed <- 12345
  mi_form <- ~ age + sex_2l + asa_nhfd + walk_nhfd + ftype_nhfd +
    uresidence_nhfd + surg_yn + mort30d + afracture

  impdat <- data |> select(id, all.vars(mi_form))

  # Build imputation matrix
  imp_matrix <- mice::make.predictorMatrix(impdat)

  imp_matrix["id", ] <- 0
  imp_matrix[, "id"] <- 0
  imp_matrix["age", ] <- 0
  imp_matrix["surg_yn", ] <- 0
  imp_matrix["asa_nhfd", ] <- 0
  imp_matrix["asa_nhfd", c("surg_yn", "age", "uresidence_nhfd", "walk_nhfd", "mort30d")] <- 1
  imp_matrix["mort30d", ] <- 0

  imp <- mice::mice(
    impdat,
    m = 10,
    maxit = 10,
    seed = rand_seed,
    method = "pmm",
    predictorMatrix = imp_matrix,
    print = TRUE
  )

  return(imp)
}

#' Process and Join Imputed Data
#'
#' This function takes a MICE mids object, extracts the completed datasets,
#' removes specific mortality/age columns to avoid duplication, and joins
#' the remaining static variables from the original analysis dataset.
#'
#' @param mids_object A 'mids' object resulting from mice::mice().
#' @param reference_data The original dataframe used for analysis (before imputation) containing static variables to rejoin.
#' @param id_col The name of the ID column used for joining (default "id").
#'
#' @return A list of data frames, where each data frame is a completed, cleaned, and joined dataset.
process_imputed_data <- function(mids_object, reference_data, id_col = "id") {
  # 1. Extract all completed datasets into a list
  completed_list <- mice::complete(mids_object, "all")

  # 2. Iterate over each dataset to clean and join
  joined_list <- purrr::map(completed_list, function(df) {
    # A. Remove specific columns that we don't want from the imputation set
    df_clean <- df |>
      dplyr::select(-age, -dplyr::starts_with("mort"))

    # B. Prepare the reference data for joining
    cols_in_imputed <- names(df_clean)
    cols_to_remove_from_ref <- setdiff(cols_in_imputed, id_col)

    ref_subset <- reference_data |>
      dplyr::select(-dplyr::all_of(cols_to_remove_from_ref))

    # C. Join and apply final selection
    df_clean |>
      dplyr::left_join(ref_subset, by = id_col) |>
      select_data() # specific custom function from your etl_functions.R
  })

  return(joined_list)
}


# Post-model estimation --------------------------------------------------------

#' Pool predictions from logistic regressions
#'
#' @param mi_rollmod A collection of logistic regressions on imputed datasets
#' @return Analysis data attached with logit and probability
pool.mice.scalar <- function(mi_rollmod) {
  set.seed(12345)

  # Generate expected probability and standard error
  mi_pred_logit <- purrr::map(
    mi_rollmod,
    ~ as.data.frame(predict(.x, type = "link", se.fit = TRUE))
  )

  # Extract info from model for pooling
  k <- length(mi_rollmod[[1]]$coefficients) # number of parameters
  n <- nrow(mi_rollmod[[1]]$data) # number of sample size

  # Pool logit
  # Convert list to array for application of pool.scalar
  logit_array <- array(
    unlist(mi_pred_logit),
    dim = c(dim(mi_pred_logit[[1]]), length(mi_pred_logit))
  )

  pooled_logit_list <- apply(logit_array, 1, function(x) {
    mice::pool.scalar(Q = x[1, ], U = x[2, ]^2, n = n, k = k)
  })

  pooled_logit <- pooled_logit_list |>
    sapply(unlist) |>
    t() |>
    as.data.frame()

  # Merge back to model data
  full_pred_dat <- mi_rollmod[[1]]$data |>
    mutate(logit = pooled_logit$qbar) |>
    mutate(logit_se = sqrt(pooled_logit$ubar)) |>
    mutate(prob = plogis(logit)) |>
    mutate(prob_se = logit_se * (plogis(logit) * (1 - plogis(logit))))

  return(full_pred_dat)
}


#' Summarise adjusted mortality rate by groups
#'
#' @param data Dataset containing predictions
#' @param set_group Grouping variable name
#' @param response Response variable name
#' @return New dataset with group-level statistics and control limits
summort_by_group <- function(data, set_group, response) {
  new_data <- data |>
    # Calculate expected mortality, crude mortality and SE of logits by group
    group_by(!!!rlang::syms(set_group)) |>
    summarise(
      n_cc = sum(!is.na(prob), na.rm = TRUE), # number of obs in the model
      n = n(), # number of obs in the data
      logit = mean(logit, na.rm = TRUE),
      n_expect = sum(prob, na.rm = TRUE),
      n_death = sum(!!rlang::sym(response) == "Deceased", na.rm = TRUE),
      cmort_se = sd(!!rlang::sym(response) == "Deceased", na.rm = TRUE) / sqrt(n),
      logit_se = sqrt(sum(logit_se^2, na.rm = TRUE) / n_cc)
    ) |>
    ungroup() |>
    mutate(cmort = n_death / n) |>
    mutate(amort = n_expect / n_cc) |>
    # Calculate SMR as observed mortality over expected mortality (Byar approx)
    mutate(smr = cmort / amort) |>
    mutate(
      smr_ll = n_death / n_expect * (1 - 1 / (9 * n_death) - qnorm(0.975) / (3 * sqrt(n_death)))^3
    ) |>
    mutate(
      smr_ul = (n_death + 1) / n_expect * (1 - (1 / (9 * (n_death + 1))) + qnorm(0.975) / (3 * sqrt(n_death + 1)))^3
    ) |>
    # Calculate CI of crude mortality
    mutate(cmort_ll = cmort - qnorm(0.975) * cmort_se) |>
    mutate(cmort_ul = cmort + qnorm(0.975) * cmort_se) |>
    # Calculate CI of expected mortality from standard errors on logit scale
    mutate(amort_se = logit_se * amort * (1 - amort)) |>
    mutate(amort_ll = amort - qnorm(0.975) * amort_se) |>
    mutate(amort_ul = amort + qnorm(0.975) * amort_se) |>
    # Calculate weighted mean
    mutate(ws_mean = weighted.mean(cmort, w = n)) |>
    # Calculate control limit CIs (Wald approximation)
    mutate(
      ws_se = sqrt((ws_mean * (1 - ws_mean)) / n),
      ws_lcl95 = ws_mean - qnorm(0.975) * ws_se,
      ws_ucl95 = ws_mean + qnorm(0.975) * ws_se,
      ws_lcl99 = ws_mean - qnorm(0.9985) * ws_se,
      ws_ucl99 = ws_mean + qnorm(0.9985) * ws_se
    ) |>
    # Calculate control limit CIs (Exact binomial)
    mutate(
      ws_lcl95_exact = Hmisc::binconf(ws_mean * n, n, alpha = 0.05, method = "exact", return.df = TRUE)[[2]],
      ws_ucl95_exact = Hmisc::binconf(ws_mean * n, n, alpha = 0.05, method = "exact", return.df = TRUE)[[3]],
      ws_lcl99_exact = Hmisc::binconf(ws_mean * n, n, alpha = 0.003, method = "exact", return.df = TRUE)[[2]],
      ws_ucl99_exact = Hmisc::binconf(ws_mean * n, n, alpha = 0.003, method = "exact", return.df = TRUE)[[3]]
    ) |>
    # Calculate mortality rate standardized to national population (Byar approx)
    mutate(smort = smr * ws_mean) |>
    mutate(smort_ll = smr_ll * ws_mean) |>
    mutate(smort_ul = smr_ul * ws_mean)

  return(new_data)
}


# Reports and Graphs -----------------------------------------------------------

#' Determine the hospitals to be included in each year's report
#'
#' @param analysis_data Analysis dataframe
#' @param years Vector of years to report
#' @return A named list of reportable hospitals in each report year
get_report_hosp <- function(analysis_data, years) {
  dat_hosp_npa <- analysis_data |>
    group_by(country, h_name, ahos_code, report_year) |>
    tally()

  hosp_reportable <- function(year) {
    dat_hosp_npa |>
      group_by(country, h_name, ahos_code) |>
      filter((report_year >= year - 3) & (report_year < year)) |>
      mutate(
        yrs = n(),
        vol = sum(n, na.rm = TRUE)
      ) |>
      # A hospital is reportable if it has 3-year look back & total surgical vol ≥ 50
      mutate(reportable = (yrs >= 3 & vol >= 50)) |>
      ungroup()
  }

  hosp_to_report <- lapply(years, hosp_reportable) |>
    set_names(years)

  return(hosp_to_report)
}


## Funnel plot (by hospital) ---------------------------------------------------

#' Generate standard funnel plot
#'
#' @param data Graph data
#' @param title Plot title
#' @return Funnel plot object
fun_funnel_hosp <- function(data, title) {
  # Plot data
  plt_dat <- data |>
    mutate(
      text = if_else(
        (smort > ws_ucl95_exact) | (smort < ws_lcl95_exact),
        h_name,
        ""
      )
    ) |>
    mutate(
      group = case_when(
        smort < ws_lcl95_exact ~ "Below average",
        ws_lcl95_exact <= smort & smort <= ws_ucl95_exact ~ "Average",
        smort > ws_ucl95_exact ~ "Above average"
      )
    ) |>
    mutate(
      v_just = case_when(
        smort < ws_lcl95_exact ~ +2,
        ws_lcl95_exact <= smort & smort <= ws_ucl95_exact ~ 0,
        smort > ws_ucl95_exact ~ -1
      )
    )

  # Set plotting parameters
  axis_x_max <- round(max(plt_dat$n) * 1.1, digits = -2)
  axis_y_max <- round(max(c(plt_dat$ws_ucl95_exact, plt_dat$smort)) * 1.2, digits = 2)

  ci_dat <- data.frame(
    n = c(55, axis_x_max),
    ws_mean = data$ws_mean[1:2]
  ) |>
    # Wald approximation
    mutate(
      ws_se = sqrt((ws_mean * (1 - ws_mean)) / n),
      ws_lcl95 = ws_mean - qnorm(0.975) * ws_se,
      ws_ucl95 = ws_mean + qnorm(0.975) * ws_se,
      ws_lcl99 = ws_mean - qnorm(0.9985) * ws_se,
      ws_ucl99 = ws_mean + qnorm(0.9985) * ws_se
    ) |>
    # Exact binomial
    mutate(
      ws_lcl95_exact = Hmisc::binconf(ws_mean * n, n, alpha = 0.05, method = "exact", return.df = TRUE)[[2]],
      ws_ucl95_exact = Hmisc::binconf(ws_mean * n, n, alpha = 0.05, method = "exact", return.df = TRUE)[[3]],
      ws_lcl99_exact = Hmisc::binconf(ws_mean * n, n, alpha = 0.003, method = "exact", return.df = TRUE)[[2]],
      ws_ucl99_exact = Hmisc::binconf(ws_mean * n, n, alpha = 0.003, method = "exact", return.df = TRUE)[[3]]
    ) |>
    bind_rows(data |> select(n, starts_with("ws")))

  # Build plot
  gph_funnel <- plt_dat |>
    ggplot(aes(x = n)) +

    # Reference lines
    geom_hline(aes(yintercept = ws_mean), color = "#003A6E", linewidth = 1) +
    geom_line(data = ci_dat, aes(x = n, y = ws_lcl95_exact), linetype = "dashed", color = "#434343", linewidth = 0.5) +
    geom_line(data = ci_dat, aes(x = n, y = ws_ucl95_exact), linetype = "dashed", color = "#434343", linewidth = 0.5) +
    geom_line(data = ci_dat, aes(x = n, y = ws_lcl99_exact), linetype = "dotted", color = "#434343", linewidth = 0.5) +
    geom_line(data = ci_dat, aes(x = n, y = ws_ucl99_exact), linetype = "dotted", color = "#434343", linewidth = 0.5) +

    # National average label
    geom_label(
      x = axis_x_max,
      y = plt_dat$ws_mean[1] + max(plt_dat$ws_se) * 1.2,
      label.padding = unit(0.5, "lines"),
      label.size = 0.3,
      hjust = 1,
      color = "#434343",
      label = paste("National mortality rate", scales::percent(plt_dat$ws_mean[1], accuracy = 0.1))
    ) +
    geom_segment(
      x = axis_x_max, xend = axis_x_max,
      y = plt_dat$ws_mean[1] + max(plt_dat$ws_se) * 1.2,
      yend = plt_dat$ws_mean[1],
      colour = "#434343",
      linewidth = 0.5,
      arrow = arrow(length = unit(0.5, "lines"))
    ) +

    # Data points
    geom_point(aes(y = smort, color = group), size = 2, show.legend = TRUE) +
    geom_text(aes(y = smort, label = text, color = group, vjust = v_just), show.legend = FALSE) +

    # Aesthetics
    scale_y_continuous(
      limits = c(0, axis_y_max),
      labels = scales::percent,
      breaks = scales::extended_breaks(10)
    ) +
    scale_x_continuous(
      limits = c(0, axis_x_max),
      breaks = scales::extended_breaks(10)
    ) +
    labs(
      x = "3-year surgical volume",
      y = "Standardised mortality rate",
      title = title
    ) +
    theme_classic() +
    theme(
      axis.title = element_text(size = 14),
      text = element_text(size = 14),
      legend.title = element_blank(),
      legend.position = "bottom"
    ) +
    scale_color_manual(
      values = c(
        "Above average" = "#FF0000",
        "Average" = "#FFA500",
        "Below average" = "#008000"
      ),
      drop = FALSE
    )

  return(gph_funnel)
}


## Caterpillar plot on expected mortality (by hospital) -------------------------

#' Generate standard caterpillar plot
#'
#' @param data Graph data
#' @param title Plot title
#' @return Caterpillar plot object
fun_smort_ctpl_hosp <- function(data, title) {
  # Plot data
  plt_dat <- data |>
    mutate(h_name = fct_reorder(h_name, desc(smort))) |>
    arrange(desc(smort)) |>
    mutate(
      group = case_when(
        ws_mean > smort_ul ~ "Below average",
        ws_mean >= smort_ll & ws_mean <= smort_ul ~ "Average",
        ws_mean < smort_ll ~ "Above average"
      )
    )

  # Plot parameters
  n_hosp <- nrow(plt_dat)
  national_avg <- plt_dat$ws_mean[1]
  national_lab_pos <- plt_dat$ws_mean[1] + 3 * mean(plt_dat$ws_se)

  # Build plot
  gph_ctpl <- plt_dat |>
    ggplot(aes(x = h_name, y = smort)) +
    geom_hline(aes(yintercept = ws_mean), color = "#434343", linewidth = 1) +
    geom_errorbar(
      aes(ymin = smort_ll, ymax = smort_ul, color = group),
      width = 0,
      linewidth = 1,
      show.legend = TRUE
    ) +
    geom_point(color = "#003a6e") +
    geom_label(
      x = n_hosp - 0.5,
      y = national_lab_pos,
      label.padding = unit(0.5, "lines"),
      label.size = 0.3,
      hjust = 0,
      color = "#434343",
      label = paste("National mortality rate", scales::percent(plt_dat$ws_mean[1], accuracy = 0.1))
    ) +
    annotate(
      "segment",
      x = n_hosp - 0.5,
      xend = n_hosp - 0.5,
      y = national_lab_pos,
      yend = national_avg,
      colour = "#434343",
      linewidth = 0.5,
      arrow = arrow(length = unit(0.5, "lines"))
    ) +
    coord_flip() +
    scale_y_continuous(
      labels = scales::percent,
      breaks = scales::extended_breaks(10)
    ) +
    labs(
      x = "Hospital",
      y = "Standardised mortality rate",
      title = title
    ) +
    theme_classic() +
    theme(
      axis.title = element_text(size = 14),
      text = element_text(size = 14),
      line = element_blank(),
      legend.title = element_blank(),
      legend.position = "bottom",
      plot.caption = element_text(hjust = -1)
    ) +
    scale_color_manual(
      values = c(
        "Above average" = "#FF0000",
        "Average" = "#FFA500",
        "Below average" = "#008000"
      ),
      drop = FALSE
    )

  return(gph_ctpl)
}

## Caterpillar plot on SMR (by hospital) ---------------------------------------

#' Generate SMR caterpillar plot
#'
#' @param data Graph data
#' @param title Plot title
#' @return Caterpillar plot object
fun_smr_ctpl_hosp <- function(data, title) {
  plt_dat <- data |>
    arrange(desc(smr)) |>
    mutate(h_name = fct_reorder(h_name, desc(smr))) |>
    mutate(
      group = case_when(
        1 > smr_ul ~ "Lower than expected",
        1 >= smr_ll & 1 <= smr_ul ~ "As expected",
        1 < smr_ll ~ "Higher than expected"
      )
    )

  gph_ctpl <- plt_dat |>
    ggplot(aes(x = h_name, y = smr, colour = group)) +
    geom_hline(aes(yintercept = 1), color = "#434343", linewidth = 1) +
    geom_errorbar(aes(ymin = smr_ll, ymax = smr_ul), width = 0, linewidth = 1) +
    geom_point(show.legend = TRUE) +
    scale_y_continuous(
      labels = scales::percent,
      breaks = scales::extended_breaks(10)
    ) +
    coord_flip() +
    labs(
      x = "Hospital",
      y = "Standardised mortality ratio (Observed/Expected)",
      title = title
    ) +
    theme_classic() +
    theme(
      axis.title = element_text(size = 14),
      text = element_text(size = 14),
      line = element_blank(),
      legend.title = element_blank(),
      legend.position = "bottom"
    ) +
    scale_color_manual(
      values = c(
        "Higher than expected" = "#FF0000",
        "As expected" = "#FFA500",
        "Lower than expected" = "#008000"
      ),
      drop = FALSE
    )

  return(gph_ctpl)
}

## Trend plot on standardised mortality rate -----------------------------------

#' Generate annual trend plot
#'
#' @param dat_au AU data
#' @param dat_au_area AU area data
#' @param dat_nz NZ data
#' @param y_lab Y-axis label
#' @return List of plots
fun_annual_trend <- function(dat_au, dat_au_area, dat_nz, y_lab = "Standardised mortality rate") {
  dat_au <- dat_au |>
    mutate(area = "AU")

  # Remove unused data
  dat_au_area <- dat_au_area |>
    # Only include state that has at least 50 cases in a year.
    filter(n > 50) |>
    # Excluded TAS 2016 due to insufficient reporting number
    filter(!(area == "TAS" & report_year == 2016))

  dat_nz <- dat_nz |>
    filter(report_year >= 2017) |>
    mutate(area = "NZ")

  dat_comb <- bind_rows(dat_au, dat_au_area, dat_nz) |>
    mutate(
      area = factor(
        area,
        levels = c("AU", "NSW", "QLD", "VIC", "WA", "SA", "TAS", "ACT", "NT", "NZ")
      )
    ) |>
    mutate(area_value = as.numeric(area))

  # Australia plot
  plt_au <- dat_comb |>
    filter(area != "NZ") |>
    ggplot(aes(x = report_year, y = smort)) +
    geom_hline(
      data = dat_au,
      aes(yintercept = ws_mean[1]),
      color = "#0A3A6E",
      linetype = "dashed",
      alpha = 0.6,
      linewidth = 1
    ) +
    geom_point(aes(group = area, color = area, size = area)) +
    geom_line(aes(group = area, color = area, linewidth = area)) +
    scale_x_continuous(
      limits = c(2016, max(dat_comb$report_year)),
      breaks = scales::extended_breaks(10)
    ) +
    scale_y_continuous(
      labels = scales::percent_format(accuracy = 1.0),
      breaks = scales::extended_breaks(10),
      limits = c(0, round(max(dat_au_area$smort) * 1.1, 2))
    ) +
    scale_color_manual(
      values = c(
        "AU"  = "#0A3A6E",
        "NSW" = "#00A7E2",
        "QLD" = "#8E3B24",
        "VIC" = "#ED1C24",
        "WA"  = "#CDA54D",
        "SA"  = "#F2C0B8",
        "TAS" = "#71A581",
        "ACT" = "black",
        "NT"  = "blue"
      )
    ) +
    scale_size_manual(
      values = c(
        "AU" = 2, "NSW" = 1, "QLD" = 1, "VIC" = 1, "WA" = 1,
        "SA" = 1, "TAS" = 1, "ACT" = 1, "NT"  = 1
      )
    ) +
    scale_linewidth_manual(
      values = c(
        "AU" = 1, "NSW" = 0.5, "QLD" = 0.5, "VIC" = 0.5, "WA" = 0.5,
        "SA" = 0.5, "TAS" = 0.5, "ACT" = 0.5, "NT" = 0.5
      )
    ) +
    labs(x = "Year", y = y_lab) +
    theme_classic() +
    theme(
      text = element_text(size = 14),
      axis.title.x = element_blank(),
      axis.title.y = element_text(size = 14),
      legend.title = element_blank(),
      legend.position.inside = c(0.5, 0.1)
    ) +
    guides(color = guide_legend(position = "inside", nrow = 1))

  # New Zealand plot
  plt_nz <- dat_nz |>
    ggplot(aes(x = report_year, y = smort)) +
    geom_hline(
      aes(yintercept = ws_mean[1]),
      color = "#0A3A6E",
      linetype = "dashed",
      alpha = 0.6,
      linewidth = 1
    ) +
    geom_point(color = "#0A3A6E", size = 2) +
    geom_line(color = "#0A3A6E", linewidth = 1) +
    scale_x_continuous(
      limits = c(2016, max(dat_comb$report_year)),
      breaks = scales::extended_breaks(10)
    ) +
    scale_y_continuous(
      labels = scales::percent_format(accuracy = 1.0),
      breaks = scales::extended_breaks(10),
      limits = c(0, round(max(dat_au_area$smort) * 1.1, 2))
    ) +
    labs(x = "Year", y = y_lab) +
    theme_classic() +
    theme(
      text = element_text(size = 14),
      axis.title.x = element_blank(),
      legend.title = element_blank()
    )

  # Table plot
  plt_tbl <- dat_comb |>
    ggplot(aes(x = report_year, y = area_value, group = area)) +
    geom_text(aes(label = scales::percent(smort, accuracy = 0.1))) +
    labs(x = "Year", y = "Region") +
    scale_x_continuous(
      position = "top",
      limits = c(2016, max(dat_comb$report_year)),
      breaks = scales::extended_breaks(10)
    ) +
    scale_y_continuous(
      trans = "reverse",
      breaks = c(1:max(dat_comb$area_value)),
      labels = levels(dat_comb$area)
    ) +
    theme_classic() +
    theme(
      text = element_text(size = 14),
      legend.title = element_blank(),
      axis.title.x = element_blank(),
      axis.text.x = element_blank(),
      axis.title.y = element_blank(),
      panel.grid.minor.y = element_line(
        color = "black",
        linewidth = 0.25,
        linetype = 1
      ),
      axis.line.x.bottom = element_line(color = "black", linewidth = 0.5),
      axis.line.y.right = element_line(color = "black", linewidth = 0.5)
    )

  # Combine plot
  plt_final <- ggpubr::ggarrange(
    plt_au, plt_nz, plt_tbl,
    ncol = 1,
    heights = c(1, 1, 0.5),
    nrow = 3,
    align = "v",
    labels = c("Australia", "New Zealand", ""),
    hjust = c(-1, -0.7, 0)
  )

  lst_plt <- list(
    plt_au = plt_au,
    plt_nz = plt_nz,
    plt_tbl = plt_tbl,
    plt_final = plt_final
  )

  return(lst_plt)
}


## Miscellaneous helper functions ----------------------------------------------

#' Remove a layer from a ggplot object
#'
#' @param ggplot2_object Plot object
#' @param geom_type Geometry type (e.g., "GeomText")
#' @return Updated ggplot object
remove_geom <- function(ggplot2_object, geom_type) {
  # Delete layers that match the requested type.
  layers <- lapply(ggplot2_object$layers, function(x) {
    if (inherits(x$geom, geom_type)) {
      NULL
    } else {
      x
    }
  })

  # Delete the unwanted layers.
  layers <- layers[!sapply(layers, is.null)]
  ggplot2_object$layers <- layers

  return(ggplot2_object)
}

#' Replace hospital name with report_id in funnel plot
#'
#' @param ggplot2_object Funnel plot object
#' @param hoscode_data Hospital codes data
#' @return Updated funnel plot
replace_funnel_hname <- function(ggplot2_object, hoscode_data) {
  tmp_dat <- ggplot2_object$data |>
    left_join(hoscode_data, by = "h_name") |>
    mutate(report_id = ifelse(text == "", "", report_id))

  ggplot2_object$data <- tmp_dat

  p <- remove_geom(ggplot2_object, "GeomText") +
    geom_text(
      aes(y = smort, label = report_id, color = group, vjust = v_just),
      show.legend = FALSE
    )

  return(p)
}

#' Replace hospital name with report_id in caterpillar plot
#'
#' @param ggplot2_object Caterpillar plot object
#' @param hoscode_data Hospital codes data
#' @return Updated caterpillar plot
replace_ctpl_hname <- function(ggplot2_object, hoscode_data) {
  labs <- left_join(
    data.frame(h_name = ggplot2_object$data$h_name),
    hoscode_data,
    by = "h_name"
  )

  p <- ggplot2_object + scale_x_discrete(labels = labs$report_id)

  return(p)
}
