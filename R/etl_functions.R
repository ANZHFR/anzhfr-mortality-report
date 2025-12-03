##
## Title: ETL Functions for ANZHFR Mortality Data
## Author: Dr Xingzhong (Jason) Jin
## Purpose: To collect functions for the ETL processes
## Last updated: 3 Dec 2025
##

# Data extraction --------------------------------------------------------------

# A list of variable types in the ANZHFR dataset for future reference
config_coltype <- list(
  start_date        = readr::col_datetime(format = "%d/%m/%Y %H:%M"),
  report_id         = readr::col_character(),
  id                = readr::col_character(),
  area              = readr::col_character(),
  age               = readr::col_double(),
  sex               = readr::col_double(),
  indig             = readr::col_double(),
  ethnic            = readr::col_double(),
  a_pcode           = readr::col_character(),
  ptype             = readr::col_double(),
  uresidence        = readr::col_double(),
  ahos_code         = readr::col_character(),
  e_dadmit          = readr::col_double(),
  athoscode         = readr::col_character(),
  tarrdatetime      = readr::col_datetime(format = "%d/%m/%Y %H:%M"),
  arrdatetime       = readr::col_datetime(format = "%d/%m/%Y %H:%M"),
  depdatetime       = readr::col_datetime(format = "%d/%m/%Y %H:%M"),
  admdatetimeop     = readr::col_datetime(format = "%d/%m/%Y %H:%M"),
  painassess        = readr::col_double(),
  painmanage        = readr::col_double(),
  ward              = readr::col_double(),
  tfanalges         = readr::col_double(),
  walk              = readr::col_double(),
  amts              = readr::col_double(),
  cogassess         = readr::col_double(),
  cogstat           = readr::col_double(),
  bonemed           = readr::col_double(),
  passess           = readr::col_double(),
  side              = readr::col_double(),
  afracture         = readr::col_double(),
  ftype             = readr::col_double(),
  surg              = readr::col_double(),
  asa               = readr::col_double(),
  frailty           = readr::col_double(),
  addelassess       = readr::col_double(),
  sdatetime         = readr::col_datetime(format = "%d/%m/%Y %H:%M"),
  delay             = readr::col_double(),
  anaesth           = readr::col_double(),
  analges           = readr::col_double(),
  consult           = readr::col_double(),
  optype            = readr::col_double(),
  inter_op_fracture = readr::col_double(),
  wbear             = readr::col_double(),
  mobil             = readr::col_double(),
  pulcers           = readr::col_double(),
  gerimed           = readr::col_double(),
  gdate             = readr::col_date(format = "%d/%m/%Y"),
  fassess           = readr::col_double(),
  dbonemed1         = readr::col_double(),
  delassess         = readr::col_double(),
  malnutrition      = readr::col_double(),
  mobil2            = readr::col_double(),
  ons               = readr::col_double(),
  wdisch            = readr::col_date(format = "%d/%m/%Y"),
  wdest             = readr::col_double(),
  hdisch            = readr::col_date(format = "%d/%m/%Y"),
  olos              = readr::col_double(),
  tlos              = readr::col_double(),
  dresidence        = readr::col_double(),
  fdate1            = readr::col_date(format = "%d/%m/%Y"),
  date30            = readr::col_date(format = "%d/%m/%Y"),
  fsurvive1         = readr::col_double(),
  fresidence1       = readr::col_double(),
  weight_bear30     = readr::col_double(),
  fwalk1            = readr::col_double(),
  fbonemed1         = readr::col_double(),
  fop1              = readr::col_double(),
  fdate2            = readr::col_date(format = "%d/%m/%Y"),
  date120           = readr::col_date(format = "%d/%m/%Y"),
  fsurvive2         = readr::col_double(),
  fresidence2       = readr::col_double(),
  weight_bear120    = readr::col_double(),
  fwalk2            = readr::col_double(),
  fbonemed2         = readr::col_double(),
  fop2              = readr::col_double(),
  predod            = readr::col_date(format = "%d/%m/%Y"),
  findod            = readr::col_date(format = "%d/%m/%Y")
)

#' Extract latest ANZHFR data from datalake
#'
#' @param latest_data A list of CSV files extracted from ANZHFR database
#' @param config_coltype A list of column types
#' @return A R dataframe
get_anzhfr_data <- function(latest_data, config_coltype) {
  # import into memory
  dat_mort <- lapply(
    latest_data,
    readr::read_csv,
    col_type = config_coltype,
    show_col_types = FALSE,
    na = "NULL",
    name_repair = janitor::make_clean_names
  ) |>
    set_names(latest_data) |>
    bind_rows(.id = "ds") |>
    mutate(country = if_else(str_detect(ds, "_NZ_"), "nz", "au")) |>
    mutate(id = paste0(country, str_pad(id, width = 6, side = "left", pad = "0")))

  return(dat_mort)
}

#' Attach ANZHFR variable labels
#'
#' @param data A R dataframe
#' @return A labelled R dataframe
anzhfr_var_labels <- function(data) {
  # Variable labels
  config_varlabs <- list(
    start_date          = "Start Date",
    report_id           = "Hospital ID for Reporting",
    id                  = "Record Unique Identifier",
    area                = "Australian and New Zealand Jurisdiction",
    age                 = "Age",
    sex                 = "Sex",
    indig               = "Australian Indigenous Status",
    ethnic              = "New Zealand Ethnic Status",
    a_pcode             = "Postcode",
    ptype               = "Patient Type",
    uresidence          = "Usual Place of Residence",
    ahos_code           = "Identifier of Operating Hospital",
    e_dadmit            = "ED Admission of Operating Hospital",
    athoscode           = "Identifier of Transfer Hospital",
    tarrdatetime        = "Transfer Hospital Arrival DateTime",
    arrdatetime         = "Operating Hospital Arrival DateTime",
    depdatetime         = "Operating Hospital Departure DateTime",
    admdatetimeop       = "In-patient Fracture DateTime",
    painassess          = "Pain Assessment",
    painmanage          = "Pain Management",
    ward                = "Ward Type",
    tfanalges           = "Nerve Block Before Transfer",
    walk                = "Pre-admission Walking Ability",
    amts                = "Abbreviated Mental Test Score (AMTS)",
    cogassess           = "Pre-admission Cognitive Assessment",
    cogstat             = "Pre-admission Cognitive Status",
    bonemed             = "Bone Protection Medication At Admission",
    passess             = "Pre-operative Medical Assessment",
    side                = "Side of Hip Fracture",
    afracture           = "Atypical Fracture",
    ftype               = "Type of Fracture",
    surg                = "Surgical Repair",
    asa                 = "ASA Grade",
    frailty             = "Clinical Frailty Scale",
    addelassess         = "Delirium Assessment Prior To Surgery",
    sdatetime           = "Hip Fracture Surgery DateTime",
    delay               = "Surgery Delay",
    anaesth             = "Type of Anaesthesia",
    analges             = "Analgesia - Nerve Block",
    consult             = "Consultant Surgeon Present",
    optype              = "Type of Operation Performed",
    inter_op_fracture   = "Intraoperative fracture",
    wbear               = "Postoperative Weight Bearing Status",
    mobil               = "First Day Mobilisation", # Variable added 1 Jan 2020 [retired 31 December 2023]
    pulcers             = "New Skin Pressure Injuries",
    gerimed             = "Assessed By Geriatric Medicine",
    gdate               = "Geriatric Medicine Assessment Date",
    fassess             = "Specialist Falls Assessment",
    dbonemed1           = "Bone Protection Medication At Hospital Discharge",
    delassess           = "Post-operative Delirium Assessment",
    malnutrition        = "Clinical Malnutrition Assessment",
    mobil2              = "First Day Walking",
    ons                 = "Oral Nutritional Supplements",
    wdisch              = "Discharge Date From Acute Ward",
    wdest               = "Discharge Destination From Acute Ward",
    hdisch              = "Discharge Date From Hospital",
    olos                = "Opearting Hospital Length of Stay",
    tlos                = "Health System Length of Stay",
    dresidence          = "Discharge Place of Residence",
    fdate1              = "30-day Follow-up Date",
    date30              = "Health System Discharge Date at 30-day Follow-up",
    fsurvive1           = "Survival at 30-day Post-surgery",
    fresidence1         = "Place of Residence at 30-day Follow-up",
    weight_bear30       = "Full Weight Bear at 30-day Follow- up",
    fwalk1              = "Walking Ability at 30-day Follow-up",
    fbonemed1           = "Bone Protection Medication at 30-day Follow-up",
    fop1                = "Re-operation within 30-day Follow-up",
    fdate2              = "120-day Follow-up Date",
    date120             = "Health System Discharge Date at 120-day Follow-up",
    fsurvive2           = "Survival at 120-day Post-surgery",
    fresidence2         = "Place of Residence at 120-day Follow-up",
    weight_bear120      = "Full Weight Bear at 120-day Follow- up",
    fwalk2              = "Walking Ability at 120-day Follow-up",
    fbonemed2           = "Bone Protection Medication at 120-day Follow-up",
    fop2                = "Re-operation within 120-day Follow-up",
    predod              = "Preliminary Date of Death",
    findod              = "Final Date of Death"
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
    sex = c(
      "Male"                              = 1,
      "Female"                            = 2,
      "Intersex or indetermine"           = 3,
      "Not stated/inadequately described" = 9
    ),
    indig = c(
      "Aboriginal but not Torres Strait Islander origin"    = 1,
      "Torres Strait Islander but not Aboriginal origin"    = 2,
      "Both Aboriginal and Torres Strait Islander origin"   = 3,
      "Neither Aboriginal or Torres Strait Islander origin" = 4,
      "Not stated / inadequately described"                 = 9
    ),
    ethnic = c(
      "European"                              = 1,
      "Māori"                                 = 2,
      "Pacific Peoples"                       = 3,
      "Asian"                                 = 4,
      "Middle Eastern/Latin American/African" = 5,
      "Other ethnicity"                       = 6,
      "Not elsewhere included"                = 9,
      "European"                              = 10,
      "New Zealand European"                  = 11,
      "Other European"                        = 12,
      "Māori"                                 = 21,
      "Pacific peoples not further defined"   = 30,
      "Samoan"                                = 31,
      "Cook Islands Māori"                    = 32,
      "Tongan"                                = 33,
      "Niuean"                                = 34,
      "Tokelauan"                             = 35,
      "Fijian"                                = 36,
      "Other Pacific Peoples"                 = 37,
      "Asian not further defined"             = 40,
      "Southeast Asian"                       = 41,
      "Chinese"                               = 42,
      "Indian"                                = 43,
      "Other Asian"                           = 44,
      "Middle Eastern"                        = 51,
      "Latin American"                        = 52,
      "African"                               = 53,
      "Other ethnicity"                       = 61,
      "Don’t know"                            = 94,
      "Refused to answer"                     = 95,
      "Response unidentifiable"               = 98,
      "Not stated"                            = 99
    ),
    ptype = c(
      "Public"    = 1,
      "Private"   = 2,
      "Overseas"  = 3,
      "Not known" = 9
    ),
    uresidence = c(
      "Private residence"              = 1,
      "Residential aged care facility" = 2,
      "Other"                          = 3,
      "Not known"                      = 4
    ),
    e_dadmit = c(
      "Yes"                                                     = 1,
      "No - transferred from another hospital (via ED)"         = 2,
      "No - in-patient fall"                                    = 3,
      "No - transferred from another hospital (direct to ward)" = 4,
      "Other / not known"                                       = 9
    ),
    painassess = c(
      "Within 30 minutes of ED presentation"       = 1,
      "Greater than 30 minutes of ED presentation" = 2,
      "Pain assessment not documented or not done" = 3,
      "Not known"                                  = 9
    ),
    painmanage = c(
      "Given within 30 minutes of ED presentation"       = 1,
      "Given more than 30 minutes after ED presentation" = 2,
      "Not required – already provided by paramedics"    = 3,
      "Not required – no pain documented on assessment"  = 4,
      "Not known"                                        = 9
    ),
    ward = c(
      "Hip fracture unit/Orthopaedic ward/ preferred ward" = 1,
      "Outlying ward"                                      = 2,
      "HDU / ICU / CCU"                                    = 3,
      "Other/ not known"                                   = 9
    ),
    tfanalges = c(
      "No"        = 1,
      "Yes"       = 2,
      "Not known" = 9
    ),
    walk = c(
      "Walks without walking aids"          = 1,
      "Walks with either a stick or crutch" = 2,
      "Walks with two aids or frame"        = 3,
      "Uses a wheelchair / bed bound"       = 4,
      "Not known"                           = 9
    ),
    cogassess = c(
      "Not assessed"                      = 1,
      "Assessed and normal"               = 2,
      "Assessed and abnormal or impaired" = 3,
      "Not known"                         = 9
    ),
    cogstat = c(
      "Normal cognition"                     = 1,
      "Impaired cognition or known dementia" = 2,
      "Not assessed"                         = 8,
      "Not known"                            = 9
    ),
    bonemed = c(
      "No bone protection medication"                                            = 0,
      "Calcium and/or vitamin D only"                                            = 1,
      "Bisphosphonates, denosumab, romosozumab, teriparitide, raloxifene or HRT" = 2,
      "Not known"                                                                = 9
    ),
    passess = c(
      "No assessment conducted"       = 0,
      "Geriatrician / Geriatric Team" = 1,
      "Physician / Physician Team"    = 2,
      "GP"                            = 3,
      "Specialist nurse"              = 4,
      "Not known"                     = 9
    ),
    side = c(
      "Left"  = 1,
      "Right" = 2
    ),
    afracture = c(
      "Not a pathological or atypical fracture" = 0,
      "Pathological fracture"                   = 1,
      "Atypical fracture"                       = 2
    ),
    ftype = c(
      "Intracapsular undisplaced/impacted displaced" = 1,
      "Intracapsular displaced"                      = 2,
      "Per/intertrochanteric"                        = 3,
      "Subtrochanteric"                              = 4
    ),
    surg = c(
      "No" = 1,
      "Yes" = 2,
      "No – surgical fixation not clinically indicated" = 3,
      "No – patient for palliation" = 4,
      "No – other reason" = 5
    ),
    asa = c(
      "Healthy individual with no systemic disease"                            = 1,
      "Mild systemic disease not limiting activity"                            = 2,
      "Severe systemic disease that limits activity but is not incapacitating" = 3,
      "Incapacitating systemic disease which is constantly life threatening"   = 4,
      "Moribund not expected to survive 24 hours with or without surgery"      = 5,
      "Not known"                                                              = 9
    ),
    frailty = c(
      "Very Fit"                                      = 1,
      "Well"                                          = 2,
      "Well, with treated comorbid disease"           = 3,
      "Vulnerable"                                    = 4,
      "Mildly frail"                                  = 5,
      "Moderately frail"                              = 6,
      "Severely frail"                                = 7,
      "Very severely frail"                           = 8,
      "Terminally ill"                                = 9,
      "Frailty assessment using other validated tool" = 10,
      "Not known"                                     = 99
    ),
    addelassess = c(
      "Not assessed"                = 1,
      "Assessed and not identified" = 2,
      "Assessed and identified"     = 3,
      "Not known"                   = 9
    ),
    delay = c(
      "No delay, surgery completed <48 hours"          = 1,
      "Delay due to patient deemed medically unfit"    = 2,
      "Delay due to issues with anticoagulation"       = 3,
      "Delay due to theatre availability"              = 4,
      "Delay due to surgeon availability"              = 5,
      "Delay due to delayed diagnosis of hip fracture" = 6,
      "Other type of delay (state reason)"             = 7,
      "Not known"                                      = 9
    ),
    anaesth = c(
      "General anaesthesia"                       = 1,
      "Spinal anaesthesia"                        = 2,
      "General and spinal anaesthesia"            = 3,
      "Spinal / regional anaesthesia"             = 5,
      "6 General and spinal/regional anaesthesia" = 6,
      "Other"                                     = 97,
      "Not known"                                 = 99
    ),
    analges = c(
      "Nerve block administered before arriving in OT" = 1,
      "Nerve block administered in OT"                 = 2,
      "Both"                                           = 3,
      "Neither"                                        = 4,
      "Not known"                                      = 99
    ),
    consult = c(
      "No"        = 0,
      "Yes"       = 1,
      "Not known" = 9
    ),
    optype = c(
      "Cannulated screws (e.g. multiple screws)" = 1,
      "Sliding hip screw"                        = 2,
      "Intramedullary nail short"                = 3,
      "Intramedullary nail long"                 = 4,
      "Hemiarthroplasty stem cemented"           = 5,
      "Hemiarthroplasty stem uncemented"         = 6,
      "Total hip replacement stem cemented"      = 7,
      "Total hip replacement stem uncemented"    = 8,
      "Femoral neck system (FNS)"                = 9,
      "Other"                                    = 97,
      "Not known"                                = 99
    ),
    inter_op_fracture = c(
      "No"           = 0,
      "Yes"          = 1,
      "No operation" = 8,
      "Not known"    = 9
    ),
    wbear = c(
      "Unrestricted weight bearing"     = 0,
      "Restricted / non weight bearing" = 1,
      "Not known"                       = 9
    ),
    mobil = c(
      "Patient out of bed and given opportunity to start mobilising day 1 post surgery" = 0,
      "Patient not given opportunity to start mobilising day 1 post surgery"            = 1,
      "Not known"                                                                       = 9
    ),
    pulcers = c(
      "No"        = 0,
      "Yes"       = 1,
      "Not known" = 9
    ),
    gerimed = c(
      "No"                                      = 0,
      "Yes"                                     = 1,
      "No geriatric medicine service available" = 8,
      "Not known"                               = 9
    ),
    fassess = c(
      "No"                                   = 0,
      "Performed during admission"           = 1,
      "Awaits falls clinic assessment"       = 2,
      "Further intervention not appropriate" = 3,
      "Not relevant"                         = 8,
      "Not known"                            = 9
    ),
    dbonemed1 = c(
      "No bone protection medication"                                                  = 0,
      "Yes - Calcium and/or vitamin D only"                                            = 1,
      "Yes - Bisphosphonates, denosumab, romosozumab, teriparatide, raloxifene or HRT" = 2,
      "No but received prescription at separation from hospital"                       = 3,
      "Not known"                                                                      = 9
    ),
    delassess = c(
      "Not assessed"                = 1,
      "Assessed and not identified" = 2,
      "Assessed and identified"     = 3,
      "Not known"                   = 9
    ),
    malnutrition = c(
      "Not done"         = 0,
      "Malnourished"     = 1,
      "Not malnourished" = 2,
      "Not known"        = 9
    ),
    mobil2 = c(
      "No"        = 0,
      "Yes"       = 1,
      "Not known" = 9
    ),
    ons = c(
      "No"        = 0,
      "Yes"       = 1,
      "Not known" = 9
    ),
    wdest = c(
      "Private residence"                                               = 1,
      "Residential aged care facility"                                  = 2,
      "Rehabilitation unit public"                                      = 3,
      "Rehabilitation unit private"                                     = 4,
      "Other hospital / ward / specialty"                               = 5,
      "Deceased"                                                        = 6,
      "Short term care in residential care facility (New Zealand only)" = 7,
      "Other"                                                           = 97,
      "Not known"                                                       = 99
    ),
    dresidence = c(
      "Private residence"              = 1,
      "Residential aged care facility" = 2,
      "Deceased"                       = 3,
      "Other"                          = 7,
      "Not known"                      = 9
    ),
    fsurvive1 = c(
      "No"        = 0,
      "Yes"       = 1,
      "Not known" = 9
    ),
    fresidence1 = c(
      "Private residence"                                               = 1,
      "Residential aged care facility"                                  = 2,
      "Rehabilitation unit public"                                      = 3,
      "Rehabilitation unit private"                                     = 4,
      "Other hospital / ward / specialty"                               = 5,
      "Deceased"                                                        = 6,
      "Short term care in residential care facility (New Zealand only)" = 7,
      "Other"                                                           = 97,
      "Not known"                                                       = 99
    ),
    weight_bear30 = c(
      "Unrestricted weight bearing"     = 0,
      "Restricted / non weight bearing" = 1,
      "Not known"                       = 9
    ),
    fwalk1 = c(
      "Walks without walking aids"          = 1,
      "Walks with either a stick or crutch" = 2,
      "Walks with two aids or frame"        = 3,
      "Uses a wheelchair / bed bound"       = 4,
      "Not relevant"                        = 8,
      "Not known"                           = 9
    ),
    fbonemed1 = c(
      "No bone protection medication"                                            = 0,
      "Calcium and/or vitamin D only"                                            = 1,
      "Bisphosphonates, denosumab, romosozumab, teriparatide, raloxifene or HRT" = 2,
      "Not known"                                                                = 9
    ),
    fop1 = c(
      "No reoperation"                      = 0,
      "Reduction of dislocated prosthesis"  = 1,
      "Washout or debridement"              = 2,
      "Implant removal"                     = 3,
      "Revision of internal fixation"       = 4,
      "Conversion to hemiarthroplasty"      = 5,
      "Conversion to total hip replacement" = 6,
      "Excision arthroplasty"               = 7,
      "Periprosthetic fracture"             = 8,
      "Revision arthroplasty"               = 9,
      "Not relevant"                        = 88,
      "Not known"                           = 99
    ),
    fsurvive2 = c(
      "No"        = 0,
      "Yes"       = 1,
      "Not known" = 9
    ),
    fresidence2 = c(
      "Private residence"                                               = 1,
      "Residential aged care facility"                                  = 2,
      "Rehabilitation unit public"                                      = 3,
      "Rehabilitation unit private"                                     = 4,
      "Other hospital / ward / specialty"                               = 5,
      "Deceased"                                                        = 6,
      "Short term care in residential care facility (New Zealand only)" = 7,
      "Other"                                                           = 97,
      "Not known"                                                       = 99
    ),
    weight_bear120 = c(
      "Unrestricted weight bearing"     = 0,
      "Restricted / non weight bearing" = 1,
      "Not known"                       = 9
    ),
    fwalk2 = c(
      "Walks without walking aids"          = 1,
      "Walks with either a stick or crutch" = 2,
      "Walks with two aids or frame"        = 3,
      "Uses a wheelchair / bed bound"       = 4,
      "Not relevant"                        = 8,
      "Not known"                           = 9
    ),
    fbonemed2 = c(
      "No bone protection medication"                                            = 0,
      "Calcium and/or vitamin D only"                                            = 1,
      "Bisphosphonates, denosumab, romosozumab, teriparatide, raloxifene or HRT" = 2,
      "Not known"                                                                = 9
    ),
    fop2 = c(
      "No reoperation"                      = 0,
      "Reduction of dislocated prosthesis"  = 1,
      "Washout or debridement"              = 2,
      "Implant removal"                     = 3,
      "Revision of internal fixation"       = 4,
      "Conversion to hemiarthroplasty"      = 5,
      "Conversion to total hip replacement" = 6,
      "Excision arthroplasty"               = 7,
      "Periprosthetic fracture"             = 8,
      "Revision arthroplasty"               = 9,
      "Not relevant"                        = 88,
      "Not known"                           = 99
    )
  )

  ## Missing value labels ----
  config_nalabs <- list(
    sex               = 9,
    indig             = 9,
    ethnic            = c(94, 95, 98, 99),
    ptype             = 9,
    uresidence        = 4,
    e_dadmit          = 9,
    painassess        = 9,
    painmanage        = 9,
    ward              = 9,
    tfanalges         = 9,
    walk              = 9,
    cogassess         = 9,
    cogstat           = c(8, 9),
    bonemed           = 9,
    passess           = 9,
    asa               = 9,
    frailty           = 99,
    addelassess       = 9,
    delay             = 9,
    anaesth           = 99,
    analges           = 99,
    consult           = 9,
    optype            = 99,
    inter_op_fracture = 9,
    wbear             = 9,
    mobil             = 9,
    pulcers           = 9,
    gerimed           = 9,
    fassess           = 9,
    dbonemed1         = 9,
    delassess         = 9,
    malnutrition      = 9,
    mobil2            = 9,
    ons               = 9,
    wdest             = 99,
    dresidence        = 9,
    fsurvive1         = 9,
    fresidence1       = 99,
    weight_bear30     = 9,
    fwalk1            = 9,
    fbonemed1         = 9,
    fop1              = 99,
    fsurvive2         = 9,
    fresidence2       = 99,
    weight_bear120    = 9,
    fwalk2            = 9,
    fbonemed2         = 9,
    fop2              = 99
  )

  dat_lbled <- data |>
    set_value_labels(.labels = config_valuelabs, .strict = FALSE) |>
    nolabel_to_na() |>
    set_na_values(.values = config_nalabs, .strict = FALSE)

  return(dat_lbled)
}


#' Create a patient journey dataset (ie. event and datetime)
#'
#' @param data A dataframe with datetime in wide format
#' @return A dataframe with events and dates in long format
pt_journey <- function(data) {
  journey_data <- data |>
    select(id, where(is.POSIXct), where(is.Date), e_dadmit, h_name, -predod) |>
    pivot_longer(
      cols = where(is.POSIXct) | where(is.Date),
      names_to = "event",
      values_to = "datetime"
    ) |>
    mutate(
      datetime = na_if(datetime, as.POSIXlt("1900-01-01 00:00:00", tz = "UTC"))
    ) |>
    mutate(
      event = factor(
        event,
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
        )
      )
    ) |>
    filter(!is.na(datetime)) |>
    arrange(id, datetime, event)

  return(journey_data)
}


#' Create a admission pattern variable for deciding date of hip fracture diagnosis
#'
#' @param data Patient journey data
#' @return A dataframe with TEDIS admission patterns
get_tedis <- function(data) {
  tmp_dat <- data |>
    filter(
      event %in% c(
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
    arrange(id, date(datetime), event) |>
    group_by(id) |>
    mutate(report_year = if_else(event == "start_date", year(datetime), NA)) |>
    fill(report_year, .direction = "downup") |>
    # keep start_date only when it's the only record
    filter(!(n() > 1 & event == "start_date")) |>
    mutate(event_index = row_number() - 1) |>
    mutate(lag_datetime = lag(datetime)) |>
    ungroup() |>
    mutate(duration = as.duration(interval(lag_datetime, datetime)))

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
      tmp_dat |> select(id, event, datetime),
      by = c("id" = "id", "dx_event" = "event")
    ) |>
    mutate(dx_date = date(datetime))

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
    tedis_info |> select(id, admittype, valid_tedis, dx_date),
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
    by = c("ahos_code" == "ahoscode")
  )
  return(labled_data)
}


#' Deduplicate records
#'
#' @param data Raw ANZHFR data
#' @return Deduplicated dataset
deduplicate_data <- function(data) {
  data <- data |> mutate(n_miss = rowSums(is_regular_na(data)))

  # all duplicate ids (incl. different sides)
  dup_ids <- data |>
    group_by(id) |>
    tally() |>
    filter(n > 1) |>
    pull(id)

  # duplicate ids (on same side)
  dup_ids_ss <- data |>
    group_by(id, side) |>
    tally() |>
    filter(n > 1) |>
    pull(id)

  dat_dd <- data |>
    # correct NZ hospital codes to make them unique identifiers.
    mutate(
      ahos_code = if_else(
        country == "nz",
        stringr::str_replace(ahos_code, "AU", "NZ"),
        ahos_code
      )
    ) |>
    # for duplicate ids that record different sides, add "sequence number" at the end of id
    group_by(id) |>
    arrange(start_date) |>
    mutate(
      id = if_else(
        (id %in% dup_ids) & !(id %in% dup_ids_ss),
        paste0(id, "_", row_number()),
        id
      )
    ) |>
    ungroup() |>
    # for duplicate ids that record same side, keep the record with least missing values.
    group_by(id) |>
    filter(n_miss == min(n_miss, na.rm = TRUE)) |>
    ungroup() |>
    select(-n_miss)

  return(dat_dd)
}

#' Clean up invalid datetime and typos
#'
#' @param data A R dataframe (duplicated raw data)
#' @return A R dataframe
clean_datetime <- function(data) {
  # New date cleaning process
  newdata <- data |>
    mutate(
      across(
        where(is.POSIXct) | where(is.Date),
        ~ na_if(.x, ymd("1900-01-01"))
      )
    ) |>
    rowwise() |>
    mutate(
      median_date = median(
        c(
          date(tarrdatetime),
          date(arrdatetime),
          date(depdatetime),
          date(admdatetimeop),
          date(sdatetime),
          gdate,
          wdisch,
          hdisch
        ),
        na.rm = TRUE
      )
    ) |>
    ungroup() |>
    filter(!is.na(median_date)) |>
    # Auto-correct date typo
    mutate(
      across(
        c(
          tarrdatetime,
          arrdatetime,
          depdatetime,
          admdatetimeop,
          sdatetime,
          gdate,
          wdisch,
          hdisch
        ),
        ~ if_else(
          .x %within% interval(median_date %m-% months(3), median_date %m+% months(3)),
          .x,
          update(.x, year = year(median_date))
        )
      )
    ) |>
    select(-median_date)

  return(newdata)
}


#' Clean up data errors and missing values
#'
#' @param raw_data A R dataframe (duplicated raw data)
#' @return A R dataframe
clean_data <- function(raw_data) {
  dat_clean <- raw_data |>
    mutate(report_year = year(start_date)) |>
    # confirm surgical indicator
    ## confirm surgical repair from other variables
    mutate(
      surg = if_else((!is.na(sdatetime) | !is.na(optype)), 2, surg)
    ) |>
    mutate(
      surg = case_when(
        !is.na(surg) ~ surg,
        year(start_date) < 2021 ~ 1,
        TRUE ~ 5
      )
    )

  return(dat_clean)
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
        labels = c("Introcapular", "Extracapsular")
      )
    ) |>
    # set variable labels
    set_variable_labels(
      age_cat5        = "5-year age group",
      sex_2l          = "Sex (2 levels)",
      asa_nhfd        = "ASA group (NHFD definition)",
      walk_nhfd       = "Walk score (NHFD definition)",
      ftype_nhfd      = "Fracture type (NHFD definition)",
      uresidence_nhfd = "Residence type (NHFD definition)",
      surg_yn         = "Surgical repair (Y/N)"
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
    # criteria 2 - remove overseas patients
    filter(a_pcode != 9998) |>
    # Need to revisit - retain records if postcode is missing.
    filter(
      !(ptype == 3 & (a_pcode == 0 | a_pcode == 8888 | a_pcode == 9999))
    ) |>
    # criteria 3 - date of death not before hip fracture diagnosis date
    filter(days_to_death >= 0 | is.na(days_to_death)) |>
    # criteria 4 - have a valid TEDIS pattern
    filter(valid_tedis == TRUE) |>
    # criteria 5 - had surgery for the hip fracture
    filter(surg_yn == "Surgical")

  return(selected_data)
}
