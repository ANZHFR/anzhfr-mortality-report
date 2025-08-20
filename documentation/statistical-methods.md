# Statistical Methods for ANZHFR Mortality Report

*Authors: Dr Xingzhong (Jason) Jin*

*Last update: 20 August 2025*

# 1. Introduction

The Australian and New Zealand Hip Fracture Registry (ANZHFR) is a clinical registry that collects data on the care provided and the outcomes of care, for older people admitted to hospital with a hip fracture in Australia and New Zealand. The ANZHFR aims to use data to improve performance and maximise outcomes for older people who have sustained a hip fracture.

This technical document outlines the data management and statistical methodology for developing a case-mix risk-adjusted mortality model following hip fracture surgery.

# 2. Data Source

The ANZHFR captures clinical care information about hip fracture patients across Australian and New Zealand hospitals. Australian data are linked with the National Death Index (NDI) by the Australian Institute of Health and Welfare (AIHW) on a routine basis to ascertain mortality status. In New Zealand, date of death is available in hospital information systems within a few days after death, and the data are linked with the National Mortality Collection within the New Zealand Ministry of Health to ensure accuracy of the collected mortality data.

## 2.2 Study Population

-   Inclusion criteria:
    -   Admitted between 1st January 2016 and 31st December 2024
    -   Patients aged between 50-110 years old
    -   Patient underwent surgical treatment for a hip fracture
-   Exclusion criteria:
    -   Overseas patients, defined as meeting at least one of the following criteria:
        -   The variable `apcode` (What was the postcode of the suburb of the usual residence of the patient?) is **9998 (Overseas).**
        -   The variable `ptype` (Patient payment status) is **3 (Overseas),** plus the `apcode` value is 0 or 8888 or 9999 or an invalid format.
    -   Hip fracture date could not be ascertained.
    -   Date of death is invalid (e.g., date of death before date of admission).

# 3. Data Quality

The following data quality checks were conducted before the mortality analysis.

## 3.1 Duplicate records

An `id` variable is designed to uniquely identify each hip fracture event. For each individual patient, only the first fracture of the same hip is recorded. In theory, one patient can only have two hip fracture records collected in the registry. When duplicated records are present on the same side of the hip, they are examined individually to exclude duplicated data entry due to interruption in clinical practice (e.g., a clinician enters half of the information but is interrupted by clinical load and subsequently returns and creates a new record to re-enter full information). The record that has relatively more missing values is dropped.

## 3.2 Ascertainment of datetime variables

The following datetime information is collected in the ANZHFR and is related to the acute hospital episode of hip fracture care:

| Variable        | Description                               |
|-----------------|-------------------------------------------|
| `tarrdatetime`  | Transferring hospital arrival date/time   |
| `arrdatetime`   | Operating hospital arrival date/time      |
| `depdatetime`   | Operating hospital ED departure date/time |
| `admdatetimeop` | In-patient fracture date/time             |
| `sdatetime`     | Hip fracture surgery date/time            |
| `gdate`         | Geriatric assessment date                 |
| `wdisch`        | Discharge date from acute ward            |

Typos and entry errors in the above datetime variables are systematically checked. The median of these datetime variables is used will as a reference for checking. We define the median ± 3 months as the likely period for acute care episode for hip fracture. Datetime information outside this time range is flagged as potential typos or data entry errors. As it is not practical nor feasible to cross-check all data entry errors against the hospital information systems at each participating hospital, we apply a systematic approach to correct typos in these datetime variables. When a datetime variable is flagged as potential typos, the data cleaning algorithm would propose to change its year component to match the year of the median (the reference). If the proposed correction falls in the acceptable acute care period (the median ± 3 months), the correction would be accepted. For example, if the median is 2019-05-13, and the `arrdatetime` appears to be 2018-04-21 (more than 1 year before the median), the proposed datetime after correction would be 2019-04-21, which is within the period between 2019-02-13 and 2019-08-13, therefore the `arrdatetime` , which is likely to be a typo, is changed to 2019-04-21 in the clean dataset. If a datetime variable is more than 2 years away from the median and the proposed datetime after correcting for the year component does not fall within acceptable acute care period, then the datetime will be deemed invalid and replaced as missing. 3.2.1 Datetime variable data cleaning

## **3.3 Ascertainment of surgical status**

The following variables are collected in the ANZHFR that are related to the surgical procedures for hip fracture:

| Variable | Description |
|------------------------------------|------------------------------------|
| `surg` | An indicator of whether the patient undergoing surgical repair for the hip fracture |
| `sdatetime` | Hip fracture surgery date/time |
| `optype` | Type of operation for the hip fracture |

The surgical repair indicator variable `surg` can take any of the following values: **1 (No)**, **2 (Yes)**, **3 (No – surgical fixation not clinically indicated)**, **4 (No – patient for palliation)** and **5 (No – other reason)**. We apply rules to ensure no missing value in the surgical repair indicator: if either variable `sdatetime` or `optype` is not missing, then `surg` value is set to **2 (Yes);** the rest of missing values in `surg` are set to be **1 (No)** for records collected prior to 2021, and **5 (No - other reason)** for records collected after 2021.

## 3.4 Ascertainment of hip fracture date

Hip fracture date is not directly recorded in the ANZHFR because of various onsets and care pathways of hip fracture, e.g., admitting at a local hospital then transferring to an operating hospital, or directly admitting to an operating hospital or fracture resulted from a fall in the in-patient ward at a hospital. We define hip fracture date/time based on the pattern of the following five timestamp variables:

**Transferring hospital arrival date/time (T)**, **Operating hospital ED arrival date/time (E)**, **Operating hospital ED departure date/time (D)**, **In-patient fracture date/time (I)** and **Hip fracture surgery date/time (S).**

The clinically probable sequence of these five timestamp variables should follow in the order of T → E → D → I → S. Records that do not follow the TEDIS order are deemed invalid and would not be used in the mortality analysis.

## 3.5 Ascertainment of death date

In Australia, the ANZHFR has undertaken probabilistic linkage of its Australian record data with the Australian Institute of Health and Welfare’s (AIHW) National Death Index (NDI) to accurately understand patient survival after hip fracture. This allows a more comprehensive and accurate reporting of this important outcome. The linkage rate was 97% in 2025.

In New Zealand, date of death is available in hospital information systems within a few days. This provides reasonable confidence that the New Zealand data on survival after hip fracture, collected in this way, is correct. Deterministic linking the registry data with the National Mortality Collection within the NZ Ministry of Health also ensure accuracy of the survival data.

The date of death obtained from data linkage is compared against the recorded hospital admission date or hip fracture date. A record with a date of death that occurs before the hospital admission date or hip fracture date is considered invalid (possibly due to inaccurate linkage) and the record is subsequently removed from the mortality analysis.

## 3.6 Other data cleaning steps

-   `sex` - Sex - replace as missing value if the original value is 3 **(Intersex or indeterminate)** or \*\*\*\* **9 (Not stated or inadequately described)**.
-   `asa` - American Society of Anesthesiologists (ASA) Physical Status Score - replace as missing value if the original value is **0** or **9 (Unknown).**
-   `walk` - Pre-admission walking ability - replace as missing value if the original value is **9 (Unknown)**.
-   `ftype` - Fracture type - replace as missing value if the original value is **9 (Unknown)**.
-   `uresidence` - Usual place of residence - replace as missing value if the original value is **3 (Other)** or **4 (Not known)**.

------------------------------------------------------------------------

# 4. Statistical Analysis

## 4.1 Case-mix adjustment logistic regression model

Numerous patient factors are associated with mortality after hip fracture. It is important that variation in patient factors is adjusted in the mortality analysis, particularly the analysis results are used to compare across hospitals or geographic regions. Case-mix adjustment logistic regression model has been widely used to report standardised mortality rates that allow benchmarking health services. We use a modified version of the [case-mixed logistic model used by the National Hip Fracture Database (NHFD)](https://boneandjoint.org.uk/Article/10.1302/2046-3758.69.BJR-2017-0020.R1) to calculate the predicted probabilities of **30-day** and **365-day** mortality after hip fracture for each eligible ANZHFR record. The predicted probabilities produced in the model are summed for each hospital/region to produce the expected number of deaths within the hospital/region. A standardised mortality ratio (SMR) is calculated using the number of observed and expected events for the hospital/region. The standardised mortality rate for the hospital/region is its standardised mortality ratio multiplied by the national average mortality rate.

$$
{Expected\: number\: of\: deaths} = \frac {\sum_{k=1}^{N}{Probability\: of\: death}}{Number\: of\: patients (N)}
$$

$$
{Standardised\: Mortality\: Ratio} = \frac{{Observed\: Number\: of\: Deaths}}{{Expected\: Number\: of\: Deaths}}
$$

$$
{Standardised\: Mortality\: Rate} = {Standardised\: Mortality\: Ratio} \times {National\: Average\: Mortality\: Rate} 
$$

## 4.2 **Case-mix adjustment variables**

The ANZHFR model contains the following six variables:

| Variable | Categories |
|------------------------------------|------------------------------------|
| Age | 5-year age groups from 50 to 99; patients aged ≥100 are grouped into one category due to small sample size. |
| Sex | ***Male*** and ***Female*** |
| ASA | Categorised into 3 groups: (i) **ASA 1-2**; (ii) **ASA 3**; (iii) **ASA 4-5**. |
| Pre-admission walking ability | Categorised into 3 groups: (i) **walks without aids**; (ii) **walks with 1 or 2 aids or frame**; and (iii) **Wheelchair or bedbound.** |
| Fracture type | Categorised into 2 groups: (i) **intracapsular**; (ii) **extracapsular.** |
| Usual place of residence | Categorised into 3 groups: (i) **own home**; (ii) **residential care.** |

The selection of variables corresponds to the variables included in the NHFD model: age; gender; American Society of Anesthesiologists (ASA) physical status grade; ability to walk indoors; fracture type; and source of admission.

## 4.3 Multiple imputations for missing values in case-mix variables

Despite meticulous checking on data collection, missing values or unknown values are present in the data. For example, ASA grade was missing or recorded as unknown in 3,970 (7.7%) of records in 2022-24. The proportion of missing values and unknowns may affect standardising mortality at the hospital/region level. Some hospitals may need to be removed in a complete-case analysis because of high proportion of missing values or unknowns. To address the issue of missing values and unknowns, we use multiple imputation by chained equation (MICE) in all surgical patients with a valid date of death. We use permuted mean matching (PMM) to create 10 imputed datasets and each with 10 iterations. The imputation matrix below is used to fit the MICE model:

|              | `age` | `sex` | `asa` | `walk` | `ftype` | `uresidence` | `afracture` | `mort30d` |
|--------|--------|--------|--------|--------|--------|--------|--------|--------|
| `age`        |       | \-    | \-    | \-     | \-      | \-           | \-          | \-        |
| `sex`        | x     |       | x     | x      | x       | x            | x           | x         |
| `asa`        | x     | \-    |       | x      | \-      | x            | \-          | x         |
| `walk`       | x     | x     | x     |        | x       | x            | x           | x         |
| `ftype`      | x     | x     | x     | x      |         | x            | x           | x         |
| `uresidence` | x     | x     | x     | x      | x       |              | x           | x         |
| `afracture`  | x     | x     | x     | x      | x       | x            |             | x         |
| `mort30d`    | \-    | \-    | \-    | \-     | \-      | \-           | \-          |           |

***Notes:***

*`age` - Age; `sex` - Sex; `asa` - ASA grade; `walk` - Pre-admission walking ability; `ftype` - Fracture type; `uresidence` - Usual place of residence; `afracture` - Atypical fracture; `mort30d` - 30-day mortality after hip fracture. First column lists the imputed variables. Each row lists the covariates (indicated by the`x` symbol) included in the imputed model for corresponding imputed variable. No missing values are in`age` and `mort30d`, so they are not imputed but are used to impute other variables.*

# 5. Mortality Data Visualisation

## 5.1 Temporal Change of Mortality Rates

Case-mix adjusted logistical model as described in Section 4 is used in the full ANZHFR data from 1st January 2016 to 31 December 2024. The standardisation reference is the average national mortality rate over the entire data collection period. Standardised mortality rates in different geographic regions over time are plotted together to allow visualisation of the trend. Australia and New Zealand data are modelled separately, but displayed side by side. In Australia, mortality rates for South Australia do not include all the hospitals for the period 2018-2020 because patient identifiers were not permitted to be collected for some hospitals, which mean the records were unable to be linked to the National Death Index (NDI), therefore are excluded from the model. Mortality rate for Tasmania is not reported for 2016 due to a very small number of records.

## 5.2 Caterpillar Plots

The 30-day and 365-day risk-adjusted mortality rates at each participating hospital, which are standardised against the national average mortality rate, are display in caterpillar plots. These plots use the same case mix adjustment model described in Section 4 but using the latest 3 years of registry data prior to the reporting to reflect the recency of hospital performance. For example, the 30-day mortality model for the 2025 report uses data collected between 2022-2024, and the 365-day mortality model uses data collected between 2021-2023. Each hospital is ranked according to the standardised mortality rate and the ‘legs’ of the caterpillar represent the 95% confidence interval, which is derived from the 95% confidence interval of the standard mortality ratio (SMR) that is estimated using Byar approximation.

$$
{Lower\: limit\: of\: SMR} = \frac{{Number\: of\: Observed\: Deaths}}{{Number\: of\: Expected\: Deaths}} \times (1-\frac{1}{9 \times {Number\: of\: Observed\: Deaths}} - \frac{1.96}{3 \times \sqrt{Number\: of\: Obeserved\: Deaths}})^3 
$$

$$
{Upper\: limit\: of\: SMR} = \frac{{Number\: of\: Observed\: Deaths} + 1}{{Number\: of\: Expected\: Deaths}} \times (1-\frac{1}{9 \times ({Number\: of\: Observed\: Deaths} + 1)} + \frac{1.96}{3 \times \sqrt{({Number\: of\: Obeserved\: Deaths} + 1)}})^3 
$$

## 5.3 Funnel Plots

The 30-day and 365-day risk-adjusted mortality rates are also presented in funnel plots using exactly the same case mix adjusted model and data for the caterpillar plots. In the funnel plots, each dot represents a hospital site, and the x-axis represents hospital surgical volume. The horizontal line represents the national average mortality rate over the three-year time period. The 95% (\~2 standard deviation) and 99.7% (\~3 standard deviation) control limits around the national average were obtained from inverse binomial distribution, expressing the uncertainty arising from sampling variability for the range of hospital sample sizes encountered. Outliers with poor or excellent performance can be seen as falling outside these control limits.

# 6. Identification of Outliers

Standardised mortality rates at hospital sites are classified into 3 groups: (i) Above average (above expected); (ii) **Average** (as expected); and (iii) Below average (below expected). In the caterpillar plots, hospitals with a 95% confidence interval that does not cross the national average are considered as outliers. In the funnel plots, hospitals fall out of the 95% control limits are considered as outliers.

# 7. References

1.  AIHW: Ben-Tovim D, Woodman R, Harrison JE, Pointer S, Hakendorf P & Henley G 2009. Measuring and reporting mortality in hospital patients. Cat. no. HSE 69. Canberra: AIHW.

2.  Tsang C, Boulton C, Burgon V, Johansen A, Wakeman R, Cromwell DA. Predicting 30-day mortality after hip fracture surgery: Evaluation of the National Hip Fracture Database case-mix adjustment model. Bone Joint Res. 2017 Sep;6(9):550-556.

3.  van Buuren S, Groothuis-Oudshoorn CGM. mice: Multivariate Imputation by Chained Equations in R. Journal of statistical software. 2011;45(3).

4.  Spiegelhalter DJ. Funnel plots for comparing institutional performance. Statistics in Medicine 2005;24(8):1185–202.
