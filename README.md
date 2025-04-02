# ANZHFR Mortality Report

This project aims to create a reproducible analysis pipeline to generate a mortality report with the data from the ANZ Hip Fracture Registry.

## Key Contributors

-   Project Lead: Lara Harvey

-   Lead Data Scientist: Xingzhong (Jason) Jin 

## Input Data

Data source:

`smb://vmfile01.ad.neura.edu.au/groups/fipg/2. ANZHFR projects/10. Jason/anzhfr_mortality_report/Data/cohort_nodod.sas7bdat`

## Analysis

-   Adjusted mortality rate at 3O days by year for Australian states and New Zealand (2O16-2O22)
-   Adjusted mortality rate at 365 days by year for Australian states and New Zealand (2O16-2O22)
-   Funnel plot of adjusted mortality rate at 3O days – New Zealand hospitals (2O20-2O22)
-   Caterpillar plot of adjusted mortality rate at 3O days – New Zealand hospitals (2O20-2O22)
-   Funnel plot of adjusted mortality rate at 365 days – New Zealand hospitals (2O20 -2O22)
-   Caterpillar plot of adjusted mortality rate at 365 days – New Zealand hospitals (2O20 -2O22)
-   Funnel plot of adjusted mortality rate at 3O days – Australian hospitals (2O20-2O22)
-   Caterpillar plot of adjusted mortality rate at 3O days – Australian hospitals (2O20-2O22)
-   Funnel plot of adjusted mortality rate at 365 days – Australian hospitals (2O20 -2O22)
-   Caterpillar plot of adjusted mortality rate at 365 day – Australian hospitals (2O20 -2O22)

## Repository content

-   `renv.lock`: list of packages used in the analysis
-   `R`: analysis scripts and helper functions
-   `output`: tables, figures and others generated from scripts
-   `report`: where generated reports are saved
