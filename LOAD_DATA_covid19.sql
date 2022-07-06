-- Create database 
CREATE SCHEMA covid_19_2;


-- Create table to hold covid deaths information 
DROP TABLE IF EXISTS coviddeaths;
CREATE TABLE coviddeaths (
iso_code TEXT,
continent TEXT,
location TEXT,
covid_date DATE,
population INT,
total_cases INT,
new_cases INT,
new_cases_smoothed DOUBLE,
total_deaths INT,
new_deaths INT,
new_deaths_smoothed DOUBLE,
total_cases_per_million DOUBLE,
new_cases_per_million DOUBLE,
new_cases_smoothed_per_million DOUBLE,
total_deaths_per_million DOUBLE,
new_deaths_per_million DOUBLE,
new_deaths_smoothed_per_million DOUBLE,
reproduction_rate DOUBLE,
icu_patients INT,
icu_patients_per_million DOUBLE,
hosp_patients INT,
hosp_patients_per_million DOUBLE,
weekly_icu_admissions INT,
weekly_icu_admissions_per_million DOUBLE,
weekly_hosp_admissions INT,
weekly_hosp_admissions_per_million DOUBLE
);


-- Create table to hold covid vaccinations information 
DROP TABLE IF EXISTS covidvaccinations;
CREATE TABLE covidvaccinations (
iso_code TEXT,
continent TEXT,
location TEXT,
covid_date DATE,
total_tests INT,
new_tests INT,
total_tests_per_thousand DOUBLE,
new_tests_per_thousand DOUBLE,
new_tests_smoothed INT,
new_tests_smoothed_per_thousand DOUBLE,
positive_rate DOUBLE,
tests_per_case DOUBLE,
tests_units TEXT,
total_vaccinations INT,
people_vaccinated INT,
people_fully_vaccinated INT,
total_boosters INT,
new_vaccinations INT,
new_vaccinations_smoothed INT,
total_vaccinations_per_hundred DOUBLE,
people_vaccinated_per_hundred DOUBLE,
people_fully_vaccinated_per_hundred DOUBLE,
total_boosters_per_hundred DOUBLE,
new_vaccinations_smoothed_per_million INT,
new_people_vaccinated_smoothed INT,
new_people_vaccinated_smoothed_per_hundred DOUBLE,
stringency_index DOUBLE,
population_density DOUBLE,
median_age DOUBLE,
aged_65_older DOUBLE,
aged_70_older DOUBLE,
gdp_per_capita DOUBLE,
extreme_poverty DOUBLE,
cardiovasc_death_rate DOUBLE,
diabetes_prevalence DOUBLE,
female_smokers DOUBLE,
male_smokers DOUBLE,
handwashing_facilities DOUBLE,
hospital_beds_per_thousand DOUBLE,
life_expectancy DOUBLE,
human_development_index DOUBLE,
excess_mortality_cumulative_absolute DOUBLE SIGNED,
excess_mortality_cumulative DOUBLE SIGNED,
excess_mortality DOUBLE SIGNED,
excess_mortality_cumulative_per_million DOUBLE SIGNED 
);


-- verify tables successfully created and describe number of columns and data types
DESC coviddeaths;
DESC covidvaccinations;


SELECT @@warning_count;
-- see current server limitations on displaying number of warning messages 
SHOW VARIABLES LIKE 'max_error_count';
-- SET new warning message storage limits for investigating 
SET max_error_count = 10000;
SHOW VARIABLES LIKE 'max_error_count';


-- Turn ON server-side LOAD DATA LOCAL configuration to successfully load data into tables from input files 
-- NOTE: client-side configuration already set under Connections 
SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;


-- Load local data into coviddeaths table from csv file 
LOAD DATA LOCAL INFILE '/Users/mubeen/Documents/Projects/SQL/Covid-19 (2)/coviddeaths.csv' INTO TABLE coviddeaths 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES  
(iso_code,continent,location,@datevar,@population,@total_cases,@new_cases,@new_cases_smoothed,@total_deaths,@new_deaths,
@new_deaths_smoothed,@total_cases_per_million,@new_cases_per_million,@new_cases_smoothed_per_million,@total_deaths_per_million,
@new_deaths_per_million,@new_deaths_smoothed_per_million,@reproduction_rate,@icu_patients,@icu_patients_per_million,@hosp_patients,
@hosp_patients_per_million,@weekly_icu_admissions,@weekly_icu_admissions_per_million,@weekly_hosp_admissions,@weekly_hosp_admissions_per_million)  

/* Perform preprocessing transformations on input data to prevent errors/warnings upon loading */
-- Convert input date string to proper format
-- If logic to replace all empty input cells with NULLS. otherwise, keep original value  
-- Alternative would have been to use UPDATE statement after data import
SET 
	covid_date = STR_TO_DATE(@datevar, '%m/%d/%y'), 
	population = (CASE WHEN @population = '' THEN NULL ELSE @population END),
	total_cases = (CASE WHEN @total_cases = '' THEN NULL ELSE @total_cases END),
	new_cases = (CASE WHEN @new_cases = '' THEN NULL ELSE @new_cases END),
	new_cases_smoothed = (CASE WHEN @new_cases_smoothed = '' THEN NULL ELSE @new_cases_smoothed END),
	total_deaths = (CASE WHEN @total_deaths = '' THEN NULL ELSE @total_deaths END),
	new_deaths = (CASE WHEN @new_deaths = '' THEN NULL ELSE @new_deaths END),
	new_deaths_smoothed = (CASE WHEN @new_deaths_smoothed = '' THEN NULL ELSE @new_deaths_smoothed END),
	total_cases_per_million = (CASE WHEN @total_cases_per_million = '' THEN NULL ELSE @total_cases_per_million END),
	new_cases_per_million = (CASE WHEN @new_cases_per_million = '' THEN NULL ELSE @new_cases_per_million END),
	new_cases_smoothed_per_million = (CASE WHEN @new_cases_smoothed_per_million = '' THEN NULL ELSE @new_cases_smoothed_per_million END),
	total_deaths_per_million = (CASE WHEN @total_deaths_per_million = '' THEN NULL ELSE @total_deaths_per_million END),
	new_deaths_per_million = (CASE WHEN @new_deaths_per_million = '' THEN NULL ELSE @new_deaths_per_million END),
	new_deaths_smoothed_per_million = (CASE WHEN @new_deaths_smoothed_per_million = '' THEN NULL ELSE @new_deaths_smoothed_per_million END),
	reproduction_rate = (CASE WHEN @reproduction_rate = '' THEN NULL ELSE @reproduction_rate END),
	icu_patients = (CASE WHEN @icu_patients  = '' THEN NULL ELSE @icu_patients  END),
	icu_patients_per_million = (CASE WHEN @icu_patients_per_million = '' THEN NULL ELSE @icu_patients_per_million END),
	hosp_patients = (CASE WHEN @hosp_patients = '' THEN NULL ELSE @hosp_patients END),
	hosp_patients_per_million = (CASE WHEN @hosp_patients_per_million = '' THEN NULL ELSE @hosp_patients_per_million END),
	weekly_icu_admissions = (CASE WHEN @weekly_icu_admissions = '' THEN NULL ELSE @weekly_icu_admissions END),
	weekly_icu_admissions_per_million = (CASE WHEN @weekly_icu_admissions_per_million = '' THEN NULL ELSE @weekly_icu_admissions_per_million END),
	weekly_hosp_admissions = (CASE WHEN @weekly_hosp_admissions = '' THEN NULL ELSE @weekly_hosp_admissions END),
	weekly_hosp_admissions_per_million = (CASE WHEN @weekly_hosp_admissions_per_million = '' THEN NULL ELSE @weekly_hosp_admissions_per_million END)
;


-- Display any warning messages from loading data
SHOW WARNINGS;


-- Load local data into covidvaccinations table from csv file 
LOAD DATA LOCAL INFILE '/Users/mubeen/Documents/Projects/SQL/Covid-19 (2)/covidvaccinations.csv' INTO TABLE covidvaccinations 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES 
(iso_code,continent,location,@datevar,@total_tests,@new_tests,@total_tests_per_thousand,@new_tests_per_thousand,@new_tests_smoothed,
@new_tests_smoothed_per_thousand,@positive_rate,@tests_per_case,tests_units,@total_vaccinations,@people_vaccinated,@people_fully_vaccinated,
@total_boosters,@new_vaccinations,@new_vaccinations_smoothed,@total_vaccinations_per_hundred,@people_vaccinated_per_hundred,@people_fully_vaccinated_per_hundred,
@total_boosters_per_hundred,@new_vaccinations_smoothed_per_million,@new_people_vaccinated_smoothed,@new_people_vaccinated_smoothed_per_hundred,
@stringency_index,@population_density,@median_age,@aged_65_older,@aged_70_older,@gdp_per_capita,@extreme_poverty,@cardiovasc_death_rate,@diabetes_prevalence,
@female_smokers,@male_smokers,@handwashing_facilities,@hospital_beds_per_thousand,@life_expectancy,@human_development_index,@excess_mortality_cumulative_absolute,
@excess_mortality_cumulative,@excess_mortality,@excess_mortality_cumulative_per_million)

/* Perform preprocessing transformations on input data to prevent errors/warnings upon loading */
-- Convert input date string to proper format
-- If logic to replace all empty input cells with NULLS. otherwise, keep original value
SET
	covid_date = STR_TO_DATE(@datevar, '%m/%d/%y'), 
    total_tests = (CASE WHEN @total_tests = '' THEN NULL ELSE @total_tests END),
	new_tests = (CASE WHEN @new_tests = '' THEN NULL ELSE @new_tests END),
	total_tests_per_thousand = (CASE WHEN @total_tests_per_thousand = '' THEN NULL ELSE @total_tests_per_thousand END),
	new_tests_per_thousand = (CASE WHEN @new_tests_per_thousand = '' THEN NULL ELSE @new_tests_per_thousand END),
	new_tests_smoothed = (CASE WHEN @new_tests_smoothed = '' THEN NULL ELSE @new_tests_smoothed END),
	new_tests_smoothed_per_thousand = (CASE WHEN @new_tests_smoothed_per_thousand = '' THEN NULL ELSE @new_tests_smoothed_per_thousand END),
	positive_rate = (CASE WHEN @positive_rate = '' THEN NULL ELSE @positive_rate END),
	tests_per_case = (CASE WHEN @tests_per_case = '' THEN NULL ELSE @tests_per_case END),
	total_vaccinations = (CASE WHEN @total_vaccinations = '' THEN NULL ELSE @total_vaccinations END),
	people_vaccinated = (CASE WHEN @people_vaccinated = '' THEN NULL ELSE @people_vaccinated END),
	people_fully_vaccinated = (CASE WHEN @people_fully_vaccinated = '' THEN NULL ELSE @people_fully_vaccinated END),
	total_boosters = (CASE WHEN @total_boosters = '' THEN NULL ELSE @total_boosters END),
	new_vaccinations = (CASE WHEN @new_vaccinations = '' THEN NULL ELSE @new_vaccinations END),
	new_vaccinations_smoothed = (CASE WHEN @new_vaccinations_smoothed = '' THEN NULL ELSE @new_vaccinations_smoothed END),
	total_vaccinations_per_hundred = (CASE WHEN @total_vaccinations_per_hundred = '' THEN NULL ELSE @total_vaccinations_per_hundred END),
	people_vaccinated_per_hundred = (CASE WHEN @people_vaccinated_per_hundred = '' THEN NULL ELSE @people_vaccinated_per_hundred END),
	people_fully_vaccinated_per_hundred = (CASE WHEN @people_fully_vaccinated_per_hundred = '' THEN NULL ELSE @people_fully_vaccinated_per_hundred END),
	total_boosters_per_hundred = (CASE WHEN @total_boosters_per_hundred = '' THEN NULL ELSE @total_boosters_per_hundred END),
	new_vaccinations_smoothed_per_million = (CASE WHEN @new_vaccinations_smoothed_per_million = '' THEN NULL ELSE @new_vaccinations_smoothed_per_million END),
	new_people_vaccinated_smoothed = (CASE WHEN @new_people_vaccinated_smoothed = '' THEN NULL ELSE @new_people_vaccinated_smoothed END),
	new_people_vaccinated_smoothed_per_hundred = (CASE WHEN @new_people_vaccinated_smoothed_per_hundred = '' THEN NULL ELSE @new_people_vaccinated_smoothed_per_hundred END),
	stringency_index = (CASE WHEN @stringency_index = '' THEN NULL ELSE @stringency_index END),
	population_density = (CASE WHEN @population_density = '' THEN NULL ELSE @population_density END),
	median_age = (CASE WHEN @median_age = '' THEN NULL ELSE @median_age END),
	aged_65_older = (CASE WHEN @aged_65_older = '' THEN NULL ELSE @aged_65_older END),
	aged_70_older = (CASE WHEN @aged_70_older = '' THEN NULL ELSE @aged_70_older END),
	gdp_per_capita = (CASE WHEN @gdp_per_capita = '' THEN NULL ELSE @gdp_per_capita END),
	extreme_poverty = (CASE WHEN @extreme_poverty = '' THEN NULL ELSE @extreme_poverty END),
	cardiovasc_death_rate = (CASE WHEN @cardiovasc_death_rate = '' THEN NULL ELSE @cardiovasc_death_rate END),
	diabetes_prevalence = (CASE WHEN @diabetes_prevalence = '' THEN NULL ELSE @diabetes_prevalence END),
	female_smokers = (CASE WHEN @female_smokers = '' THEN NULL ELSE @female_smokers END),
	male_smokers = (CASE WHEN @male_smokers = '' THEN NULL ELSE @male_smokers END),
	handwashing_facilities = (CASE WHEN @handwashing_facilities = '' THEN NULL ELSE @handwashing_facilities END),
	hospital_beds_per_thousand = (CASE WHEN @hospital_beds_per_thousand = '' THEN NULL ELSE @hospital_beds_per_thousand END),
	life_expectancy = (CASE WHEN @life_expectancy = '' THEN NULL ELSE @life_expectancy END),
	human_development_index = (CASE WHEN @human_development_index = '' THEN NULL ELSE @human_development_index END),
	excess_mortality_cumulative_absolute = (CASE WHEN @excess_mortality_cumulative_absolute = '' THEN NULL ELSE @excess_mortality_cumulative_absolute END),
	excess_mortality_cumulative = (CASE WHEN @excess_mortality_cumulative = '' THEN NULL ELSE @excess_mortality_cumulative END),
	excess_mortality = (CASE WHEN @excess_mortality = '' THEN NULL ELSE @excess_mortality END),
	excess_mortality_cumulative_per_million = (CASE WHEN @excess_mortality_cumulative_per_million = '' THEN NULL ELSE @excess_mortality_cumulative_per_million END)
;


-- Check successful loading of data
SELECT*
FROM coviddeaths;

SELECT *
FROM covidvaccinations;



