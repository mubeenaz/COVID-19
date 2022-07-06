
/* Data broken down by countries */

-- Select data of interest 
SELECT location, covid_date, population, total_cases, new_cases, total_deaths
FROM coviddeaths
WHERE continent IS NOT NULL
ORDER BY 1, 2;


-- Of the total positive cases each day, what percentage of people died in the U.S. (death percentage per day) 
-- represents likelihood of dying if you contract covid in your country 
SELECT location, covid_date, total_cases, total_deaths, (total_deaths/total_cases)*100 DeathPercentage 
FROM coviddeaths
WHERE location LIKE '%states%'
ORDER BY 1, 2;


-- Total Cases vs. U.S. population
-- represents what percentage of population got infected per day
SELECT location, covid_date, population, total_cases, (total_cases/population)*100 PercentPopulationInfected
FROM coviddeaths
WHERE continent IS NOT NULL AND location LIKE '%states%'
ORDER BY 1, 2;


-- Countries with highest infection rate compared to population 
SELECT location, population, MAX(total_cases) HighestInfectionCount, MAX((total_cases/population)*100) PercentPopulationInfected
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY 1, 2
ORDER BY 4 DESC;


-- check sql safe update mode variable before performing below UPDATE operation
SHOW VARIABLES LIKE 'sql_safe_updates';
SET sql_safe_updates = 0;
SHOW VARIABLES LIKE 'sql_safe_updates';			-- mode successfully turned OFF


-- UPDATE coviddeaths table to replace all empty cells in continent field with NULLs
UPDATE coviddeaths SET
continent = NULL 
WHERE continent = '';


-- check to see if empty continent cells successfully replaced by NULLs
SELECT *
FROM coviddeaths
WHERE continent IS NULL;


-- Turn sql safe update mode variable back on for safety precautions 
SET sql_safe_updates = 1;
SHOW VARIABLES LIKE 'sql_safe_updates';			-- mode successfully turned ON


-- Countries with highest total death count compared to population 
SELECT location, population, MAX(total_deaths) TotalDeathCount, MAX((total_deaths/population)*100) DeathPercentage
FROM coviddeaths
WHERE continent IS NOT NULL 
GROUP BY 1, 2
ORDER BY 3 DESC;


/* Data broken down by continents */

-- Continents with highest total death count compared to population 
SELECT location, population, MAX(total_deaths) TotalDeathCount
FROM coviddeaths
WHERE continent IS NULL AND location NOT IN ('World', 'Upper middle income', 'High income', 'Lower middle income', 'European Union', 'Low income', 'International')
GROUP BY 1, 2
ORDER BY 3 DESC;


/* The following queries pull global numbers */

-- Totals by day
SELECT covid_date, SUM(new_cases) new_cases_pday_world, SUM(new_deaths) new_deaths_pday_world, (SUM(new_deaths)/SUM(new_cases))*100 DeathPercentagePerDayWorld
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- Totals all-time 
SELECT SUM(new_cases) total_cases_world, SUM(new_deaths) total_deaths_world, (SUM(new_deaths)/SUM(new_cases))*100 DeathPercentageWorld
FROM coviddeaths
WHERE continent IS NOT NULL
ORDER BY 1;


-- Total Population vs Vaccinations 
WITH populationVSvaccination AS (SELECT dea.continent, dea.location, dea.covid_date, dea.population, vac.new_vaccinations, 		-- Running sum of total vaccinations by day
			SUM(new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.covid_date) running_total_vaccinations
			FROM coviddeaths dea
			JOIN covidvaccinations vac
				ON dea.location = vac.location AND dea.covid_date = vac.covid_date
			WHERE dea.continent IS NOT NULL 
			ORDER BY 2, 3), 
	total_doses_by_country AS (SELECT location, population, MAX(running_total_vaccinations) total_doses_given		-- Total vaccine doses administered in each country 
			FROM populationVSvaccination
			GROUP BY 1, 2
			ORDER BY 3 DESC)
        
SELECT SUM(population) world_population, SUM(total_doses_given) world_vax_doses_given		-- Worldwide vaccine doses and population totals 
FROM total_doses_by_country;


-- Create Views to be used for visualizations 
CREATE VIEW DeathsByContinent AS
SELECT location, population, MAX(total_deaths) TotalDeathCount
FROM coviddeaths
WHERE continent IS NULL AND location NOT IN ('World', 'Upper middle income', 'High income', 'Lower middle income', 'European Union', 'Low income', 'International')
GROUP BY 1, 2
ORDER BY 3 DESC;
    
CREATE VIEW CovidNumbersWorld AS
SELECT SUM(new_cases) total_cases_world, SUM(new_deaths) total_deaths_world, (SUM(new_deaths)/SUM(new_cases))*100 DeathPercentageWorld
FROM coviddeaths
WHERE continent IS NOT NULL
ORDER BY 1;

CREATE VIEW PercentPopulationInfected_Country AS
SELECT location, population, MAX(total_cases) HighestInfectionCount, MAX((total_cases/population)*100) PercentPopulationInfected
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY 1, 2
ORDER BY 4 DESC;

CREATE VIEW PercentPopulationInfected_Day AS
SELECT location, covid_date, population, total_cases, (total_cases/population)*100 PercentPopulationInfected
FROM coviddeaths
WHERE continent IS NOT NULL AND location LIKE '%states%'
ORDER BY 1, 2;
