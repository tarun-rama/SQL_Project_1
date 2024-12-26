-- SQL Project --


SELECT * FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the data (spelling mistake)
-- 3. Null Values or blank values 
-- 4. Remove Any Columns 


CREATE TABLE layoffs_staging 
LIKE layoffs; 

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging 
SELECT * FROM layoffs;


SELECT * 
FROM layoffs_staging;

-- 1. Remove Duplicates
-- 2. Standardize the data (spelling mistake)
-- 3. Null Values or blank values 
-- 4. Remove Any Columns 

# 1 identify duplicates



WITH duplicate_cte AS 
(
	SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,
    stage,country,funds_raised_millions) AS row_num
	FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging 
WHERE company = 'Oda'; 
# make changes in partition like add loc, other cols

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


WITH duplicate_cte AS 
(
	SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,
    stage,country,funds_raised_millions) AS row_num
	FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2; 

INSERT INTO layoffs_staging2
SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,
    stage,country,funds_raised_millions) AS row_num
	FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM layoffs_staging2 
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2 
WHERE row_num > 1;

-- 2. Standardizing data

SELECT company, TRIM(company) 
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT company, TRIM(company) 
FROM layoffs_staging2;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2 
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry  --
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country 
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2 
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';



SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 
WHERE company LIKE 'Bally%';

SELECT t2.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;


SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

# -----------------------------------------------------------
# PERFORM EXPLORATORY DATA ANALYSIS ON THE CLEANED DATA------
# -----------------------------------------------------------

SELECT company, MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY MAX(total_laid_off) desc;


SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;



SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT SUM(total_laid_off) 
FROM layoffs_staging2
WHERE percentage_laid_off = 1;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 desc;

SELECT *FROM layoffs_staging2 ;

# after pandamic
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

# what industry was hit
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 desc;

# which country was hit the most
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country 
ORDER BY 2 desc;

SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY date
ORDER BY 1 desc;


SELECT `date`,company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY date,company
ORDER BY 1 desc;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 desc;

# stage the companies were in 

SELECT stage , SUM(total_laid_off)
FROM layoffs_staging2 
GROUP BY stage 
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;


WITH Rolling_Total AS 
(
	SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off) AS total_off
	FROM layoffs_staging2
	WHERE SUBSTRING(`date`,1,7) IS NOT NULL
	GROUP BY `MONTH`
	ORDER BY 1 ASC
)
SELECT `MONTH`, total_off,
SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off) 
FROM layoffs_staging2 
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year(company,years,total_laid_off) AS 
(
	SELECT company, YEAR(`date`), SUM(total_laid_off) 
	FROM layoffs_staging2 
	GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS(
SELECT *, dense_ranK() OVER (partition by years ORDER BY total_laid_off desc) aS ranking
FROM Company_Year
WHERE years is not null
)
SELECT *
FROM Company_Year_Rank
WHERE ranking <=5;












































































































