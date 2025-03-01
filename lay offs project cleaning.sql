select * from layoffs;


create table layoffs_staging
like layoffs;

insert layoffs_staging
select * from  layoffs;


select * from layoffs_staging;

#finding duplicates
with rnk_duplicate as(
       select *,
              row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) rank_count
      from layoffs_staging)
      
select * from rnk_duplicate
where rank_count>1;

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
  `rank_count` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

#creating 2nd table so orignal data will remain same

insert into layoffs_staging2
select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) rank_count
 from layoffs_staging;

#checking dulicates
select * from layoffs_staging2
where rank_count>1;

SET SQL_SAFE_UPDATES = 1;

#deleting duplicates
DELETE 
FROM layoffs_staging2
where rank_count>1;

use layoffs;

#removing/triming white spaces in company column
select company,trim(company) from layoffs_staging2;

update layoffs_staging
set company=trim(company);


#checking company column
select distinct(industry) from layoffs_staging2
where industry like'Crypto%';

#updating crypto industry column 
update layoffs_staging2
set industry ='Crypto'
where industry like 'Crypto %';


select * from layoffs_staging2;


select distinct(country) from layoffs_staging2
order by 1;

#problem where country is united states
select distinct(country) from layoffs_staging2
where country like'united states%';

#updating country  column
update layoffs_staging2
set country ='united states'
where country like'united states%';

#checking date 
select `date` ,
str_to_DATE(`date`,'%m/%d/%Y')
from layoffs_staging2;

#updating date text form to date date fomate
update layoffs_staging2
set `date`=str_to_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

#checking the total_laid_off and percentage_laid_off is null or not
select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;


# these column wont help at all so deleting them is best option
delete
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;


select * from layoffs_staging2;


#drop the column rank 
alter table layoffs_staging2
drop column rank_count;