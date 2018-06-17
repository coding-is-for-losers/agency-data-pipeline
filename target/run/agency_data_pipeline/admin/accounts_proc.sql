create or replace table `agency_data_pipeline`.`accounts_proc`
  
  as (
    SELECT
site,
bigquery_name,
account,
platform
FROM  (
 
	SELECT  
	site,
	bigquery_name,
	account,
	platform,
	time_of_entry,
	first_value(time_of_entry) OVER (PARTITION BY site ORDER BY time_of_entry DESC) lv
	FROM `adp-apprenticeship.agency_data_pipeline.accounts`
) 

WHERE lv = time_of_entry
  );

    