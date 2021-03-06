create or replace table `agency_data_pipeline`.`ga_conversions`
  
  as (
    SELECT 
site,
bigquery_name,
platform,
goal_name,
goal_type,
account,
time_of_entry
FROM `adp-apprenticeship`.`agency_data_pipeline`.`conversion_goals_proc`
WHERE platform = 'Google Analytics'
  );

    