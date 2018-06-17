create or replace table `agency_data_pipeline`.`ga_proc`
  
  as (
    -- depends_on: `adp-apprenticeship`.`agency_data_pipeline`.`ga_conversions`




with ga_report as (

	    

	    	
	    	
		   	SELECT
		   	'cifl' as bigquery_name,
		   	'Google Analytics' as lookup_platform,
			lower(trim(regexp_replace(replace(replace(replace(replace(CONCAT(hostname,landingpagepath),'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url,
			date,
			lower(source) source,
			lower(medium) medium,
			sessions,
			## goal completion columns
			
				
					cast(goal1completions as int64) 
					 
					 as goal_completions,  
				
			
			_sdc_sequence,
			first_value(_sdc_sequence) OVER (PARTITION BY hostname, landingpagepath, date, source, medium ORDER BY _sdc_sequence DESC) lv
			FROM `adp-apprenticeship.ga_cifl.report` 

		    
	   

)


SELECT 
bigquery_name,
lookup_platform,
date,
url,
source,
medium,
sum(sessions) sessions,
sum(goal_completions) goal_completions
FROM ga_report
where lv = _sdc_sequence
group by bigquery_name, lookup_platform, date, url, source, medium


  );

    