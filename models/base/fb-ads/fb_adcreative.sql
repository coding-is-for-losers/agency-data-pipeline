{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='FB Ads') %}

{% if accounts != [] %}

with fb_adcreative as (


	    {% for account in accounts %}
		   	SELECT 
			id,
			link_url object_url,
			url_tags,
			_sdc_sequence
			FROM `{{ target.project }}.fb_ads_{{account}}.adcreative`
		    {% if not loop.last %} UNION ALL {% endif %}
	   {% endfor %}
)

select creative_id, max(url) url
from (
  select
  id creative_id,
  lower(trim(regexp_replace(replace(replace(replace(replace(object_url,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url,
  first_value(_sdc_sequence) OVER (PARTITION BY id ORDER BY _sdc_sequence DESC) lv,
  _sdc_sequence
  from fb_adcreative
  )
where lv = _sdc_sequence
group by creative_id

{% endif %}
