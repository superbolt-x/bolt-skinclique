{{ config (
    alias = target.database + '_zenoti_daily_performance'
)}}
    
{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}

WITH initial_data as
  (SELECT *, invoice_item_created_date_and_time::date as date, {{ get_date_parts('date') }} 
  FROM {{ source('azure_raw_superbolt','sales_fact_consolidated') }} 
  WHERE _fivetran_deleted IS false),

  final_data as
  ({%- for date_granularity in date_granularity_list %}    
  SELECT 
      '{{date_granularity}}' as date_granularity,
      {{date_granularity}} as date,
      service_name, service_category, product_name, product_category,
      COUNT(appointment_id) as appointments_requested,
      COUNT(CASE WHEN appointment_status IN ('Confirmed','Closed','Open') THEN appointment_id END) as appointments_booked,
      COUNT(CASE WHEN appointment_cancel_or_no_show_status = 'Fulfilled Appointment' THEN appointment_id END) as visits,
      COALESCE(SUM(total_adjusted_revenue),0) as total_adjusted_revenue,
      COALESCE(SUM(total_revenue),0) as total_revenue
      FROM initial_data
      GROUP BY 1,2,3,4,5,6
      {% if not loop.last %}UNION ALL
      {% endif %}
  {% endfor %})

SELECT 
  date_granularity,
  date,
  service_name, 
  service_category, 
  product_name, 
  product_category,
  appointments_requested,
  appointments_booked,
  visits,
  total_adjusted_revenue,
  total_revenue
FROM final_data
