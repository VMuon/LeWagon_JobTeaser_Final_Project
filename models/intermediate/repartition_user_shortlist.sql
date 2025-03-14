--WITH agg AS (
--SELECT shortlist_id, count(*) as nb_user
--FROM {{ ref('stg_jobteaser_lewagon__candidate_status_aggregated') }}
--GROUP BY shortlist_id
--)

--SELECT agg.nb_user, count(shortlist_id) as nb_shortlist
--FROM agg
--GROUP BY agg.nb_user
--ORDER BY agg.nb_user DESC



WITH nb_users AS (

SELECT 
  *,
  count(*) OVER (PARTITION BY shortlist_id) as nb_users_in_shortlist,
  SUM(CASE WHEN last_status_update = "approved" THEN 1 ELSE 0 END) OVER (PARTITION BY shortlist_id) as nb_approved_per_shortlist
FROM
  {{ ref('stg_jobteaser_lewagon__candidate_status_aggregated') }}
)
, 

agg AS (
SELECT 
  nb_users_in_shortlist, 
  count(distinct shortlist_id) as nb_shortlist,
  AVG(nb_approved_per_shortlist/nb_users_in_shortlist)*100 as avg_approved_rate
FROM nb_users
GROUP BY nb_users_in_shortlist
ORDER BY nb_users_in_shortlist ASC
)

SELECT 
    *, 
    CASE WHEN nb_users_in_shortlist < 21 THEN CAST(nb_users_in_shortlist AS string) ELSE "20+" END AS shortlist_bucket
FROM agg