WITH agg AS (
SELECT shortlist_id, count(*) as nb_user
FROM {{ ref('stg_jobteaser_lewagon__candidate_status_aggregated') }}
GROUP BY shortlist_id
)

SELECT agg.nb_user, count(shortlist_id) as nb_shortlist
FROM agg
GROUP BY agg.nb_user
ORDER BY agg.nb_user DESC