# Data Hub Query

The following query is done by Data Hub to retrieve the Top-N statements:

```
SELECT DISTINCT usr,
        query,
        "catalog",
        "schema",
        query_type,
        accessed_metadata,
        create_time,
        end_time
FROM {audit_catalog}.{audit_schema}.completed_queries
WHERE 1 = 1
AND query_type  = 'SELECT'
AND create_time >= timestamp '{start_time}'
AND end_time < timestamp '{end_time}'
AND query_state  = 'FINISHED'
ORDER BY end_time desc
``