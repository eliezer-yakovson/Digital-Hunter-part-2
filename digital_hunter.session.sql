SELECT entity_id, COUNT(*) AS count
FROM intel_signals
WHERE priority_level = 99
GROUP BY entity_id
ORDER BY count DESC
LIMIT 3
