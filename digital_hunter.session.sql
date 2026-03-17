SELECT signal_type, COUNT(*) AS count
FROM intel_signals
GROUP BY signal_type
ORDER BY count DESC