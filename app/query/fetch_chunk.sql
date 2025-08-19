-- fetch_chunk.sql
-- This query now uses named parameters for offset and chunk size

SELECT * FROM dbo.vw_ReportStatus_test
WHERE invoice_date < '2025-05-21'
ORDER BY {id_column}
OFFSET :offset ROWS
FETCH NEXT :chunk_size ROWS ONLY;