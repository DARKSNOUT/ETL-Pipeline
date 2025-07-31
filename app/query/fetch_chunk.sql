SELECT * FROM test_analysis_data WHERE {id_column} > :last_id 
ORDER BY {id_column} 
OFFSET 0 ROWS
FETCH NEXT :chunk_size ROWS ONLY;
