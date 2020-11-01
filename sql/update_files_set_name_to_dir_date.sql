UPDATE files
SET Name = dir.CreatedDate
FROM [./] files
INNER JOIN [./] dir on dir.path = files.path AND dir.type = 'DIR'
WHERE files.type = 'FILE'