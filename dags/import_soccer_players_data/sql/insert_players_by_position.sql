INSERT INTO {{params.table_name}}
SELECT 
    *
FROM soccer_players
WHERE position = '{{ params.position }}'