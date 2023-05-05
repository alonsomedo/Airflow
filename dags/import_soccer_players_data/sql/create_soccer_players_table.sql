CREATE TABLE IF NOT EXISTS {{ params.table_name }} 
(
    player_id int,
    name varchar(100),
    age int,
    number int,
    position varchar(100),
    photo varchar(200) 
);