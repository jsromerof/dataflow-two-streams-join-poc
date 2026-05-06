Technical stack:
    Apache Beam 2.71
    Python 3.11
    Postgres DB
Requirements:
    - I need to read from two kafka topics : chassis and english_statement and write them in postgres.
    - Logically chassis is a entity parent of english_statement
    - Chassis message has the following structure: {"chassis_id" : "id" , "chassis_number":"number"}
    english_statement message has the following structure: {"english_statement_id" : "id" , "chassis_id" : "chassis_id" , "description":"description"}
    - In postgres I have the three chassis and english_statement_stage with the same fields of the kafka message.
    - Aditionally i have a table in postgres called chassis_proccessed which have the same structure of chassis table.
    - I need a pardo function that execute this logic :
        - If a get an event from english_statement i want to make a lookup in chassis table with the field "chassis_id".
        - If i get a result i will add to the english_statement message the field "chassis_number" that we get in the returned record from the database and persist it in english_statement table.
        - If im not get a result i make a lookup over the table chassis_proccessed and see if the chassis already was processed by the pardo function . If so, persist english_statement in postgres.
        - If no one of the two conditions above is true then put the english_statement into the postgres table: english_statement_stage
        - If we get an event from chassis we want to put the event in the table chassis_proccessed and after that i will make a lookup per chassis_id in the table english_statement_stage .
        - If a get results from english_statement_stage i want to create to persist those english statements into postgres , and finally  make this in one transaction : insert chassis in chassis table and delete this chassis in chassis_proccessed.
        - If there is not result we put the chassis in the postgres table: chassis.