
CREATE DATABASE planes;

use planes;

CREATE TABLE planes
(   
    pri_key int PRIMARY KEY,
    tailnum varchar(255) NOT NULL ,
    year SMALLINT,
    type varchar(255),
    manufacturer varchar(255),
    model varchar(255),	
    engines SMALLINT,	
    seats SMALLINT,	
    speed varchar(255),	
    engine VARCHAR(255)

);
