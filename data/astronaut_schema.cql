CREATE TABLE astronauts (name TEXT PRIMARY KEY, dob DATE, birthplace TEXT, university_name TEXT);

CREATE TABLE astronauts_by_mission (astronaut_name TEXT, ship_name TEXT, mission_name TEXT, mission_start_date TIMESTAMP, mission_end_date TIMESTAMP, PRIMARY KEY(mission_name, astronaut_name));
