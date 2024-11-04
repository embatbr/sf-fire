DROP SCHEMA IF EXISTS landing CASCADE;

DROP SCHEMA IF EXISTS business CASCADE;


CREATE SCHEMA landing;

CREATE TABLE landing.fire_incidents (
    "Incident Number" TEXT,
    "Exposure Number" INT4,
    "ID" TEXT,
    "Address" TEXT,
    "Incident Date" DATE,
    "Call Number" TEXT,
    "Alarm DtTm" TIMESTAMP,
    "Arrival DtTm" TIMESTAMP,
    "Close DtTm" TIMESTAMP,
    "City" TEXT,
    zipcode TEXT,
    "Battalion" TEXT,
    "Station Area" TEXT,
    "Box" TEXT,
    "Suppression Units" INT4,
    "Suppression Personnel" INT4,
    "EMS Units" INT4,
    "EMS Personnel" INT4,
    "Other Units" INT4,
    "Other Personnel" INT4,
    "First Unit On Scene" TEXT,
    "Estimated Property Loss" FLOAT4,
    "Estimated Contents Loss" FLOAT4,
    "Fire Fatalities" INT4,
    "Fire Injuries" INT4,
    "Civilian Fatalities" INT4,
    "Civilian Injuries" INT4,
    "Number of Alarms" INT4,
    "Primary Situation" TEXT,
    "Mutual Aid" TEXT,
    "Action Taken Primary" TEXT,
    "Action Taken Secondary" TEXT,
    "Action Taken Other" TEXT,
    "Detector Alerted Occupants" TEXT,
    "Property Use" TEXT,
    "Area of Fire Origin" TEXT,
    "Ignition Cause" TEXT,
    "Ignition Factor Primary" TEXT,
    "Ignition Factor Secondary" TEXT,
    "Heat Source" TEXT,
    "Item First Ignited" TEXT,
    "Human Factors Associated with Ignition" TEXT,
    "Structure Type" TEXT,
    "Structure Status" TEXT,
    "Floor of Fire Origin" INT4,
    "Fire Spread" TEXT,
    "No Flame Spread" TEXT,
    "Number of floors with minimum damage" INT4,
    "Number of floors with significant damage" INT4,
    "Number of floors with heavy damage" INT4,
    "Number of floors with extreme damage" INT4,
    "Detectors Present" TEXT,
    "Detector Type" TEXT,
    "Detector Operation" TEXT,
    "Detector Effectiveness" TEXT,
    "Detector Failure Reason" TEXT,
    "Automatic Extinguishing System Present" TEXT,
    "Automatic Extinguishing Sytem Type" TEXT,
    "Automatic Extinguishing Sytem Perfomance" TEXT,
    "Automatic Extinguishing Sytem Failure Reason" TEXT,
    "Number of Sprinkler Heads Operating" INT4,
    "Supervisor District" TEXT,
    neighborhood_district TEXT,
    point POINT,
    data_as_of TIMESTAMP,
    data_loaded_at TIMESTAMP
);


CREATE SCHEMA business;

CREATE TABLE business.dim_incident_dates (
    _table_id SERIAL NOT NULL PRIMARY KEY,

    incident_date DATE UNIQUE NOT NULL -- should have a sort_key for this column
);

CREATE TABLE business.dim_battalions (
    _table_id SERIAL NOT NULL PRIMARY KEY,

    battalion TEXT UNIQUE NOT NULL -- should have a dist_key for this column
);

CREATE TABLE business.dim_supervisor_districts (
    _table_id SERIAL NOT NULL PRIMARY KEY,

    supervisor_district TEXT UNIQUE NOT NULL -- should have a dist_key for this column
);

CREATE TABLE business.dim_neighborhood_districts (
    _table_id SERIAL NOT NULL PRIMARY KEY,

    neighborhood_district TEXT UNIQUE NOT NULL -- should have a dist_key for this column
);

CREATE TABLE business.fact_fire_incidents (
    _table_id SERIAL NOT NULL PRIMARY KEY,

    incident_number TEXT,
    exposure_number INT4,
    id TEXT,
    address TEXT,
    incident_date_id INT4 REFERENCES business.dim_incident_dates (_table_id),
    call_number TEXT,
    alarm_dttm TIMESTAMP,
    arrival_dttm TIMESTAMP,
    close_dttm TIMESTAMP,
    city TEXT,
    zipcode TEXT,
    battalion_id INT4 REFERENCES business.dim_battalions (_table_id),
    station_area TEXT,
    city_box TEXT,
    suppression_units INT4,
    suppression_personnel INT4,
    ems_units INT4,
    ems_personnel INT4,
    other_units INT4,
    other_personnel INT4,
    first_unit_on_scene TEXT,
    estimated_property_loss FLOAT4,
    estimated_contents_loss FLOAT4,
    fire_fatalities INT4,
    fire_injuries INT4,
    civilian_fatalities INT4,
    civilian_injuries INT4,
    number_of_alarms INT4,
    primary_situation TEXT,
    mutual_aid TEXT,
    action_taken_primary TEXT,
    action_taken_secondary TEXT,
    action_taken_other TEXT,
    detector_alerted_occupants TEXT,
    property_use TEXT,
    area_of_fire_origin TEXT,
    ignition_cause TEXT,
    ignition_factor_primary TEXT,
    ignition_factor_secondary TEXT,
    heat_source TEXT,
    item_first_ignited TEXT,
    human_factors_associated_with_ignition TEXT,
    structure_type TEXT,
    structure_status TEXT,
    floor_of_fire_origin INT4,
    fire_spread TEXT,
    no_flame_spread TEXT,
    number_of_floors_with_minimum_damage INT4,
    number_of_floors_with_significant_damage INT4,
    number_of_floors_with_heavy_damage INT4,
    number_of_floors_with_extreme_damage INT4,
    detectors_present TEXT,
    detector_type TEXT,
    detector_operation TEXT,
    detector_effectiveness TEXT,
    detector_failure_reason TEXT,
    automatic_extinguishing_system_present TEXT,
    automatic_extinguishing_sytem_type TEXT,
    automatic_extinguishing_sytem_perfomance TEXT,
    automatic_extinguishing_sytem_failure_reason TEXT,
    number_of_sprinkler_heads_operating INT4,
    supervisor_district_id INT4 REFERENCES business.dim_supervisor_districts (_table_id),
    neighborhood_district_id INT4 REFERENCES business.dim_neighborhood_districts (_table_id),
    point_location POINT,
    data_as_of TIMESTAMP,
    data_loaded_at TIMESTAMP
);
