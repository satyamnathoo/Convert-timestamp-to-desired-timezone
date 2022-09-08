CREATE OR REPLACE FUNCTION schema.stym_fn_convert_time(
                                                      "time" timestamp without time zone,
                                                      source_time_zone character varying,
                                                      target_time_zone character varying
                                                      )
    RETURNS timestamp without time zone
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE GMT_time timestamp without time zone;
        Targetzone_timestamp timestamp without time zone;
BEGIN
    SELECT 
        CASE 
         WHEN source_time_zone = 'ECT' THEN "time" - time '01:00:00'
         WHEN source_time_zone = 'EET' THEN "time" - time '02:00:00'
         WHEN source_time_zone = 'ART' THEN "time" - time '02:00:00'
         WHEN source_time_zone = 'EAT' THEN "time" - time '03:00:00'
         WHEN source_time_zone = 'MET' THEN "time" - time '03:30:00'
         WHEN source_time_zone = 'NET' THEN "time" - time '04:00:00'
         WHEN source_time_zone = 'PLT' THEN "time" - time '05:00:00'
         WHEN source_time_zone = 'IST' THEN "time" - time '05:30:00'
         WHEN source_time_zone = 'BST' THEN "time" - time '06:00:00'
         WHEN source_time_zone = 'VST' THEN "time" - time '07:00:00'
         WHEN source_time_zone = 'CTT' THEN "time" - time '08:00:00'
         WHEN source_time_zone = 'JST' THEN "time" - time '09:00:00'
         WHEN source_time_zone = 'ACT' THEN "time" - time '09:30:00'
         WHEN source_time_zone = 'AET' THEN "time" - time '10:00:00'
         WHEN source_time_zone = 'SST' THEN "time" - time '11:00:00'
         WHEN source_time_zone = 'NST' THEN "time" - time '12:00:00'
         WHEN source_time_zone = 'MIT' THEN "time" + time '11:00:00'
         WHEN source_time_zone = 'HST' THEN "time" + time '10:00:00'
         WHEN source_time_zone = 'AST' THEN "time" + time '09:00:00'
         WHEN source_time_zone = 'PST' THEN "time" + time '08:00:00'
         WHEN source_time_zone = 'PNT' THEN "time" + time '07:00:00'
         WHEN source_time_zone = 'MST' THEN "time" + time '07:00:00'
         WHEN source_time_zone = 'CST' THEN "time" + time '06:00:00'
         WHEN source_time_zone = 'EST' THEN "time" + time '05:00:00'
         WHEN source_time_zone = 'IET' THEN "time" + time '05:00:00'
         WHEN source_time_zone = 'PRT' THEN "time" + time '04:00:00'
         WHEN source_time_zone = 'CNT' THEN "time" + time '03:30:00'
         WHEN source_time_zone = 'AGT' THEN "time" + time '03:00:00'
         WHEN source_time_zone = 'BET' THEN "time" + time '03:00:00'
         WHEN source_time_zone = 'CAT' THEN "time" + time '01:00:00'
         WHEN source_time_zone = 'UTC' THEN "time"
         WHEN source_time_zone = 'GMT' THEN "time"
        ELSE Null
        END AS gmt_timestamp
        INTO GMT_time;
        
    SELECT 
        CASE 
         WHEN  target_time_zone = 'ECT' THEN GMT_time + time '01:00:00'
         WHEN  target_time_zone = 'EET' THEN GMT_time + time '02:00:00'
         WHEN  target_time_zone = 'ART' THEN GMT_time + time '02:00:00'
         WHEN  target_time_zone = 'EAT' THEN GMT_time + time '03:00:00'
         WHEN  target_time_zone = 'MET' THEN GMT_time + time '03:30:00'
         WHEN  target_time_zone = 'NET' THEN GMT_time + time '04:00:00'
         WHEN  target_time_zone = 'PLT' THEN GMT_time + time '05:00:00'
         WHEN  target_time_zone = 'IST' THEN GMT_time + time '05:30:00'
         WHEN  target_time_zone = 'BST' THEN GMT_time + time '06:00:00'
         WHEN  target_time_zone = 'VST' THEN GMT_time + time '07:00:00'
         WHEN  target_time_zone = 'CTT' THEN GMT_time + time '08:00:00'
         WHEN  target_time_zone = 'JST' THEN GMT_time + time '09:00:00'
         WHEN  target_time_zone = 'ACT' THEN GMT_time + time '09:30:00'
         WHEN  target_time_zone = 'AET' THEN GMT_time + time '10:00:00'
         WHEN  target_time_zone = 'SST' THEN GMT_time + time '11:00:00'
         WHEN  target_time_zone = 'NST' THEN GMT_time + time '12:00:00'
         WHEN  target_time_zone = 'MIT' THEN GMT_time - time '11:00:00'
         WHEN  target_time_zone = 'HST' THEN GMT_time - time '10:00:00'
         WHEN  target_time_zone = 'AST' THEN GMT_time - time '09:00:00'
         WHEN  target_time_zone = 'PST' THEN GMT_time - time '08:00:00'
         WHEN  target_time_zone = 'PNT' THEN GMT_time - time '07:00:00'
         WHEN  target_time_zone = 'MST' THEN GMT_time - time '07:00:00'
         WHEN  target_time_zone = 'CST' THEN GMT_time - time '06:00:00'
         WHEN  target_time_zone = 'EST' THEN GMT_time - time '05:00:00'
         WHEN  target_time_zone = 'IET' THEN GMT_time - time '05:00:00'
         WHEN  target_time_zone = 'PRT' THEN GMT_time - time '04:00:00'
         WHEN  target_time_zone = 'CNT' THEN GMT_time - time '03:30:00'
         WHEN  target_time_zone = 'AGT' THEN GMT_time - time '03:00:00'
         WHEN  target_time_zone = 'BET' THEN GMT_time - time '03:00:00'
         WHEN  target_time_zone = 'CAT' THEN GMT_time - time '01:00:00'
         WHEN  target_time_zone = 'UTC' THEN GMT_time
         WHEN  target_time_zone = 'GMT' THEN GMT_time
        ELSE Null
        END AS target_timestamp
        INTO Targetzone_timestamp;
    Return Targetzone_timestamp;
END;
$BODY$;
