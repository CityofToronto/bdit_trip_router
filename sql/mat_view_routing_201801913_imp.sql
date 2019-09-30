-- View: ptc.trip_data_routing_20180913_imputed

DROP MATERIALIZED VIEW ptc.trip_data_routing_20180913_imputed;

CREATE MATERIALIZED VIEW ptc.trip_data_routing_20180913_imputed
TABLESPACE pg_default
AS
 WITH trips AS (SELECT trip_data.mlsassignedptc,
    trip_data.ptctripid,
    trip_data.trippassengerid,
    trip_data.tripstatusid,
    trip_data.accessible,
pickup_datetime_precise AS pickup_datetime,
	dropoff_datetime_precise AS dropoff_datetime,
    trip_data.distance,
    trip_data.provider_id,
    trip_data.pickup_mun_id,
    trip_data.pickup_gc_intersection_id,
    trip_data.pickup_gc_centreline_id,
    COALESCE(origin_gateway.source, origin_nodes.source) AS source,
    trip_data.dropoff_mun_id,
    trip_data.dropoff_gc_intersection_id,
    trip_data.dropoff_gc_centreline_id,
    COALESCE(dest_gateway.target, dest_nodes.target) AS target,
    trip_data.driverwaittime,
    trip_data.elapsedtime,
    trip_data.rideduration,
    trip_data.requestdriverarrival,
    trip_data.requestacceptance 
   FROM ptc.trip_data_ci_20180913 trip_data
     JOIN ptc.pickup_intersections_new origint ON origint.int_id = trip_data.pickup_gc_intersection_id
     LEFT JOIN LATERAL (
		 SELECT z.node_id AS source
           FROM here_gis.zlevels_18_3 z
             JOIN here.routing_streets_18_3b USING (link_id)
          WHERE z.intrsect = 'Y' AND trip_data.pickup_mun_id = 69
          ORDER BY (z.geom <-> origint.geom)
         LIMIT 1) origin_nodes ON true
     LEFT JOIN LATERAL ( 
		 SELECT dt_gate.node_id AS target
           FROM ptc.routing_gateways dt_gate
          WHERE trip_data.dropoff_mun_id = dt_gate.mun_id AND (bound is NULL OR bound = 'outbound')
		 		AND ((st_dwithin(st_transform(origint.geom, 98012), st_transform(dt_gate.geom, 98012), 3000) AND dt_gate.short_long = 'shorter_points' )
					 OR (NOT st_dwithin(st_transform(origint.geom, 98012), st_transform(dt_gate.geom, 98012), 3000) AND dt_gate.short_long = 'longer_points'))
          ORDER BY (st_transform(origint.geom, 98012) <-> st_transform(dt_gate.geom, 98012))
         LIMIT 1) dest_gateway ON true
     JOIN ptc.pickup_intersections_new destinint ON destinint.int_id = trip_data.dropoff_gc_intersection_id
     LEFT JOIN LATERAL ( 
		 SELECT z.node_id AS target
           FROM here_gis.zlevels_18_3 z
             JOIN here.routing_streets_18_3b USING (link_id)
          WHERE z.intrsect = 'Y' AND trip_data.dropoff_mun_id = 69
          ORDER BY (z.geom <-> destinint.geom)
         LIMIT 1) dest_nodes ON true
     LEFT JOIN LATERAL ( 
		 SELECT og_gate.node_id AS source
           FROM ptc.routing_gateways og_gate
          WHERE trip_data.pickup_mun_id = og_gate.mun_id AND (bound is NULL OR bound = 'inbound')
		 	AND ((st_dwithin(st_transform(destinint.geom, 98012), st_transform(og_gate.geom, 98012), 3000) AND og_gate.short_long = 'shorter_points' )
				 OR (NOT st_dwithin(st_transform(destinint.geom, 98012), st_transform(og_gate.geom, 98012), 3000) AND og_gate.short_long = 'longer_points'))
          ORDER BY (st_transform(destinint.geom, 98012)<-> st_transform(og_gate.geom, 98012))
         LIMIT 1) origin_gateway ON true
  WHERE pickup_datetime_precise >= '2018-09-13 00:00:00'::timestamp without time zone AND pickup_datetime_precise < ('2018-09-13 00:00:00'::timestamp without time zone + '1 day'::interval)
  ), pooled_trips AS (
         SELECT reordered_stops.mlsassignedptc,
            reordered_stops.ptctripid,
            reordered_stops.trippassengerid,
            reordered_stops.pickup_datetime,
            reordered_stops.source,
            reordered_stops.int_id,
            reordered_stops.next_int,
            reordered_stops.dropoff_datetime,
            reordered_stops.target,
    		CASE WHEN dropoff_mun_id IN( 61, 38, 71, 57, 8, 41) OR pickup_mun_id IN (61, 38, 71, 57, 8, 41) THEN TRUE ELSE FALSE END AS ext_trip
           FROM ( SELECT trips.mlsassignedptc,
                    trips.ptctripid,
                    trips.trippassengerid,
                    t.ts AS pickup_datetime,
                    t.node AS source,
                    t.mun_id AS pickup_mun_id,
                    lead(t.mun_id) OVER w AS dropoff_mun_id,
                    t.int_id,
                    lead(t.int_id) OVER w AS next_int,
                    lead(t.ts) OVER w AS dropoff_datetime,
                    lead(t.node) OVER w AS target
                   FROM trips
                     JOIN ( SELECT trips_1.mlsassignedptc,
                            trips_1.ptctripid
                           FROM trips trips_1
                          WHERE trips_1.provider_id = ANY (ARRAY[28608558, 1608409])
                          GROUP BY trips_1.mlsassignedptc, trips_1.ptctripid
                         HAVING count(DISTINCT trips_1.trippassengerid) > 1) pooled_trips USING (mlsassignedptc, ptctripid)
                     CROSS JOIN LATERAL unnest(ARRAY[ROW(trips.pickup_datetime, trips.source, trips.pickup_mun_id, trips.pickup_gc_intersection_id), ROW(trips.dropoff_datetime, trips.target, trips.dropoff_mun_id, trips.dropoff_gc_intersection_id)]) t(ts timestamp without time zone, node numeric, mun_id integer, int_id integer)
                  WHERE trips.dropoff_datetime > '1900-01-01 00:00:00'::timestamp without time zone AND trips.pickup_datetime > '1900-01-01 00:00:00'::timestamp without time zone
                  WINDOW w AS (PARTITION BY trips.ptctripid ORDER BY t.ts)) reordered_stops
          WHERE reordered_stops.dropoff_mun_id IN(69, 61, 38, 71, 57, 8, 41) OR reordered_stops.pickup_mun_id IN (61, 38, 71, 57, 8, 41, 69)
        )
 SELECT trips.mlsassignedptc,
    trips.ptctripid,
    trips.trippassengerid,
    trips.pickup_datetime,
    trips.pickup_gc_intersection_id,
    trips.source,
    trips.dropoff_datetime,
    trips.dropoff_gc_intersection_id,
    trips.target,
    false AS pooled,
	dropoff_mun_id IN( 61, 38, 71, 57, 8, 41) OR pickup_mun_id IN (61, 38, 71, 57, 8, 41) AS ext_trip
   FROM trips
     LEFT JOIN ( SELECT pooled_trips.ptctripid,
            pooled_trips.trippassengerid
           FROM pooled_trips) pooled USING (ptctripid, trippassengerid)
  WHERE pooled.ptctripid IS NULL AND (trips.dropoff_mun_id IN (69, 61, 38, 71, 57, 8, 41) OR trips.pickup_mun_id IN (69, 61, 38, 71, 57, 8, 41))
UNION ALL
 SELECT pooled_trips.mlsassignedptc,
    pooled_trips.ptctripid,
    pooled_trips.trippassengerid,
    pooled_trips.pickup_datetime,
    pooled_trips.int_id AS pickup_gc_intersection_id,
    pooled_trips.source,
    pooled_trips.dropoff_datetime,
    pooled_trips.next_int AS dropoff_gc_intersection_id,
    pooled_trips.target,
    true AS pooled,
	ext_trip
   FROM pooled_trips
   WHERE dropoff_datetime IS NOT NULL
WITH DATA;

ALTER TABLE ptc.trip_data_routing_20180913_imputed
    OWNER TO rdumas;

COMMENT ON MATERIALIZED VIEW ptc.trip_data_routing_20180913_imputed
    IS 'Data for routing with imputed timestamps and reordered pooled trips, and trips outside the city for 2018-09-13';

GRANT ALL ON TABLE ptc.trip_data_routing_20180913_imputed TO rdumas;
GRANT SELECT ON TABLE ptc.trip_data_routing_20180913_imputed TO bdit_humans;

CREATE INDEX 
    ON ptc.trip_data_routing_20180913_imputed USING btree
    (ptctripid COLLATE pg_catalog."default", trippassengerid)
    TABLESPACE pg_default;