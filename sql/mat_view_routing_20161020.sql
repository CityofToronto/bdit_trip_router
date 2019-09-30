-- View: ptc.trip_data_routing_20161020

-- DROP MATERIALIZED VIEW ptc.trip_data_routing_20161020;

CREATE MATERIALIZED VIEW ptc.trip_data_routing_20161020
TABLESPACE pg_default
AS
 WITH trips AS (
         SELECT trip_data_clean.mlsassignedptc,
            trip_data_clean.ptctripid,
            trip_data_clean.trippassengerid,
            trip_data_clean.tripstatusid,
            trip_data_clean.accessible,
            trip_data_clean.request_datetime,
            trip_data_clean.acceptance_datetime,
            trip_data_clean.arrival_datetime,
            trip_data_clean.pickup_datetime,
            trip_data_clean.dropoff_datetime,
            trip_data_clean.distance,
            trip_data_clean.provider_id,
            trip_data_clean.pickup_mun_id,
            trip_data_clean.pickup_gc_intersection_id,
            trip_data_clean.pickup_gc_centreline_id,
            origint.geom IS NULL AS has_orig,
	 		origin_nodes.source,
            trip_data_clean.dropoff_mun_id,
            trip_data_clean.dropoff_gc_intersection_id,
            trip_data_clean.dropoff_gc_centreline_id,
	 		destinint.geom IS NULL AS has_dest,
            dest_nodes.target,
            trip_data_clean.driverwaittime,
            trip_data_clean.elapsedtime,
            trip_data_clean.rideduration,
            trip_data_clean.requestdriverarrival,
            trip_data_clean.requestacceptance
           FROM ptc.trip_data_clean
             JOIN ptc.pickup_intersections_new origint ON origint.int_id = trip_data_clean.pickup_gc_intersection_id
             CROSS JOIN LATERAL ( SELECT z.node_id AS source
                   FROM here_gis.zlevels_18_3 z
                     JOIN here.routing_streets_18_3b USING (link_id)
                  WHERE z.intrsect::text = 'Y'::text
                  ORDER BY (z.geom <-> origint.geom)
                 LIMIT 1) origin_nodes
             JOIN ptc.pickup_intersections_new destinint ON destinint.int_id = trip_data_clean.dropoff_gc_intersection_id
             CROSS JOIN LATERAL ( SELECT z.node_id AS target
                   FROM here_gis.zlevels_18_3 z
                     JOIN here.routing_streets_18_3b USING (link_id)
                  WHERE z.intrsect::text = 'Y'::text
                  ORDER BY (z.geom <-> destinint.geom)
                 LIMIT 1) dest_nodes
          WHERE trip_data_clean.request_datetime >= '2016-10-20 00:00:00'::timestamp without time zone AND trip_data_clean.request_datetime < '2016-10-21 00:00:00'::timestamp without time zone
        ), pooled_trips AS (
         SELECT reordered_stops.mlsassignedptc,
            reordered_stops.ptctripid,
            reordered_stops.trippassengerid,
            reordered_stops.pickup_datetime,
            reordered_stops.source,
            reordered_stops.int_id,
            reordered_stops.next_int,
            reordered_stops.dropoff_datetime,
            reordered_stops.target
           FROM ( SELECT trips.mlsassignedptc,
                    trips.ptctripid,
                    trips.trippassengerid,
                    t.ts AS pickup_datetime,
                    t.node AS source,
                    has_point AS has_orig,
                    lead(has_point) OVER w AS has_dest,
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
                     CROSS JOIN LATERAL unnest(ARRAY[ROW(trips.pickup_datetime, trips.source, has_orig, pickup_gc_intersection_id), ROW(trips.dropoff_datetime, trips.target, has_dest, dropoff_gc_intersection_id)]) t(ts timestamp without time zone, node numeric, has_point BOOLEAN, int_id INT)
                  WHERE trips.dropoff_datetime > '1900-01-01 00:00:00'::timestamp without time zone AND trips.pickup_datetime > '1900-01-01 00:00:00'::timestamp without time zone
                  WINDOW w AS (PARTITION BY trips.ptctripid ORDER BY t.ts)) reordered_stops
          WHERE has_orig AND has_dest
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
    false AS pooled
   FROM trips
     LEFT JOIN ( SELECT pooled_trips.ptctripid,
            pooled_trips.trippassengerid
           FROM pooled_trips) pooled USING (ptctripid, trippassengerid)
  WHERE pooled.ptctripid IS NULL AND has_orig AND has_dest
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
    true AS pooled
   FROM pooled_trips
WITH DATA;

ALTER TABLE ptc.trip_data_routing_20161020
    OWNER TO rdumas;

COMMENT ON MATERIALIZED VIEW ptc.trip_data_routing_20161020
    IS 'Representative day of PTC data for Oct ''16, with nodes routable on the routing_streets_18_3b network and pooled trip stops reordered. Fixing pickup_datetimes for pooled trips and adding a pooled flag.';

GRANT ALL ON TABLE ptc.trip_data_routing_20161020 TO rdumas;
GRANT SELECT ON TABLE ptc.trip_data_routing_20161020 TO bdit_humans;

CREATE INDEX trip_data_routing_20161020_ptctripid_trippassengerid_idx
    ON ptc.trip_data_routing_20161020 USING btree
    (ptctripid COLLATE pg_catalog."default", trippassengerid)
    TABLESPACE pg_default;