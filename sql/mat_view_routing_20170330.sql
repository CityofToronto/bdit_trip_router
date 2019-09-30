-- View: ptc.trip_data_routing_20170330

-- DROP MATERIALIZED VIEW ptc.trip_data_routing_20170330;

CREATE MATERIALIZED VIEW ptc.trip_data_routing_20170330
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
			origin_nodes.source,
			trip_data_clean.dropoff_mun_id,
			trip_data_clean.dropoff_gc_intersection_id,
			trip_data_clean.dropoff_gc_centreline_id,
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
			WHERE trip_data_clean.request_datetime >= '2017-03-30 00:00:00'::timestamp without time zone AND trip_data_clean.request_datetime < '2017-03-31 00:00:00'::timestamp without time zone
	)
	, pooled_trips as (
		/*Reordering stops in true pooled trips*/
		SELECT * 
		FROM
			(SELECT mlsassignedptc, ptctripid, trippassengerid, pickup_datetime, node source, int_id,lead(int_id) OVER w as next_int,  lead(ts) OVER w as dropoff_datetime, lead(node) over w AS target

			FROM trips
			INNER JOIN (
					SELECT mlsassignedptc, ptctripid
					FROM trips
					WHERE provider_id IN (28608558, 1608409) AND dropoff_datetime > '1900-01-01' AND pickup_datetime > '1900-01-01'
					GROUP BY mlsassignedptc, ptctripid
					HAVING COUNT(DISTINCT trippassengerid) >1
				) pooled_trips USING(mlsassignedptc, ptctripid)
			CROSS JOIN LATERAL unnest(ARRAY[(pickup_datetime, source, pickup_gc_intersection_id), (dropoff_datetime, target, dropoff_gc_intersection_id) ]) AS t(ts TIMESTAMP, node NUMERIC, int_id int)

			WINDOW w AS (PARTITION BY ptctripid ORDER BY ts)
			) reordered_stops
		WHERE int_id != 99999999 AND next_int != 99999999 --Only filter external trip segments after reordering nodes, so we can get at least some of the segment
	)
	SELECT mlsassignedptc,
    ptctripid,
    trippassengerid,
    pickup_datetime,
	pickup_gc_intersection_id,
    source,
    dropoff_datetime,
	dropoff_gc_intersection_id,
    target
   FROM trips
     LEFT JOIN ( SELECT ptctripid,
            trippassengerid
           FROM pooled_trips) pooled USING (ptctripid, trippassengerid)
  WHERE pooled.ptctripid IS NULL AND pickup_gc_intersection_id != 99999999 AND dropoff_gc_intersection_id != 99999999 --No routing trips outside the city.
UNION ALL
 SELECT mlsassignedptc,
    ptctripid,
    trippassengerid,
    pickup_datetime,
	int_id,
    source,
    dropoff_datetime,
	next_int,
    target
   FROM pooled_trips
	
		 
WITH DATA;

ALTER TABLE ptc.trip_data_routing_20170330
    OWNER TO rdumas;

COMMENT ON MATERIALIZED VIEW ptc.trip_data_routing_20170330
    IS 'Busiest representative day of PTC data before April 2017, with nodes routable on the routing_streets_18_3b network and pooled trip stops reordered';

GRANT ALL ON TABLE ptc.trip_data_routing_20170330 TO rdumas;
GRANT SELECT ON TABLE ptc.trip_data_routing_20170330 TO bdit_humans;

CREATE INDEX trip_data_routing_20170330_ptctripid_trippassengerid_idx
    ON ptc.trip_data_routing_20170330 USING btree
    (ptctripid COLLATE pg_catalog."default", trippassengerid)
    TABLESPACE pg_default;