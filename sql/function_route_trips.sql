-- FUNCTION: ptc.route_trips_5min_b(timestamp without time zone)

-- DROP FUNCTION ptc.route_trips_5min_b(timestamp without time zone);

CREATE OR REPLACE FUNCTION ptc.route_trips_5min(
	_timestamp timestamp without time zone, _trips_table TEXT, _dest_table TEXT)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
BEGIN

EXECUTE format($$
	WITH distinct_nodes AS(SELECT DISTINCT vertex_id, node_id FROM here.routing_nodes) 
	INSERT INTO ptc.%I

	SELECT 
	mlsassignedptc,
	 ptctripid,
	 trippassengerid, 
	routing_results.*

	FROM (SELECT array_agg(source_id)::INT[] as sources, 
			array_agg(target_id)::INT[] as targets 
	  FROM (SELECT dense_rank() OVER(ORDER BY ptctripid) as id, src.vertex_id  AS source_id, trg.vertex_id AS target_id 
			FROM ptc.%I 
			INNER JOIN distinct_nodes src ON src.node_id = source
			INNER JOIN distinct_nodes trg ON trg.node_id = target 
			WHERE pickup_datetime >= %L::TIMESTAMP
					AND pickup_datetime < %L::TIMESTAMP + Interval '5 minutes'
		   ) sample
	 GROUP BY id/250 ) ods,
	LATERAL pgr_dijkstra('SELECT id, source, target, cost FROM here.get_network_for_tx_b('''||%L||''', TRUE)', sources, targets, TRUE) routing_results
	INNER JOIN (SELECT DISTINCT mlsassignedptc,
	ptctripid,
	trippassengerid, src.vertex_id  AS source, trg.vertex_id AS target FROM ptc.%I 
			INNER JOIN distinct_nodes src ON src.node_id = source
			INNER JOIN distinct_nodes trg ON trg.node_id = target
			WHERE pickup_datetime >= %L::TIMESTAMP
				  AND pickup_datetime < %L::TIMESTAMP + Interval '5 minutes'
	 ) trips ON source = start_vid AND target = end_vid
	$$, _dest_table, _trips_table, _timestamp, _timestamp, _timestamp,  _trips_table, _timestamp, _timestamp);
END;
$BODY$;

COMMENT ON FUNCTION ptc.route_trips_5min(
	 timestamp without time zone, TEXT, TEXT) IS '2019-05-10 version of 5min routing function allowing for specification of source and destination tables.';
