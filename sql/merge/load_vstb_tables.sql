--===========================================================================
-- Merge data into the historical tables.
--   T=Target
--   S=Source
-- admin.dc_projections_used
-- 20131126 - DH - Creation
--============================================================================

--=========================================================================
-- Define Parameters
--=========================================================================
\set query_duration_seconds 0

\set AUTOCOMMIT on
\set ON_ERROR_STOP on


\qecho =========================================================================
\qecho  projection_usage
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$projection_usage)  */
INTO admin.projection_usage
(
    query_start_timestamp
   ,node_name
   ,user_name
   ,session_id
   ,request_id
   ,transaction_id
   ,statement_id
   ,io_type
   ,projection_id
   ,projection_name
   ,anchor_table_id
   ,anchor_table_schema
   ,anchor_table_name
)
SELECT 
    query_start_timestamp
   ,node_name
   ,user_name
   ,session_id
   ,request_id
   ,transaction_id
   ,statement_id
   ,io_type
   ,projection_id
   ,projection_name
   ,anchor_table_id
   ,anchor_table_schema
   ,anchor_table_name
FROM v_monitor.projection_usage
WHERE anchor_table_schema NOT LIKE 'v_%'  -- exclude system projections
EXCEPT
SELECT
    query_start_timestamp
   ,node_name
   ,user_name
   ,session_id
   ,request_id
   ,transaction_id
   ,statement_id
   ,io_type
   ,projection_id
   ,projection_name
   ,anchor_table_id
   ,anchor_table_schema
   ,anchor_table_name
FROM admin.projection_usage   
WHERE query_start_timestamp >= (SELECT MIN(query_start_timestamp) 
                                  FROM v_monitor.projection_usage)
;
 


\qecho =========================================================================
\qecho  execution_engine_profiles
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$execution_engine_profiles)  */
INTO admin.execution_engine_profiles
(
    node_name
   ,user_id
   ,user_name
   ,session_id
   ,transaction_id
   ,statement_id
   ,plan_id
   ,operator_name
   ,operator_id
   ,baseplan_id
   ,path_id
   ,localplan_id
   ,activity_id
   ,resource_id
   ,counter_name
   ,counter_tag
   ,counter_value
   ,is_executing
)
SELECT
    node_name
   ,user_id
   ,user_name
   ,session_id
   ,transaction_id
   ,statement_id
   ,plan_id
   ,operator_name
   ,operator_id
   ,baseplan_id
   ,path_id
   ,localplan_id
   ,activity_id
   ,resource_id
   ,counter_name
   ,counter_tag
   ,counter_value
   ,is_executing
FROM v_monitor.execution_engine_profiles
WHERE is_executing = 0
  AND transaction_id <> 0 
EXCEPT
SELECT
    node_name
   ,user_id
   ,user_name
   ,session_id
   ,transaction_id
   ,statement_id
   ,plan_id
   ,operator_name
   ,operator_id
   ,baseplan_id
   ,path_id
   ,localplan_id
   ,activity_id
   ,resource_id
   ,counter_name
   ,counter_tag
   ,counter_value
   ,is_executing
FROM admin.execution_engine_profiles
WHERE        (node_name, user_id, session_id, transaction_id, statement_id)
   IN (SELECT DISTINCT
              node_name, user_id, session_id, transaction_id, statement_id
         FROM execution_engine_profiles )
;


\qecho =========================================================================
\qecho  admin.load_streams
\qecho  Only pick up statistics for completed load jobs
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$load_streams)  */
INTO admin.load_streams
(
    session_id
   ,transaction_id
   ,statement_id
   ,stream_name
   ,schema_name
   ,table_id
   ,table_name
   ,load_start
   ,load_duration_ms
   ,is_executing
   ,accepted_row_count
   ,rejected_row_count
   ,read_bytes
   ,input_file_size_bytes
   ,parse_complete_percent
   ,unsorted_row_count
   ,sorted_row_count
   ,sort_complete_percent
)
SELECT
    session_id
   ,transaction_id
   ,statement_id
   ,stream_name
   ,schema_name
   ,table_id
   ,table_name
   ,load_start
   ,load_duration_ms
   ,is_executing
   ,accepted_row_count
   ,rejected_row_count
   ,read_bytes
   ,input_file_size_bytes
   ,parse_complete_percent
   ,unsorted_row_count
   ,sorted_row_count
   ,sort_complete_percent
FROM v_monitor.load_streams 
WHERE  is_executing = 0
EXCEPT
SELECT
    session_id
   ,transaction_id
   ,statement_id
   ,stream_name
   ,schema_name
   ,table_id
   ,table_name
   ,load_start
   ,load_duration_ms
   ,is_executing
   ,accepted_row_count
   ,rejected_row_count
   ,read_bytes
   ,input_file_size_bytes
   ,parse_complete_percent
   ,unsorted_row_count
   ,sorted_row_count
   ,sort_complete_percent
FROM admin.load_streams  
WHERE       ( session_id, transaction_id, statement_id )
   IN (SELECT DISTINCT
              session_id, transaction_id, statement_id
         FROM v_monitor.load_streams )
;

\qecho =========================================================================
\qecho  admin.query_events
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$query_events)  */
INTO admin.query_events
(
    event_timestamp
   ,node_name
   ,user_id
   ,user_name
   ,session_id
   ,request_id
   ,transaction_id
   ,statement_id
   ,event_category
   ,event_type
   ,event_description
   ,operator_name
   ,path_id
   ,object_id
   ,event_details
   ,suggested_action
)
SELECT
    event_timestamp
   ,node_name
   ,user_id
   ,user_name
   ,session_id
   ,request_id
   ,transaction_id
   ,statement_id
   ,event_category
   ,event_type
   ,event_description
   ,operator_name
   ,path_id
   ,object_id
   ,event_details
   ,suggested_action
FROM v_monitor.query_events
WHERE  transaction_id <> 0 
EXCEPT
SELECT
    event_timestamp
   ,node_name
   ,user_id
   ,user_name
   ,session_id
   ,request_id
   ,transaction_id
   ,statement_id
   ,event_category
   ,event_type
   ,event_description
   ,operator_name
   ,path_id
   ,object_id
   ,event_details
   ,suggested_action
FROM admin.query_events
WHERE
  (
           event_timestamp
          ,user_id
          ,session_id
          ,transaction_id
  )
  IN
  ( SELECT DISTINCT
           event_timestamp
          ,user_id
          ,session_id
          ,transaction_id
     FROM v_monitor.query_events )
;



\qecho =========================================================================
\qecho  admin.query_plan_profiles
\qecho  Only select records from completed queries.
\qecho  Only select records where queries run time >= :query_duration_seconds 
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$query_plan_profiles)  */
INTO admin.query_plan_profiles
(
    transaction_id
   ,statement_id
   ,path_id
   ,path_line_index
   ,path_is_started
   ,path_is_completed
   ,is_executing
   ,running_time
   ,memory_allocated_bytes
   ,read_from_disk_bytes
   ,received_bytes
   ,sent_bytes
   ,path_line
)
SELECT
    S.transaction_id
   ,S.statement_id
   ,S.path_id
   ,S.path_line_index
   ,S.path_is_started
   ,S.path_is_completed
   ,S.is_executing
   ,S.running_time
   ,S.memory_allocated_bytes
   ,S.read_from_disk_bytes
   ,S.received_bytes
   ,S.sent_bytes
   ,S.path_line
FROM v_monitor.query_plan_profiles AS S
LEFT JOIN
          (SELECT DISTINCT
            transaction_id
             ,statement_id
          FROM admin.query_plan_profiles) AS T
  ON S.transaction_id = T.transaction_id
 AND S.statement_id   = T.statement_id
JOIN v_monitor.query_profiles AS QP
  ON S.transaction_id = QP.transaction_id
 AND S.statement_id   = QP.statement_id
WHERE T.transaction_id IS NULL
  AND S.transaction_id <> 0 
  AND QP.is_executing = 0
  AND QP.query_duration_us >= CASE WHEN :query_duration_seconds = 0 
                                       THEN 0
                                       ELSE ( :query_duration_seconds * 1e6)::INT 
                               END                                       
;


\qecho =========================================================================
\qecho  admin.query_profiles
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$query_profiles)  */
INTO admin.query_profiles
(
    session_id
   ,transaction_id
   ,statement_id
   ,identifier
   ,node_name
   ,query
   ,query_search_path
   ,schema_name
   ,table_name
   ,projections_used
   ,query_duration_us
   ,query_start_epoch
   ,query_start
   ,query_type
   ,error_code
   ,user_name
   ,processed_row_count
   ,reserved_extra_memory
   ,is_executing
)
SELECT
    session_id
   ,transaction_id
   ,statement_id
   ,identifier
   ,node_name
   ,query
   ,query_search_path
   ,schema_name
   ,table_name
   ,'N/A' as projections_used
   ,query_duration_us
   ,query_start_epoch
   ,query_start
   ,query_type
   ,error_code
   ,user_name
   ,processed_row_count
   ,reserved_extra_memory
   ,is_executing
FROM v_monitor.query_profiles
WHERE transaction_id <> 0
  AND is_executing=0
  AND query_type in ('LOAD','QUERY','UTILITY')
EXCEPT
SELECT
    session_id
   ,transaction_id
   ,statement_id
   ,identifier
   ,node_name
   ,query
   ,query_search_path
   ,schema_name
   ,table_name
   ,projections_used
   ,query_duration_us
   ,query_start_epoch
   ,query_start
   ,query_type
   ,error_code
   ,user_name
   ,processed_row_count
   ,reserved_extra_memory
   ,is_executing
FROM admin.query_profiles
WHERE query_start >= (SELECT MIN(query_start) FROM query_profiles)
;

   

\qecho =========================================================================
\qecho  admin.query_requests
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$query_requests)  */
INTO admin.query_requests
(
    node_name
   ,user_name
   ,session_id
   ,request_id
   ,transaction_id
   ,statement_id
   ,request_type
   ,request
   ,request_label
   ,search_path
   ,memory_acquired_mb
   ,success
   ,error_count
   ,start_timestamp
   ,end_timestamp
   ,request_duration_ms
   ,is_executing
)
SELECT
    node_name
   ,user_name
   ,session_id
   ,request_id
   ,transaction_id
   ,statement_id
   ,request_type
   ,request
   ,request_label
   ,search_path
   ,memory_acquired_mb
   ,success
   ,error_count
   ,start_timestamp
   ,end_timestamp
   ,request_duration_ms
   ,is_executing
FROM v_monitor.query_requests
WHERE is_executing=0
  AND transaction_id <> 0 
  AND request_type IN ('LOAD','QUERY','UTILITY')  
EXCEPT
SELECT
    node_name
   ,user_name
   ,session_id
   ,request_id
   ,transaction_id
   ,statement_id
   ,request_type
   ,request
   ,request_label
   ,search_path
   ,memory_acquired_mb
   ,success
   ,error_count
   ,start_timestamp
   ,end_timestamp
   ,request_duration_ms
   ,is_executing
FROM admin.query_requests
WHERE start_timestamp >= (SELECT MIN(start_timestamp)
                            FROM v_monitor.query_requests)
;

\qecho =========================================================================
\qecho  admin.resource_rejection_details
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$resource_rejection_details)  */
INTO admin.resource_rejection_details
(
    rejected_timestamp
   ,node_name
   ,user_name
   ,session_id
   ,request_id
   ,transaction_id
   ,statement_id
   ,pool_id
   ,pool_name
   ,reason
   ,resource_type
   ,rejected_value
)
SELECT
    rejected_timestamp
   ,node_name
   ,user_name
   ,session_id
   ,request_id
   ,transaction_id
   ,statement_id
   ,pool_id
   ,pool_name
   ,reason
   ,resource_type
   ,rejected_value
FROM v_monitor.resource_rejection_details
WHERE transaction_id <> 0 
EXCEPT
SELECT
    rejected_timestamp
   ,node_name
   ,user_name
   ,session_id
   ,request_id
   ,transaction_id
   ,statement_id
   ,pool_id
   ,pool_name
   ,reason
   ,resource_type
   ,rejected_value
FROM  admin.resource_rejection_details
WHERE rejected_timestamp >= (SELECT MIN(rejected_timestamp)
                               FROM v_monitor.resource_rejection_details)
;

 
\qecho =========================================================================
\qecho  admin.user_sessions
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$user_sessions)  */
INTO admin.user_sessions
(
    node_name
   ,user_name
   ,session_id
   ,transaction_id
   ,statement_id
   ,session_start_timestamp
   ,session_end_timestamp
   ,is_active
   ,client_hostname
   ,client_pid
   ,client_label
   ,ssl_state
   ,authentication_method
)
SELECT 
    node_name
   ,user_name
   ,session_id
   ,transaction_id
   ,statement_id
   ,session_start_timestamp
   ,session_end_timestamp
   ,is_active
   ,client_hostname
   ,client_pid
   ,client_label
   ,ssl_state
   ,authentication_method
FROM v_monitor.user_sessions
WHERE is_active=0
EXCEPT 
SELECT 
    node_name
   ,user_name
   ,session_id
   ,transaction_id
   ,statement_id
   ,session_start_timestamp
   ,session_end_timestamp
   ,is_active
   ,client_hostname
   ,client_pid
   ,client_label
   ,ssl_state
   ,authentication_method
FROM admin.user_sessions
WHERE session_start_timestamp >= (SELECT MIN(session_start_timestamp)
                                    FROM v_monitor.user_sessions)
;



\qecho =========================================================================
\qecho  admin.transactions
\qecho  end_timestamp=NULL for transactions that are in process.
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$transactions)  */
INTO admin.transactions
(
    start_timestamp
   ,end_timestamp
   ,node_name
   ,user_id
   ,user_name
   ,session_id
   ,transaction_id
   ,description
   ,start_epoch
   ,end_epoch
   ,number_of_statements
   ,isolation
   ,is_read_only
   ,is_committed
   ,is_local
   ,is_initiator
   ,is_ddl
) 
SELECT DISTINCT 
    start_timestamp
   ,end_timestamp
   ,node_name
   ,user_id
   ,user_name
   ,session_id
   ,transaction_id
   ,description
   ,start_epoch
   ,end_epoch
   ,number_of_statements
   ,isolation
   ,is_read_only
   ,is_committed
   ,is_local
   ,is_initiator
   ,is_ddl
FROM v_monitor.transactions
WHERE end_timestamp IS NOT NULL  -- completed transactions
AND transaction_id <> 0 
AND (COALESCE(is_ddl,FALSE)=FALSE
/*      OR transaction_id IN 
        (SELECT DISTINCT transaction_id 
           FROM v_monitor.query_requests
          WHERE  transaction_id <> 0 
            AND request_type IN ('LOAD','QUERY','UTILITY')  */
        )   
EXCEPT
SELECT
    start_timestamp
   ,end_timestamp
   ,node_name
   ,user_id
   ,user_name
   ,session_id
   ,transaction_id
   ,description
   ,start_epoch
   ,end_epoch
   ,number_of_statements
   ,isolation
   ,is_read_only
   ,is_committed
   ,is_local
   ,is_initiator
   ,is_ddl
FROM admin.transactions 
WHERE start_timestamp >= (SELECT MIN(start_timestamp) FROM v_monitor.transactions)
;
 
 
\qecho =========================================================================
\qecho  admin.projection_storage
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$projection_storage)  */
INTO admin.projection_storage 
(
    node_name
   ,projection_id
   ,projection_name
   ,projection_schema
   ,projection_column_count
   ,row_count
   ,used_bytes
   ,wos_row_count
   ,wos_used_bytes
   ,ros_row_count
   ,ros_used_bytes
   ,ros_count
   ,anchor_table_name
   ,anchor_table_schema
   ,anchor_table_id
   ,last_refresh_ts
)
SELECT 
    node_name
   ,projection_id
   ,projection_name
   ,projection_schema
   ,projection_column_count
   ,row_count
   ,used_bytes
   ,wos_row_count
   ,wos_used_bytes
   ,ros_row_count
   ,ros_used_bytes
   ,ros_count
   ,anchor_table_name
   ,anchor_table_schema
   ,anchor_table_id
   ,TRANSACTION_TIMESTAMP() AS last_refresh_ts
FROM v_monitor.projection_storage
;

\qecho =========================================================================
\qecho  Delete records from earlier in the day to save space.
\qecho =========================================================================
DELETE FROM admin.projection_storage 
WHERE last_refresh_ts::DATE = (SELECT MAX(last_refresh_ts)::DATE 
                                 FROM admin.projection_storage)
  AND last_refresh_ts       < (SELECT MAX(last_refresh_ts) 
                                 FROM admin.projection_storage); 



\qecho =========================================================================
\qecho  dc_execution_engine_events
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$dc_execution_engine_events)  */
INTO admin.dc_execution_engine_events
(
    time
   ,node_name
   ,session_id
   ,user_id
   ,user_name
   ,transaction_id
   ,statement_id
   ,request_id
   ,event_type
   ,event_description
   ,operator_name
   ,path_id
   ,event_oid
   ,event_details
   ,suggested_action
)
SELECT
    time
   ,node_name
   ,session_id
   ,user_id
   ,user_name
   ,transaction_id
   ,statement_id
   ,request_id
   ,event_type
   ,event_description
   ,operator_name
   ,path_id
   ,event_oid
   ,event_details
   ,suggested_action
FROM v_internal.dc_execution_engine_events  
WHERE  transaction_id <> 0 
EXCEPT
SELECT
    time
   ,node_name
   ,session_id
   ,user_id
   ,user_name
   ,transaction_id
   ,statement_id
   ,request_id
   ,event_type
   ,event_description
   ,operator_name
   ,path_id
   ,event_oid
   ,event_details
   ,suggested_action
FROM admin.dc_execution_engine_events  
WHERE time >= (SELECT MIN(time) FROM dc_execution_engine_events)
;


\qecho =========================================================================
\qecho  system_resource_usage
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$system_resource_usage)  */
INTO admin.system_resource_usage
(
     node_name
    ,end_time
    ,average_memory_usage_percent
    ,average_cpu_usage_percent
    ,net_rx_kbytes_per_second
    ,net_tx_kbytes_per_second
    ,io_read_kbytes_per_second
    ,io_written_kbytes_per_second
)
SELECT  
     node_name
    ,end_time
    ,average_memory_usage_percent
    ,average_cpu_usage_percent
    ,net_rx_kbytes_per_second
    ,net_tx_kbytes_per_second
    ,io_read_kbytes_per_second
    ,io_written_kbytes_per_second
FROM v_monitor.system_resource_usage 
WHERE end_time > (SELECT MAX(end_time) from admin.system_resource_usage)
;

\qecho =========================================================================
\qecho  resource_acquisitions
\qecho =========================================================================
INSERT /*+direct, label(load_historical$v0_1$resource_acquisitions)  */
INTO admin.resource_acquisitions
(
    node_name
   ,transaction_id
   ,statement_id
   ,request_type
   ,pool_id
   ,pool_name
   ,thread_count
   ,open_file_handle_count
   ,memory_inuse_kb
   ,queue_entry_timestamp
   ,acquisition_timestamp
   ,release_timestamp
   ,duration_ms
   ,is_executing
)
SELECT
    node_name
   ,transaction_id
   ,statement_id
   ,request_type
   ,pool_id
   ,pool_name
   ,thread_count
   ,open_file_handle_count
   ,memory_inuse_kb
   ,queue_entry_timestamp
   ,acquisition_timestamp
   ,release_timestamp
   ,duration_ms
   ,is_executing
FROM  v_monitor.resource_acquisitions
WHERE is_executing=0
  AND           (transaction_id, statement_id, queue_entry_timestamp) 
  NOT IN (SELECT transaction_id, statement_id, queue_entry_timestamp FROM admin.resource_acquisitions)
;
  


\qecho =========================================================================
\qecho  Collect Statistics
\qecho =========================================================================
SELECT ANALYZE_STATISTICS('admin.dc_execution_engine_events' );
SELECT ANALYZE_STATISTICS('admin.projection_usage'           );
SELECT ANALYZE_STATISTICS('admin.execution_engine_profiles'  );
SELECT ANALYZE_STATISTICS('admin.load_streams'               );
SELECT ANALYZE_STATISTICS('admin.projection_storage'         );
SELECT ANALYZE_STATISTICS('admin.query_events'               );
SELECT ANALYZE_STATISTICS('admin.query_plan_profiles'        );
SELECT ANALYZE_STATISTICS('admin.query_profiles'             );
SELECT ANALYZE_STATISTICS('admin.query_requests'             );
SELECT ANALYZE_STATISTICS('admin.resource_rejection_details' );
SELECT ANALYZE_STATISTICS('admin.user_sessions'              );
SELECT ANALYZE_STATISTICS('admin.transactions'               );
SELECT ANALYZE_STATISTICS('admin.system_resource_usage'      );
SELECT ANALYZE_STATISTICS('admin.resource_acquisitions'      );

 
