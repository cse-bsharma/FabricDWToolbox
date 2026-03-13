"""
Fabric DW Query Performance Analyzer - Web Application
Flask-based web interface for query analysis
"""

from flask import Flask, render_template, request, jsonify, session
import os
import sys
import traceback
from datetime import datetime
import logging

# Set up file logging for debugging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('C:\\Projects\\fabric-query-tuner\\debug.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.connection import FabricConnection, FabricConnectionConfig
from src.query_plan import QueryPlanRetriever
from src.plan_parser import FabricPlanParser
from src.analyzer import FabricQueryAnalyzer

app = Flask(__name__, 
            template_folder=os.path.join(os.path.dirname(__file__), '..', 'templates'),
            static_folder=os.path.join(os.path.dirname(__file__), '..', 'static'))
app.secret_key = os.urandom(24)

# Disable caching for static files during development
@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


def get_statistics_details(connection, stats_names: list) -> dict:
    """
    Query sys.stats to get column names, datatype, character length and table row counts.
    Returns a dict mapping stats_name to {column_name, table_name, data_type, char_length, total_rows}.
    """
    if not stats_names:
        return {}
    
    try:
        # Make sure SHOWPLAN_XML is off before running the query
        connection.execute_non_query("SET SHOWPLAN_XML OFF")
        
        # Strip brackets from stats names - XML plan includes [], sys.stats doesn't
        clean_names = [name.strip('[]') for name in stats_names]
        
        # Build mapping from clean name back to original name
        clean_to_original = {name.strip('[]'): name for name in stats_names}
        
        logger.info(f"Looking up stats: {clean_names[:3]}...")
        
        # Look up column names by stats names (no row counts yet)
        stats_query = """
        SELECT 
            s.name AS stats_name,
            t.name AS table_name,
            SCHEMA_NAME(t.schema_id) AS schema_name,
            c.name AS column_name
        FROM sys.stats AS s
        JOIN sys.stats_columns AS sc 
            ON s.object_id = sc.object_id 
            AND s.stats_id = sc.stats_id
        JOIN sys.columns AS c 
            ON c.object_id = sc.object_id
            AND c.column_id = sc.column_id
        JOIN sys.tables AS t 
            ON t.object_id = s.object_id
        WHERE s.name IN ({})
        ORDER BY s.name, sc.stats_column_id
        """.format(','.join(f"'{name}'" for name in clean_names))
        
        logger.info(f"Stats query for {len(stats_names)} stats")
        
        conn = connection.connect()
        cursor = conn.cursor()
        cursor.execute(stats_query)
        
        # Collect stats info and unique tables
        stats_info = []
        unique_tables = set()  # (schema_name, table_name)
        
        for row in cursor.fetchall():
            clean_stats_name = row[0]
            table_name = row[1]
            schema_name = row[2]
            column_name = row[3]
            
            stats_info.append({
                'clean_stats_name': clean_stats_name,
                'table_name': table_name,
                'schema_name': schema_name,
                'column_name': column_name
            })
            unique_tables.add((schema_name, table_name))
        
        # Get row counts using COUNT_BIG(*) for each unique table
        table_row_counts = {}
        for schema_name, table_name in unique_tables:
            try:
                full_table = f"[{schema_name}].[{table_name}]"
                count_query = f"SELECT COUNT_BIG(*) FROM {full_table}"
                cursor.execute(count_query)
                count_row = cursor.fetchone()
                if count_row and count_row[0] is not None:
                    table_row_counts[(schema_name, table_name)] = int(count_row[0])
                    logger.info(f"Row count for {full_table}: {count_row[0]}")
            except Exception as count_err:
                logger.warning(f"Could not get count for {schema_name}.{table_name}: {count_err}")
                table_row_counts[(schema_name, table_name)] = None
        
        # Get column datatypes and character lengths from INFORMATION_SCHEMA.COLUMNS
        column_info = {}  # (schema, table, column) -> {data_type, char_length}
        if unique_tables:
            # Build WHERE clause for all schema.table combinations
            table_conditions = ' OR '.join(
                f"(TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}')"
                for schema, table in unique_tables
            )
            info_schema_query = f"""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE {table_conditions}
            """
            cursor.execute(info_schema_query)
            for row in cursor.fetchall():
                key = (row[0], row[1], row[2])  # schema, table, column
                column_info[key] = {
                    'data_type': row[3],
                    'char_length': row[4]
                }
        
        # Build results with all details
        results = {}
        for info in stats_info:
            clean_stats_name = info['clean_stats_name']
            table_name = info['table_name']
            schema_name = info['schema_name']
            column_name = info['column_name']
            
            full_table = f"{schema_name}.{table_name}" if schema_name else table_name
            
            # Get row count from our lookup
            total_rows = table_row_counts.get((schema_name, table_name))
            
            # Get column datatype info
            col_key = (schema_name, table_name, column_name)
            col_info = column_info.get(col_key, {})
            data_type = col_info.get('data_type', '-')
            char_length = col_info.get('char_length')
            
            # Map back to original name with brackets
            original_name = clean_to_original.get(clean_stats_name, clean_stats_name)
            
            results[original_name] = {
                'column_name': column_name,
                'table_name': full_table,
                'data_type': data_type,
                'char_length': char_length,
                'total_rows': total_rows
            }
        
        logger.info(f"Found column names for {len(results)} stats")
        cursor.close()
        return results
    except Exception as e:
        logger.error(f"Could not get stats details: {e}")
        logger.error(traceback.format_exc())
        return {}


def get_varchar8000_columns(connection, tables: set) -> list:
    """
    Query INFORMATION_SCHEMA.COLUMNS to find varchar(8000) columns in the given tables.
    Returns list of {table_name, column_name}.
    """
    if not tables:
        return []
    
    try:
        connection.execute_non_query("SET SHOWPLAN_XML OFF")
        
        # Build WHERE clause for table names (tables is a set of table names without schema)
        table_conditions = ' OR '.join(f"TABLE_NAME = '{t}'" for t in tables if t)
        if not table_conditions:
            return []
        
        query = f"""
        SELECT 
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE ({table_conditions})
          AND DATA_TYPE IN ('varchar', 'nvarchar', 'char', 'nchar')
          AND CHARACTER_MAXIMUM_LENGTH = 8000
        """
        
        conn = connection.connect()
        cursor = conn.cursor()
        cursor.execute(query)
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'table_name': f"{row[0]}.{row[1]}",
                'column_name': row[2],
                'data_type': row[3],
                'char_length': row[4]
            })
        
        cursor.close()
        logger.info(f"Found {len(results)} varchar(8000) columns")
        return results
    except Exception as e:
        logger.error(f"Could not get varchar(8000) columns: {e}")
        return []


def get_column_datatypes(connection, columns_info: list) -> dict:
    """
    Query INFORMATION_SCHEMA.COLUMNS to get the data type of specific columns.
    
    columns_info: list of {table_name, column_name, schema_name (optional)}
    Returns: dict mapping (schema.table, column) -> data_type
    """
    if not columns_info:
        return {}
    
    try:
        connection.execute_non_query("SET SHOWPLAN_XML OFF")
        
        # Build unique table.column pairs
        unique_cols = set()
        for col in columns_info:
            table = col.get('table_name', '').strip('[]')
            column = col.get('column_name', '').strip('[]')
            schema = col.get('schema_name', 'dbo').strip('[]')
            if table and column:
                unique_cols.add((schema, table, column))
        
        if not unique_cols:
            return {}
        
        # Build WHERE clause
        conditions = ' OR '.join(
            f"(TABLE_SCHEMA = '{s}' AND TABLE_NAME = '{t}' AND COLUMN_NAME = '{c}')"
            for s, t, c in unique_cols
        )
        
        query = f"""
        SELECT 
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE {conditions}
        """
        
        conn = connection.connect()
        cursor = conn.cursor()
        cursor.execute(query)
        
        results = {}
        for row in cursor.fetchall():
            schema = row[0]
            table = row[1]
            column = row[2]
            data_type = row[3]
            precision = row[4]
            scale = row[5]
            char_length = row[6]
            
            # Build full type string
            if data_type in ('decimal', 'numeric') and precision:
                full_type = f"{data_type}({precision},{scale or 0})"
            elif data_type in ('varchar', 'nvarchar', 'char', 'nchar') and char_length:
                if char_length == -1:
                    full_type = f"{data_type}(max)"
                else:
                    full_type = f"{data_type}({char_length})"
            else:
                full_type = data_type
            
            key = (f"{schema}.{table}", column)
            results[key] = {
                'data_type': data_type,
                'full_type': full_type,
                'precision': precision,
                'scale': scale,
                'char_length': char_length
            }
        
        cursor.close()
        logger.info(f"Got datatypes for {len(results)} columns")
        return results
    except Exception as e:
        logger.error(f"Could not get column datatypes: {e}")
        return {}


def detect_join_type_mismatches(plan_info, column_datatypes: dict) -> list:
    """
    Compare datatypes of left and right join columns.
    Detects:
    - Different base types (decimal vs bigint, varchar vs nvarchar)
    - Different precision/scale for numeric types
    - Different lengths for string types
    
    Returns list of mismatches with details.
    """
    mismatches = []
    
    # Define type families for comparison
    numeric_types = {'int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney'}
    string_types = {'varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext'}
    
    for join in plan_info.joins:
        for i, left_col in enumerate(join.left_columns):
            if i < len(join.right_columns):
                right_col = join.right_columns[i]
                
                # Build lookup keys
                left_key = (f"{left_col.schema or 'dbo'}.{left_col.table}", left_col.name)
                right_key = (f"{right_col.schema or 'dbo'}.{right_col.table}", right_col.name)
                
                left_type_info = column_datatypes.get(left_key)
                right_type_info = column_datatypes.get(right_key)
                
                if left_type_info and right_type_info:
                    left_type = left_type_info.get('data_type', '').lower()
                    right_type = right_type_info.get('data_type', '').lower()
                    left_full = left_type_info.get('full_type', '')
                    right_full = right_type_info.get('full_type', '')
                    
                    mismatch_reason = None
                    
                    # Check 1: Different base types
                    if left_type != right_type:
                        mismatch_reason = f"Different types: {left_full} vs {right_full}"
                    
                    # Check 2: Same numeric type but different precision/scale
                    elif left_type in ('decimal', 'numeric'):
                        left_prec = left_type_info.get('precision')
                        left_scale = left_type_info.get('scale') or 0
                        right_prec = right_type_info.get('precision')
                        right_scale = right_type_info.get('scale') or 0
                        
                        if left_prec != right_prec or left_scale != right_scale:
                            mismatch_reason = f"Precision/scale mismatch: {left_full} vs {right_full}"
                    
                    # Check 3: Same string type but different lengths
                    elif left_type in string_types:
                        left_len = left_type_info.get('char_length')
                        right_len = right_type_info.get('char_length')
                        
                        if left_len and right_len and left_len != right_len:
                            mismatch_reason = f"Length mismatch: {left_full} vs {right_full}"
                    
                    if mismatch_reason:
                        # Update the join object
                        join.has_type_mismatch = True
                        join.type_mismatch_details = (
                            f"{left_col.table}.{left_col.name} ({left_full}) vs "
                            f"{right_col.table}.{right_col.name} ({right_full})"
                        )
                        
                        mismatches.append({
                            'left_table': left_col.table,
                            'left_column': left_col.name,
                            'left_type': left_full,
                            'right_table': right_col.table,
                            'right_column': right_col.name,
                            'right_type': right_full,
                            'reason': mismatch_reason
                        })
                        logger.info(f"Type mismatch: {mismatch_reason}")
    
    return mismatches


def calculate_column_skew(connection, columns_info: list) -> list:
    """
    Calculate data skew for columns used in joins, group by, order by.
    Skew = (distinct_count / total_rows) * 100
    If skew < 50%, it indicates high skew (few distinct values relative to total rows).
    
    columns_info: list of {table_name, column_name, schema_name (optional)}
    Returns: list of {table_name, column_name, distinct_count, total_rows, skew_percent}
    """
    if not columns_info:
        return []
    
    try:
        connection.execute_non_query("SET SHOWPLAN_XML OFF")
        conn = connection.connect()
        cursor = conn.cursor()
        
        results = []
        seen = set()  # Avoid duplicate calculations
        
        for col_info in columns_info:
            table_name = col_info.get('table_name', '').strip('[]')
            column_name = col_info.get('column_name', '').strip('[]')
            schema_name = col_info.get('schema_name', 'dbo').strip('[]')
            
            if not table_name or not column_name:
                continue
            
            key = (schema_name, table_name, column_name)
            if key in seen:
                continue
            seen.add(key)
            
            try:
                full_table = f"[{schema_name}].[{table_name}]"
                
                # Get distinct count and total rows - use APPROX_COUNT_DISTINCT for speed
                skew_query = f"""
                SELECT 
                    APPROX_COUNT_DISTINCT([{column_name}]) as distinct_count,
                    COUNT_BIG(*) as total_rows
                FROM {full_table}
                """
                
                cursor.execute(skew_query)
                row = cursor.fetchone()
                
                if row and row[1] and row[1] > 0:
                    distinct_count = int(row[0]) if row[0] else 0
                    total_rows = int(row[1])
                    skew_percent = (distinct_count / total_rows) * 100
                    
                    results.append({
                        'table_name': f"{schema_name}.{table_name}",
                        'column_name': column_name,
                        'distinct_count': distinct_count,
                        'total_rows': total_rows,
                        'skew_percent': round(skew_percent, 2)
                    })
                    logger.info(f"Skew for {full_table}.{column_name}: {skew_percent:.2f}% ({distinct_count}/{total_rows})")
            except Exception as col_err:
                logger.warning(f"Could not calculate skew for {table_name}.{column_name}: {col_err}")
        
        cursor.close()
        return results
    except Exception as e:
        logger.error(f"Could not calculate column skew: {e}")
        return []


def get_sql_pool_info(connection) -> list:
    """
    Query queryinsights.sql_pool_insights for the latest SQL pool configuration.
    Returns the last 2 records showing pool configuration changes.
    """
    logger.info("Starting get_sql_pool_info function")
    try:
        logger.info("Attempting to query sql_pool_insights view")
        connection.execute_non_query("SET SHOWPLAN_XML OFF")
        
        pool_query = """
        SELECT TOP 2 * 
        FROM [queryinsights].[sql_pool_insights] 
        ORDER BY timestamp DESC
        """
        
        # Use connection.execute_query() which handles cursor internally
        results = connection.execute_query(pool_query)
        
        # Log the raw results for debugging
        logger.info(f"Raw SQL pool results: {results}")
        
        # Convert datetime values to string for JSON serialization
        serialized_results = []
        for record in results:
            serialized_record = {}
            for key, val in record.items():
                if hasattr(val, 'isoformat'):
                    serialized_record[key] = val.isoformat()
                elif val is not None:
                    serialized_record[key] = val
                else:
                    serialized_record[key] = None
            serialized_results.append(serialized_record)
        
        logger.info(f"Retrieved {len(serialized_results)} SQL pool insight records")
        return serialized_results
    
    except Exception as e:
        logger.error(f"Could not get SQL pool info: {e}")
        logger.error(traceback.format_exc())
        if 'sql_pool_insights' in str(e).lower() or 'invalid object' in str(e).lower():
            logger.error("The queryinsights.sql_pool_insights view may not exist or you may not have access.")
        return []


def get_query_history(connection, query_hash: str) -> dict:
    """
    Query queryinsights.exec_requests_history for historical execution data.
    Returns time series data and aggregate statistics for the query hash.
    """
    if not query_hash:
        logger.warning("No query hash provided - cannot look up history")
        return {'time_series': [], 'aggregates': None}
    
    logger.info(f"Looking up query history for hash: {query_hash}")
    
    try:
        connection.execute_non_query("SET SHOWPLAN_XML OFF")
        
        conn = connection.connect()
        cursor = conn.cursor()
        
        # Try both the exact hash and lowercase version (Fabric may store differently)
        query_hash_lower = query_hash.lower() if query_hash else query_hash
        
        # Get time series data for chart (submit_time vs elapsed time) - last 30 days
        time_series_query = f"""
        SELECT 
            submit_time,
            total_elapsed_time_ms / 1000.0 AS elapsed_seconds,
            data_scanned_remote_storage_mb,
            data_scanned_disk_mb,
            result_cache_hit,
            row_count
        FROM queryinsights.exec_requests_history
        WHERE query_hash = '{query_hash}' OR query_hash = '{query_hash_lower}'
          AND submit_time >= DATEADD(day, -30, GETDATE())
        ORDER BY submit_time DESC
        """
        
        cursor.execute(time_series_query)
        time_series = []
        for row in cursor.fetchall():
            time_series.append({
                'submit_time': row[0].isoformat() if row[0] else None,
                'elapsed_seconds': float(row[1]) if row[1] else 0,
                'data_scanned_remote_mb': float(row[2]) if row[2] else 0,
                'data_scanned_disk_mb': float(row[3]) if row[3] else 0,
                'result_cache_hit': int(row[4]) if row[4] is not None else 0,  # 0=N/A, 1=Created, 2=Hit
                'row_count': int(row[5]) if row[5] else 0
            })
        
        logger.info(f"Found {len(time_series)} historical executions for query hash {query_hash}")
        
        # Get aggregate statistics - last 30 days
        aggregates_query = f"""
        SELECT
            COUNT(*) AS execution_count,
            MIN(total_elapsed_time_ms/1000.0) AS min_elapsed_seconds,
            MAX(total_elapsed_time_ms/1000.0) AS max_elapsed_seconds,
            AVG(total_elapsed_time_ms/1000.0) AS avg_elapsed_seconds,
            MIN(data_scanned_remote_storage_mb) AS min_data_scanned_remote_mb,
            MAX(data_scanned_remote_storage_mb) AS max_data_scanned_remote_mb,
            AVG(data_scanned_remote_storage_mb) AS avg_data_scanned_remote_mb,
            MIN(data_scanned_disk_mb) AS min_data_scanned_disk_mb,
            MAX(data_scanned_disk_mb) AS max_data_scanned_disk_mb,
            AVG(data_scanned_disk_mb) AS avg_data_scanned_disk_mb,
            SUM(CASE WHEN result_cache_hit = 2 THEN 1 ELSE 0 END) AS cache_hits,
            MIN(row_count) AS min_row_count,
            MAX(row_count) AS max_row_count,
            AVG(CAST(row_count AS FLOAT)) AS avg_row_count
        FROM queryinsights.exec_requests_history
        WHERE (query_hash = '{query_hash}' OR query_hash = '{query_hash_lower}')
          AND submit_time >= DATEADD(day, -30, GETDATE())
        """
        
        cursor.execute(aggregates_query)
        agg_row = cursor.fetchone()
        
        aggregates = None
        if agg_row and agg_row[0] > 0:
            execution_count = int(agg_row[0])
            cache_hits = int(agg_row[10]) if agg_row[10] else 0
            aggregates = {
                'execution_count': execution_count,
                'min_elapsed_seconds': round(float(agg_row[1]), 2) if agg_row[1] else 0,
                'max_elapsed_seconds': round(float(agg_row[2]), 2) if agg_row[2] else 0,
                'avg_elapsed_seconds': round(float(agg_row[3]), 2) if agg_row[3] else 0,
                'min_data_scanned_remote_mb': round(float(agg_row[4]), 2) if agg_row[4] else 0,
                'max_data_scanned_remote_mb': round(float(agg_row[5]), 2) if agg_row[5] else 0,
                'avg_data_scanned_remote_mb': round(float(agg_row[6]), 2) if agg_row[6] else 0,
                'min_data_scanned_disk_mb': round(float(agg_row[7]), 2) if agg_row[7] else 0,
                'max_data_scanned_disk_mb': round(float(agg_row[8]), 2) if agg_row[8] else 0,
                'avg_data_scanned_disk_mb': round(float(agg_row[9]), 2) if agg_row[9] else 0,
                'cache_hit_count': cache_hits,
                'cache_hit_percent': round((cache_hits / execution_count) * 100, 1) if execution_count > 0 else 0,
                'min_row_count': int(agg_row[11]) if agg_row[11] else 0,
                'max_row_count': int(agg_row[12]) if agg_row[12] else 0,
                'avg_row_count': round(float(agg_row[13]), 0) if agg_row[13] else 0
            }
        
        cursor.close()
        
        if not time_series and not aggregates:
            logger.warning(f"No query history found for hash {query_hash}. This query may not have been executed yet, only its estimated plan was retrieved.")
        
        return {'time_series': time_series, 'aggregates': aggregates}
    
    except Exception as e:
        logger.error(f"Could not get query history: {e}")
        logger.error(traceback.format_exc())
        # Check if this might be a permission or view access issue
        if 'queryinsights' in str(e).lower() or 'invalid object' in str(e).lower():
            logger.error("The queryinsights.exec_requests_history view may not exist or you may not have access. Query Insights must be enabled in your Fabric workspace.")
        return {'time_series': [], 'aggregates': None}


def analyze_query(server: str, database: str, auth_method: str, 
                  username: str, password: str, query: str) -> dict:
    """
    Connect to Fabric DW, get execution plan, and analyze it.
    Returns analysis results as a dictionary.
    """
    try:
        # Create connection config
        config = FabricConnectionConfig(
            server=server,
            database=database,
            authentication=auth_method,
            username=username if username else None,
            password=password if password else None
        )
        
        # Connect and get execution plan
        connection = FabricConnection(config)
        connection.connect()
        
        retriever = QueryPlanRetriever(connection)
        plan_xml = retriever.get_estimated_plan_xml(query)
        
        # Parse the plan
        parser = FabricPlanParser(plan_xml)
        plan_info = parser.parse()
        
        # Note: Analyzer will be called after we get actual_rows
        
        # Enrich statistics with column names from sys.stats
        stats_names = [s.stats_name for s in plan_info.statistics_used if s.stats_name]
        logger.info(f"Stats names from plan: {stats_names}")
        
        # Get column names and table row counts for each stats_name from sys.stats
        stats_details = get_statistics_details(connection, stats_names)
        logger.info(f"Mapped {len(stats_details)} stats to column names")
        
        # Build a table cardinality map from operators (from TableCardinality in index scans)
        table_cardinality_map = {}
        for op in plan_info.operators:
            if op.table_name and op.table_cardinality is not None:
                # Use the first cardinality we find for each table
                if op.table_name not in table_cardinality_map:
                    table_cardinality_map[op.table_name] = op.table_cardinality
        
        # Build statistics info list with column names, using XML data for dates and percentages
        enriched_stats = []
        for s in plan_info.statistics_used:
            # Get details from sys.stats lookup
            details = stats_details.get(s.stats_name, {})
            column_name = details.get('column_name', s.stats_name or '-')
            table_name = details.get('table_name', s.table_name or '-')
            total_rows = details.get('total_rows')
            data_type = details.get('data_type', '-')
            char_length = details.get('char_length')
            
            # Get table cardinality from scan operators
            # Strip schema prefix if present for lookup
            table_name_simple = table_name.split('.')[-1] if table_name else ''
            table_cardinality = table_cardinality_map.get(table_name_simple)
            
            enriched_stats.append({
                'column_name': column_name,
                'table_name': table_name,
                'data_type': data_type,
                'char_length': char_length,
                'last_update': s.last_update,  # From XML plan
                'total_rows': total_rows,  # From COUNT(*) - will be renamed to Actual Rows
                'table_cardinality': table_cardinality,  # From TableCardinality in index scans
                'sampling_percent': s.sampling_percent  # From XML plan
            })
        
        # Collect all tables from the plan for varchar(8000) detection
        tables_in_plan = set()
        for op in plan_info.operators:
            if op.table_name:
                tables_in_plan.add(op.table_name.strip('[]'))
        
        # Get varchar(8000) columns from schema
        varchar8000_from_schema = get_varchar8000_columns(connection, tables_in_plan)
        logger.info(f"Found {len(varchar8000_from_schema)} varchar(8000) columns from schema")
        
        # Collect columns used in joins for skew calculation
        join_columns_for_skew = []
        for join in plan_info.joins:
            for col in join.left_columns + join.right_columns:
                if col.table and col.name:
                    join_columns_for_skew.append({
                        'table_name': col.table,
                        'column_name': col.name,
                        'schema_name': col.schema or 'dbo'
                    })
        
        # Also collect columns from sort operators (ORDER BY)
        for op in plan_info.sort_operators:
            for col in op.columns:
                if col.table and col.name:
                    join_columns_for_skew.append({
                        'table_name': col.table,
                        'column_name': col.name,
                        'schema_name': col.schema or 'dbo'
                    })
        
        # Get column datatypes for join columns to detect type mismatches
        column_datatypes = get_column_datatypes(connection, join_columns_for_skew)
        logger.info(f"Got datatypes for {len(column_datatypes)} columns")
        
        # Detect type mismatches between join columns
        type_mismatches = detect_join_type_mismatches(plan_info, column_datatypes)
        if type_mismatches:
            logger.info(f"Detected {len(type_mismatches)} join type mismatches from schema comparison")
        
        # Calculate column skew for these columns
        column_skew_results = calculate_column_skew(connection, join_columns_for_skew)
        logger.info(f"Calculated skew for {len(column_skew_results)} columns")
        
        # Actual row count query disabled - not running the query to avoid performance impact
        # To enable, uncomment the block below
        actual_rows = None
        # # Get actual row count by running the query wrapped in COUNT_BIG(*)
        # logger.info("About to get actual row count...")
        # try:
        #     # Get a fresh connection/cursor and ensure SHOWPLAN_XML is OFF
        #     logger.info("Getting connection...")
        #     conn = connection.connect()
        #     cursor = conn.cursor()
        #     logger.info("Turning off SHOWPLAN_XML...")
        #     cursor.execute("SET SHOWPLAN_XML OFF")
        #     
        #     # Strip trailing semicolons and whitespace from query
        #     clean_query = query.strip().rstrip(';').strip()
        #     count_query = f"SELECT COUNT_BIG(*) FROM ({clean_query}) dt2;"
        #     logger.info(f"Executing count query: {count_query[:200]}...")
        #     
        #     cursor.execute(count_query)
        #     logger.info("Query executed, fetching row...")
        #     row = cursor.fetchone()
        #     logger.info(f"Count result row = {row}")
        #     
        #     if row and row[0] is not None:
        #         actual_rows = int(row[0])
        #         logger.info(f"actual_rows = {actual_rows}")
        #     else:
        #         logger.warning("Row was None or row[0] was None")
        #     cursor.close()
        # except Exception as count_err:
        #     logger.error(f"Could not get actual row count: {count_err}")
        #     logger.error(traceback.format_exc())
        #     actual_rows = None
        # 
        # logger.info(f"Final actual_rows = {actual_rows}")
        
        # Build table stats data for low stats detection (comparing Table Cardinality vs Actual Rows)
        # This uses enriched_stats which has: table_name, column_name, table_cardinality, total_rows (actual), sampling_percent
        table_stats_data = []
        for stat in enriched_stats:
            if stat.get('table_cardinality') is not None and stat.get('total_rows') is not None:
                table_stats_data.append({
                    'table_name': stat.get('table_name', ''),
                    'column_name': stat.get('column_name', ''),
                    'table_cardinality': stat.get('table_cardinality'),
                    'actual_rows': stat.get('total_rows'),  # total_rows is from COUNT_BIG, i.e. actual rows
                    'sampling_percent': stat.get('sampling_percent')
                })
        logger.info(f"Built table stats data for {len(table_stats_data)} columns")
        
        # Now run the analyzer with actual_rows and skew data available
        analyzer = FabricQueryAnalyzer(
            plan_info, 
            actual_rows=actual_rows,
            varchar8000_columns=varchar8000_from_schema,
            column_skew_data=column_skew_results,
            table_stats_data=table_stats_data
        )
        report = analyzer.analyze()
        
        # Get query history based on query hash
        query_hash = plan_info.query_hash
        query_history = {'time_series': [], 'aggregates': None}
        if query_hash:
            logger.info(f"Fetching query history for hash: {query_hash}")
            query_history = get_query_history(connection, query_hash)
        
        # Get SQL pool configuration info
        sql_pool_info = get_sql_pool_info(connection)
        
        connection.disconnect()
        
        # Build response
        return {
            'success': True,
            'query': query,
            'is_xml_only': False,  # This is a live connection analysis
            # Query hash for history lookup
            'query_hash': query_hash,
            'query_history': query_history,
            # SQL pool configuration info
            'sql_pool_info': sql_pool_info,
            # Statement info
            'statement_type': plan_info.statement_type,
            'estimated_rows': plan_info.statement_estimated_rows,
            # 'actual_rows': actual_rows,  # Disabled - not running actual query
            # 'retrieved_from_cache': plan_info.retrieved_from_cache,  # Disabled
            # Compile info
            'compile_time': plan_info.compile_time,
            'compile_cpu': plan_info.compile_cpu,
            'compile_memory': plan_info.compile_memory,
            # Parallelism
            'estimated_cost': plan_info.estimated_total_cost,
            'dop': plan_info.degree_of_parallelism,
            'estimated_available_dop': plan_info.estimated_available_dop,
            # Score and summary
            'score': report.score,
            'report_summary': report.summary,
            # All operators with details
            'all_operators': [
                {
                    'name': op.physical_op,
                    'logical_op': op.logical_op,
                    'table': op.table_name or '',
                    'estimated_rows': int(op.estimated_rows),
                    'estimated_cpu': round(op.estimated_cpu, 6),
                    'avg_row_size': op.avg_row_size,
                    'cost_percent': round(op.cost_percent, 1),
                    'io_cost': round(op.estimated_io, 6),
                    'subtree_cost': round(op.estimated_subtree_cost, 6),
                    'is_high_cost': op.cost_percent > 20,  # Highlight > 20%
                    'category': op.operator_category
                }
                for op in sorted(plan_info.operators, 
                               key=lambda x: x.cost_percent, 
                               reverse=True)
            ],
            # Top operators (backward compat)
            'top_operators': [
                {
                    'name': op.physical_op,
                    'table': op.table_name or '',
                    'cost_percent': round(op.cost_percent, 1),
                    'rows': int(op.estimated_rows),
                    'cpu_cost': round(op.estimated_cpu, 4),
                    'io_cost': round(op.estimated_io, 4)
                }
                for op in sorted(plan_info.operators, 
                               key=lambda x: x.cost_percent, 
                               reverse=True)[:10]
            ],
            # High-cost data movement operators (Shuffle only, > 20% of query cost)
            'high_cost_operators': sorted([
                {
                    'name': f"{op.physical_op}, {op.logical_op}",
                    'physical_op': op.physical_op,
                    'logical_op': op.logical_op,
                    'table': op.table_name or '',
                    'cost_percent': round(op.cost_percent, 1),
                    'estimated_rows': int(op.estimated_rows),
                    'estimated_cpu': round(op.estimated_cpu, 6),
                    'estimated_io': round(op.estimated_io, 6),
                    'avg_row_size': op.avg_row_size,
                    'distribution_type': op.distribution_type,
                    'move_topology': op.move_topology,
                    'distribution_key': [c.name for c in op.distribution_key] if op.distribution_key else [],
                    'output_columns': [f"{c.table}.{c.name}" if c.table else c.name for c in op.output_columns] if op.output_columns else []
                }
                for op in plan_info.operators if op.cost_percent > 20 and op.physical_op.lower() == 'shuffle'
            ], key=lambda x: x['cost_percent'], reverse=True),
            # Join operators section
            'join_operators': [
                {
                    'name': op.physical_op,
                    'logical_op': op.logical_op,
                    'estimated_rows': int(op.estimated_rows),
                    'estimated_cpu': round(op.estimated_cpu, 6),
                    'cost_percent': round(op.cost_percent, 1)
                }
                for op in plan_info.join_operators
            ],
            # Aggregate operators section
            'aggregate_operators': [
                {
                    'name': op.physical_op,
                    'logical_op': op.logical_op,
                    'estimated_rows': int(op.estimated_rows),
                    'estimated_cpu': round(op.estimated_cpu, 6),
                    'cost_percent': round(op.cost_percent, 1)
                }
                for op in plan_info.aggregate_operators
            ],
            # Shuffle/Distribution operators section
            'shuffle_operators': [
                {
                    'name': op.physical_op,
                    'logical_op': op.logical_op,
                    'estimated_rows': int(op.estimated_rows),
                    'estimated_cpu': round(op.estimated_cpu, 6),
                    'cost_percent': round(op.cost_percent, 1)
                }
                for op in plan_info.shuffle_operators
            ],
            # Sort/Order By operators section
            'sort_operators': [
                {
                    'name': op.physical_op,
                    'logical_op': op.logical_op,
                    'estimated_rows': int(op.estimated_rows),
                    'estimated_cpu': round(op.estimated_cpu, 6),
                    'cost_percent': round(op.cost_percent, 1)
                }
                for op in plan_info.sort_operators
            ],
            # Statistics info enriched with sys.stats details
            'statistics_info': enriched_stats,
            'recommendations': [
                {
                    'severity': rec.severity.value,
                    'category': rec.category,
                    'title': rec.title,
                    'description': rec.description,
                    'suggestion': rec.recommendation,
                    'affected_objects': [x for x in [rec.table, rec.operator] if x]
                }
                for rec in report.issues
            ],
            'summary': {
                'critical': sum(1 for r in report.issues if r.severity.value == 'CRITICAL'),
                'high': sum(1 for r in report.issues if r.severity.value == 'HIGH'),
                'medium': sum(1 for r in report.issues if r.severity.value == 'MEDIUM'),
                'low': sum(1 for r in report.issues if r.severity.value == 'LOW'),
                'total': len(report.issues)
            },
            'joins': [
                {
                    'type': j.join_type.value if hasattr(j.join_type, 'value') else str(j.join_type),
                    'tables': f"{j.left_columns[0].table if j.left_columns else 'Left'} ↔ {j.right_columns[0].table if j.right_columns else 'Right'}",
                    'left_join_columns': [c.name for c in j.left_columns] if j.left_columns else [],
                    'right_join_columns': [c.name for c in j.right_columns] if j.right_columns else [],
                    'is_many_to_many': j.is_many_to_many,
                    'has_type_mismatch': j.has_type_mismatch,
                    'left_rows': int(j.estimated_input_rows_left),
                    'right_rows': int(j.estimated_input_rows_right),
                    'output_rows': int(j.estimated_output_rows),
                    'cost': round(j.estimated_subtree_cost, 4),
                    'cpu': round(j.estimated_cpu, 6)
                }
                for j in plan_info.joins
            ],
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }


def analyze_plan_xml(plan_xml: str) -> dict:
    """
    Analyze an existing execution plan XML.
    """
    try:
        parser = FabricPlanParser(plan_xml)
        plan_info = parser.parse()
        
        # For XML-only analysis, we don't have actual_rows
        analyzer = FabricQueryAnalyzer(plan_info, actual_rows=None)
        report = analyzer.analyze()
        
        # Build a table cardinality map from operators
        table_cardinality_map = {}
        for op in plan_info.operators:
            if op.table_name and op.table_cardinality is not None:
                if op.table_name not in table_cardinality_map:
                    table_cardinality_map[op.table_name] = op.table_cardinality
        
        return {
            'success': True,
            'query': plan_info.statement_text or '(from XML)',
            # Statement info
            'statement_type': plan_info.statement_type,
            'estimated_rows': plan_info.statement_estimated_rows,
            'actual_rows': None,  # Cannot get actual rows from XML-only analysis
            'is_xml_only': True,  # Flag to indicate XML-only mode
            'retrieved_from_cache': plan_info.retrieved_from_cache,
            # Compile info
            'compile_time': plan_info.compile_time,
            'compile_cpu': plan_info.compile_cpu,
            'compile_memory': plan_info.compile_memory,
            # Parallelism
            'estimated_cost': plan_info.estimated_total_cost,
            'dop': plan_info.degree_of_parallelism,
            'estimated_available_dop': plan_info.estimated_available_dop,
            # Score and summary
            'score': report.score,
            'report_summary': report.summary,
            # All operators with details
            'all_operators': [
                {
                    'name': op.physical_op,
                    'logical_op': op.logical_op,
                    'table': op.table_name or '',
                    'estimated_rows': int(op.estimated_rows),
                    'estimated_cpu': round(op.estimated_cpu, 6),
                    'avg_row_size': op.avg_row_size,
                    'cost_percent': round(op.cost_percent, 1),
                    'io_cost': round(op.estimated_io, 6),
                    'subtree_cost': round(op.estimated_subtree_cost, 6),
                    'is_high_cost': op.cost_percent > 20,
                    'category': op.operator_category
                }
                for op in sorted(plan_info.operators, 
                               key=lambda x: x.cost_percent, 
                               reverse=True)
            ],
            # Top operators (backward compat)
            'top_operators': [
                {
                    'name': op.physical_op,
                    'table': op.table_name or '',
                    'cost_percent': round(op.cost_percent, 1),
                    'rows': int(op.estimated_rows),
                    'cpu_cost': round(op.estimated_cpu, 4),
                    'io_cost': round(op.estimated_io, 4)
                }
                for op in sorted(plan_info.operators, 
                               key=lambda x: x.cost_percent, 
                               reverse=True)[:10]
            ],
            # High-cost data movement operators (Shuffle only, > 20% of query cost)
            'high_cost_operators': sorted([
                {
                    'name': f"{op.physical_op}, {op.logical_op}",
                    'physical_op': op.physical_op,
                    'logical_op': op.logical_op,
                    'table': op.table_name or '',
                    'cost_percent': round(op.cost_percent, 1),
                    'estimated_rows': int(op.estimated_rows),
                    'estimated_cpu': round(op.estimated_cpu, 6),
                    'estimated_io': round(op.estimated_io, 6),
                    'avg_row_size': op.avg_row_size,
                    'distribution_type': op.distribution_type,
                    'move_topology': op.move_topology,
                    'distribution_key': [c.name for c in op.distribution_key] if op.distribution_key else [],
                    'output_columns': [f"{c.table}.{c.name}" if c.table else c.name for c in op.output_columns] if op.output_columns else []
                }
                for op in plan_info.operators if op.cost_percent > 20 and op.physical_op.lower() == 'shuffle'
            ], key=lambda x: x['cost_percent'], reverse=True),
            # Join operators section
            'join_operators': [
                {
                    'name': op.physical_op,
                    'logical_op': op.logical_op,
                    'estimated_rows': int(op.estimated_rows),
                    'estimated_cpu': round(op.estimated_cpu, 6),
                    'cost_percent': round(op.cost_percent, 1)
                }
                for op in plan_info.join_operators
            ],
            # Aggregate operators section
            'aggregate_operators': [
                {
                    'name': op.physical_op,
                    'logical_op': op.logical_op,
                    'estimated_rows': int(op.estimated_rows),
                    'estimated_cpu': round(op.estimated_cpu, 6),
                    'cost_percent': round(op.cost_percent, 1)
                }
                for op in plan_info.aggregate_operators
            ],
            # Shuffle/Distribution operators section
            'shuffle_operators': [
                {
                    'name': op.physical_op,
                    'logical_op': op.logical_op,
                    'estimated_rows': int(op.estimated_rows),
                    'estimated_cpu': round(op.estimated_cpu, 6),
                    'cost_percent': round(op.cost_percent, 1)
                }
                for op in plan_info.shuffle_operators
            ],
            # Sort/Order By operators section
            'sort_operators': [
                {
                    'name': op.physical_op,
                    'logical_op': op.logical_op,
                    'estimated_rows': int(op.estimated_rows),
                    'estimated_cpu': round(op.estimated_cpu, 6),
                    'cost_percent': round(op.cost_percent, 1)
                }
                for op in plan_info.sort_operators
            ],
            # Statistics info from plan (no sys.stats lookup for XML-only analysis)
            'statistics_info': [
                {
                    'column_name': s.stats_name or '-',  # Use stats_name as column indicator
                    'table_name': s.table_name or '-',
                    'last_update': s.last_update,
                    'table_cardinality': table_cardinality_map.get((s.table_name or '').split('.')[-1] or s.table_name),
                    'total_rows': s.total_rows,
                    'sampling_percent': s.sampling_percent
                }
                for s in plan_info.statistics_used
            ],
            'recommendations': [
                {
                    'severity': rec.severity.value,
                    'category': rec.category,
                    'title': rec.title,
                    'description': rec.description,
                    'suggestion': rec.recommendation,
                    'affected_objects': [x for x in [rec.table, rec.operator] if x]
                }
                for rec in report.issues
            ],
            'summary': {
                'critical': sum(1 for r in report.issues if r.severity.value == 'CRITICAL'),
                'high': sum(1 for r in report.issues if r.severity.value == 'HIGH'),
                'medium': sum(1 for r in report.issues if r.severity.value == 'MEDIUM'),
                'low': sum(1 for r in report.issues if r.severity.value == 'LOW'),
                'total': len(report.issues)
            },
            'joins': [
                {
                    'type': j.join_type.value if hasattr(j.join_type, 'value') else str(j.join_type),
                    'tables': f"{j.left_columns[0].table if j.left_columns else 'Left'} ↔ {j.right_columns[0].table if j.right_columns else 'Right'}",
                    'left_join_columns': [c.name for c in j.left_columns] if j.left_columns else [],
                    'right_join_columns': [c.name for c in j.right_columns] if j.right_columns else [],
                    'is_many_to_many': j.is_many_to_many,
                    'has_type_mismatch': j.has_type_mismatch,
                    'left_rows': int(j.estimated_input_rows_left),
                    'right_rows': int(j.estimated_input_rows_right),
                    'output_rows': int(j.estimated_output_rows),
                    'cost': round(j.estimated_subtree_cost, 4),
                    'cpu': round(j.estimated_cpu, 6)
                }
                for j in plan_info.joins
            ],
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }


@app.route('/')
def index():
    """Main page with query input form."""
    return render_template('index.html')


@app.route('/analyze', methods=['POST'])
def analyze():
    """Analyze a query by connecting to Fabric DW."""
    data = request.get_json()
    
    server = data.get('server', '').strip()
    database = data.get('database', '').strip()
    auth_method = data.get('auth_method', 'ActiveDirectoryInteractive')
    username = data.get('username', '').strip()
    password = data.get('password', '')
    query = data.get('query', '').strip()
    
    # Validation
    if not server or not database:
        return jsonify({'success': False, 'error': 'Server and database are required'})
    
    if not query:
        return jsonify({'success': False, 'error': 'Query is required'})
    
    if auth_method == 'ActiveDirectoryPassword' and (not username or not password):
        return jsonify({'success': False, 'error': 'Username and password required for Azure AD Password auth'})
    
    result = analyze_query(server, database, auth_method, username, password, query)
    return jsonify(result)


@app.route('/analyze-xml', methods=['POST'])
def analyze_xml():
    """Analyze an uploaded execution plan XML."""
    if 'planFile' in request.files:
        file = request.files['planFile']
        plan_xml = file.read().decode('utf-8')
    else:
        data = request.get_json()
        plan_xml = data.get('plan_xml', '')
    
    if not plan_xml:
        return jsonify({'success': False, 'error': 'No execution plan XML provided'})
    
    result = analyze_plan_xml(plan_xml)
    return jsonify(result)


@app.route('/run', methods=['POST'])
def run_query():
    """Execute a query and return the results."""
    data = request.get_json()
    
    server = data.get('server', '').strip()
    database = data.get('database', '').strip()
    auth_method = data.get('auth_method', 'ActiveDirectoryInteractive')
    username = data.get('username', '').strip()
    password = data.get('password', '')
    query = data.get('query', '').strip()
    max_rows = data.get('max_rows', 1000)  # Limit results for safety
    
    # Validation
    if not server or not database:
        return jsonify({'success': False, 'error': 'Server and database are required'})
    
    if not query:
        return jsonify({'success': False, 'error': 'Query is required'})
    
    if auth_method == 'ActiveDirectoryPassword' and (not username or not password):
        return jsonify({'success': False, 'error': 'Username and password required for Azure AD Password auth'})
    
    try:
        # Create connection
        config = FabricConnectionConfig(
            server=server,
            database=database,
            authentication=auth_method,
            username=username if username else None,
            password=password if password else None
        )
        
        connection = FabricConnection(config)
        conn = connection.connect()
        cursor = conn.cursor()
        
        # Execute query
        import time
        start_time = time.time()
        cursor.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        
        # Fetch rows (limited)
        rows = []
        row_count = 0
        for row in cursor:
            if row_count >= max_rows:
                break
            rows.append([str(val) if val is not None else None for val in row])
            row_count += 1
        
        # Check if there are more rows
        has_more = False
        try:
            next_row = cursor.fetchone()
            has_more = next_row is not None
        except:
            pass
        
        execution_time = time.time() - start_time
        
        cursor.close()
        connection.disconnect()
        
        return jsonify({
            'success': True,
            'columns': columns,
            'rows': rows,
            'row_count': row_count,
            'has_more': has_more,
            'max_rows': max_rows,
            'execution_time': round(execution_time, 3),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        })


@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})


def run_webapp(host='127.0.0.1', port=5000, debug=False):
    """Start the Flask web application."""
    print(f"\n{'='*60}")
    print("FABRIC DW QUERY PERFORMANCE ANALYZER - Web Application")
    print(f"{'='*60}")
    print(f"\n  Starting server at: http://{host}:{port}")
    print(f"  Debug mode: {debug}")
    print(f"\n  Press Ctrl+C to stop the server\n")
    print(f"{'='*60}\n")
    
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Fabric DW Query Performance Analyzer Web App')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
    parser.add_argument('--port', '-p', type=int, default=5000, help='Port to run on')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    run_webapp(host=args.host, port=args.port, debug=args.debug)
