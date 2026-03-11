"""
Execution Plan XML Parser for Microsoft Fabric Data Warehouse
Parses SHOWPLAN_XML output and extracts key performance information.
Optimized for Fabric DW (Delta Parquet / OLAP workloads).
"""

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Optional, List
from enum import Enum
import re


# SQL Server showplan XML namespace
SHOWPLAN_NS = {'sp': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}


class JoinType(Enum):
    """Types of join operations."""
    NESTED_LOOPS = "Nested Loops"
    HASH_MATCH = "Hash Match"
    MERGE_JOIN = "Merge Join"
    UNKNOWN = "Unknown"


@dataclass
class ColumnInfo:
    """Information about a column including data type."""
    name: str
    table: Optional[str] = None
    schema: Optional[str] = None
    database: Optional[str] = None
    data_type: Optional[str] = None
    max_length: Optional[int] = None
    source_expression: Optional[str] = None  # For implicit conversions


@dataclass
class JoinInfo:
    """Information about a join operation."""
    join_type: JoinType
    logical_op: str  # Inner Join, Left Outer Join, etc.
    left_columns: List[ColumnInfo]
    right_columns: List[ColumnInfo]
    estimated_input_rows_left: float
    estimated_input_rows_right: float
    estimated_output_rows: float
    estimated_cpu: float
    estimated_io: float
    estimated_subtree_cost: float
    is_many_to_many: bool = False
    data_explosion_ratio: float = 1.0
    has_type_mismatch: bool = False
    type_mismatch_details: Optional[str] = None


@dataclass
class PlanOperator:
    """Represents an operator in the execution plan."""
    node_id: int
    physical_op: str
    logical_op: str
    estimated_rows: float
    estimated_cpu: float
    estimated_io: float
    estimated_total_cost: float
    estimated_subtree_cost: float
    avg_row_size: int = 0  # AvgRowSize from plan
    table_name: Optional[str] = None
    index_name: Optional[str] = None
    table_cardinality: Optional[int] = None  # TableCardinality from index scans
    columns: List[ColumnInfo] = field(default_factory=list)
    predicates: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    parallel: bool = False
    actual_rows: Optional[int] = None
    actual_executions: Optional[int] = None
    memory_grant_kb: Optional[int] = None
    # Cost percentages of total query cost
    cost_percent: float = 0.0
    cpu_percent: float = 0.0
    io_percent: float = 0.0
    # Operator category for grouping
    operator_category: Optional[str] = None  # 'join', 'aggregate', 'shuffle', 'sort'
    # Shuffle/Move operator specific fields
    distribution_type: Optional[str] = None  # Hash, Broadcast, etc.
    move_topology: Optional[str] = None  # ManyToMany, etc.
    distribution_key: List[ColumnInfo] = field(default_factory=list)  # DistributionKey columns
    output_columns: List[ColumnInfo] = field(default_factory=list)  # OutputList columns


@dataclass
class ImplicitConversion:
    """Tracks implicit type conversions in the plan."""
    expression: str
    from_type: Optional[str] = None
    to_type: Optional[str] = None
    column_name: Optional[str] = None
    table_name: Optional[str] = None


@dataclass
class StatisticsInfo:
    """Statistics information for a column."""
    stats_name: str
    table_name: str
    column_name: Optional[str] = None
    sampling_percent: Optional[float] = None
    last_update: Optional[str] = None
    modification_count: Optional[int] = None
    total_rows: Optional[int] = None


@dataclass
class ExecutionPlanInfo:
    """High-level information about an execution plan for Fabric DW."""
    statement_text: str
    query_hash: Optional[str]
    query_plan_hash: Optional[str]
    estimated_total_cost: float
    degree_of_parallelism: int
    memory_grant_kb: Optional[int]
    operators: List[PlanOperator]
    joins: List[JoinInfo]
    warnings: list
    all_columns: List[ColumnInfo]
    implicit_conversions: List[ImplicitConversion]
    varchar_8000_columns: List[ColumnInfo]
    # Cost breakdown
    total_cpu_cost: float
    total_io_cost: float
    # NEW: Statement level info
    statement_type: str = "SELECT"  # SELECT, INSERT, UPDATE, DELETE, etc.
    statement_estimated_rows: float = 0.0  # StatementEstRows
    retrieved_from_cache: bool = False  # RetrievedFromCache
    # NEW: Compile info
    compile_time: int = 0  # CompileTime in ms
    compile_cpu: int = 0  # CompileCPU in ms
    compile_memory: int = 0  # CompileMemory in KB
    # NEW: Parallelism info
    estimated_available_dop: int = 1  # EstimatedAvailableDegreeOfParallelism
    # NEW: Statistics referenced in plan
    statistics_used: List[StatisticsInfo] = field(default_factory=list)
    # NEW: Categorized operators
    join_operators: List[PlanOperator] = field(default_factory=list)
    aggregate_operators: List[PlanOperator] = field(default_factory=list)
    shuffle_operators: List[PlanOperator] = field(default_factory=list)
    sort_operators: List[PlanOperator] = field(default_factory=list)


class FabricPlanParser:
    """Parses SQL Server/Fabric execution plan XML with focus on OLAP patterns."""
    
    def __init__(self, plan_xml: str):
        self.plan_xml = plan_xml
        self.root = ET.fromstring(plan_xml)
        self.all_columns: List[ColumnInfo] = []
        self.joins: List[JoinInfo] = []
        self.implicit_conversions: List[ImplicitConversion] = []
        self.varchar_8000_columns: List[ColumnInfo] = []
    
    def parse(self) -> ExecutionPlanInfo:
        """Parse the execution plan XML and extract all relevant information."""
        
        # Find the statement element
        stmt_elem = self.root.find('.//sp:StmtSimple', SHOWPLAN_NS)
        if stmt_elem is None:
            stmt_elem = self.root.find('.//sp:StmtCond', SHOWPLAN_NS)
        
        statement_text = stmt_elem.get('StatementText', '') if stmt_elem else ''
        query_hash = stmt_elem.get('QueryHash') if stmt_elem else None
        query_plan_hash = stmt_elem.get('QueryPlanHash') if stmt_elem else None
        
        # Get query plan element
        query_plan = self.root.find('.//sp:QueryPlan', SHOWPLAN_NS)
        
        # Extract plan-level info
        estimated_cost = float(stmt_elem.get('StatementSubTreeCost', 0)) if stmt_elem else 0
        dop = int(query_plan.get('DegreeOfParallelism', 1)) if query_plan else 1
        memory_grant = None
        if query_plan is not None:
            mem_str = query_plan.get('MemoryGrant')
            if mem_str:
                memory_grant = int(mem_str)
        
        # Parse operators
        operators = self._parse_operators(query_plan) if query_plan else []
        
        # Extract joins with detailed info
        self._extract_joins(query_plan)
        
        # Scan for varchar(8000) and implicit conversions
        self._scan_for_type_issues()
        
        # Extract warnings
        warnings = self._extract_warnings()
        
        # Calculate total costs
        total_cpu = sum(op.estimated_cpu for op in operators)
        total_io = sum(op.estimated_io for op in operators)
        
        # Calculate cost percentages for each operator based on EstimateCPU
        for op in operators:
            if total_cpu > 0:
                op.cost_percent = (op.estimated_cpu / total_cpu) * 100
            else:
                op.cost_percent = 0
            op.cpu_percent = op.cost_percent  # Same as cost_percent now
            op.io_percent = (op.estimated_io / max(total_io, 0.0001)) * 100
        
        # NEW: Extract statement-level info
        statement_type = stmt_elem.get('StatementType', 'SELECT') if stmt_elem else 'SELECT'
        statement_est_rows = float(stmt_elem.get('StatementEstRows', 0)) if stmt_elem else 0.0
        retrieved_from_cache = stmt_elem.get('RetrievedFromCache', 'false').lower() == 'true' if stmt_elem else False
        
        # NEW: Extract compile info from query plan
        compile_time = 0
        compile_cpu = 0
        compile_memory = 0
        estimated_available_dop = 1
        
        if query_plan is not None:
            compile_time = int(query_plan.get('CompileTime', 0))
            compile_cpu = int(query_plan.get('CompileCPU', 0))
            compile_memory = int(query_plan.get('CompileMemory', 0))
            estimated_available_dop = int(query_plan.get('EstimatedAvailableDegreeOfParallelism', 1))
        
        # NEW: Extract statistics used in the plan
        statistics_used = self._extract_statistics_used()
        
        # NEW: Categorize operators
        join_operators = []
        aggregate_operators = []
        shuffle_operators = []
        sort_operators = []
        
        for op in operators:
            physical_op_lower = op.physical_op.lower()
            logical_op_lower = op.logical_op.lower()
            
            # Join operators
            if any(x in physical_op_lower for x in ['nested loops', 'hash match', 'merge join']):
                if 'join' in logical_op_lower or 'inner' in logical_op_lower or 'outer' in logical_op_lower:
                    op.operator_category = 'join'
                    join_operators.append(op)
                elif 'aggregate' in logical_op_lower:
                    op.operator_category = 'aggregate'
                    aggregate_operators.append(op)
            # Aggregate operators
            elif any(x in physical_op_lower for x in ['stream aggregate', 'hash aggregate', 'hash match']) and 'aggregate' in logical_op_lower:
                op.operator_category = 'aggregate'
                aggregate_operators.append(op)
            # Shuffle/Distribution operators (common in Fabric DW)
            elif any(x in physical_op_lower for x in ['distribute', 'shuffle', 'broadcast', 'repartition', 'parallelism']):
                op.operator_category = 'shuffle'
                shuffle_operators.append(op)
            # Sort operators
            elif any(x in physical_op_lower for x in ['sort', 'order']):
                op.operator_category = 'sort'
                sort_operators.append(op)
        
        return ExecutionPlanInfo(
            statement_text=statement_text,
            query_hash=query_hash,
            query_plan_hash=query_plan_hash,
            estimated_total_cost=estimated_cost,
            degree_of_parallelism=dop,
            memory_grant_kb=memory_grant,
            operators=operators,
            joins=self.joins,
            warnings=warnings,
            all_columns=self.all_columns,
            implicit_conversions=self.implicit_conversions,
            varchar_8000_columns=self.varchar_8000_columns,
            total_cpu_cost=total_cpu,
            total_io_cost=total_io,
            statement_type=statement_type,
            statement_estimated_rows=statement_est_rows,
            retrieved_from_cache=retrieved_from_cache,
            compile_time=compile_time,
            compile_cpu=compile_cpu,
            compile_memory=compile_memory,
            estimated_available_dop=estimated_available_dop,
            statistics_used=statistics_used,
            join_operators=join_operators,
            aggregate_operators=aggregate_operators,
            shuffle_operators=shuffle_operators,
            sort_operators=sort_operators
        )
    
    def _parse_operators(self, query_plan_elem) -> List[PlanOperator]:
        """Parse all operators in the execution plan."""
        operators = []
        
        for relop in query_plan_elem.findall('.//sp:RelOp', SHOWPLAN_NS):
            operator = self._parse_single_operator(relop)
            operators.append(operator)
        
        return operators
    
    def _parse_single_operator(self, relop_elem) -> PlanOperator:
        """Parse a single RelOp element."""
        node_id = int(relop_elem.get('NodeId', 0))
        physical_op = relop_elem.get('PhysicalOp', 'Unknown')
        logical_op = relop_elem.get('LogicalOp', 'Unknown')
        estimated_rows = float(relop_elem.get('EstimateRows', 0))
        estimated_cpu = float(relop_elem.get('EstimateCPU', 0))
        estimated_io = float(relop_elem.get('EstimateIO', 0))
        estimated_total = estimated_cpu + estimated_io
        estimated_subtree = float(relop_elem.get('EstimatedTotalSubtreeCost', 0))
        avg_row_size = int(relop_elem.get('AvgRowSize', 0))  # NEW: Extract AvgRowSize
        parallel = relop_elem.get('Parallel', '0') == '1'
        
        # Extract table and index names
        table_name = None
        index_name = None
        
        obj_elem = relop_elem.find('.//sp:Object', SHOWPLAN_NS)
        if obj_elem is not None:
            table_name = obj_elem.get('Table', '').strip('[]')
            index_name = obj_elem.get('Index', '').strip('[]')
        
        # Extract TableCardinality from index scan elements or RelOp attributes
        table_cardinality = None
        
        # First check the RelOp element itself
        tc_str = relop_elem.get('TableCardinality')
        if tc_str:
            try:
                table_cardinality = int(float(tc_str))
            except (ValueError, TypeError):
                pass
        
        # Check various scan type elements
        if table_cardinality is None:
            for scan_type in ['sp:IndexScan', 'sp:TableScan', 'sp:RemoteQuery', 'sp:RemoteScan', 
                              'sp:RemoteRange', 'sp:RowsetScans', 'sp:Spool', 'sp:ConstantScan']:
                scan_elem = relop_elem.find(f'.//{scan_type}', SHOWPLAN_NS)
                if scan_elem is not None:
                    tc_str = scan_elem.get('TableCardinality')
                    if tc_str:
                        try:
                            table_cardinality = int(float(tc_str))
                        except (ValueError, TypeError):
                            pass
                        break
        
        # Also check any element with TableCardinality attribute
        if table_cardinality is None:
            for elem in relop_elem.iter():
                tc_str = elem.get('TableCardinality')
                if tc_str:
                    try:
                        table_cardinality = int(float(tc_str))
                        break
                    except (ValueError, TypeError):
                        pass
        
        # Extract output columns
        columns = []
        output_list = relop_elem.find('sp:OutputList', SHOWPLAN_NS)
        if output_list is not None:
            for col_ref in output_list.findall('.//sp:ColumnReference', SHOWPLAN_NS):
                col_info = self._parse_column_reference(col_ref)
                if col_info:
                    columns.append(col_info)
        
        # Extract predicates
        predicates = []
        for pred in relop_elem.findall('.//sp:Predicate', SHOWPLAN_NS):
            pred_text = self._extract_predicate_text(pred)
            if pred_text:
                predicates.append(pred_text)
        
        # Also check SeekPredicates
        for seek_pred in relop_elem.findall('.//sp:SeekPredicates', SHOWPLAN_NS):
            for seek_pred_new in seek_pred.findall('.//sp:SeekPredicateNew', SHOWPLAN_NS):
                for seek_keys in seek_pred_new.findall('.//sp:SeekKeys', SHOWPLAN_NS):
                    pred_text = self._extract_predicate_text(seek_keys)
                    if pred_text:
                        predicates.append(pred_text)
        
        # Extract warnings
        warnings = []
        for warning in relop_elem.findall('.//sp:Warnings', SHOWPLAN_NS):
            for child in warning:
                tag = child.tag.replace('{http://schemas.microsoft.com/sqlserver/2004/07/showplan}', '')
                warnings.append(tag)
        
        # Extract Move/Shuffle specific info (DistributionType, MoveTopology, DistributionKey)
        distribution_type = None
        move_topology = None
        distribution_key = []
        output_columns = columns[:]  # Copy the columns as output_columns
        
        # Check for Move element (used in Shuffle operators)
        move_elem = relop_elem.find('.//sp:Move', SHOWPLAN_NS)
        if move_elem is not None:
            distribution_type = move_elem.get('DistributionType')
            move_topology = move_elem.get('MoveTopology')
            
            # Extract DistributionKey columns
            dist_key_elem = move_elem.find('sp:DistributionKey', SHOWPLAN_NS)
            if dist_key_elem is not None:
                for col_ref in dist_key_elem.findall('sp:ColumnReference', SHOWPLAN_NS):
                    col_info = self._parse_column_reference(col_ref)
                    if col_info:
                        distribution_key.append(col_info)
        
        return PlanOperator(
            node_id=node_id,
            physical_op=physical_op,
            logical_op=logical_op,
            estimated_rows=estimated_rows,
            estimated_cpu=estimated_cpu,
            estimated_io=estimated_io,
            estimated_total_cost=estimated_total,
            estimated_subtree_cost=estimated_subtree,
            avg_row_size=avg_row_size,
            table_name=table_name,
            index_name=index_name,
            table_cardinality=table_cardinality,
            columns=columns,
            predicates=predicates,
            warnings=warnings,
            parallel=parallel,
            distribution_type=distribution_type,
            move_topology=move_topology,
            distribution_key=distribution_key,
            output_columns=output_columns
        )
    
    def _parse_column_reference(self, col_ref_elem) -> Optional[ColumnInfo]:
        """Parse a ColumnReference element."""
        name = col_ref_elem.get('Column', '')
        if not name:
            return None
        
        return ColumnInfo(
            name=name.strip('[]'),
            table=col_ref_elem.get('Table', '').strip('[]') or None,
            schema=col_ref_elem.get('Schema', '').strip('[]') or None,
            database=col_ref_elem.get('Database', '').strip('[]') or None
        )
    
    def _extract_joins(self, query_plan_elem):
        """Extract detailed join information."""
        if query_plan_elem is None:
            return
        
        for relop in query_plan_elem.findall('.//sp:RelOp', SHOWPLAN_NS):
            physical_op = relop.get('PhysicalOp', '')
            logical_op = relop.get('LogicalOp', '')
            
            # Check if this is a join operation
            is_join = logical_op in ('Inner Join', 'Left Outer Join', 'Right Outer Join', 
                                     'Full Outer Join', 'Left Semi Join', 'Right Semi Join',
                                     'Left Anti Semi Join', 'Right Anti Semi Join')
            
            if not is_join:
                continue
            
            # Determine join type
            if physical_op == 'Nested Loops':
                join_type = JoinType.NESTED_LOOPS
            elif physical_op == 'Hash Match':
                join_type = JoinType.HASH_MATCH
            elif physical_op == 'Merge Join':
                join_type = JoinType.MERGE_JOIN
            else:
                join_type = JoinType.UNKNOWN
            
            estimated_output = float(relop.get('EstimateRows', 0))
            estimated_cpu = float(relop.get('EstimateCPU', 0))
            estimated_io = float(relop.get('EstimateIO', 0))
            estimated_subtree = float(relop.get('EstimatedTotalSubtreeCost', 0))
            
            # Get child operators to determine input sizes
            child_relops = []
            for child in relop:
                child_relops.extend(child.findall('sp:RelOp', SHOWPLAN_NS))
            
            left_input_rows = 0.0
            right_input_rows = 0.0
            
            if len(child_relops) >= 2:
                left_input_rows = float(child_relops[0].get('EstimateRows', 0))
                right_input_rows = float(child_relops[1].get('EstimateRows', 0))
            elif len(child_relops) == 1:
                left_input_rows = float(child_relops[0].get('EstimateRows', 0))
            
            # Extract join columns
            left_columns = []
            right_columns = []
            has_type_mismatch = False
            type_mismatch_details = None
            
            # Check Hash join keys
            hash_elem = relop.find('.//sp:Hash', SHOWPLAN_NS)
            if hash_elem is not None:
                build_keys = hash_elem.find('sp:HashKeysBuild', SHOWPLAN_NS)
                probe_keys = hash_elem.find('sp:HashKeysProbe', SHOWPLAN_NS)
                
                if build_keys is not None:
                    for col_ref in build_keys.findall('.//sp:ColumnReference', SHOWPLAN_NS):
                        col = self._parse_column_reference(col_ref)
                        if col:
                            left_columns.append(col)
                
                if probe_keys is not None:
                    for col_ref in probe_keys.findall('.//sp:ColumnReference', SHOWPLAN_NS):
                        col = self._parse_column_reference(col_ref)
                        if col:
                            right_columns.append(col)
            
            # Check Nested Loops outer references
            nested_loop = relop.find('.//sp:NestedLoops', SHOWPLAN_NS)
            if nested_loop is not None:
                outer_refs = nested_loop.find('sp:OuterReferences', SHOWPLAN_NS)
                if outer_refs is not None:
                    for col_ref in outer_refs.findall('.//sp:ColumnReference', SHOWPLAN_NS):
                        col = self._parse_column_reference(col_ref)
                        if col:
                            left_columns.append(col)
            
            # Check for implicit conversions in join predicates (type mismatch)
            for pred in relop.findall('.//sp:Predicate', SHOWPLAN_NS):
                pred_text = self._extract_predicate_text(pred)
                if pred_text and 'CONVERT' in pred_text.upper():
                    has_type_mismatch = True
                    type_mismatch_details = pred_text
            
            # Check for many-to-many (data explosion)
            max_input = max(left_input_rows, right_input_rows, 1)
            min_input = min(left_input_rows, right_input_rows) if min(left_input_rows, right_input_rows) > 0 else 1
            data_explosion_ratio = estimated_output / max_input if max_input > 0 else 1.0
            
            # Many-to-many if output is significantly larger than max input
            is_many_to_many = data_explosion_ratio > 2.0
            
            join_info = JoinInfo(
                join_type=join_type,
                logical_op=logical_op,
                left_columns=left_columns,
                right_columns=right_columns,
                estimated_input_rows_left=left_input_rows,
                estimated_input_rows_right=right_input_rows,
                estimated_output_rows=estimated_output,
                estimated_cpu=estimated_cpu,
                estimated_io=estimated_io,
                estimated_subtree_cost=estimated_subtree,
                is_many_to_many=is_many_to_many,
                data_explosion_ratio=data_explosion_ratio,
                has_type_mismatch=has_type_mismatch,
                type_mismatch_details=type_mismatch_details
            )
            
            self.joins.append(join_info)
    
    def _scan_for_type_issues(self):
        """Scan the plan for varchar(8000) and implicit conversions."""
        
        # Scan all ScalarOperator elements for type issues
        for scalar_op in self.root.findall('.//sp:ScalarOperator', SHOWPLAN_NS):
            scalar_string = scalar_op.get('ScalarString', '')
            
            if not scalar_string:
                continue
            
            # Check for varchar(8000)
            if re.search(r'varchar\s*\(\s*8000\s*\)', scalar_string, re.IGNORECASE):
                col_info = ColumnInfo(
                    name='(expression)',
                    data_type='varchar(8000)',
                    max_length=8000,
                    source_expression=scalar_string[:200]  # Truncate long expressions
                )
                self.varchar_8000_columns.append(col_info)
            
            # Check for implicit CONVERT operations
            if 'CONVERT' in scalar_string.upper():
                conversion = ImplicitConversion(expression=scalar_string[:200])
                
                # Try to extract types from CONVERT expression
                convert_match = re.search(r'CONVERT\s*\(\s*(\w+(?:\s*\([^)]+\))?)', scalar_string, re.IGNORECASE)
                if convert_match:
                    conversion.to_type = convert_match.group(1)
                
                # Extract column name if present
                col_match = re.search(r'\[(\w+)\]\.\[(\w+)\]\.\[(\w+)\]', scalar_string)
                if col_match:
                    conversion.table_name = col_match.group(2)
                    conversion.column_name = col_match.group(3)
                
                self.implicit_conversions.append(conversion)
        
        # Also check Convert elements directly
        for convert_elem in self.root.findall('.//sp:Convert', SHOWPLAN_NS):
            data_type = convert_elem.get('DataType', '')
            implicit = convert_elem.get('Implicit', '0') == '1'
            
            if implicit:
                conversion = ImplicitConversion(
                    expression=f"Implicit conversion to {data_type}",
                    to_type=data_type
                )
                self.implicit_conversions.append(conversion)
            
            # Check specifically for varchar(8000)
            length = convert_elem.get('Length', '')
            if data_type.lower() == 'varchar' and length == '8000':
                col_info = ColumnInfo(
                    name='(conversion)',
                    data_type='varchar(8000)',
                    max_length=8000
                )
                self.varchar_8000_columns.append(col_info)
    
    def _extract_predicate_text(self, pred_elem) -> Optional[str]:
        """Extract predicate text from a Predicate element."""
        scalar_ops = pred_elem.findall('.//sp:ScalarOperator', SHOWPLAN_NS)
        if scalar_ops:
            scalar_string = scalar_ops[0].get('ScalarString', '')
            return scalar_string if scalar_string else None
        return None
    
    def _extract_warnings(self) -> list:
        """Extract all warnings from the plan."""
        warnings = []
        
        for warning_elem in self.root.findall('.//sp:Warnings', SHOWPLAN_NS):
            for child in warning_elem:
                tag = child.tag.replace('{http://schemas.microsoft.com/sqlserver/2004/07/showplan}', '')
                
                if tag == 'SpillToTempDb':
                    spill_level = child.get('SpillLevel', 'Unknown')
                    warnings.append(f"SpillToTempDb (Level: {spill_level})")
                elif tag == 'NoJoinPredicate':
                    warnings.append("NoJoinPredicate - Cartesian product detected!")
                elif tag == 'ColumnsWithNoStatistics':
                    warnings.append("ColumnsWithNoStatistics - Statistics may be outdated")
                elif tag == 'PlanAffectingConvert':
                    warnings.append("PlanAffectingConvert - Implicit conversion affecting plan")
                else:
                    warnings.append(tag)
        
        return warnings
    
    def _extract_statistics_used(self) -> List[StatisticsInfo]:
        """Extract statistics information referenced in the plan."""
        statistics_list = []
        seen_stats = set()  # Avoid duplicates
        
        # Find all StatisticsInfo elements in the plan
        for stats_elem in self.root.findall('.//sp:StatisticsInfo', SHOWPLAN_NS):
            stats_name = stats_elem.get('Statistics', '')
            if not stats_name or stats_name in seen_stats:
                continue
            seen_stats.add(stats_name)
            
            table_name = stats_elem.get('Table', '').strip('[]')
            schema_name = stats_elem.get('Schema', '').strip('[]')
            sampling_percent = stats_elem.get('SamplingPercent')
            last_update = stats_elem.get('LastUpdate')  # Extract LastUpdate from XML
            mod_count = stats_elem.get('ModificationCount')
            
            # Create full table name with schema if available
            full_table = f"{schema_name}.{table_name}" if schema_name else table_name
            
            stat_info = StatisticsInfo(
                stats_name=stats_name,
                table_name=full_table,
                sampling_percent=float(sampling_percent) if sampling_percent else None,
                last_update=last_update,
                modification_count=int(mod_count) if mod_count else None
            )
            statistics_list.append(stat_info)
        
        # Also look for OptimizerStatsUsage elements (newer format)
        for usage_elem in self.root.findall('.//sp:OptimizerStatsUsage', SHOWPLAN_NS):
            for stat_elem in usage_elem.findall('.//sp:StatisticsInfo', SHOWPLAN_NS):
                stats_name = stat_elem.get('Statistics', '')
                if not stats_name or stats_name in seen_stats:
                    continue
                seen_stats.add(stats_name)
                
                table_name = stat_elem.get('Table', '').strip('[]')
                sampling_percent = stat_elem.get('SamplingPercent')
                mod_count = stat_elem.get('ModificationCount')
                last_update = stat_elem.get('LastUpdate')
                
                stat_info = StatisticsInfo(
                    stats_name=stats_name,
                    table_name=table_name,
                    sampling_percent=float(sampling_percent) if sampling_percent else None,
                    last_update=last_update,
                    modification_count=int(mod_count) if mod_count else None
                )
                statistics_list.append(stat_info)
        
        return statistics_list
    
    def get_operator_tree_text(self) -> str:
        """Get a text representation of the operator tree."""
        query_plan = self.root.find('.//sp:QueryPlan', SHOWPLAN_NS)
        if query_plan is None:
            return "No query plan found"
        
        lines = []
        self._build_operator_tree(query_plan.find('sp:RelOp', SHOWPLAN_NS), lines, 0)
        return '\n'.join(lines)
    
    def _build_operator_tree(self, relop, lines: list, depth: int):
        """Recursively build the operator tree text."""
        if relop is None:
            return
        
        indent = "  " * depth
        physical_op = relop.get('PhysicalOp', 'Unknown')
        cost = float(relop.get('EstimatedTotalSubtreeCost', 0))
        rows = float(relop.get('EstimateRows', 0))
        cpu = float(relop.get('EstimateCPU', 0))
        io = float(relop.get('EstimateIO', 0))
        
        # Get table info if available
        obj_info = ""
        obj_elem = relop.find('.//sp:Object', SHOWPLAN_NS)
        if obj_elem is not None:
            table = obj_elem.get('Table', '').strip('[]')
            if table:
                obj_info = f" [{table}]"
        
        lines.append(f"{indent}├─ {physical_op}{obj_info} (Cost: {cost:.4f}, Rows: {rows:.0f}, CPU: {cpu:.4f}, I/O: {io:.4f})")
        
        # Find child operators
        for child in relop:
            child_relops = child.findall('sp:RelOp', SHOWPLAN_NS)
            for child_relop in child_relops:
                self._build_operator_tree(child_relop, lines, depth + 1)


def parse_plan_from_file(filepath: str) -> ExecutionPlanInfo:
    """Parse an execution plan from an XML file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        xml_content = f.read()
    
    parser = FabricPlanParser(xml_content)
    return parser.parse()


# Backward compatibility alias
PlanParser = FabricPlanParser
