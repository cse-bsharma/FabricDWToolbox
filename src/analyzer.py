"""
Fabric Data Warehouse Query Tuning Analyzer
Analyzes execution plans with focus on OLAP workloads and Delta Parquet storage.

Checks:
1. Datatype Check (HIGH) - varchar(8000), join datatype mismatches
2. Stats and Skew Check (CRITICAL) - estimated vs actual mismatch, skew detection
3. Join Check (HIGH) - many-to-many joins, anti-join patterns
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, List
from .plan_parser import ExecutionPlanInfo, PlanOperator, JoinInfo, JoinType


class IssueSeverity(Enum):
    """Severity levels for identified issues."""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


@dataclass
class TuningIssue:
    """Represents a performance issue found in the query."""
    severity: IssueSeverity
    category: str
    title: str
    description: str
    recommendation: str
    operator: Optional[str] = None
    table: Optional[str] = None
    estimated_impact: Optional[str] = None


@dataclass 
class StatisticsRecommendation:
    """Recommendation for updating statistics."""
    table: str
    columns: List[str]
    command: str
    reason: str


@dataclass
class TuningReport:
    """Complete tuning analysis report for Fabric DW."""
    query: str
    total_cost: float
    issues: List[TuningIssue]
    statistics_recommendations: List[StatisticsRecommendation]
    high_cost_operators: List[dict]  # Top expensive operators
    summary: str
    score: int  # 0-100 score (higher is better)


class FabricQueryAnalyzer:
    """
    Analyzes query execution plans for Microsoft Fabric Data Warehouse.
    
    Checks:
    1. Datatype Check (HIGH) - varchar(8000), join datatype mismatches
    2. Stats and Skew Check (CRITICAL) - estimated vs actual mismatch, skew detection
    3. Join Check (HIGH) - many-to-many joins, anti-join patterns
    """
    
    # Thresholds
    MANY_TO_MANY_THRESHOLD = 1.5  # Output rows > 1.5x max input is many-to-many
    SKEW_THRESHOLD = 50.0  # < 50% distinct values = skewed data
    STATS_SAMPLING_THRESHOLD = 20.0  # < 20% sampling needs full stats
    CARDINALITY_MISMATCH_THRESHOLD = 20.0  # > 20% variation between table cardinality and actual rows
    ROW_ESTIMATE_MISMATCH_FACTOR = 10.0  # 10x difference = drastic mismatch
    
    def __init__(self, plan_info: ExecutionPlanInfo, actual_rows: Optional[int] = None,
                 varchar8000_columns: Optional[List[dict]] = None,
                 column_skew_data: Optional[List[dict]] = None,
                 table_stats_data: Optional[List[dict]] = None):
        self.plan_info = plan_info
        self.actual_rows = actual_rows
        self.varchar8000_columns = varchar8000_columns or []
        self.column_skew_data = column_skew_data or []
        self.table_stats_data = table_stats_data or []  # {table_name, column_name, table_cardinality, actual_rows, sampling_percent}
        self.issues: List[TuningIssue] = []
        self.statistics_recommendations: List[StatisticsRecommendation] = []
        self.high_cost_operators: List[dict] = []
    
    def analyze(self) -> TuningReport:
        """Perform Fabric DW-specific analysis with 3 focused checks."""
        
        # Check 1: Datatype Check (HIGH)
        self._check_datatypes()
        
        # Check 2: Stats and Skew Check (CRITICAL)
        self._check_stats_and_skew()
        
        # Check 3: Join Check (HIGH)
        self._check_joins()
        
        # Calculate score
        score = self._calculate_score()
        
        # Generate summary
        summary = self._generate_summary()
        
        return TuningReport(
            query=self.plan_info.statement_text,
            total_cost=self.plan_info.estimated_total_cost,
            issues=self.issues,
            statistics_recommendations=self.statistics_recommendations,
            high_cost_operators=self.high_cost_operators,
            summary=summary,
            score=score
        )
    
    def _check_datatypes(self):
        """
        Check 1: Datatype Check (HIGH)
        - Check for varchar(8000) columns and recommend correct length
        - Check joining columns have same datatype, recommend to change if different
        """
        # Check for VARCHAR(8000) from schema lookup (more reliable than plan detection)
        if self.varchar8000_columns:
            for col in self.varchar8000_columns:
                self.issues.append(TuningIssue(
                    severity=IssueSeverity.HIGH,
                    category="Datatype Check",
                    title="VARCHAR(8000) Detected",
                    description=(
                        f"Column [{col.get('column_name')}] in table [{col.get('table_name')}] "
                        f"is defined as {col.get('data_type', 'varchar')}(8000). "
                        f"This is an inefficient datatype that impacts storage and query performance."
                    ),
                    recommendation=(
                        f"Change the column [{col.get('column_name')}] datatype to the correct length "
                        f"based on actual data requirements. For example, if the max data length is 100 characters, "
                        f"use VARCHAR(100) instead of VARCHAR(8000).\n\n"
                        f"ALTER TABLE [{col.get('table_name')}] ALTER COLUMN [{col.get('column_name')}] VARCHAR(appropriate_length);"
                    ),
                    table=col.get('table_name'),
                    estimated_impact="High - Inefficient storage and memory usage"
                ))
        
        # Also check from plan detection (for expressions causing varchar(8000))
        if self.plan_info.varchar_8000_columns:
            for col in self.plan_info.varchar_8000_columns:
                self.issues.append(TuningIssue(
                    severity=IssueSeverity.HIGH,
                    category="Datatype Check",
                    title="VARCHAR(8000) in Expression",
                    description=(
                        f"Found varchar(8000) in expression: {col.source_expression or col.name}. "
                        f"This is an inefficient datatype that impacts storage and query performance."
                    ),
                    recommendation=(
                        "Review the expression and use explicit CAST/CONVERT with proper lengths. "
                        "For example: CAST(column AS VARCHAR(100)) instead of implicit varchar(8000)."
                    ),
                    estimated_impact="High - Inefficient storage and memory usage"
                ))
        
        # Check for join column datatype mismatches
        for join in self.plan_info.joins:
            if join.has_type_mismatch:
                left_cols = ', '.join(c.name for c in join.left_columns) or 'unknown'
                right_cols = ', '.join(c.name for c in join.right_columns) or 'unknown'
                
                self.issues.append(TuningIssue(
                    severity=IssueSeverity.HIGH,
                    category="Datatype Check",
                    title=f"Join Column Datatype Mismatch",
                    description=(
                        f"Joining columns have different datatypes. "
                        f"Left: {left_cols}. Right: {right_cols}. "
                        f"Conversion: {join.type_mismatch_details or 'Implicit CONVERT detected'}. "
                        f"This causes implicit type conversion on every row."
                    ),
                    recommendation=(
                        "Ensure both joining columns have the same datatype. "
                        "Modify the table schema to align datatypes (e.g., both INT or both BIGINT, "
                        "both VARCHAR(50), etc.). Avoid mixing varchar/nvarchar or int/bigint."
                    ),
                    operator=join.join_type.value,
                    estimated_impact="High - Runtime type conversion overhead"
                ))
    
    def _check_stats_and_skew(self):
        """
        Check 2: Stats and Skew Check (CRITICAL)
        - Low Stats: Compare Table Cardinality vs Actual Rows - if variation > 20% AND sampling < 20%, recommend full stats
        - Data Skew: Only flag when BOTH left and right columns in a join are skewed (< 50%)
        """
        # Build a lookup for skew data: (table_name, column_name) -> skew_percent
        skew_lookup = {}
        for skew_info in self.column_skew_data:
            table_name = skew_info.get('table_name', '').strip()
            column_name = skew_info.get('column_name', '').strip()
            if table_name and column_name:
                # Normalize key - try with/without schema
                skew_lookup[(table_name, column_name)] = skew_info
                # Also store without schema prefix
                if '.' in table_name:
                    simple_table = table_name.split('.')[-1]
                    skew_lookup[(simple_table, column_name)] = skew_info
        
        # Check for data skew ONLY when BOTH join columns are skewed
        # This causes many-to-many join issues; one-to-many joins with one skewed column are OK
        for join in self.plan_info.joins:
            left_skew_info = None
            right_skew_info = None
            
            # Get skew for left columns
            for col in join.left_columns:
                if col.table and col.name:
                    key = (col.table.strip('[]'), col.name.strip('[]'))
                    if key in skew_lookup:
                        left_skew_info = skew_lookup[key]
                        break
            
            # Get skew for right columns
            for col in join.right_columns:
                if col.table and col.name:
                    key = (col.table.strip('[]'), col.name.strip('[]'))
                    if key in skew_lookup:
                        right_skew_info = skew_lookup[key]
                        break
            
            # Only flag if BOTH sides are skewed (< 50%)
            if left_skew_info and right_skew_info:
                left_skew_pct = left_skew_info.get('skew_percent', 100)
                right_skew_pct = right_skew_info.get('skew_percent', 100)
                
                if left_skew_pct < self.SKEW_THRESHOLD and right_skew_pct < self.SKEW_THRESHOLD:
                    left_table = left_skew_info.get('table_name', 'unknown')
                    left_col = left_skew_info.get('column_name', 'unknown')
                    right_table = right_skew_info.get('table_name', 'unknown')
                    right_col = right_skew_info.get('column_name', 'unknown')
                    
                    self.issues.append(TuningIssue(
                        severity=IssueSeverity.CRITICAL,
                        category="Stats and Skew Check",
                        title=f"Data Skew in Join Columns: {left_col} = {right_col}",
                        description=(
                            f"Both join columns have high data skew which causes many-to-many join performance issues. "
                            f"Left: [{left_col}] in [{left_table}] has {left_skew_pct:.1f}% cardinality. "
                            f"Right: [{right_col}] in [{right_table}] has {right_skew_pct:.1f}% cardinality. "
                            f"When both columns are skewed, it leads to data explosion and hotspots."
                        ),
                        recommendation=(
                            f"Consider pre-aggregating data before the join to reduce many-to-many relationships:\n\n"
                            f"1. Aggregate or deduplicate one side of the join before joining\n"
                            f"2. Collect full statistics on both columns:\n"
                            f"   UPDATE STATISTICS [{left_table}] WITH FULLSCAN;\n"
                            f"   UPDATE STATISTICS [{right_table}] WITH FULLSCAN;\n\n"
                            f"3. Review join logic - ensure join predicates are complete"
                        ),
                        operator=join.join_type.value if hasattr(join.join_type, 'value') else str(join.join_type),
                        estimated_impact=f"Critical - Both columns skewed ({left_skew_pct:.1f}% and {right_skew_pct:.1f}%)"
                    ))
                    
                    # Add stats recommendations
                    self.statistics_recommendations.append(StatisticsRecommendation(
                        table=left_table,
                        columns=[left_col],
                        command=f"UPDATE STATISTICS [{left_table}] WITH FULLSCAN;",
                        reason=f"Join column skew ({left_skew_pct:.1f}%)"
                    ))
                    self.statistics_recommendations.append(StatisticsRecommendation(
                        table=right_table,
                        columns=[right_col],
                        command=f"UPDATE STATISTICS [{right_table}] WITH FULLSCAN;",
                        reason=f"Join column skew ({right_skew_pct:.1f}%)"
                    ))
        
        # Check for Low Statistics: Compare Table Cardinality vs Actual Rows
        # If variation > 20% AND sampling < 20%, recommend full stats on the column
        already_flagged_tables = set()
        for stats_info in self.table_stats_data:
            table_name = stats_info.get('table_name', '')
            column_name = stats_info.get('column_name', '')
            table_cardinality = stats_info.get('table_cardinality')
            actual_rows = stats_info.get('actual_rows')
            sampling_percent = stats_info.get('sampling_percent')
            
            # Skip if missing data or already flagged
            if not table_name or table_cardinality is None or actual_rows is None:
                continue
            if actual_rows <= 0 or table_cardinality <= 0:
                continue
            
            # Calculate variation percentage
            variation_pct = abs(table_cardinality - actual_rows) / max(actual_rows, 1) * 100
            
            # Flag if variation > 20% AND sampling < 20%
            if variation_pct > self.CARDINALITY_MISMATCH_THRESHOLD:
                if sampling_percent is not None and sampling_percent < self.STATS_SAMPLING_THRESHOLD:
                    # Create a unique key to avoid duplicate recommendations
                    flag_key = (table_name, column_name)
                    if flag_key in already_flagged_tables:
                        continue
                    already_flagged_tables.add(flag_key)
                    
                    self.issues.append(TuningIssue(
                        severity=IssueSeverity.CRITICAL,
                        category="Stats and Skew Check",
                        title=f"Low Statistics - {column_name}",
                        description=(
                            f"Table Cardinality ({table_cardinality:,}) differs from Actual Rows ({actual_rows:,}) "
                            f"by {variation_pct:.1f}%, and statistics sampling is only {sampling_percent:.1f}%. "
                            f"Column: [{column_name}] in table [{table_name}]."
                        ),
                        recommendation=(
                            f"Collect full statistics on the column:\n\n"
                            f"UPDATE STATISTICS [{table_name}]([{column_name}]) WITH FULLSCAN;\n\n"
                            f"Or update all statistics on the table:\n"
                            f"UPDATE STATISTICS [{table_name}] WITH FULLSCAN;"
                        ),
                        table=table_name,
                        estimated_impact=f"Critical - {variation_pct:.1f}% cardinality mismatch with {sampling_percent:.1f}% sampling"
                    ))
                    
                    self.statistics_recommendations.append(StatisticsRecommendation(
                        table=table_name,
                        columns=[column_name],
                        command=f"UPDATE STATISTICS [{table_name}]([{column_name}]) WITH FULLSCAN;",
                        reason=f"Cardinality mismatch ({variation_pct:.1f}%) with low sampling ({sampling_percent:.1f}%)"
                    ))
    
    def _check_joins(self):
        """
        Check 3: Join Check (HIGH)
        - Many-to-many: if output rows > 1.5x either input table, recommend pre-aggregation
        - Anti join: recommend rewriting as inclusion join
        """
        for join in self.plan_info.joins:
            left_rows = join.estimated_input_rows_left
            right_rows = join.estimated_input_rows_right
            output_rows = join.estimated_output_rows
            max_input = max(left_rows, right_rows)
            
            # Check for many-to-many join (output > 1.5x max input)
            if max_input > 0 and output_rows > max_input * self.MANY_TO_MANY_THRESHOLD:
                explosion_ratio = output_rows / max_input
                
                self.issues.append(TuningIssue(
                    severity=IssueSeverity.HIGH,
                    category="Join Check",
                    title=f"Many-to-Many Join: {explosion_ratio:.1f}x Output",
                    description=(
                        f"Join output ({output_rows:,.0f} rows) is {explosion_ratio:.1f}x larger than "
                        f"the largest input table ({max_input:,.0f} rows). "
                        f"Left input: {left_rows:,.0f}, Right input: {right_rows:,.0f}. "
                        f"This indicates a many-to-many relationship causing data explosion."
                    ),
                    recommendation=(
                        "Use Pre-Aggregation to reduce data before joining:\n"
                        "1. Aggregate one or both tables before the join to create 1-to-many relationship\n"
                        "2. Use GROUP BY on the join columns to eliminate duplicates\n"
                        "3. Consider adding a DISTINCT or additional join predicates\n"
                        "4. Verify join conditions are complete - missing predicates cause Cartesian effects"
                    ),
                    operator=join.join_type.value,
                    estimated_impact=f"High - {explosion_ratio:.1f}x data multiplication"
                ))
            
            # Check for anti semi join patterns
            if 'Anti' in join.logical_op:
                self.issues.append(TuningIssue(
                    severity=IssueSeverity.HIGH,
                    category="Join Check",
                    title="Anti Join Pattern Detected",
                    description=(
                        f"Anti join detected: {join.logical_op}. "
                        f"Anti joins (NOT EXISTS, NOT IN with subquery) can be expensive for large datasets."
                    ),
                    recommendation=(
                        "Rewrite as Inclusion Join:\n"
                        "1. Instead of NOT IN/NOT EXISTS, consider using LEFT JOIN with NULL check:\n"
                        "   SELECT a.* FROM TableA a LEFT JOIN TableB b ON a.key = b.key WHERE b.key IS NULL\n"
                        "2. Or use EXCEPT for set-based operation:\n"
                        "   SELECT key FROM TableA EXCEPT SELECT key FROM TableB\n"
                        "3. Ensure the inner table has good statistics for optimal plan selection"
                    ),
                    operator=join.join_type.value,
                    estimated_impact="High - Anti joins can be expensive for large datasets"
                ))
    
    def _calculate_score(self) -> int:
        """Calculate a performance score from 0-100."""
        score = 100
        
        for issue in self.issues:
            if issue.severity == IssueSeverity.CRITICAL:
                score -= 25
            elif issue.severity == IssueSeverity.HIGH:
                score -= 15
            elif issue.severity == IssueSeverity.MEDIUM:
                score -= 5
            elif issue.severity == IssueSeverity.LOW:
                score -= 2
        
        return max(0, min(100, score))
    
    def _generate_summary(self) -> str:
        """Generate a summary of the analysis."""
        critical = sum(1 for i in self.issues if i.severity == IssueSeverity.CRITICAL)
        high = sum(1 for i in self.issues if i.severity == IssueSeverity.HIGH)
        
        summary_parts = []
        
        if critical > 0:
            summary_parts.append(f"{critical} critical issue(s)")
        if high > 0:
            summary_parts.append(f"{high} high-priority issue(s)")
        
        if not summary_parts:
            return "No issues found. Query appears well-optimized for Fabric DW."
        
        return f"Found: {', '.join(summary_parts)}. Review recommendations for optimization."


def analyze_plan(plan_info: ExecutionPlanInfo, actual_rows: Optional[int] = None) -> TuningReport:
    """Convenience function to analyze an execution plan for Fabric DW."""
    analyzer = FabricQueryAnalyzer(plan_info, actual_rows)
    return analyzer.analyze()


# Backward compatibility
QueryAnalyzer = FabricQueryAnalyzer
