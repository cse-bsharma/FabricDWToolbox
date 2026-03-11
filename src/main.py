"""
Fabric Query Tuner - Main Entry Point
A tool for analyzing and tuning SQL queries for Microsoft Fabric Data Warehouse.
"""

import argparse
import sys
import os
import json
from typing import Optional

from .connection import FabricConnection, FabricConnectionConfig, create_connection_from_env
from .query_plan import QueryPlanRetriever, format_query_for_display
from .plan_parser import PlanParser, parse_plan_from_file
from .analyzer import QueryAnalyzer, analyze_plan, IssueSeverity


class Colors:
    """ANSI color codes for terminal output."""
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


def severity_color(severity: IssueSeverity) -> str:
    """Get color for a severity level."""
    colors = {
        IssueSeverity.CRITICAL: Colors.RED + Colors.BOLD,
        IssueSeverity.HIGH: Colors.RED,
        IssueSeverity.MEDIUM: Colors.YELLOW,
        IssueSeverity.LOW: Colors.CYAN,
        IssueSeverity.INFO: Colors.WHITE
    }
    return colors.get(severity, Colors.WHITE)


def print_header(text: str):
    """Print a header line."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}")


def print_subheader(text: str):
    """Print a subheader line."""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{text}{Colors.RESET}")
    print(f"{Colors.CYAN}{'-'*40}{Colors.RESET}")


def print_report(report):
    """Print the tuning report to the console."""
    print_header("FABRIC DW QUERY TUNER - ANALYSIS REPORT")
    
    # Query summary
    print_subheader("Query Summary")
    print(f"Query: {format_query_for_display(report.query, 80)}")
    print(f"Estimated Total Cost: {Colors.BOLD}{report.total_cost:.4f}{Colors.RESET}")
    
    # Score
    score_color = Colors.GREEN if report.score >= 70 else Colors.YELLOW if report.score >= 40 else Colors.RED
    print(f"\n{Colors.BOLD}Performance Score: {score_color}{report.score}/100{Colors.RESET}")
    print(f"Summary: {report.summary}")
    
    # High Cost Operators
    if report.high_cost_operators:
        print_subheader("Top Cost Operators")
        for op in report.high_cost_operators:
            print(f"  #{op['rank']} {Colors.BOLD}{op['operator']}{Colors.RESET} [{op.get('table', 'N/A')}]")
            print(f"     Cost: {op['cost_percent']:.1f}% | Rows: {op['estimated_rows']:,.0f} | CPU: {op['cpu_cost']:.4f} | I/O: {op['io_cost']:.4f}")
    
    # Issues
    if report.issues:
        print_subheader(f"Issues Found ({len(report.issues)})")
        
        # Sort by severity
        sorted_issues = sorted(report.issues, 
                               key=lambda x: list(IssueSeverity).index(x.severity))
        
        for i, issue in enumerate(sorted_issues, 1):
            color = severity_color(issue.severity)
            print(f"\n{color}[{issue.severity.value}]{Colors.RESET} {Colors.BOLD}{issue.title}{Colors.RESET}")
            print(f"  Category: {issue.category}")
            print(f"  {issue.description}")
            print(f"  {Colors.GREEN}Recommendation:{Colors.RESET} {issue.recommendation}")
            if issue.estimated_impact:
                print(f"  Impact: {issue.estimated_impact}")
    else:
        print_subheader("No Issues Found")
        print(f"{Colors.GREEN}The query appears well-optimized for Fabric DW!{Colors.RESET}")
    
    # Statistics Recommendations
    if report.statistics_recommendations:
        print_subheader("Statistics Update Commands")
        for stat in report.statistics_recommendations:
            print(f"\n{Colors.CYAN}-- {stat.reason}{Colors.RESET}")
            print(f"{Colors.MAGENTA}{stat.command}{Colors.RESET}")
    
    print(f"\n{Colors.BLUE}{'='*60}{Colors.RESET}\n")


def export_report_json(report, filepath: str):
    """Export the report to a JSON file."""
    data = {
        'query': report.query,
        'total_cost': report.total_cost,
        'score': report.score,
        'summary': report.summary,
        'issues': [
            {
                'severity': issue.severity.value,
                'category': issue.category,
                'title': issue.title,
                'description': issue.description,
                'recommendation': issue.recommendation,
                'operator': issue.operator,
                'table': issue.table,
                'estimated_impact': issue.estimated_impact
            }
            for issue in report.issues
        ],
        'high_cost_operators': report.high_cost_operators,
        'statistics_recommendations': [
            {
                'table': stat.table,
                'columns': stat.columns,
                'command': stat.command,
                'reason': stat.reason
            }
            for stat in report.statistics_recommendations
        ]
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    
    print(f"✓ Report exported to: {filepath}")


def analyze_from_connection(query: str, server: str, database: str, 
                            auth: str = "ActiveDirectoryInteractive",
                            username: Optional[str] = None,
                            password: Optional[str] = None,
                            save_plan: Optional[str] = None,
                            export_json: Optional[str] = None):
    """Analyze a query by connecting to Fabric DW."""
    
    config = FabricConnectionConfig(
        server=server,
        database=database,
        authentication=auth,
        username=username,
        password=password
    )
    
    auth_display = f"({auth})" if auth != "SqlPassword" else "(SQL Auth)"
    print(f"Connecting to Fabric DW: {server}/{database} {auth_display}...")
    
    with FabricConnection(config) as conn:
        retriever = QueryPlanRetriever(conn)
        
        print(f"Retrieving estimated execution plan...")
        plan_xml = retriever.get_estimated_plan_xml(query)
        
        if save_plan:
            retriever.save_plan_to_file(plan_xml, save_plan)
        
        print(f"Parsing execution plan...")
        parser = PlanParser(plan_xml)
        plan_info = parser.parse()
        
        print(f"Analyzing plan for tuning opportunities...")
        report = analyze_plan(plan_info)
        
        print_report(report)
        
        if export_json:
            export_report_json(report, export_json)
        
        return report


def analyze_from_file(filepath: str, export_json: Optional[str] = None):
    """Analyze a query plan from an XML file."""
    
    print(f"Reading execution plan from: {filepath}")
    plan_info = parse_plan_from_file(filepath)
    
    print(f"Analyzing plan for tuning opportunities...")
    report = analyze_plan(plan_info)
    
    print_report(report)
    
    if export_json:
        export_report_json(report, export_json)
    
    return report


def interactive_mode():
    """Run in interactive mode, prompting for connection details and queries."""
    print_header("FABRIC DW QUERY TUNER - Interactive Mode")
    
    print("\nEnter Fabric Data Warehouse connection details:")
    server = input("Server (e.g., workspace.datawarehouse.fabric.microsoft.com): ").strip()
    database = input("Database name: ").strip()
    
    print("\nAzure AD Authentication options:")
    print("1. ActiveDirectoryInteractive (browser login) [recommended]")
    print("2. ActiveDirectoryPassword (Azure AD username/password)")
    print("3. ActiveDirectoryDefault (Azure CLI/managed identity)")
    auth_choice = input("Choose authentication [1]: ").strip() or "1"
    
    username = None
    password = None
    
    if auth_choice == "1":
        auth = "ActiveDirectoryInteractive"
    elif auth_choice == "2":
        auth = "ActiveDirectoryPassword"
        username = input("Azure AD Username (email): ").strip()
        password = input("Password: ").strip()
    else:
        auth = "ActiveDirectoryDefault"
    
    config = FabricConnectionConfig(
        server=server,
        database=database,
        authentication=auth,
        username=username,
        password=password
    )
    
    try:
        with FabricConnection(config) as conn:
            retriever = QueryPlanRetriever(conn)
            
            while True:
                print_subheader("Enter your SQL query")
                print("(Enter a blank line to finish, or 'quit' to exit)")
                
                lines = []
                while True:
                    line = input()
                    if line.lower() == 'quit':
                        print("Goodbye!")
                        return
                    if line == '':
                        break
                    lines.append(line)
                
                query = '\n'.join(lines)
                if not query.strip():
                    print("No query entered. Try again or type 'quit' to exit.")
                    continue
                
                try:
                    print(f"\nRetrieving estimated execution plan...")
                    plan_xml = retriever.get_estimated_plan_xml(query)
                    
                    parser = PlanParser(plan_xml)
                    plan_info = parser.parse()
                    
                    report = analyze_plan(plan_info)
                    print_report(report)
                    
                except Exception as e:
                    print(f"{Colors.RED}Error analyzing query: {e}{Colors.RESET}")
                
    except Exception as e:
        print(f"{Colors.RED}Connection error: {e}{Colors.RESET}")
        sys.exit(1)


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Fabric DW Query Tuner - Analyze and optimize SQL queries for Microsoft Fabric Data Warehouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze with Azure AD Interactive (browser login)
  python -m src.main --server my-server.datawarehouse.fabric.microsoft.com \\
                     --database MyDW --query "SELECT * FROM Sales"
  
  # Analyze with Azure AD Password
  python -m src.main --server my-server.datawarehouse.fabric.microsoft.com \\
                     --database MyDW --auth ActiveDirectoryPassword \\
                     --username user@domain.com --password yourpass \\
                     --query "SELECT * FROM Sales"
  
  # Analyze from a SQL file
  python -m src.main -s myserver -d MyDW -f query.sql
  
  # Analyze an existing execution plan XML file (no connection needed)
  python -m src.main --plan-file execution_plan.xml
  
  # Interactive mode
  python -m src.main --interactive
        """
    )
    
    # Connection options
    conn_group = parser.add_argument_group('Connection Options')
    conn_group.add_argument('--server', '-s', help='Fabric DW server address')
    conn_group.add_argument('--database', '-d', help='Database name')
    conn_group.add_argument('--username', '-u', help='Azure AD username (email) for ActiveDirectoryPassword auth')
    conn_group.add_argument('--password', '-P', help='Azure AD password for ActiveDirectoryPassword auth')
    conn_group.add_argument('--auth', '-a', 
                            choices=['ActiveDirectoryInteractive', 'ActiveDirectoryPassword', 'ActiveDirectoryDefault'],
                            default='ActiveDirectoryInteractive',
                            help='Azure AD authentication method (default: ActiveDirectoryInteractive)')
    
    # Query input options
    query_group = parser.add_argument_group('Query Input')
    query_group.add_argument('--query', '-q', help='SQL query to analyze')
    query_group.add_argument('--query-file', '-f', help='File containing the SQL query')
    query_group.add_argument('--plan-file', '-p', help='Existing execution plan XML file to analyze')
    
    # Output options
    output_group = parser.add_argument_group('Output Options')
    output_group.add_argument('--save-plan', help='Save the execution plan XML to a file')
    output_group.add_argument('--export-json', '-o', help='Export report to JSON file')
    
    # Mode options
    parser.add_argument('--interactive', '-i', action='store_true', 
                       help='Run in interactive mode')
    
    args = parser.parse_args()
    
    # Interactive mode
    if args.interactive:
        interactive_mode()
        return
    
    # Analyze from plan file
    if args.plan_file:
        analyze_from_file(args.plan_file, args.export_json)
        return
    
    # Get query
    query = args.query
    if args.query_file:
        with open(args.query_file, 'r', encoding='utf-8') as f:
            query = f.read()
    
    if not query:
        print("Error: No query provided. Use --query, --query-file, --plan-file, or --interactive")
        parser.print_help()
        sys.exit(1)
    
    # Get connection details
    server = args.server or os.environ.get('FABRIC_SERVER')
    database = args.database or os.environ.get('FABRIC_DATABASE')
    username = args.username or os.environ.get('FABRIC_USERNAME')
    password = args.password or os.environ.get('FABRIC_PASSWORD')
    
    if not server or not database:
        print("Error: Server and database are required. Use --server/--database or set FABRIC_SERVER/FABRIC_DATABASE environment variables")
        sys.exit(1)
    
    # Determine auth method
    auth = args.auth
    if not auth:
        # Auto-detect based on whether username/password provided
        if username and password:
            auth = "SqlPassword"
        else:
            auth = "ActiveDirectoryInteractive"
    
    # Validate credentials for password-based auth
    if auth in ("SqlPassword", "ActiveDirectoryPassword") and (not username or not password):
        print(f"Error: {auth} requires --username and --password")
        sys.exit(1)
    
    # Run analysis
    analyze_from_connection(
        query=query,
        server=server,
        database=database,
        auth=auth,
        username=username,
        password=password,
        save_plan=args.save_plan,
        export_json=args.export_json
    )


if __name__ == "__main__":
    main()
