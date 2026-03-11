"""
Query Plan Retrieval Module
Retrieves estimated execution plans from Fabric DW using SHOWPLAN_XML.
"""

from typing import Optional
from .connection import FabricConnection


class QueryPlanRetriever:
    """Retrieves query execution plans from Fabric Data Warehouse."""
    
    def __init__(self, connection: FabricConnection):
        self.connection = connection
    
    def get_estimated_plan_xml(self, query: str) -> str:
        """
        Get the estimated execution plan XML for a query using SHOWPLAN_XML.
        
        This returns the estimated plan without actually executing the query.
        
        Args:
            query: The SQL query to analyze
            
        Returns:
            XML string containing the execution plan
        """
        conn = self.connection.connect()
        cursor = conn.cursor()
        
        try:
            # Enable SHOWPLAN_XML to get the estimated execution plan
            cursor.execute("SET SHOWPLAN_XML ON")
            
            # Execute the query - this returns the plan XML, not actual results
            cursor.execute(query)
            
            # Fetch the XML plan
            row = cursor.fetchone()
            plan_xml = row[0] if row else None
            
            # Disable SHOWPLAN_XML
            cursor.execute("SET SHOWPLAN_XML OFF")
            
            if not plan_xml:
                raise ValueError("No execution plan returned for the query")
            
            return plan_xml
            
        except Exception as e:
            # Make sure to turn off SHOWPLAN_XML even on error
            try:
                cursor.execute("SET SHOWPLAN_XML OFF")
            except:
                pass
            raise e
        finally:
            cursor.close()
    
    def get_actual_plan_xml(self, query: str) -> tuple[str, list]:
        """
        Get the actual execution plan XML by running the query with statistics.
        
        WARNING: This actually executes the query!
        
        Args:
            query: The SQL query to analyze
            
        Returns:
            Tuple of (XML plan string, query results)
        """
        conn = self.connection.connect()
        cursor = conn.cursor()
        
        try:
            # Enable statistics XML to capture actual plan
            cursor.execute("SET STATISTICS XML ON")
            
            # Execute the query
            cursor.execute(query)
            
            # Get results
            results = []
            if cursor.description:
                columns = [column[0] for column in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Move to the next result set which contains the XML plan
            plan_xml = None
            if cursor.nextset():
                row = cursor.fetchone()
                plan_xml = row[0] if row else None
            
            # Disable statistics XML
            cursor.execute("SET STATISTICS XML OFF")
            
            return plan_xml, results
            
        except Exception as e:
            try:
                cursor.execute("SET STATISTICS XML OFF")
            except:
                pass
            raise e
        finally:
            cursor.close()
    
    def save_plan_to_file(self, plan_xml: str, filepath: str):
        """Save the execution plan XML to a file."""
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(plan_xml)
        print(f"✓ Plan saved to: {filepath}")


def format_query_for_display(query: str, max_length: int = 100) -> str:
    """Format a query for display, truncating if necessary."""
    # Normalize whitespace
    normalized = ' '.join(query.split())
    if len(normalized) > max_length:
        return normalized[:max_length] + "..."
    return normalized
