"""
Fabric Data Warehouse Connection Module
Handles authentication and connection to Microsoft Fabric DW using Azure AD.
"""

import pyodbc
from dataclasses import dataclass
from typing import Optional
import struct


@dataclass
class FabricConnectionConfig:
    """Configuration for Fabric DW connection."""
    server: str  # e.g., "your-workspace.datawarehouse.fabric.microsoft.com"
    database: str  # Your Fabric DW database name
    authentication: str = "ActiveDirectoryInteractive"  # Azure AD auth only
    # For ActiveDirectoryPassword
    username: Optional[str] = None  # Azure AD username (email)
    password: Optional[str] = None  # Azure AD password
    # For ActiveDirectoryServicePrincipal
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    tenant_id: Optional[str] = None


class FabricConnection:
    """Manages connection to Microsoft Fabric Data Warehouse."""
    
    def __init__(self, config: FabricConnectionConfig):
        self.config = config
        self._connection: Optional[pyodbc.Connection] = None
    
    def _build_connection_string(self) -> str:
        """Build the ODBC connection string for Fabric DW."""
        
        base_conn = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={self.config.server};"
            f"Database={self.config.database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
        )
        
        if self.config.authentication == "ActiveDirectoryPassword":
            # Azure AD with username/password (non-interactive)
            if not self.config.username or not self.config.password:
                raise ValueError("ActiveDirectoryPassword auth requires username and password")
            base_conn += (
                f"Authentication=ActiveDirectoryPassword;"
                f"UID={self.config.username};"
                f"PWD={self.config.password};"
            )
        elif self.config.authentication == "ActiveDirectoryInteractive":
            # Interactive login - will prompt for credentials in browser
            base_conn += "Authentication=ActiveDirectoryInteractive;"
        elif self.config.authentication == "ActiveDirectoryServicePrincipal":
            # Service Principal authentication
            if not all([self.config.client_id, self.config.client_secret, self.config.tenant_id]):
                raise ValueError("Service Principal auth requires client_id, client_secret, and tenant_id")
            base_conn += (
                f"Authentication=ActiveDirectoryServicePrincipal;"
                f"UID={self.config.client_id};"
                f"PWD={self.config.client_secret};"
            )
        elif self.config.authentication == "ActiveDirectoryDefault":
            # Uses Azure Identity DefaultAzureCredential (CLI, managed identity, etc.)
            base_conn += "Authentication=ActiveDirectoryDefault;"
        else:
            raise ValueError(f"Unsupported authentication method: {self.config.authentication}. Fabric DW only supports Azure AD authentication.")
        
        return base_conn
    
    def connect(self) -> pyodbc.Connection:
        """Establish connection to Fabric DW."""
        if self._connection is not None:
            return self._connection
        
        connection_string = self._build_connection_string()
        
        try:
            self._connection = pyodbc.connect(connection_string, timeout=30, autocommit=True)
            print(f"✓ Connected to Fabric DW: {self.config.database}")
            return self._connection
        except pyodbc.Error as e:
            raise ConnectionError(f"Failed to connect to Fabric DW: {e}")
    
    def disconnect(self):
        """Close the connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            print("✓ Disconnected from Fabric DW")
    
    def execute_query(self, query: str) -> list:
        """Execute a query and return results."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [column[0] for column in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        cursor.close()
        
        return [dict(zip(columns, row)) for row in rows]
    
    def execute_non_query(self, statement: str):
        """Execute a statement that doesn't return results (SET commands, etc.)."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(statement)
        cursor.close()
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


def create_connection_from_env() -> FabricConnection:
    """Create a connection using environment variables."""
    import os
    
    config = FabricConnectionConfig(
        server=os.environ.get("FABRIC_SERVER", ""),
        database=os.environ.get("FABRIC_DATABASE", ""),
        authentication=os.environ.get("FABRIC_AUTH", "ActiveDirectoryInteractive"),
        client_id=os.environ.get("FABRIC_CLIENT_ID"),
        client_secret=os.environ.get("FABRIC_CLIENT_SECRET"),
        tenant_id=os.environ.get("FABRIC_TENANT_ID")
    )
    
    if not config.server or not config.database:
        raise ValueError("FABRIC_SERVER and FABRIC_DATABASE environment variables are required")
    
    return FabricConnection(config)
