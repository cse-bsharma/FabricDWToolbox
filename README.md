# Fabric DW Query Tuner

A Python tool for analyzing and tuning SQL queries for **Microsoft Fabric Data Warehouse**. Optimized for OLAP workloads with Delta Parquet storage - no traditional index recommendations, focuses on Fabric-specific patterns.

## Description

**Fabric DW Query Tuner** is a comprehensive SQL query performance analysis tool specifically designed for Microsoft Fabric Data Warehouse. Unlike traditional SQL Server tuning tools, this analyzer understands the unique architecture of Fabric DW, which uses Delta Parquet storage and is optimized for analytical (OLAP) workloads.

### Key Features

- **Web-based Interface**: Easy-to-use browser interface for real-time query analysis
- **Command Line Interface**: Full CLI support for automation and scripting
- **Offline Analysis**: Analyze execution plan XML files without a database connection
- **Fabric-Specific Recommendations**: Provides tuning advice tailored to Fabric DW's architecture
- **Performance Scoring**: Get a 0-100 score for your query performance
- **Statistics Recommendations**: Generates UPDATE STATISTICS commands for tables needing attention

### What Makes This Different?

Microsoft Fabric Data Warehouse doesn't use traditional row-store indexes or key lookups. Instead, it leverages:
- **Delta Parquet** columnar storage format
- **Hash Match** and **Merge Join** strategies optimized for large datasets
- **Distribution-aware** query processing

This tool focuses on detecting issues that matter for Fabric DW - data type mismatches, inefficient join strategies, memory spills, and statistics problems - rather than traditional SQL Server index recommendations.

---

## Getting Started from GitHub

### Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.10 or higher** - [Download Python](https://www.python.org/downloads/)
- **Git** - [Download Git](https://git-scm.com/downloads)
- **ODBC Driver 18 for SQL Server** - [Download ODBC Driver](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

### Step 1: Clone the Repository

```powershell
# Clone the repository from GitHub
git clone https://github.com/cse-bsharma/FabricDWToolbox.git

# Navigate to the project directory
cd FabricDWToolbox
```

### Step 2: Create a Virtual Environment (Recommended)

```powershell
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
.\venv\Scripts\Activate.ps1

# On macOS/Linux:
# source venv/bin/activate
```

### Step 3: Install Dependencies

```powershell
# Install required Python packages
pip install -r requirements.txt
```

### Step 4: Configure Your Connection (Optional)

If you want to connect to a Fabric Data Warehouse, create a configuration file:

```powershell
# Copy the example configuration
Copy-Item config.example.json config.json
```

Edit `config.json` with your Fabric DW details:
```json
{
    "server": "your-workspace.datawarehouse.fabric.microsoft.com",
    "database": "YourDatabaseName",
    "authentication": "ActiveDirectoryInteractive"
}
```

### Step 5: Run the Application

**Option A: Start the Web Application**
```powershell
python src\webapp.py
```
Then open http://127.0.0.1:5000 in your browser.

**Option B: Use the Command Line**
```powershell
# Analyze a sample execution plan (no connection required)
python -m src.main --plan-file examples\sample_plan.xml

# Or connect to your Fabric DW and analyze a query
python -m src.main -s "your-server.datawarehouse.fabric.microsoft.com" -d "YourDB" -q "SELECT * FROM Sales"
```

---

## What It Checks

| Check | Severity | Description |
|-------|----------|-------------|
| **VARCHAR(8000)** | HIGH | Detects inefficient varchar(8000) usage in Delta Parquet |
| **Join Type Mismatch** | HIGH | Identifies implicit conversions in join predicates |
| **Nested Loop Joins** | CRITICAL | Flags row-by-row joins (bad for OLAP) |
| **Many-to-Many Joins** | CRITICAL | Detects data explosion (4x, 5x+ row multiplication) |
| **High-Cost Operators** | HIGH/MEDIUM | Highlights CPU, memory, I/O intensive operations |
| **Missing Statistics** | MEDIUM | Recommends FULLSCAN for large join tables |
| **TempDB Spills** | HIGH | Memory grant insufficient, spilling to disk |
| **Cartesian Products** | CRITICAL | Cross joins without predicates |

## Key Differences from SQL Server Tuning

Fabric DW uses **Delta Parquet** storage and is optimized for **OLAP**:
- No nonclustered index recommendations (not applicable)
- No key lookup detection (Fabric doesn't have this pattern)
- Focuses on join strategies optimal for analytical workloads
- Detects data type issues affecting Delta Parquet efficiency
- Recommends FULLSCAN statistics for accurate cardinality

## Web Application

The web application provides a user-friendly interface for query analysis:

```powershell
python src\webapp.py
```

Then open http://127.0.0.1:5000 in your browser.

**Optional Command Line Arguments:**
- `--port 8080` - Run on a different port
- `--host 0.0.0.0` - Allow external connections
- `--debug` - Enable debug mode

## Command Line Usage

### Analyze an Execution Plan File (No Connection Needed)

```powershell
python -m src.main --plan-file examples\sample_plan.xml
```

### Connect with Azure AD Interactive (Browser Login) - Recommended

```powershell
python -m src.main -s "your-server.datawarehouse.fabric.microsoft.com" -d "YourDB" -q "SELECT * FROM Sales"
```

### Connect with Azure AD Password

```powershell
python -m src.main -s "your-server.datawarehouse.fabric.microsoft.com" -d "YourDB" --auth ActiveDirectoryPassword -u "user@domain.com" -P "password" -q "SELECT * FROM Sales"
```

### Analyze Query from SQL File

```powershell
python -m src.main -s "server" -d "DB" --query-file myquery.sql
```

### Interactive Mode

```powershell
python -m src.main --interactive
```

### Export to JSON

```powershell
python -m src.main --plan-file plan.xml --export-json report.json
```

## License

MIT License

---

## Troubleshooting

### Common Issues

**"ODBC Driver 18 for SQL Server not found"**
- Download and install the ODBC Driver from [Microsoft's website](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

**"ModuleNotFoundError: No module named 'flask'"**
- Ensure you've activated your virtual environment and run `pip install -r requirements.txt`

**"Authentication failed"**
- For Azure AD Interactive, a browser window should open for login
- Ensure your Azure AD account has access to the Fabric workspace
- Check that your server URL ends with `.datawarehouse.fabric.microsoft.com`

**Web app not loading**
- Check that port 5000 is not in use by another application
- Try running with `--port 8080` to use a different port

### Getting Help

If you encounter issues not covered here, please [open an issue](https://github.com/cse-bsharma/FabricDWToolbox/issues) on GitHub.
