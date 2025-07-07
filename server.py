import asyncio
import logging
import re
import time
import uuid
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict, Any, Optional
from collections import OrderedDict
import requests
import pyodbc
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from openai import AsyncAzureOpenAI
from pydantic import BaseModel, validator
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import os
import json
import pandas as pd
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
import sqlparse
import structlog
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError

# Rate limiting
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

import traceback
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.ai.projects.models import *

from reportlab.platypus import HRFlowable
from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_JUSTIFY
# Microsoft Graph API for email
try:
    from msgraph import GraphServiceClient
    from azure.identity import ClientSecretCredential
    GRAPH_AVAILABLE = True
except ImportError:
    GRAPH_AVAILABLE = False
    print("Microsoft Graph SDK not available. Install with: pip install microsoft-graph")

# Report generation libraries
try:
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    import xlsxwriter
    REPORT_LIBS_AVAILABLE = True
except ImportError:
    REPORT_LIBS_AVAILABLE = False

import base64
from io import BytesIO

# Load environment variables FIRST
load_dotenv()

# CONSOLIDATED: Setup structured logging
structlog.configure(
    processors=[structlog.processors.JSONRenderer()],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logger = structlog.get_logger()

# CONSOLIDATED: Configuration and validation
class ConfigManager:
    """Centralized configuration management"""
    
    REQUIRED_VARS = [
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_ENDPOINT", 
        "AZURE_OPENAI_DEPLOYMENT",
        "FABRIC_SQL_ENDPOINT",
        "FABRIC_DATABASE",
        "FABRIC_CLIENT_ID",
        "FABRIC_CLIENT_SECRET",
        "KUSTO_CLUSTER",
        "KUSTO_DATABASE", 
        "FABRIC_TENANT_ID"
    ]
    
    OPTIONAL_AI_VARS = [
        "AI_PROJECT_ENDPOINT",
        "GRAPH_CLIENT_ID",
        "GRAPH_CLIENT_SECRET",
        "GRAPH_TENANT_ID"
    ]
    
    @classmethod
    def validate_environment(cls):
        """Validate all required environment variables"""
        missing = []
        for var in cls.REQUIRED_VARS:
            value = os.getenv(var)
            if not value or value.strip() == "":
                missing.append(var)
                
        if missing:
            logger.error("Missing environment variables", missing=missing)
            raise RuntimeError(f"Missing required environment variables: {missing}")
        
        ai_vars_available = all(os.getenv(var) for var in cls.OPTIONAL_AI_VARS)
        
        logger.info("Environment validation passed", 
                    total_vars=len(cls.REQUIRED_VARS),
                    ai_foundry_enabled=ai_vars_available,
                    graph_enabled=GRAPH_AVAILABLE and ai_vars_available)
        return True

# CONSOLIDATED: Database connection setup
class DatabaseManager:
    """Centralized database connection management"""
    
    def __init__(self):
        self.kusto_client = None
        self.kusto_database = None
        self.sql_engine = None
        self.setup_connections()
    
    def setup_connections(self):
        """Initialize both KQL and SQL connections"""
        ConfigManager.validate_environment()
        self.setup_kql_client()
        self.setup_sql_engine()
    
    def setup_kql_client(self):
        """Initialize KQL client with proper error handling"""
        try:
            # Get KQL environment variables (strip whitespace)
            KUSTO_CLUSTER = os.getenv("KUSTO_CLUSTER", "").strip()
            KUSTO_DATABASE = os.getenv("KUSTO_DATABASE", "").strip()
            FABRIC_TENANT_ID = os.getenv("FABRIC_TENANT_ID", "").strip()
            FABRIC_CLIENT_ID = os.getenv("FABRIC_CLIENT_ID", "").strip()
            FABRIC_CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET", "").strip()
            
            # Log configuration (without secrets)
            logger.info("KQL configuration loaded", 
                       cluster=KUSTO_CLUSTER,
                       database=KUSTO_DATABASE,
                       tenant_id=FABRIC_TENANT_ID,
                       client_id=FABRIC_CLIENT_ID,
                       has_secret=bool(FABRIC_CLIENT_SECRET))
            
            # Validate KQL cluster URI format
            if not KUSTO_CLUSTER.startswith("https://"):
                raise ValueError(f"KUSTO_CLUSTER must start with 'https://'. Got: {KUSTO_CLUSTER}")
                
            if "ingest" in KUSTO_CLUSTER.lower():
                logger.warning("KUSTO_CLUSTER appears to be an ingestion endpoint", cluster=KUSTO_CLUSTER)
                raise ValueError(
                    f"KUSTO_CLUSTER should be a query endpoint, not ingestion. "
                    f"Expected format: https://<eventhouse>.<region>.kusto.fabric.microsoft.com"
                )
            
            # Initialize KQL connection
            kusto_connection_string = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                KUSTO_CLUSTER,
                FABRIC_CLIENT_ID, 
                FABRIC_CLIENT_SECRET,
                FABRIC_TENANT_ID
            )
            
            self.kusto_client = KustoClient(kusto_connection_string)
            self.kusto_database = KUSTO_DATABASE
            logger.info("KQL client initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize KQL client", error=str(e))
            raise RuntimeError(f"KQL client initialization failed: {str(e)}")
    
    def setup_sql_engine(self):
        """Initialize SQL database connection"""
        connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={os.getenv('FABRIC_SQL_ENDPOINT')};"
            f"Database={os.getenv('FABRIC_DATABASE')};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"UID={os.getenv('FABRIC_CLIENT_ID')};"
            f"PWD={os.getenv('FABRIC_CLIENT_SECRET')};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
        )
        self.sql_engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={connection_string}",
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600
        )
    
    def execute_sql_query(self, query: str, params=None) -> List[Dict[str, Any]]:
        """Enhanced execute_query with better GROUP BY error messages and number formatting"""
        try:
            with self.sql_engine.connect() as conn:
                executable_query = text(query)
                cursor = conn.execute(executable_query, params or {})
                columns = cursor.keys()
                rows = cursor.fetchall()
                result = []
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    for key, value in row_dict.items():
                        if isinstance(value, datetime):
                            row_dict[key] = value.isoformat()
                        elif hasattr(value, 'date') and callable(getattr(value, 'date')):
                            row_dict[key] = value.isoformat()
                        elif isinstance(value, (bytes, bytearray)):
                            row_dict[key] = value.decode('utf-8', errors='ignore')
                        elif isinstance(value, Decimal):
                            row_dict[key] = Utils.format_number(value, 2)  # ✅ Format to 2 decimal places
                        elif isinstance(value, float):
                            row_dict[key] = Utils.format_number(value, 2)  # ✅ Format to 2 decimal places
                        # Note: Keep integers as-is, only format decimal numbers
                    result.append(row_dict)
                
                # ✅ Apply formatting to all numeric results
                formatted_result = Utils.format_results_data(result, 2)
                return formatted_result
                
        except Exception as e:
            error_str = str(e)
            logger.error("Query execution error", query=query, params=params, error=str(e))
            
            # Improved error handling for GROUP BY issues
            if "8120" in error_str or "GROUP BY" in error_str:
                if "is not contained in either an aggregate function or the GROUP BY clause" in error_str:
                    raise Exception(
                        "SQL GROUP BY error: All non-aggregate columns in SELECT must be included in GROUP BY clause. "
                        "Fix: Add missing columns to GROUP BY, or use aggregate functions like COUNT(), SUM(), AVG() for calculated fields. "
                        f"Original error: {error_str}"
                    )
                else:
                    raise Exception(
                        "SQL GROUP BY error: When using GROUP BY, all SELECT columns must either be in the GROUP BY clause "
                        "or use aggregate functions (COUNT, SUM, AVG, etc.). "
                        f"Original error: {error_str}"
                    )
            else:
                raise
    
    async def test_kql_connection(self):
        """Test the KQL connection with a simple query"""
        try:
            test_query = "print 'KQL connection test successful'"
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.kusto_client.execute(self.kusto_database, test_query)
            )
            logger.info("KQL connection test passed")
            return True
        except Exception as e:
            logger.error("KQL connection test failed", error=str(e))
            return False

db_manager = DatabaseManager()

# CONSOLIDATED: Session management
class SessionManager:
    """Centralized session management"""
    
    @staticmethod
    def generate_new_session_id():
        """Generate a new unique session ID for the current day"""
        now = datetime.now()
        date_string = now.strftime("%Y%m%d")
        timestamp = int(time.time() * 1000)  # milliseconds for uniqueness
        return f"powerbi_{date_string}_{timestamp}"

    @staticmethod
    def get_session_id_from_request(session: Optional[str] = None):
        """Enhanced session management with multiple sessions per day"""
        if session and (session.startswith('powerbi_') or session == 'new'):
            if session == 'new':
                # Generate a completely new session
                return SessionManager.generate_new_session_id()
            return session
        
        # Default fallback
        now = datetime.now()
        date_string = now.strftime("%Y%m%d")
        return f"powerbi_{date_string}_default"



# CONSOLIDATED: Schema management with caching
class SchemaManager:
    """Centralized schema management with caching"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.cached_tables_info = None
        self.schema_cache_timestamp = None
        self.schema_cache_duration = 3600  # Cache for 1 hour
    
    async def get_cached_tables_info(self):
        """Get schema from memory cache - NO KQL storage for schema"""
        current_time = time.time()
        
        # Check if we have valid cached data
        if (self.cached_tables_info is not None and 
            self.schema_cache_timestamp is not None and 
            (current_time - self.schema_cache_timestamp) < self.schema_cache_duration):
            logger.info("Schema cache hit from memory")
            return self.cached_tables_info
        
        # Cache is empty or expired, fetch fresh data
        logger.info("Fetching fresh schema data")
        start_time = time.time()
        
        try:
            tables_info = await self.get_tables_info()
            
            # Cache in memory only - NOT in KQL
            self.cached_tables_info = tables_info
            self.schema_cache_timestamp = current_time
            
            duration = time.time() - start_time
            logger.info("Schema fetched and cached in memory", 
                       duration=duration, 
                       table_count=len(tables_info))
            
            return tables_info
            
        except Exception as e:
            logger.error("Failed to fetch schema", error=str(e))
            # Return cached data if available, even if expired
            if self.cached_tables_info is not None:
                logger.warning("Using expired schema cache due to fetch error")
                return self.cached_tables_info
            raise
    
    async def get_tables_info(self):
        """Get detailed table information"""
        loop = asyncio.get_event_loop()
        query = """
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        tables = await loop.run_in_executor(None, lambda: self.db_manager.execute_sql_query(query))
        tables_info = []

        async def fetch_table_metadata(table):
            column_query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
            ORDER BY ORDINAL_POSITION
            """
            fk_query = """
            SELECT
                C.CONSTRAINT_NAME,
                C.TABLE_NAME,
                C.COLUMN_NAME,
                R.TABLE_NAME AS REFERENCED_TABLE,
                R.COLUMN_NAME AS REFERENCED_COLUMN
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C
                ON C.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE R
                ON R.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME
            WHERE C.TABLE_SCHEMA = :schema AND C.TABLE_NAME = :table
            """
            sample_query = f"SELECT TOP 3 * FROM [{table['TABLE_SCHEMA']}].[{table['TABLE_NAME']}]"
            
            columns, fks, sample_data = await asyncio.gather(
                loop.run_in_executor(None, lambda: self.db_manager.execute_sql_query(column_query, {"schema": table["TABLE_SCHEMA"], "table": table["TABLE_NAME"]})),
                loop.run_in_executor(None, lambda: self.db_manager.execute_sql_query(fk_query, {"schema": table["TABLE_SCHEMA"], "table": table["TABLE_NAME"]})),
                loop.run_in_executor(None, lambda: self.db_manager.execute_sql_query(sample_query)),
                return_exceptions=True
            )
            
            if isinstance(columns, Exception) or isinstance(fks, Exception) or isinstance(sample_data, Exception):
                logger.warning("Failed to fetch metadata for table", table=table["TABLE_NAME"], error=str(columns or fks or sample_data))
                return None
            
            # Process table metadata
            fk_info = [f"{fk['COLUMN_NAME']} references {fk['REFERENCED_TABLE']}.{fk['REFERENCED_COLUMN']}" for fk in fks]
            enhanced_columns = []
            numeric_columns = []
            text_columns = []
            date_columns = []
            column_values = {}
            
            for col in columns:
                col_name = col['COLUMN_NAME']
                data_type = col['DATA_TYPE'].lower()
                nullable = 'Nullable' if col['IS_NULLABLE'] == 'YES' else 'Not Nullable'
                
                if data_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney']:
                    numeric_columns.append(col_name)
                    enhanced_columns.append(f"[{col_name}] ({data_type.upper()}, {nullable}) - NUMERIC: Use AVG(), SUM(), MAX(), MIN()")
                elif data_type in ['varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext']:
                    text_columns.append(col_name)
                    enhanced_columns.append(f"[{col_name}] ({data_type.upper()}, {nullable}) - TEXT: Use COUNT(), CASE statements, GROUP BY - NEVER AVG()")
                    try:
                        distinct_query = f"SELECT DISTINCT TOP 10 [{col_name}] FROM [{table['TABLE_SCHEMA']}].[{table['TABLE_NAME']}] WHERE [{col_name}] IS NOT NULL"
                        distinct_values = await loop.run_in_executor(None, lambda: self.db_manager.execute_sql_query(distinct_query))
                        column_values[col_name] = [row[col_name] for row in distinct_values]
                    except:
                        column_values[col_name] = []
                elif data_type in ['datetime', 'datetime2', 'date', 'time', 'datetimeoffset', 'smalldatetime']:
                    date_columns.append(col_name)
                    enhanced_columns.append(f"[{col_name}] ({data_type.upper()}, {nullable}) - DATE: Use MAX(), MIN(), date functions")
                else:
                    enhanced_columns.append(f"[{col_name}] ({data_type.upper()}, {nullable})")
            
            return {
                "table": f"[{table['TABLE_SCHEMA']}].[{table['TABLE_NAME']}]",
                "columns": enhanced_columns,
                "numeric_columns": numeric_columns,
                "text_columns": text_columns,
                "date_columns": date_columns,
                "foreign_keys": fk_info,
                "sample_data": sample_data[:2] if sample_data else [],
                "column_values": column_values
            }

        tables_info = await asyncio.gather(*(fetch_table_metadata(table) for table in tables))
        return [info for info in tables_info if info]
    
    async def preload_schema(self):
        """Preload schema during application startup"""
        try:
            logger.info("Preloading database schema...")
            start_time = time.time()
            
            tables_info = await self.get_cached_tables_info()
            
            duration = time.time() - start_time
            logger.info("Schema preloaded successfully", 
                       duration=duration,
                       table_count=len(tables_info),
                       total_columns=sum(len(t.get('columns', [])) for t in tables_info))
            
            return True
            
        except Exception as e:
            logger.error("Schema preload failed", error=str(e))
            print(f"⚠️  Schema preload failed: {e}")
            print("    App will still work, but first query may be slower")
            return False
    
    def refresh_cache(self):
        """Manually refresh the schema cache"""
        self.cached_tables_info = None
        self.schema_cache_timestamp = None

schema_manager = SchemaManager(db_manager)

# CONSOLIDATED: Utility functions
# ENHANCED: Utility functions with number formatting
class Utils:
    """Consolidated utility functions with number formatting"""
    
    @staticmethod
    def format_number(value, decimal_places=2):
        """Format numbers to specified decimal places"""
        if value is None:
            return None
        
        try:
            if isinstance(value, (int, float, Decimal)):
                # Round to specified decimal places
                if isinstance(value, Decimal):
                    return float(round(value, decimal_places))
                else:
                    return round(float(value), decimal_places)
            elif isinstance(value, str):
                # Try to convert string to number
                try:
                    num_val = float(value)
                    return round(num_val, decimal_places)
                except (ValueError, TypeError):
                    return value  # Return original if not a number
            else:
                return value  # Return original for non-numeric types
        except (ValueError, TypeError, OverflowError):
            return value  # Return original if conversion fails
    
    @staticmethod
    def format_results_data(results: List[Dict[str, Any]], decimal_places=2) -> List[Dict[str, Any]]:
        """Format all numeric values in query results to specified decimal places"""
        if not results:
            return results
        
        formatted_results = []
        for row in results:
            formatted_row = {}
            for key, value in row.items():
                formatted_row[key] = Utils.format_number(value, decimal_places)
            formatted_results.append(formatted_row)
        
        return formatted_results
    
    @staticmethod
    def safe_json_serialize(obj):
        """Safe JSON serialization for various data types with number formatting"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return Utils.format_number(obj, 2)  # Format Decimal to 2 decimal places
        elif isinstance(obj, float):
            return Utils.format_number(obj, 2)  # Format float to 2 decimal places
        elif hasattr(obj, '__dict__'):
            return str(obj)
        return obj
    
    @staticmethod
    def normalize_question(question: str) -> str:
        """Normalize question for better cache hits"""
        question = question.lower().strip()
        question = re.sub(r'\s+', ' ', question)
        return question
    
    @staticmethod
    def remove_sql_comments(sql: str) -> str:
        """Remove SQL comments while preserving string literals"""
        if not sql:
            return sql
        
        result = []
        i = 0
        in_string = False
        string_char = None
        
        while i < len(sql):
            char = sql[i]
            
            # Handle string literals
            if char in ("'", '"') and not in_string:
                in_string = True
                string_char = char
                result.append(char)
            elif char == string_char and in_string:
                in_string = False
                string_char = None
                result.append(char)
            elif in_string:
                result.append(char)
            # Handle comments only when not in string
            elif not in_string and char == '-' and i + 1 < len(sql) and sql[i + 1] == '-':
                # Skip single-line comment
                while i < len(sql) and sql[i] != '\n':
                    i += 1
                if i < len(sql):  # Add newline back
                    result.append(' ')  # Replace comment with space
            elif not in_string and char == '/' and i + 1 < len(sql) and sql[i + 1] == '*':
                # Skip multi-line comment
                i += 2
                while i + 1 < len(sql):
                    if sql[i] == '*' and sql[i + 1] == '/':
                        i += 1
                        break
                    i += 1
                result.append(' ')  # Replace comment with space
            else:
                result.append(char)
            
            i += 1
        
        return ''.join(result)
    
    @staticmethod
    def parse_select_columns(select_part: str) -> list:
        """Parse SELECT clause to identify non-aggregate columns"""
        columns = []
        current_col = ""
        paren_count = 0
        
        # Split by comma, respecting parentheses
        for char in select_part:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0:
                if current_col.strip():
                    columns.append(current_col.strip())
                current_col = ""
                continue
            current_col += char
        
        if current_col.strip():
            columns.append(current_col.strip())
        
        # Filter out aggregate functions
        non_aggregate_columns = []
        aggregate_functions = ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(', 'STDEV(', 'VAR(']
        
        for col in columns:
            col_upper = col.upper()
            is_aggregate = any(func in col_upper for func in aggregate_functions)
            
            if not is_aggregate:
                # Extract column expression (before AS alias)
                if ' AS ' in col_upper:
                    col_expr = col[:col_upper.find(' AS ')].strip()
                else:
                    col_expr = col.strip()
                non_aggregate_columns.append(col_expr)
        
        return non_aggregate_columns
    
    @staticmethod
    def is_column_in_group_by(select_col: str, group_by_columns: list) -> bool:
        """Check if a SELECT column is present in GROUP BY clause"""
        sel_normalized = select_col.replace('[', '').replace(']', '').replace(' ', '').upper()
        
        for grp_col in group_by_columns:
            grp_normalized = grp_col.replace('[', '').replace(']', '').replace(' ', '').upper()
            if sel_normalized == grp_normalized or sel_normalized in grp_normalized:
                return True
        
        return False
    
    @staticmethod
    def validate_group_by_syntax(sql: str) -> tuple[str, str]:
        """Enhanced GROUP BY validation with automatic fixing"""
        try:
            sql = sql.replace('GROUP BYDATEPART', 'GROUP BY DATEPART')
            sql = sql.replace('ORDER BYDATEPART', 'ORDER BY DATEPART')
            sql = sql.replace('GROUP BY BY', 'GROUP BY')
            sql_upper = sql.upper()
            
            # Check if this query uses GROUP BY
            if 'GROUP BY' not in sql_upper:
                return sql, "No GROUP BY clause"
            
            # Extract SELECT and GROUP BY clauses
            select_start = sql_upper.find('SELECT')
            from_start = sql_upper.find('FROM')
            group_by_start = sql_upper.find('GROUP BY')
            
            if select_start == -1 or from_start == -1 or group_by_start == -1:
                return sql, "Invalid SQL structure"
            
            # Extract SELECT clause
            select_part = sql[select_start + 6:from_start].strip()
            
            # Find GROUP BY clause boundaries
            order_by_start = sql_upper.find('ORDER BY', group_by_start)
            having_start = sql_upper.find('HAVING', group_by_start)
            
            group_by_end = len(sql)
            if order_by_start != -1:
                group_by_end = min(group_by_end, order_by_start)
            if having_start != -1:
                group_by_end = min(group_by_end, having_start)
                
            group_by_part = sql[group_by_start + 8:group_by_end].strip()
            
            # Parse SELECT columns to find non-aggregates
            select_columns = Utils.parse_select_columns(select_part)
            group_by_columns = [col.strip() for col in group_by_part.split(',') if col.strip()]
            
            # Find missing columns
            missing_columns = []
            for sel_col in select_columns:
                if not Utils.is_column_in_group_by(sel_col, group_by_columns):
                    missing_columns.append(sel_col)
            
            # Auto-fix if needed
            if missing_columns:
                if group_by_part:
                    new_group_by = group_by_part + ", " + ", ".join(missing_columns)
                else:
                    new_group_by = ", ".join(missing_columns)
                
                # Reconstruct SQL
                fixed_sql = sql[:group_by_start + 8] + new_group_by
                if group_by_end < len(sql):
                    fixed_sql += " " + sql[group_by_end:]
                
                fixed_sql = fixed_sql.replace('GROUP BYDATEPART', 'GROUP BY DATEPART')
                fixed_sql = fixed_sql.replace('ORDER BYDATEPART', 'ORDER BY DATEPART')
                
                return fixed_sql, f"Auto-fixed GROUP BY: Added {missing_columns}"
            
            return sql, "GROUP BY validation passed"
            
        except Exception as e:
            logger.warning("GROUP BY validation failed", error=str(e))
            return sql, f"GROUP BY validation error: {str(e)}"
    
    
    @staticmethod
    def clean_generated_sql(sql_text: str) -> str:
        """Enhanced SQL cleaning with comment removal, GROUP BY validation, and syntax fixes"""
        print(f"🔍 CLEANING SQL INPUT: {sql_text[:100]}...") 
        if not sql_text:
            return ""
            
        sql = sql_text.strip()
        
        # Remove code block markers
        if sql.startswith('```'):
            lines = sql.split('\n')
            start_idx = 1 if lines[0].startswith('```') else 0
            end_idx = len(lines)
            for i, line in enumerate(lines[1:], 1):
                if line.strip().startswith('```'):
                    end_idx = i
                    break
            sql = '\n'.join(lines[start_idx:end_idx])
        
        # CRITICAL: Remove all comments from SQL
        sql = Utils.remove_sql_comments(sql)
        
        # ⭐ ENHANCED SYNTAX FIXES
        sql = sql.replace('GROUP BY', 'GROUP BY ')  # Add space after
        sql = sql.replace('ORDER BY', 'ORDER BY ')  # Add space after
        sql = sql.replace('GROUP  BY', 'GROUP BY ')  # Fix double spaces
        sql = sql.replace('ORDER  BY', 'ORDER BY ')  # Fix double spaces
        
        
        # Fix specific spacing issues
        sql = sql.replace('GROUP BY[', 'GROUP BY [')  # Add space before bracket
        sql = sql.replace('ORDER BY[', 'ORDER BY [')  # Add space before bracket
        sql = sql.replace('GROUP BYDATEPART', 'GROUP BY DATEPART')  # Fix DATEPART spacing
        sql = sql.replace('ORDER BYDATEPART', 'ORDER BY DATEPART')  # Fix DATEPART spacing
        
        sql = sql.replace(') GROUP BY', ' GROUP BY')  # Remove ) before GROUP BY
        sql = sql.replace('GROUP BY GROUP BY', 'GROUP BY')  # Fix double GROUP BY
        sql = sql.replace('WHERE SUM(', 'HAVING SUM(')  # Move SUM to HAVING
        sql = sql.replace('WHERE COUNT(', 'HAVING COUNT(')  # Move COUNT to HAVING
        sql = sql.replace('WHERE AVG(', 'HAVING AVG(')  # Move AVG to HAVING
        
        # Clean up the SQL (existing logic)
        lines = sql.split('\n')
        sql_lines = []
        in_select = False
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Start of a SELECT statement
            if line.upper().startswith('SELECT'):
                in_select = True
                sql_lines = [line]
            elif in_select:
                # Valid SQL keywords and constructs
                if any(keyword in line.upper() for keyword in
                    ['FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'ON', 'GROUP', 'HAVING', 'ORDER', 'AND', 'OR', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END']):
                    sql_lines.append(line)
                elif line.upper().startswith('SELECT'):
                    # New SELECT statement, stop here
                    break
                elif any(char in line for char in ['[', ']', '.', ',', '(', ')', '=', '<', '>', '!', "'", '"']):
                    # Looks like SQL content
                    sql_lines.append(line)
                else:
                    # Doesn't look like SQL, might be end of query
                    if not any(char in line for char in ['[', ']', '.', ',']):
                        break
                    sql_lines.append(line)
        
        # Join and clean up
        sql = ' '.join(sql_lines).strip().rstrip(';').rstrip(',')
        
        # Remove any remaining inline comments
        sql = Utils.remove_sql_comments(sql)
        
        # ⭐ FINAL CLEANUP - Fix any remaining syntax issues
        sql = sql.replace('GROUP BYDATEPART', 'GROUP BY DATEPART')
        sql = sql.replace('ORDER BYDATEPART', 'ORDER BY DATEPART')
        sql = sql.replace('GROUP BY BY', 'GROUP BY')
        sql = sql.replace('ORDER BY BY', 'ORDER BY')
        
        # Clean up extra spaces but preserve single spaces
        import re
        sql = re.sub(r'\s+', ' ', sql)  # Replace multiple spaces with single space
        
        # Basic validation
        if sql:
            sql_upper = sql.upper()
            
            # Must start with SELECT and contain FROM
            if not sql_upper.startswith('SELECT') or 'FROM' not in sql_upper:
                return ""
                
            # Check for obvious issues
            if any(issue in sql_upper for issue in ['FROM FROM', ', FROM', 'SELECT FROM', 'WHERE FROM']):
                return ""
                
            # Check for incomplete statements
            if any(sql_upper.endswith(keyword) for keyword in ['FROM', 'SELECT', 'WHERE', 'AND', 'OR', 'JOIN', 'ON', 'GROUP BY']):
                return ""
        
        print(f"🔍 CLEANED SQL OUTPUT: {sql[:100]}...")
        return sql
    
    @staticmethod
    def sanitize_sql(sql: str) -> str:
        """Enhanced SQL sanitization with GROUP BY validation"""
        try:
            # First validate GROUP BY syntax
            sql, group_by_msg = Utils.validate_group_by_syntax(sql)
            if "error" in group_by_msg.lower():
                raise ValueError(f"GROUP BY validation failed: {group_by_msg}")
            
            parsed = sqlparse.parse(sql)[0]
            
            # Allowed keywords for security
            allowed_keywords = {
                'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER',
                'GROUP', 'BY', 'ORDER', 'HAVING', 'AND', 'OR', 'ON', 'AS', 'IN', 'NOT',
                'IS', 'NULL', 'LIKE', 'BETWEEN', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
                'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'TOP', 'DISTINCT', 'CAST', 'CONVERT',
                'DATEPART', 'DATEADD', 'DATEDIFF', 'GETDATE', 'YEAR', 'MONTH', 'DAY',
                'ASC', 'DESC'
            }
            
            # Check for dangerous keywords
            dangerous_keywords = {
                'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE',
                'EXEC', 'EXECUTE', 'SP_', 'XP_', 'OPENROWSET', 'OPENDATASOURCE'
            }
            
            sql_upper = sql.upper()
            for keyword in dangerous_keywords:
                if keyword in sql_upper:
                    raise ValueError(f"Dangerous SQL keyword detected: {keyword}")
            
            return str(parsed)
            
        except Exception as e:
            logger.error("SQL sanitization failed", error=str(e))
            raise ValueError(f"SQL sanitization failed: {str(e)}")
    
    
    @staticmethod
    def extract_context_from_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """✅ KEEP - Simplified version for KQL storage"""
        context = {}
        
        if not results:
            return context
        
        # Simple extraction for KQL storage
        sample_row = results[0]
        all_columns = list(sample_row.keys())
        
        # Store basic info for KQL
        context['_query_metadata'] = {
            'total_records': len(results),
            'columns_analyzed': all_columns,
            'timestamp': datetime.now().isoformat()
        }
        
        return context

# Add SharePoint configuration to your environment variables or config
class SharePointConfig:
    """SharePoint configuration"""
    
    @staticmethod
    def get_config():
        return {
            'tenant_id': os.getenv('SHAREPOINT_TENANT_ID') or os.getenv('FABRIC_TENANT_ID'),
            'client_id': os.getenv('SHAREPOINT_CLIENT_ID') or os.getenv('FABRIC_CLIENT_ID'), 
            'client_secret': os.getenv('SHAREPOINT_CLIENT_SECRET') or os.getenv('FABRIC_CLIENT_SECRET'),
            'site_id': os.getenv('SHAREPOINT_SITE_ID'),
            'document_library_id': os.getenv('SHAREPOINT_DOCUMENT_LIBRARY_ID'),
            'scope': 'https://graph.microsoft.com/.default'
        }

class SharePointUploader:
    """Handle SharePoint file uploads"""
    
    def __init__(self):
        self.config = SharePointConfig.get_config()
        self.access_token = None
    
    def get_access_token(self):
        """Get access token for SharePoint"""
        try:
            token_url = f'https://login.microsoftonline.com/{self.config["tenant_id"]}/oauth2/v2.0/token'
            token_post_data = {
                'client_id': self.config['client_id'],
                'client_secret': self.config['client_secret'],
                'grant_type': 'client_credentials',
                'scope': self.config['scope']
            }
            
            token_request = requests.post(token_url, data=token_post_data)
            if token_request.status_code == 200:
                self.access_token = token_request.json()['access_token']
                logger.info("SharePoint access token obtained successfully")
                return True
            else:
                logger.error("Error obtaining SharePoint access token", response=token_request.text)
                return False
                
        except Exception as e:
            logger.error("Failed to get SharePoint access token", error=str(e))
            return False
    
    def upload_pdf_to_sharepoint(self, pdf_data: bytes, file_name: str) -> bool:
        """Upload PDF to SharePoint"""
        try:
            if not self.access_token and not self.get_access_token():
                logger.error("Cannot upload to SharePoint: No access token")
                return False
            
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/octet-stream',
            }
            
            # Clean filename
            clean_filename = file_name.replace(' ', '_').replace(':', '-')
            if not clean_filename.endswith('.pdf'):
                clean_filename += '.pdf'
            
            upload_url = f"https://graph.microsoft.com/v1.0/sites/{self.config['site_id']}/drives/{self.config['document_library_id']}/root:/{clean_filename}:/content"
            
            # Upload with retry logic
            upload_response = self._upload_with_retry(upload_url, headers, pdf_data)
            
            if upload_response and (upload_response.status_code == 200 or upload_response.status_code == 201):
                logger.info("PDF uploaded to SharePoint successfully", filename=clean_filename)
                return True
            else:
                logger.error("Failed to upload PDF to SharePoint", 
                           status_code=upload_response.status_code if upload_response else "No response",
                           filename=clean_filename)
                return False
                
        except Exception as e:
            logger.error("SharePoint upload error", error=str(e), filename=file_name)
            return False
    
    def _upload_with_retry(self, upload_url: str, headers: dict, file_content: bytes, max_retries: int = 3):
        """Upload with retry logic"""
        retry_delay = 5  # seconds
        
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.put(upload_url, headers=headers, data=file_content)
                
                if response.status_code == 200 or response.status_code == 201:
                    logger.info("File uploaded successfully to SharePoint")
                    return response
                elif attempt < max_retries:
                    logger.warning(f"SharePoint upload attempt {attempt}/{max_retries} failed. Retrying in {retry_delay} seconds...",
                                 status_code=response.status_code)
                    time.sleep(retry_delay)
                else:
                    logger.error("SharePoint upload failed after multiple attempts",
                               status_code=response.status_code,
                               response=response.text)
                    return response
                    
            except Exception as e:
                logger.error(f"SharePoint upload attempt {attempt} error", error=str(e))
                if attempt == max_retries:
                    return None
                time.sleep(retry_delay)
        
        return None

# Add this line after the report_generator and email_service initialization (around line 1950)
# Initialize SharePoint uploader
sharepoint_uploader = SharePointUploader()
# KQL storage operations
class KQLStorage:
    """Centralized KQL storage operations"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    async def initialize_kql_table(self):
        """Create ChatHistory_CFO table if it doesn't exist"""
        create_table_query = """
        .create table ChatHistory_CFO (
            SessionID: string,
            Timestamp: datetime,
            ConversationID: string,
            Question: string,
            Response: string,
            Context: string
        )
        """
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.db_manager.kusto_client.execute(self.db_manager.kusto_database, create_table_query)
            )
            logger.info("KQL table ChatHistory_CFO created or verified")
        except KustoServiceError as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg or "entityalreadyexists" in error_msg:
                logger.info("KQL table ChatHistory_CFO already exists")
            else:
                logger.error("Failed to create KQL table", error=str(e))
                raise
        except Exception as e:
            logger.error("Unexpected error creating KQL table", error=str(e))
            raise
    async def get_last_query_response(self, session_id: str = None) -> Dict[str, Any]:
        """Get the most recent query response from KQL for context"""
        actual_session_id = session_id if session_id else "default-session-1234567890"
        
        last_response_query = f"""
        ChatHistory_CFO
        | where SessionID == '{actual_session_id}'
        | where Question != 'tables_info' and Question != 'schema_info'
        | order by Timestamp desc
        | take 1
        | project Question, Response, Context, Timestamp
        """
        
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.db_manager.kusto_client.execute(self.db_manager.kusto_database, last_response_query)
            )
            
            if result.primary_results and len(result.primary_results[0]) > 0:
                row = result.primary_results[0][0]
                
                # Parse the stored response
                response_data = json.loads(row["Response"])
                context_data = json.loads(row["Context"]) if row.get("Context") else {}
                
                return {
                    "previous_question": row["Question"],
                    "previous_response": response_data,
                    "previous_context": context_data,
                    "timestamp": row["Timestamp"],
                    "has_data": True
                }
            
            return {"has_data": False}
            
        except Exception as e:
            logger.error("Failed to retrieve last query response", error=str(e), session_id=actual_session_id)
            return {"has_data": False}
    
    # ✅ NEW: Get the last N query responses for richer context
    async def get_recent_query_responses(self, session_id: str = None, limit: int = 3) -> List[Dict[str, Any]]:
        """Get the last N query responses from KQL for richer context"""
        actual_session_id = session_id if session_id else "default-session-1234567890"
        
        recent_responses_query = f"""
        ChatHistory_CFO
        | where SessionID == '{actual_session_id}'
        | where Question != 'tables_info' and Question != 'schema_info'
        | order by Timestamp desc
        | take {limit}
        | order by Timestamp asc
        | project Question, Response, Context, Timestamp
        """
        
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.db_manager.kusto_client.execute(self.db_manager.kusto_database, recent_responses_query)
            )
            
            responses = []
            for row in result.primary_results[0]:
                try:
                    response_data = json.loads(row["Response"])
                    context_data = json.loads(row["Context"]) if row.get("Context") else {}
                    
                    responses.append({
                        "question": row["Question"],
                        "response": response_data,
                        "context": context_data,
                        "timestamp": row["Timestamp"]
                    })
                    
                except json.JSONDecodeError:
                    continue
            
            logger.info("Retrieved recent responses for context", 
                       session_id=actual_session_id, 
                       count=len(responses))
            return responses
            
        except Exception as e:
            logger.error("Failed to retrieve recent responses", error=str(e), session_id=actual_session_id)
            return []

    
    async def store_in_kql(self, question: str, response: Dict, context: List[Dict], session_id: str = None):
        """Store query and response in KQL - FIXED KustoResultRow access"""
        print(f"🔍 DEBUG: store_in_kql CALLED")
        print(f"🔍 DEBUG: Question: '{question}'")
        print(f"🔍 DEBUG: Raw Session ID: {repr(session_id)}")
        
        # Skip storing schema-related queries
        if question.lower() in ['tables_info', 'schema_info'] or 'tables_info' in str(response):
            print(f"🔍 DEBUG: Skipping KQL storage for schema query: {question}")
            return
        
        # Use provided session ID or fall back to fixed session
        actual_session_id = session_id if session_id else "default-session-1234567890"
        
        conversation_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            # Better context extraction
            if response.get('sample_data'):
                extracted_context = Utils.extract_context_from_results(response['sample_data'])
            else:
                extracted_context = context if isinstance(context, dict) else {}
            
            # Safely serialize
            response_json = json.dumps(response, default=Utils.safe_json_serialize, ensure_ascii=False)
            context_json = json.dumps(extracted_context, default=Utils.safe_json_serialize, ensure_ascii=False)
        
            print(f"🔍 DEBUG: Response JSON length: {len(response_json)}")
            print(f"🔍 DEBUG: Response JSON preview: {response_json[:100]}...")
        
            # Clean session ID (keep your existing logic)
            clean_session_id = str(actual_session_id).strip()
            
            if clean_session_id.startswith('"') and clean_session_id.endswith('"'):
                clean_session_id = clean_session_id[1:-1]
            if clean_session_id.startswith("'") and clean_session_id.endswith("'"):
                clean_session_id = clean_session_id[1:-1]
                
            clean_session_id = clean_session_id.replace('"', '').replace("'", "")
            
            print(f"🔍 DEBUG: Cleaned Session ID: '{clean_session_id}' (no quotes)")
            
            # ✅ CRITICAL FIX: Use base64 encoding to prevent JSON corruption
            import base64
            
            # Clean question but encode JSON data as base64
            clean_question = question.replace('\n', ' ').replace('\r', ' ').strip()
            
            # Encode JSON as base64 to avoid CSV parsing issues
            response_b64 = base64.b64encode(response_json.encode('utf-8')).decode('ascii')
            context_b64 = base64.b64encode(context_json.encode('utf-8')).decode('ascii')
            
            print(f"🔍 DEBUG: Using base64 encoding for JSON (lengths: response={len(response_b64)}, context={len(context_b64)})")
            
            # ✅ FIXED: Store base64 encoded data (no quote escaping needed)
            ingest_query = f'''.ingest inline into table ChatHistory_CFO <|
    {clean_session_id},datetime({timestamp}),{conversation_id},{clean_question},{response_b64},{context_b64}'''
            
            print(f"🔍 DEBUG: KQL Ingest Preview:")
            print(f"  SessionID: '{clean_session_id}'")
            print(f"  ConversationID: '{conversation_id}'")
            print(f"  Question: '{clean_question[:50]}...'")
            print(f"  Response: base64 encoded ({len(response_b64)} chars)")
            
            # Execute the ingest
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.db_manager.kusto_client.execute(self.db_manager.kusto_database, ingest_query)
            )
            
            print(f"✅ DEBUG: KQL ingest completed successfully")
            
            # ✅ FIXED: Proper KustoResultRow access
            verify_query = f"""
            ChatHistory_CFO
            | where SessionID has '{clean_session_id}'
            | where ConversationID == '{conversation_id}'
            | extend Decoded_Response = base64_decode_tostring(Response)
            | project SessionID, Question, Decoded_Response
            """
            
            await asyncio.sleep(1)
            
            verify_result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.db_manager.kusto_client.execute(self.db_manager.kusto_database, verify_query)
            )
            
            verify_records = verify_result.primary_results[0] if verify_result.primary_results else []
            
            if verify_records:
                # ✅ FIXED: Access KustoResultRow by column name, not .get()
                for record in verify_records:
                    try:
                        # Access by column name directly
                        decoded_response = record["Decoded_Response"]
                        session_id_check = record["SessionID"]
                        question_check = record["Question"]
                        
                        print(f"✅ DEBUG: Verification successful")
                        print(f"  SessionID: {session_id_check}")
                        print(f"  Question: {question_check[:50]}...")
                        print(f"  Decoded response: {decoded_response[:100]}...")
                        
                        # Test JSON parsing
                        try:
                            test_json = json.loads(decoded_response)
                            print(f"✅ DEBUG: JSON parsing test PASSED")
                        except json.JSONDecodeError as e:
                            print(f"❌ DEBUG: JSON parsing test FAILED: {e}")
                            
                    except KeyError as e:
                        print(f"❌ DEBUG: Column access error: {e}")
                    except Exception as e:
                        print(f"❌ DEBUG: Verification error: {e}")
            else:
                print(f"❌ DEBUG: Verification failed - no records found")
            
            logger.info("KQL storage successful with base64 encoding", 
                    session_id=clean_session_id,
                    conversation_id=conversation_id,
                    verification_count=len(verify_records))
            
        except Exception as e:
            print(f"❌ DEBUG: KQL storage failed: {e}")
            logger.error("KQL storage failed", error=str(e))
            raise

    
    async def get_from_kql_cache(self, question: str, session_id: str = None) -> Optional[Dict]:
        """Retrieve cached response from KQL"""
        actual_session_id = session_id if session_id else "default-session-1234567890"
        normalized_question = Utils.normalize_question(question)
        
        cache_query = f"""
        ChatHistory_CFO
        | where SessionID == '{actual_session_id}'
        | where Question == '{normalized_question.replace("'", "''")}'
        | project Response
        | take 1
        """
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.db_manager.kusto_client.execute(self.db_manager.kusto_database, cache_query)
            )
            if result.primary_results and len(result.primary_results[0]) > 0:
                response = json.loads(result.primary_results[0][0]["Response"])
                response["session_id"] = actual_session_id
                logger.info("KQL cache hit", question=normalized_question, session_id=actual_session_id)
                return response
            return None
        except Exception as e:
            logger.error("KQL cache retrieval failed", error=str(e), session_id=actual_session_id)
            return None
    
    async def get_latest_responses(self, session_id: str = None) -> List[Dict]:
        """Retrieve latest 10 responses for UI"""
        actual_session_id = session_id if session_id else "default-session-1234567890"
        
        history_query = f"""
        ChatHistory_CFO
        | where SessionID == '{actual_session_id}'
        | order by Timestamp desc
        | take 10
        | project Timestamp, Question, Response
        """
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.db_manager.kusto_client.execute(self.db_manager.kusto_database, history_query)
            )
            responses = []
            for row in result.primary_results[0]:
                response = json.loads(row["Response"])
                responses.append({
                    "timestamp": row["Timestamp"],
                    "question": row["Question"],
                    "response": response
                })
            logger.info("Retrieved latest responses", count=len(responses))
            return responses
        except Exception as e:
            logger.error("Failed to retrieve latest responses", error=str(e))
            return []

# Initialize KQL storage
kql_storage = KQLStorage(db_manager)

# CONSOLIDATED: AI Services
# FIXED: AI Services with proper agent initialization
class AIServiceManager:
    """Consolidated AI service management"""
    
    def __init__(self):
        self.ai_foundry_enabled = False
        self.project_client = None
        self.graph_client = None
        self.intelligent_agent = None  # ✅ Initialize as None
        self.openai_client = None
        self.setup_services()
    
    def setup_services(self):
        """Initialize all AI services"""
        self.setup_openai_client()
        self.setup_ai_foundry()
        if GRAPH_AVAILABLE:
            self.setup_graph_client()
    
    def setup_openai_client(self):
        """Initialize Azure OpenAI client"""
        self.openai_client = AsyncAzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
    
    def setup_ai_foundry(self):
        """Initialize Azure AI Foundry client"""
        try:
            project_endpoint = os.getenv("AI_PROJECT_ENDPOINT")
            if not project_endpoint:
                logger.info("AI_PROJECT_ENDPOINT not set, AI Foundry features disabled")
                self.intelligent_agent = None  # ✅ Explicitly set to None
                self.ai_foundry_enabled = False
                return False
                
            credential = DefaultAzureCredential()
            
            self.project_client = AIProjectClient(
                endpoint=project_endpoint,
                credential=credential
            )
            
            logger.info("Azure AI Foundry client initialized successfully")
            self.ai_foundry_enabled = True
            
            
            try:
                self.intelligent_agent = IntelligentAnalyticsAgent(self.project_client)
                logger.info("Intelligent analytics agent initialized successfully")
            except Exception as agent_error:
                logger.error("Failed to initialize intelligent agent", error=str(agent_error))
                self.intelligent_agent = None  # ✅ Set to None on agent failure
                # Keep AI Foundry enabled but without the intelligent agent
            
            return True
            
        except Exception as e:
            logger.warning("Azure AI Foundry setup failed", error=str(e))
            logger.info("Continuing with standard OpenAI integration")
            self.intelligent_agent = None  # ✅ Set to None on failure
            return False
    
    def setup_graph_client(self):
        """Initialize Microsoft Graph client for email"""
        try:
            tenant_id = os.getenv("GRAPH_TENANT_ID")
            client_id = os.getenv("GRAPH_CLIENT_ID") 
            client_secret = os.getenv("GRAPH_CLIENT_SECRET")
            
            if not all([tenant_id, client_id, client_secret]):
                logger.info("Graph API credentials not complete, email features disabled")
                return False
                
            credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
            
            self.graph_client = GraphServiceClient(credential)
            logger.info("Microsoft Graph client initialized successfully")
            return True
            
        except Exception as e:
            logger.warning("Microsoft Graph setup failed", error=str(e))
            return False
    
    async def ask_intelligent_llm_async(self, prompt: str) -> str:
        """Ask LLM with consolidated error handling"""
        deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")
        if not deployment:
            raise ValueError("AZURE_OPENAI_DEPLOYMENT not set")
        try:
            response = await self.openai_client.chat.completions.create(
                model=deployment,
                messages=[
                    {"role": "system", "content": "You are a helpful, friendly AI assistant with expertise in data analysis."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=1000,
                seed=42
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error("LLM request failed", error=str(e))
            raise

# Initialize AI services
ai_services = AIServiceManager()

# CONSOLIDATED: Agent classes
class IntelligentAnalyticsAgent:
    """Enhanced agent for data analytics with AI capabilities"""
    
    def __init__(self, project_client):
        self.project_client = project_client
        self.data_agent_id = None
        self.report_agent_id = None
        self.email_agent_id = None
        
        if project_client:
            self.setup_agents()
    
    def setup_agents(self):
        """Create specialized agents for different tasks"""
        try:
            self.data_agent_id = self.create_data_agent()
            self.report_agent_id = self.create_report_agent()
            self.email_agent_id = self.create_email_agent()
            logger.info("All AI agents created successfully")
            
        except Exception as e:
            logger.error("Failed to setup AI agents", error=str(e))
    
    def create_data_agent(self):
        """Create agent specialized in data analysis"""
        try:
            agent = self.project_client.agents.create_agent(
                model="gpt-4o",
                name="data-analyst-agent",
                instructions="""You are an expert data analyst specializing in cybersecurity and business intelligence.
                
                Your capabilities:
                - Analyze SQL query results and identify patterns
                - Provide insights into cybersecurity metrics
                - Explain trends and anomalies in data
                - Suggest actionable recommendations
                - Format analysis in clear, business-friendly language
                
                Always provide:
                1. Key findings summary
                2. Detailed insights with numbers
                3. Business implications
                4. Actionable recommendations
                """,
                tools=[]
            )
            return agent.id
        except Exception as e:
            logger.error("Failed to create data agent", error=str(e))
            return None
    
    def create_report_agent(self):
        """Create agent specialized in report generation"""
        try:
            agent = self.project_client.agents.create_agent(
                model="gpt-4o-mini",
                name="report-generator-agent",
                instructions="""You are a professional report writer specializing in executive summaries and data reports.
                
                Your capabilities:
                - Generate executive summaries from data analysis
                - Create structured reports with clear sections
                - Write professional, concise content
                - Format content suitable for PDF/Excel reports
                - Include key metrics and recommendations
                
                Report structure should include:
                1. Executive Summary
                2. Key Metrics
                3. Detailed Findings
                4. Recommendations
                5. Next Steps
                """,
                tools=[]
            )
            return agent.id
        except Exception as e:
            logger.error("Failed to create report agent", error=str(e))
            return None
    
    def create_email_agent(self):
        """Create agent specialized in email communications"""
        try:
            agent = self.project_client.agents.create_agent(
                model="gpt-4o-mini", 
                name="email-agent",
                instructions="""You are a professional communication specialist for sending analytics reports.
                
                Your capabilities:
                - Write professional email content
                - Create compelling subject lines
                - Format emails appropriately for business context
                - Include proper context and next steps
                
                Email should be:
                - Professional and concise
                - Include key highlights from the report
                - Provide context for recipients
                - Include clear call-to-action if needed
                """,
                tools=[]
            )
            return agent.id
        except Exception as e:
            logger.error("Failed to create email agent", error=str(e))
            return None
    
    async def analyze_with_ai(self, data, question, context=None):
        """Use AI agent to analyze data and provide insights"""
        if not self.project_client or not self.data_agent_id:
            return None
            
        try:
            thread = self.project_client.agents.create_thread()
            
            analysis_prompt = f"""
            Original Question: {question}
            
            Data Results: {json.dumps(data[:10], default=str)}
            Total Records: {len(data)}
            
            {f"Additional Context: {context}" if context else ""}
            
            Please provide a comprehensive analysis of this data including key insights, trends, and actionable recommendations.
            """
            
            message = self.project_client.agents.create_message(
                thread_id=thread.id,
                role="user",
                content=analysis_prompt
            )
            
            run = self.project_client.agents.create_run(
                thread_id=thread.id,
                assistant_id=self.data_agent_id
            )
            
            response = await self.wait_for_run_completion(thread.id, run.id)
            return response
            
        except Exception as e:
            logger.error("AI analysis failed", error=str(e))
            return None
    
    async def wait_for_run_completion(self, thread_id, run_id, timeout=60):
        """Wait for agent run to complete and return response"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                run = self.project_client.agents.get_run(thread_id=thread_id, run_id=run_id)
                
                if run.status == "completed":
                    messages = self.project_client.agents.list_messages(thread_id=thread_id)
                    if messages.data:
                        latest_message = messages.data[0]
                        if latest_message.role == "assistant":
                            return latest_message.content[0].text.value
                    break
                elif run.status in ["failed", "cancelled", "expired"]:
                    logger.error(f"Agent run failed with status: {run.status}")
                    break
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error("Error waiting for run completion", error=str(e))
                break
        
        return None

# FIXED: IntelligentReportGenerator with robust error handling
class SimpleReportGenerator:
    """Simplified report generator with improved formatting"""
    
    def __init__(self, ai_services):
        self.ai_services = ai_services
    
    async def generate_stakeholder_report(self, question: str, data: List[Dict], analysis: str) -> bytes:
        """Generate professional report - simplified approach"""
        
        if not REPORT_LIBS_AVAILABLE:
            raise ImportError("Report generation libraries not available")
        
        try:
            # Let AI do all the content work
            report_content = await self._generate_professional_content(question, data, analysis)
            
            # Simple PDF creation
            return self._create_pdf(question, report_content, data)
            
        except Exception as e:
            print(f"Report generation failed: {e}")
            return self._simple_fallback(question, data, analysis)
    
    async def _generate_professional_content(self, question: str, data: List[Dict], analysis: str) -> str:
        """Generate intelligent report content - give AI full visibility into rich dataset"""
        
        # Enhanced data summary - show AI what's really available
        data_summary = f"No data available"
        if data and len(data) > 0:
            # Show all available columns
            all_columns = list(data[0].keys())
            
            # Identify financial categories
            revenue_cols = [col for col in all_columns if any(term in col.lower() for term in ['revenue', 'sales', 'income'])]
            cost_cols = [col for col in all_columns if any(term in col.lower() for term in ['cost', 'expense', 'cogs'])]
            profit_cols = [col for col in all_columns if any(term in col.lower() for term in ['profit', 'margin', 'ebitda', 'operating'])]
            balance_cols = [col for col in all_columns if any(term in col.lower() for term in ['assets', 'liabilities', 'equity', 'debt', 'cash'])]
            
            # Show date range
            years = list(set([record.get('Year', record.get('year', '')) for record in data if record.get('Year') or record.get('year')]))
            years = [y for y in years if y]  # Remove empty values
            
            data_summary = f"""COMPREHENSIVE FINANCIAL DATASET:
SCOPE: {len(data)} records spanning {min(years) if years else 'N/A'} to {max(years) if years else 'N/A'}

AVAILABLE FINANCIAL DATA:
- All Columns: {all_columns}
- Revenue Metrics: {revenue_cols if revenue_cols else 'None identified'}
- Cost/Expense Data: {cost_cols if cost_cols else 'None identified'} 
- Profitability Metrics: {profit_cols if profit_cols else 'None identified'}
- Balance Sheet Items: {balance_cols if balance_cols else 'None identified'}

SAMPLE DATA STRUCTURE:
{str(data[:3])}

DATA RANGE: This dataset contains {len(data)} records with detailed financial information across multiple years."""
        
        # Enhanced prompt to leverage rich dataset and prevent hallucination
        prompt = f"""You are a senior financial analyst creating a comprehensive P&L report using REAL FINANCIAL DATA.

QUESTION: {question}
ANALYSIS: {analysis}

AVAILABLE FINANCIAL DATA:
{data_summary}

CRITICAL DATA RESTRICTIONS:
- Use ONLY the exact data provided above
- Do NOT make up any months, numbers, or financial figures
- If data only goes to May 2025, do NOT mention June, July, August, September, October, etc.
- Only reference actual months and values that appear in the real dataset
- When asked for 2025 data, use ONLY 2025 records from the dataset with monthly break down if not specified otherwise

FORMAT REQUIREMENTS:
- Use clean, professional formatting
- Keep paragraphs concise (2-3 sentences each)
- Use simple bullet points (just • symbol)
- No sub-headings within sections
- No excessive spacing between sections
- Write everything under sub-headings in regular paragraph text without emphasis

Create a structured financial report with these sections:

EXECUTIVE SUMMARY
Write a comprehensive 10-15 sentence executive overview that includes:
- Overall financial performance assessment with key metrics
- Most significant trends and patterns identified in the data
- Critical business implications requiring executive attention
- Strategic context and forward-looking perspective

KEY INSIGHTS
Use simple bullet points for specific findings from the REAL data:
• Revenue performance (use actual months/values only)
• Cost management (use actual figures only)
• Profitability trends (use actual data only)
• Debt and liability analysis (use actual figures only)
Do NOT create sub-headings like "Revenue Performance:" or "Cost Management:" - just use simple bullets.

BUSINESS IMPLICATIONS
Concise analysis (2-3 short paragraphs):
- Financial performance assessment based on actual data
- Cost structure opportunities from real figures
- Risk factors identified from actual trends
- Strategic recommendations based on real patterns

NEXT STEPS
Clear action items with bullet points:
• Immediate actions (next 30 days)
• Short-term initiatives (next 3 months)  
• Medium-term priorities (next 6-12 months)
• Key metrics to monitor
• Follow-up analysis needed

IMPORTANT: Base everything on the actual data provided. Do not invent any financial figures, months, or trends that don't exist in the real dataset. Do not use sub-headings within sections."""

        try:
            content = await self.ai_services.ask_intelligent_llm_async(prompt)
            # Additional cleanup to remove any remaining symbols
            content = content.replace('###', '').replace('##', '').replace('**', '').replace('---', '').replace('####', '')
            return content
        except Exception as e:
            return f"""EXECUTIVE SUMMARY
Comprehensive financial analysis completed on {len(data) if data else 0} records spanning multiple years of detailed financial data.

KEY INSIGHTS
• Multi-year financial dataset analyzed covering revenue, costs, and profitability metrics
• Performance patterns identified across {len(data) if data else 0} financial records
• Year-over-year trends analyzed for strategic financial planning
• Detailed cost structure and margin analysis conducted

BUSINESS IMPLICATIONS
The comprehensive financial analysis reveals detailed insights into revenue performance, cost management effectiveness, and profitability trends. The multi-year dataset provides valuable perspective on financial health and operational efficiency.

Strategic financial planning should incorporate the identified trends in revenue growth, cost optimization opportunities, and margin improvement initiatives. The analysis supports data-driven decision making for sustainable financial performance.

Risk assessment indicates areas requiring focused attention to maintain financial stability and growth trajectory. Investment priorities should align with identified opportunities for enhanced profitability and market expansion.

NEXT STEPS
• Immediate Actions (Next 30 Days): Schedule executive financial review meeting with key stakeholders to discuss findings
• Short-term Initiatives (Next 3 Months): Implement enhanced financial monitoring dashboard for real-time performance tracking
• Medium-term Priorities (Next 6-12 Months): Execute strategic initiatives based on identified financial optimization opportunities
• Key Metrics to Monitor: Monthly profit margins, cost-to-revenue ratios, cash flow indicators
• Follow-up Analysis: Quarterly trend analysis and competitive benchmark review"""
    
    def _create_pdf(self, question: str, content: str, data: List[Dict]) -> bytes:
        """Create professional PDF with improved formatting"""
        
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer, 
            pagesize=A4, 
            topMargin=0.75*inch, 
            bottomMargin=0.75*inch, 
            leftMargin=0.75*inch, 
            rightMargin=0.75*inch
        )
        
        elements = []
        styles = getSampleStyleSheet()
        
        cell_wrap_style = ParagraphStyle(
    'CellWrap',
    parent=styles['Normal'],
    fontSize=8,
    leading=10,
    wordWrap='LTR'
)
        header_wrap_style = ParagraphStyle(
    'HeaderWrap',
    parent=styles['Normal'],
    fontSize=7,  # Smaller font to fit more text
    leading=9,
    wordWrap='LTR',
    alignment=1,  # Center align
    fontName='Helvetica-Bold',
    textColor=colors.white
)
        # Improved professional styles
        title_style = ParagraphStyle(
            'CustomTitle', 
            parent=styles['Title'], 
            fontSize=18, 
            textColor=colors.black, 
            alignment=TA_CENTER, 
            spaceAfter=15,
            fontName='Helvetica-Bold'
        )
        
        header_style = ParagraphStyle(
            'CustomHeader', 
            parent=styles['Heading2'], 
            fontSize=13, 
            textColor=colors.black, 
            spaceBefore=20, 
            spaceAfter=10,
            fontName='Helvetica-Bold',
            backColor=colors.Color(0.95, 0.95, 0.95),
            borderPadding=6
        )
        
        body_style = ParagraphStyle(
            'CustomBody',
            parent=styles['Normal'],
            fontSize=10,
            spaceBefore=2,
            spaceAfter=3,
            leading=12
        )
        
        bullet_style = ParagraphStyle(
            'Bullet',
            parent=styles['Normal'],
            fontSize=10,
            spaceBefore=2,
            spaceAfter=2,
            leftIndent=0,
            firstLineIndent=0,
            leading=12
        )
        
        # Professional Header Section
        current_date = datetime.now().strftime('%B %d, %Y')
        header_table = Table([["CONFIDENTIAL EXECUTIVE REPORT", current_date]], colWidths=[4*inch, 2.5*inch])
        header_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.black),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.white),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 12),
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ('PADDING', (0, 0), (-1, -1), 12),
        ]))
        elements.append(header_table)
        elements.append(Spacer(1, 20))
        
        # Title
        title = self._generate_title_from_question(question)
        elements.append(Paragraph(title, title_style))
        elements.append(Spacer(1, 20))
        
        # Process content with improved formatting
        sections = content.split('\n\n')
        first_section = True
        
        for section in sections:
            if not section.strip():
                continue
            
            # Clean the entire section of symbols first
            section = section.replace('###', '').replace('##', '').replace('**', '').replace('---', '').replace('*', '').replace('#', '')
            
            lines = section.strip().split('\n')
            first_line = lines[0].strip()
            
            # Skip empty sections
            if not first_line:
                continue
            
            # Special handling for EXECUTIVE SUMMARY - just show content, not header
            if 'EXECUTIVE SUMMARY' in first_line.upper():
                # Add content without the header
                for line in lines[1:]:
                    line = line.strip()
                    if line and not line.startswith('---'):
                        elements.append(Paragraph(line, body_style))
                        elements.append(Spacer(1, 4))
                elements.append(Spacer(1, 10))
                first_section = False
                continue
            
            # Skip the first section if it's just the title repeat
            if first_section and any(word in first_line.lower() for word in ['profit', 'loss', 'analysis', 'report']):
                first_section = False
                continue
            
            # Check if it's a section header
            if self._is_section_header(first_line):
                # Add section header
                elements.append(Paragraph(first_line, header_style))
                elements.append(Spacer(1, 6))
                
                # Add section content with improved formatting
                for line in lines[1:]:
                    line = line.strip()
                    if line and not line.startswith('---'):
                        # Skip sub-headings (lines ending with colon)
                        if line.endswith(':') and len(line.split()) <= 4:
                            continue
                        
                        if line.startswith('•') or line.startswith('-'):
                            # Clean bullet points
                            elements.append(Paragraph(line, bullet_style))
                        else:
                            elements.append(Paragraph(line, body_style))
                            elements.append(Spacer(1, 2))
                
                elements.append(Spacer(1, 12))
            else:
                # Regular content without header - skip if looks like formatting
                if not any(marker in first_line for marker in ['###', '---', '**']):
                    for line in lines:
                        line = line.strip()
                        if line and not line.startswith('---'):
                            elements.append(Paragraph(line, body_style))
                            elements.append(Spacer(1, 3))
                    elements.append(Spacer(1, 10))
            
            first_section = False
        
        # Enhanced data table - show comprehensive financial data
        if data and len(data) <= 25:
            elements.append(Spacer(1, 15))
            elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))
            elements.append(Spacer(1, 10))
            elements.append(Paragraph("<b>DETAILED DATA ANALYSIS</b>", header_style))
            elements.append(Spacer(1, 8))
            
            # Comprehensive financial table - show all relevant columns
            if data:
                all_columns = list(data[0].keys())
                
                # Prioritize financial columns for comprehensive P&L display
                financial_priority = ['year', 'month', 'revenue', 'income', 'profit', 'cost', 'expense', 'margin', 'ebitda', 'assets', 'liabilities', 'equity', 'debt', 'cash', 'sales']
                selected_columns = []
                
                # Pick financial priority fields first
                for field in financial_priority:
                    for col in all_columns:
                        if field.lower() in col.lower() and col not in selected_columns:
                            selected_columns.append(col)
                
                # Add remaining important columns up to reasonable limit
                for col in all_columns:
                    if col not in selected_columns:
                        selected_columns.append(col)
                    if len(selected_columns) >= 8:
                        break
                
                # Create comprehensive table
                #headers = [col.replace('_', ' ').title() for col in selected_columns]
                
               
                headers = []
                for col in selected_columns:
                    # Create shorter, wrapped header text
                    header_text = col.replace('_', ' ').replace('Total ', '').title()
                    # Wrap long headers in Paragraph for automatic line breaks
                    if len(header_text) > 8:
                        headers.append(Paragraph(header_text, header_wrap_style))
                    else:
                        headers.append(Paragraph(header_text, header_wrap_style))
                    
                table_data = [headers]
                # Show more data rows for better analysis (up to 15 records)
                for record in data[:15]:
                    row = []
                    for col in selected_columns:
                        value = record.get(col, '')
                        # Enhanced formatting for financial data
                        if col.lower() == 'month' and isinstance(value, (int, float)):
                            # Convert month number to name
                            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                                         'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                            month_idx = int(value) - 1
                            if 0 <= month_idx < 12:
                                row.append(month_names[month_idx])
                            else:
                                row.append(str(value))
                        elif col.lower() == 'year' and isinstance(value, (int, float)):
                            # Format year as integer
                            row.append(str(int(value)))
                        elif isinstance(value, float):
                            # Enhanced financial formatting
                            if abs(value) >= 1000000000:  # Billions
                                row.append(f"${value/1000000000:.1f}B" if any(term in col.lower() for term in ['revenue', 'profit', 'cost', 'sales', 'amount', 'income', 'assets', 'debt']) else f"{value/1000000000:.1f}B")
                            elif abs(value) >= 1000000:  # Millions
                                row.append(f"${value/1000000:.1f}M" if any(term in col.lower() for term in ['revenue', 'profit', 'cost', 'sales', 'amount', 'income', 'assets', 'debt']) else f"{value/1000000:.1f}M")
                            elif abs(value) >= 1000:  # Thousands
                                row.append(f"${value/1000:.1f}K" if any(term in col.lower() for term in ['revenue', 'profit', 'cost', 'sales', 'amount', 'income', 'assets', 'debt']) else f"{value:,.0f}")
                            else:
                                # Small numbers or percentages
                                if 'ratio' in col.lower() or 'margin' in col.lower() or 'percent' in col.lower():
                                    row.append(f"{value:.2f}%")
                                else:
                                    row.append(f"{value:.2f}")
                        elif isinstance(value, int) and value > 1000:
                            row.append(f"{value:,}")
                        elif isinstance(value, str) and len(value) > 15:
                            row.append(Paragraph(value, cell_wrap_style))
                        else:
                            row.append(str(value))
                    table_data.append(row)
                
                # Create professional financial table
                #table = Table(table_data, repeatRows=1)
                page_width = 6.5 * inch
                col_width = page_width / len(selected_columns)
                table = Table(table_data, colWidths=[col_width] * len(selected_columns), repeatRows=1)
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.black),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 7),
                    ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                    ('FONTSIZE', (0, 1), (-1, -1), 8),
                    ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
                    ('VALIGN', (0, 1), (-1, -1), 'TOP'),
                    ('PADDING', (0, 0), (-1, -1), 3),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.Color(0.95, 0.95, 0.95)]),
                ]))
                elements.append(table)
        
        # Professional Footer
        elements.append(Spacer(1, 30))
        elements.append(HRFlowable(width="100%", thickness=2, color=colors.black))
        elements.append(Spacer(1, 10))
        
        footer_table = Table([
            ["CONFIDENTIAL BUSINESS REPORT", f"Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}"]
        ], colWidths=[3.5*inch, 3*inch])
        footer_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
        ]))
        elements.append(footer_table)
        
        doc.build(elements)
        buffer.seek(0)
        return buffer.getvalue()
    
    def _generate_title_from_question(self, question: str) -> str:
        """Generate appropriate title from question"""
        question_lower = question.lower()
        
        if any(word in question_lower for word in ['p&l', 'pnl', 'profit', 'loss']):
            return "Profit & Loss Analysis Report"
        elif any(word in question_lower for word in ['sales', 'revenue']):
            return "Sales Performance Analysis"
        elif any(word in question_lower for word in ['financial', 'finance']):
            return "Financial Analysis Report"
        elif any(word in question_lower for word in ['performance', 'kpi']):
            return "Performance Analysis Report"
        else:
            return "Executive Business Analysis"
    
    def _is_section_header(self, text: str) -> bool:
        """Check if text is a section header - cleaned up"""
        headers = [
            'KEY INSIGHTS', 'BUSINESS IMPLICATIONS',
            'NEXT STEPS', 'ANALYSIS', 'FINDINGS'
        ]
        text_upper = text.upper().strip()
        # Remove any symbols
        clean_text = text_upper.replace('#', '').replace('*', '').strip()
        return any(header in clean_text for header in headers) or (
            len(clean_text.split()) <= 4 and clean_text.isupper()
        )
    
    def _simple_fallback(self, question: str, data: List[Dict], analysis: str) -> bytes:
        """Ultra-simple fallback"""
        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=A4)
        
        # Simple content
        c.setFont("Helvetica-Bold", 16)
        c.drawString(100, 750, "Business Analysis Report")
        c.setFont("Helvetica", 12)
        c.drawString(100, 720, f"Question: {question[:60]}")
        c.drawString(100, 700, f"Records: {len(data) if data else 0}")
        c.drawString(100, 680, "Analysis completed successfully")
        c.drawString(100, 640, "Executive review recommended")
        
        c.save()
        buffer.seek(0)
        return buffer.getvalue()
        
# Integration with your existing system
async def generate_stakeholder_report_with_your_prompt(question: str, data: List[Dict], analysis: str, ai_services) -> bytes:
    """Main function to integrate with your existing system"""
    
    generator = SimpleReportGenerator(ai_services)
    return await generator.generate_stakeholder_report(question, data, analysis)


# UPDATED: Main ReportGenerator class - FIXED VERSION
class ReportGenerator:
    """Enhanced report generator using your simple prompt approach"""
    
    def __init__(self):
        # Initialize the simple generator
        self.simple_generator = SimpleReportGenerator(ai_services)
    
    async def generate_pdf_report(self, data, analysis, report_title="Analytics Report", question=""):
        """Generate professional PDF report using your excellent prompt approach"""
        
        if not data:
            data = []
        if not analysis:
            analysis = "Analysis not available"
        if not question:
            question = "Data analysis request"
        
        try:
            return await self.simple_generator.generate_stakeholder_report(question, data, analysis)
        except Exception as e:
            logger.error("Report generation failed", error=str(e))
            return None


class EmailService:
    """Email service using Microsoft Graph API"""
    
    def __init__(self, graph_client):
        self.graph_client = graph_client
    
    async def send_email_with_report(self, recipients, subject, body, report_data, report_filename, report_type="pdf"):
        """Send email with report attachment"""
        if not self.graph_client:
            logger.error("Graph client not available")
            return False
            
        try:
            report_base64 = base64.b64encode(report_data).decode('utf-8')
            
            message = {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": body
                },
                "toRecipients": [
                    {"emailAddress": {"address": email}} 
                    for email in recipients if email
                ],
                "attachments": [{
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": report_filename,
                    "contentBytes": report_base64
                }]
            }
            
            await self.graph_client.me.send_mail.post({"message": message})
            logger.info("Email sent successfully", recipients=recipients, filename=report_filename)
            return True
            
        except Exception as e:
            logger.error("Email sending failed", error=str(e))
            return False
    
    async def send_notification_email(self, recipients, subject, body):
        """Send simple notification email without attachments"""
        if not self.graph_client:
            logger.error("Graph client not available")
            return False
            
        try:
            message = {
                "subject": subject,
                "body": {
                    "contentType": "HTML", 
                    "content": body
                },
                "toRecipients": [
                    {"emailAddress": {"address": email}}
                    for email in recipients if email
                ]
            }
            
            await self.graph_client.me.send_mail.post({"message": message})
            logger.info("Notification email sent", recipients=recipients)
            return True
            
        except Exception as e:
            logger.error("Notification email failed", error=str(e))
            return False

# Initialize services
report_generator = ReportGenerator()
email_service = EmailService(ai_services.graph_client)

# CONSOLIDATED: Visualization and analysis logic
class VisualizationManager:
    """Consolidated visualization management"""
    
    def __init__(self, ai_services):
        self.ai_services = ai_services
    
    def should_generate_visualization(self, question: str, sql: str, results: List[Dict[str, Any]]) -> bool:
        """Enhanced visualization detection"""
        if not results or len(results) < 1:
            return False
        
        # More comprehensive keyword detection
        chart_keywords = [
            "chart", "graph", "visualize", "plot", "display", "show", 
            "trend", "distribution", "compare", "comparison", "percentage", 
            "over time", "by", "breakdown", "analysis", "visual",
            "bar chart", "pie chart", "line chart", "histogram"
        ]
        
        # Check question for visualization intent
        question_lower = question.lower()
        has_viz_keywords = any(keyword in question_lower for keyword in chart_keywords)
        
        # Check if data is suitable for visualization
        if len(results) > 100:  # Too many data points
            return False
            
        columns = list(results[0].keys())
        numeric_cols = []
        categorical_cols = []
        
        # Better column type detection
        for col in columns:
            sample_values = [row[col] for row in results[:5] if row[col] is not None]
            if sample_values:
                if any(isinstance(val, (int, float)) for val in sample_values):
                    numeric_cols.append(col)
                elif any(isinstance(val, str) for val in sample_values):
                    categorical_cols.append(col)
        
        # Need at least one numeric and one categorical column, OR aggregated data
        has_suitable_data = (len(numeric_cols) >= 1 and len(categorical_cols) >= 1) or len(results) <= 20
        
        # Always generate chart if explicitly requested
        explicit_chart_request = any(word in question_lower for word in ["chart", "graph", "plot", "visualize"])
        
        return explicit_chart_request or (has_viz_keywords and has_suitable_data)
    
    async def generate_visualization(self, question: str, results: List[Dict], sql: str) -> Optional[Dict]:
        """Enhanced visualization generation with explanatory text"""
        if not results:
            return None
        
        try:
            # Analyze the best chart type
            prompt = f"""
            Analyze this data and recommend the best Chart.js configuration:
            
            Question: {question}
            Data Sample: {json.dumps(results[:3], default=Utils.safe_json_serialize)}
            Total Records: {len(results)}
            
            Choose the best chart type from: bar, line, pie, doughnut
            
            Rules:
            - Use 'bar' for comparisons, counts, categories
            - Use 'line' for trends over time
            - Use 'pie' for distributions, percentages, parts of whole
            - Use 'doughnut' for proportions with emphasis on total
            
            Respond with JSON only:
            {{
                "chart_type": "recommended_type",
                "reasoning": "brief explanation"
            }}
            """
            
            response = await self.ai_services.ask_intelligent_llm_async(prompt)
            chart_analysis = json.loads(response.strip().lstrip('```json').rstrip('```').strip())
            chart_type = chart_analysis.get("chart_type", "bar")
            
        except Exception as e:
            logger.warning("Chart type analysis failed, using fallback", error=str(e))
            # Fallback logic
            question_lower = question.lower()
            if any(word in question_lower for word in ["trend", "over time", "timeline"]):
                chart_type = "line"
            elif any(word in question_lower for word in ["distribution", "percentage", "proportion"]):
                chart_type = "pie"
            else:
                chart_type = "bar"
        
        # Prepare data for chart
        columns = list(results[0].keys())
        
        # Find the best label and value columns
        numeric_cols = []
        categorical_cols = []
        
        for col in columns:
            sample_values = [row[col] for row in results[:5] if row[col] is not None]
            if sample_values:
                if all(isinstance(val, (int, float)) for val in sample_values):
                    numeric_cols.append(col)
                else:
                    categorical_cols.append(col)
        
        if not numeric_cols:
            return None
        
        # Choose label column (categorical first, then first column)
        label_col = categorical_cols[0] if categorical_cols else columns[0]
        value_col = numeric_cols[0]
        
        # Limit data points for better visualization
        chart_data = results[:20]  # Limit to 20 points max
        
        # Extract labels and values with formatting
        labels = []
        values = []
        
        for row in chart_data:
            label = str(row.get(label_col, 'Unknown'))[:30]  # Truncate long labels
            value = row.get(value_col, 0)
            
            # Convert value to number and format to 2 decimal places
            try:
                if isinstance(value, str):
                    value = float(value) if '.' in value else int(value)
                elif value is None:
                    value = 0
                # ✅ Format numeric values to 2 decimal places
                value = Utils.format_number(value, 2)
            except:
                value = 0
                
            labels.append(label)
            values.append(value)
        
        # Generate chart explanation with formatted numbers
        total_value = sum(values)
        max_value = max(values) if values else 0
        min_value = min(values) if values else 0
        max_index = values.index(max_value) if values else 0
        min_index = values.index(min_value) if values else 0
        
        # Create contextual explanation based on chart type with formatted numbers
        if chart_type in ["pie", "doughnut"]:
            max_percentage = (max_value / total_value * 100) if total_value > 0 else 0
            explanation = f"This {chart_type} chart shows the distribution of {value_col.replace('_', ' ').lower()} across different {label_col.replace('_', ' ').lower()}. "
            explanation += f"The largest segment is '{labels[max_index]}' with {max_value:,.2f} ({max_percentage:.1f}% of total). "
            explanation += f"Total across all categories: {total_value:,.2f}."
        
        elif chart_type == "line":
            explanation = f"This line chart displays the trend of {value_col.replace('_', ' ').lower()} over {label_col.replace('_', ' ').lower()}. "
            if len(values) > 1:
                trend = "increasing" if values[-1] > values[0] else "decreasing" if values[-1] < values[0] else "stable"
                explanation += f"The overall trend appears to be {trend}. "
            explanation += f"Peak value: {max_value:,.2f} at '{labels[max_index]}', lowest: {min_value:,.2f} at '{labels[min_index]}'."
        
        else:  # bar chart
            explanation = f"This bar chart compares {value_col.replace('_', ' ').lower()} across different {label_col.replace('_', ' ').lower()}. "
            explanation += f"'{labels[max_index]}' has the highest value at {max_value:,.2f}, while '{labels[min_index]}' has the lowest at {min_value:,.2f}. "
            if len(values) > 2:
                avg_value = sum(values) / len(values)
                explanation += f"Average value: {avg_value:,.2f}."
        
        # Create Chart.js configuration - FIXED: Added missing closing brace
        chart_config = {
            "type": chart_type,
            "data": {
                "labels": labels,
                "datasets": [{
                    "label": value_col.replace('_', ' ').title(),
                    "data": values,
                    "backgroundColor": [
                        "rgba(75, 192, 192, 0.8)",
                        "rgba(255, 99, 132, 0.8)", 
                        "rgba(54, 162, 235, 0.8)",
                        "rgba(255, 206, 86, 0.8)",
                        "rgba(153, 102, 255, 0.8)",
                        "rgba(255, 159, 64, 0.8)",
                        "rgba(199, 199, 199, 0.8)",
                        "rgba(83, 102, 255, 0.8)",
                        "rgba(255, 99, 71, 0.8)",
                        "rgba(50, 205, 50, 0.8)"
                    ][:len(values)],
                    "borderColor": [
                        "rgba(75, 192, 192, 1)",
                        "rgba(255, 99, 132, 1)",
                        "rgba(54, 162, 235, 1)", 
                        "rgba(255, 206, 86, 1)",
                        "rgba(153, 102, 255, 1)",
                        "rgba(255, 159, 64, 1)",
                        "rgba(199, 199, 199, 1)",
                        "rgba(83, 102, 255, 1)",
                        "rgba(255, 99, 71, 1)",
                        "rgba(50, 205, 50, 1)"
                    ][:len(values)],
                    "borderWidth": 2
                }]
            },
            "options": {
                "responsive": True,
                "maintainAspectRatio": False,
                "plugins": {
                    "title": {
                        "display": True,
                        "text": f"{value_col.replace('_', ' ').title()} by {label_col.replace('_', ' ').title()}",
                        "font": {"size": 16, "weight": "bold"}
                    },
                    "legend": {
                        "display": chart_type in ["pie", "doughnut"]
                    }
                }
            }
        }
        
        # Add scales for non-pie charts
        if chart_type not in ["pie", "doughnut"]:
            chart_config["options"]["scales"] = {
                "y": {
                    "beginAtZero": True,
                    "title": {
                        "display": True,
                        "text": value_col.replace('_', ' ').title()
                    }
                },
                "x": {
                    "title": {
                        "display": True,
                        "text": label_col.replace('_', ' ').title()
                    }
                }
            }
        
        # Return both chart config and explanation
        return {
            "chart_config": chart_config,
            "explanation": explanation,
            "chart_type": chart_type,
            "data_points": len(values),
            "total_value": total_value if chart_type in ["pie", "doughnut"] else None
        }
    
    async def add_visualization_to_response(self, question: str, sql: str, results: List[Dict], response: Dict):
        """Add visualization with explanation to response if appropriate"""
        try:
            if self.should_generate_visualization(question, sql, results):
                logger.info("Generating visualization", question=question, result_count=len(results))
                
                # Generate chart config and explanation
                viz_data = await self.generate_visualization(question, results, sql)
                
                if viz_data:
                    response["visualization"] = viz_data["chart_config"]
                    response["chart_explanation"] = viz_data["explanation"]
                    response["chart_type"] = viz_data["chart_type"]
                    response["has_visualization"] = True
                    logger.info("Visualization added to response with explanation")
                    
                    # Enhance the main analysis with chart context
                    if "analysis" in response:
                        response["analysis"] += f"\n\n**📊 Chart Insights:**\n{viz_data['explanation']}"
                else:
                    logger.warning("Failed to generate chart config")
            else:
                logger.info("No visualization needed", question=question)
                
        except Exception as e:
            logger.error("Visualization generation failed", error=str(e))

# Initialize visualization manager
viz_manager = VisualizationManager(ai_services)

# Fix the PromptManager class - remove the enhanced_prompt_manager reference

class PromptManager:
    """Centralized prompt and intent management with enhanced GROUP BY rules"""
    
    def __init__(self, ai_services):
        self.ai_services = ai_services
        # Remove the enhanced_prompt_manager initialization that was causing the error
    
    def load_base_prompt(self):
        """Enhanced base prompt with comprehensive few-shot learning"""
        
        few_shot_examples = """
------ EXAMPLE 1: Revenue Analysis with Proper Client Filtering
User: Show me revenue for Brown Ltd in 2024 and 2025
System: You need to filter by client and use DATEPART for year extraction from date columns:

SELECT 
    [Client],
    DATEPART(YEAR, [Date]) AS [Year],
    SUM([Revenue]) AS [Total_Revenue]
FROM [dbo].[Financial] 
WHERE [Client] = 'Brown Ltd' 
    AND DATEPART(YEAR, [Date]) IN (2024, 2025)
GROUP BY [Client], DATEPART(YEAR, [Date])
ORDER BY [Year];

------ EXAMPLE 2: Contextual Follow-up Analysis with Context Preservation
User: Why does the revenue behave this way?
System: To analyze revenue behavior, calculate growth rates and trends using the same client filter from previous query:

SELECT 
    [Client],
    DATEPART(YEAR, [Date]) AS [Year],
    DATEPART(QUARTER, [Date]) AS [Quarter],
    SUM([Revenue]) AS Quarterly_Revenue,
    LAG(SUM([Revenue])) OVER (
        PARTITION BY [Client] 
        ORDER BY DATEPART(YEAR, [Date]), DATEPART(QUARTER, [Date])
    ) AS Previous_Quarter,
    CASE 
        WHEN LAG(SUM([Revenue])) OVER (
            PARTITION BY [Client] 
            ORDER BY DATEPART(YEAR, [Date]), DATEPART(QUARTER, [Date])
        ) > 0 
        THEN ((SUM([Revenue]) - LAG(SUM([Revenue])) OVER (
            PARTITION BY [Client] 
            ORDER BY DATEPART(YEAR, [Date]), DATEPART(QUARTER, [Date])
        )) / LAG(SUM([Revenue])) OVER (
            PARTITION BY [Client] 
            ORDER BY DATEPART(YEAR, [Date]), DATEPART(QUARTER, [Date])
        )) * 100 
        ELSE NULL 
    END AS Growth_Rate_Percent
FROM [dbo].[Financial] 
WHERE [Client] = 'Brown Ltd' 
    AND DATEPART(YEAR, [Date]) IN (2024, 2025)
GROUP BY [Client], DATEPART(YEAR, [Date]), DATEPART(QUARTER, [Date])
ORDER BY [Year], [Quarter];

------ EXAMPLE 3: Profit & Loss Report Structure
User: Create a P&L report for 2025
System: For comprehensive P&L analysis, aggregate multiple financial metrics by time period:

SELECT 
    DATEPART(QUARTER, [Date]) AS [Quarter],
    DATEPART(MONTH, [Date]) AS [Month],
    SUM([Revenue]) AS Total_Revenue,
    SUM([Gross Profit]) AS Total_Gross_Profit,
    SUM([Net Income]) AS Total_Net_Income,
    (SUM([Gross Profit]) / NULLIF(SUM([Revenue]), 0)) * 100 AS Gross_Margin_Percent,
    (SUM([Net Income]) / NULLIF(SUM([Revenue]), 0)) * 100 AS Net_Margin_Percent
FROM [dbo].[Financial] 
WHERE DATEPART(YEAR, [Date]) = 2025
GROUP BY DATEPART(QUARTER, [Date]), DATEPART(MONTH, [Date])
ORDER BY [Quarter], [Month];

------ EXAMPLE 4: Context Preservation for Follow-ups
User: Show me their departments (after previous client query)
System: When user refers to "their" or "them", preserve the client filter from previous query:

SELECT 
    [Client],
    [Department], 
    COUNT(*) AS Employee_Count,
    AVG([Revenue]) AS Avg_Revenue_Per_Employee
FROM [dbo].[Financial] 
WHERE [Client] = 'Brown Ltd'  -- Preserve client context
GROUP BY [Client], [Department]
ORDER BY Avg_Revenue_Per_Employee DESC;

------ EXAMPLE 5: Date Handling Best Practices
User: Show trends over the last 6 months
System: Use DATEADD for relative date filtering and handle NULL dates properly:

SELECT 
    DATEPART(YEAR, [Date]) AS [Year],
    DATEPART(MONTH, [Date]) AS [Month],
    COUNT(*) AS Record_Count,
    SUM([Revenue]) AS Monthly_Revenue
FROM [dbo].[Financial] 
WHERE [Date] >= DATEADD(MONTH, -6, GETDATE())
    AND [Date] IS NOT NULL
GROUP BY DATEPART(YEAR, [Date]), DATEPART(MONTH, [Date])
ORDER BY [Year], [Month];
"""
        
        return f"""You are an expert SQL analyst specializing in financial data analysis. You must respond in this EXACT format:

SQL_QUERY:
[Complete SQL statement using exact column names from schema]

ANALYSIS:
[Brief explanation of results and insights]

🎯 CRITICAL SUCCESS FACTORS:

1. **Schema Adherence**: Use ONLY columns that exist in the provided schema
2. **Context Preservation**: For follow-up questions, maintain client/entity filters from previous queries
3. **Professional Formatting**: Use clean, readable SQL formatting with proper indentation
4. **Date Handling**: Use DATEPART(YEAR/MONTH/QUARTER, [Date]) for date-based grouping
5. **Analytical Depth**: For "why" questions, include growth rates, trends, and comparisons

📚 LEARN FROM THESE EXAMPLES:
{few_shot_examples}

✅ PROVEN PATTERNS:
- Client Analysis: WHERE [Client] = 'ClientName' AND DATEPART(YEAR, [Date]) IN (2024, 2025)
- Growth Calculation: LAG() OVER (PARTITION BY [Client] ORDER BY [Year], [Quarter])  
- Trend Analysis: Use DATEPART for time-based grouping
- Context Questions: Preserve WHERE conditions from previous successful queries
- P&L Reports: Aggregate Revenue, Gross Profit, Net Income with margin calculations

❌ AVOID THESE COMMON MISTAKES:
- Missing spaces: GROUP BY[column] → should be GROUP BY [column]
- Wrong columns: Use exact names from schema, not assumptions
- Lost context: Follow-up questions must preserve client/entity filters
- Incomplete GROUP BY: All non-aggregate SELECT columns must be in GROUP BY
- Date errors: Use DATEPART() for date extraction, handle NULLs

🔧 SQL SYNTAX RULES:
- Always space after keywords: "GROUP BY [column]" not "GROUP BY[column]"
- Use proper WHERE clause combining: WHERE condition1 AND condition2
- Handle division by zero: NULLIF(denominator, 0) in calculations
- Order results logically: ORDER BY [Year], [Quarter], [Month]

REMEMBER: Your goal is to generate SQL that a financial analyst can immediately execute to get business insights!
"""
    
    def format_schema_for_prompt(self, tables_info: List[Dict]) -> str:
        return f"AVAILABLE SCHEMA:\n{json.dumps(tables_info, indent=2, default=Utils.safe_json_serialize)}"
    
    def filter_schema_for_question(self, question: str, tables_info: List[Dict]) -> List[Dict]:
        question_lower = question.lower()
        
        # For P&L/financial questions, force Financial table to the top
        if any(word in question_lower for word in ['p&l', 'profit', 'loss', 'financial', 'revenue']):
            result = []
            financial_table = None
            other_financial = []
            remaining = []
            
            for table in tables_info:
                table_name = table.get('table', '').lower()
                
                # Find Financial table first
                if 'financial' in table_name:
                    financial_table = table
                elif any(term in table_name for term in ['sales', 'revenue', 'balance', 'income']):
                    other_financial.append(table)
                else:
                    remaining.append(table)
            
            # Put Financial table first, then other financial tables
            if financial_table:
                result.append(financial_table)
            result.extend(other_financial[:2])  # Max 2 other financial tables
            result.extend(remaining[:2])       # Max 2 other tables
            
            return result
        
        # For other questions, use existing logic
        question_terms = set(term for term in question_lower.split() if len(term) > 2)
        relevant_tables = []
        
        for table_info in tables_info:
            table_name = table_info['table'].lower()
            table_base_name = table_name.split('.')[-1].strip('[]')
            columns = [col.lower() for col in table_info.get('columns', [])]
            table_terms = set([table_base_name] + [col.split()[0] for col in columns])
            
            if question_terms.intersection(table_terms):
                relevant_tables.append(table_info)
        
        return relevant_tables or tables_info
    
    
    
        
    async def build_chatgpt_system_prompt(self, question: str, tables_info: List[Dict], conversation_history: List[Dict] = None) -> str:
        """✅ UPDATE - Simplified without complex context logic"""
        
        base_prompt = self.load_base_prompt()
        schema_section = self.format_schema_for_prompt(self.filter_schema_for_question(question, tables_info))
        
        # ✅ SIMPLIFIED: Basic question analysis without complex context
        question_analysis = f"""
    🎯 CURRENT REQUEST ANALYSIS:
    User Question: "{question}"

    INSTRUCTIONS:
    1. **Schema Validation**: Use ONLY the tables and columns shown below in the schema
    2. **Professional Output**: Format SQL with proper spacing and readable structure
    3. **Business Focus**: Provide SQL that delivers actionable business insights

    🆕 NEW QUERY PROCESSING: Comprehensive analysis of the dataset.
    """
        
        return f"{base_prompt}\n\n{schema_section}\n\n{question_analysis}"

    
    def extract_filters_from_sql(self, sql: str) -> List[str]:
        """Extract WHERE conditions from previous SQL to preserve context"""
        
        if not sql:
            return []
        
        try:
            sql_upper = sql.upper()
            
            # Find WHERE clause
            where_start = sql_upper.find(' WHERE ')
            if where_start == -1:
                return []
            
            # Find end of WHERE clause (before GROUP BY, ORDER BY, etc.)
            where_end = len(sql)
            for keyword in [' GROUP BY', ' ORDER BY', ' HAVING']:
                pos = sql_upper.find(keyword, where_start)
                if pos != -1:
                    where_end = min(where_end, pos)
            
            where_clause = sql[where_start + 7:where_end].strip()
            
            # Split by AND/OR and clean up
            conditions = []
            for condition in where_clause.split(' AND '):
                condition = condition.strip()
                if condition and not condition.upper().startswith('OR'):
                    # Clean up the condition
                    if condition.startswith('(') and condition.endswith(')'):
                        condition = condition[1:-1]
                    conditions.append(condition)
            
            logger.info("Extracted SQL filters", original_sql=sql, filters=conditions)
            return conditions
            
        except Exception as e:
            logger.warning("Failed to extract filters from SQL", error=str(e), sql=sql)
            return []
    

# Initialize prompt manager
prompt_manager = PromptManager(ai_services)

# CONSOLIDATED: Request models
# FIXED: Request models
class IntelligentRequest(BaseModel):
    question: str
    enable_ai_insights: Optional[bool] = True
    enable_email_notification: Optional[bool] = False
    email_recipients: Optional[List[str]] = []

    @validator("question")
    def validate_question(cls, value):
        if not value.strip():
            raise ValueError("Question cannot be empty")
        if len(value) < 3:
            raise ValueError("Question is too short; please provide more details")
        return value.strip()

class ReportRequest(BaseModel):
    data_query: str
    report_type: Optional[str] = "executive"  # executive, detailed, summary
    report_format: Optional[str] = "pdf"  # pdf, excel, both
    email_recipients: List[str]
    subject_hint: Optional[str] = None
    include_ai_analysis: Optional[bool] = True

    @validator("email_recipients")
    def validate_emails(cls, value):
        if not value:
            raise ValueError("At least one email recipient is required")
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        for email in value:
            if not email_pattern.match(email):
                raise ValueError(f"Invalid email address: {email}")
        return value


# CONSOLIDATED: Main analysis function (removing duplication)
class AnalyticsEngine:
    """Main analytics engine - consolidated logic"""
    
    def __init__(self, db_manager: DatabaseManager, 
                 schema_manager: SchemaManager, 
                 kql_storage: KQLStorage,  # ✅ Explicit type hint
                 ai_services: AIServiceManager, 
                 viz_manager: VisualizationManager, 
                 prompt_manager: PromptManager):
        self.db_manager = db_manager
        self.schema_manager = schema_manager
        self.kql_storage = kql_storage
        self.ai_services = ai_services
        self.viz_manager = viz_manager
        self.prompt_manager = prompt_manager
    
    def is_contextual_question(self, question: str) -> bool:
        """Detect if a question refers to previous context - FIXED VERSION"""
        question_lower = question.lower().strip()
        
        # ✅ STRONG contextual indicators (high confidence)
        strong_contextual = [
            # Complete phrases that are definitely contextual
            'why does it', 'why does this', 'why does that',
            'how does it', 'how does this', 'how does that', 
            'what does it', 'what does this', 'what does that',
            'how do i improve this', 'how can i improve this', 'how to improve this',
            'how do i increase this', 'how can i increase this', 'how to increase this',
            'explain this', 'explain it', 'analyze this', 'analyze it',
            'what causes this', 'what causes it', 'what makes this', 'what makes it',
            'this behavior', 'this pattern', 'this trend', 'this way',
            'why is this', 'why is it', 'how is this', 'how is it',
            'what should we do about this', 'what should we do',
            'what can we do about this', 'what can we do',
            'how should we handle this', 'how should we handle',
            'what action should we take', 'what actions should we take',
            'how do we fix this', 'how can we fix this',
            'what steps should we take', 'what do we need to do',
            'how do we improve this', 'how can we improve this',
            'what recommendations do you have', 'what do you recommend',
            'what should be our next steps', 'what are the next steps'
            ]
        
        # Check for strong indicators first (these are definitely contextual)
        for indicator in strong_contextual:
            if indicator in question_lower:
                print(f"🔍 DEBUG: Strong contextual pattern detected: '{indicator}' in '{question}'")
                return True
        
        # ✅ WEAK contextual indicators (only contextual in specific contexts)
        weak_contextual = ['this', 'it', 'that', 'them', 'their']
        
        # Only consider weak indicators if:
        # 1. Question is very short (<=4 words)
        # 2. AND contains improvement/explanation words
        # 3. AND doesn't contain data analysis words
        if len(question_lower.split()) <= 4:
            has_weak_pronoun = any(pronoun in question_lower for pronoun in weak_contextual)
            has_improvement_words = any(word in question_lower for word in ['improve', 'increase', 'fix', 'change', 'why', 'how', 'explain'])
            has_data_words = any(word in question_lower for word in ['show', 'get', 'find', 'list', 'display', 'revenue', 'profit', 'sales'])
            
            if has_weak_pronoun and has_improvement_words and not has_data_words:
                print(f"🔍 DEBUG: Weak contextual pattern detected in short question: '{question}'")
                return True
        
        # ✅ Questions starting with contextual words
        contextual_starters = [
            'why does', 'how does', 'what does', 'explain why', 'explain how',
            'why is this', 'how is this', 'what is this'
        ]
        
        for starter in contextual_starters:
            if question_lower.startswith(starter):
                print(f"🔍 DEBUG: Contextual starter detected: '{starter}' in '{question}'")
                return True
        
        print(f"🔍 DEBUG: Question NOT detected as contextual: '{question}'")
        return False     
      
    
    async def cached_intelligent_analyze(self, question: str, session_id: str = None, enable_ai_insights: bool = False) -> Dict[str, Any]:
        """Main entry point with caching support - FIXED CONTEXT VERSION"""
    
        actual_session_id = session_id if session_id else "default-session-1234567890"
        
        print(f"🔍 ENTRY: Question='{question}', Session={actual_session_id}")
         
        # Skip KQL cache lookup for schema queries
        if question.lower() in ['tables_info', 'schema_info']:
            print(f"🔍 FLOW: Schema query - skipping storage")
            return await self.intelligent_analyze_with_context(question, actual_session_id, enable_ai_insights, None)
        
        is_contextual = self.is_contextual_question(question)
        is_data = self.is_data_question(question) 
        
        print(f"🔍 FLOW: is_contextual = {is_contextual}")
    
    # For contextual questions, always process fresh (don't use cache)
        if is_contextual:
            print(f"🔍 FLOW: Processing contextual question - NO STORAGE")
            logger.info("Contextual question detected, processing fresh", 
                    question=question, session_id=actual_session_id)
            
            conversation_history = await self.get_simple_conversation_history(actual_session_id)
            logger.info("Retrieved conversation history for contextual question",
                    session_id=actual_session_id,
                    history_items=len(conversation_history))
            
            #Check if we have previous data context for this contextual question
            if conversation_history and len(conversation_history) > 0:
                # Check if the last conversation was a data query with results
                last_assistant_msg = None
                for msg in reversed(conversation_history):
                    if msg.get('role') == 'assistant':
                        last_assistant_msg = msg
                        break
                
                has_previous_data = False
                if last_assistant_msg and 'SQL Query:' in last_assistant_msg.get('content', ''):
                    has_previous_data = True
                    print(f"🔍 CONTEXT: Found previous data query in conversation history")
                
                # 🆕 If contextual + we have previous data, force data analysis
                if has_previous_data and not is_data:
                    print(f"🔍 OVERRIDE: Contextual question with previous data - forcing data analysis")
                    # Override the data detection for contextual questions with data context
                    is_data = True
        
            
            result = await self.intelligent_analyze_with_context(question, actual_session_id, enable_ai_insights, conversation_history)
           
            try:
                print(f"🔍 DEBUG: Storing contextual question in KQL for session: {actual_session_id}")
                await self.kql_storage.store_in_kql(question, result, [], actual_session_id)
                print(f"✅ DEBUG: Contextual question stored successfully")
            except Exception as e:
                logger.error("KQL storage failed for contextual question", error=str(e), question=question, session_id=actual_session_id)
            
            return result    
        
        print(f"🔍 FLOW: Processing non-contextual question")          
        # Check KQL cache for user questions only (with session context)
        cached_result = await self.kql_storage.get_from_kql_cache(question, actual_session_id)
        logger.info("Cache check", 
                question=question, 
                session_id=actual_session_id, 
                cache_hit=bool(cached_result),
                is_contextual=is_contextual
        )
                
        
        if cached_result:
            logger.info("Cache hit for non-contextual question", question=question, session_id=actual_session_id)
            cached_result["session_id"] = actual_session_id          
            return cached_result
        
        # Process new question WITH CONTEXT
        result = await self.intelligent_analyze_with_context(question, actual_session_id, enable_ai_insights, None)
        
        try:
            print(f"🔍 DEBUG: About to store in KQL for session: {actual_session_id}")
            await self.kql_storage.store_in_kql(question, result, [], actual_session_id)
            print(f"✅ DEBUG: KQL storage completed for session: {actual_session_id}")
            
            # ✅ Add small delay to ensure KQL consistency
            await asyncio.sleep(0.1)
            
            logger.info("Processed and stored result", question=question, session_id=actual_session_id)
        except Exception as e:
            logger.error("KQL storage failed", error=str(e), question=question, session_id=actual_session_id)
            # Don't fail the entire request if storage fails
        
        return result
    
    
    def is_data_question(self, question: str) -> bool:
        """Detect if this is a data analysis question that should generate SQL"""
        
        question_lower = question.lower().strip()
        
        # Data analysis keywords
        data_keywords = [
            'show', 'get', 'find', 'list', 'display', 'what is', 'what are',
            'how much', 'how many', 'count', 'sum', 'total', 'average', 'avg',
            'max', 'min', 'calculate', 'analysis', 'analyze', 'report',
            'revenue', 'profit', 'margin', 'sales', 'cost', 'expense',
            'gross', 'net', 'income', 'client', 'customer', 'year', 'quarter',
            'month', 'trend', 'growth', 'performance', 'breakdown'
        ]
        
        # Time indicators
        time_keywords = [
            '2020', '2021', '2022', '2023', '2024', '2025',
            'quarter', 'q1', 'q2', 'q3', 'q4', 'monthly', 'yearly'
        ]
        
        # Metric keywords
        metric_keywords = [
            'margin', 'profit', 'revenue', 'income', 'cost', 'expense',
            'ratio', 'percentage', 'rate', 'amount', 'value'
        ]
        
        has_data_keywords = any(keyword in question_lower for keyword in data_keywords)
        has_time_keywords = any(keyword in question_lower for keyword in time_keywords)
        has_metric_keywords = any(keyword in question_lower for keyword in metric_keywords)
        
        # It's a data question if it has data keywords OR metric keywords OR time keywords
        return has_data_keywords or has_metric_keywords or has_time_keywords
               
    async def force_sql_generation_simple(self, question: str, tables_info: List[Dict], 
                                     conversation_history: List[Dict], session_id: str, 
                                     enable_ai_insights: bool) -> Dict[str, Any]:
        """Simple fallback SQL generation when LLM doesn't generate SQL"""
        print(f"🔍 DEBUG: force_sql_generation_simple called")
        print(f"🔍 DEBUG: Question: {question}")
        print(f"🔍 DEBUG: Conversation history items: {len(conversation_history)}")
        
        context_section = ""
        if conversation_history and len(conversation_history) > 0:
            context_section = "\n\nCONVERSATION CONTEXT:\n"
            for msg in conversation_history[-4:]:  # Last 2 Q&A pairs
                role = "User" if msg["role"] == "user" else "Assistant"
                context_section += f"{role}: {msg['content']}\n\n"
            
            context_section += "CONTEXT INSTRUCTIONS:\n"
            context_section += "- This question may refer to previous results\n"
            context_section += "- If the question uses words like 'this', 'it', 'them', preserve filters from previous queries\n"
            context_section += "- For contextual questions, enhance the previous SQL with additional analysis\n\n"
        
        simple_prompt = f"""
    GENERATE SQL FOR DATA QUESTION:

    Question: "{question}"

    Available tables: {[t['table'] for t in tables_info[:3]]}

    Schema sample: {json.dumps(tables_info[0] if tables_info else {}, indent=2, default=Utils.safe_json_serialize)}

    {context_section}

CRITICAL REQUIREMENTS:
1. Use ONLY tables from the provided schema
2. MUST respond with SQL_QUERY: and ANALYSIS: format
3. Start SQL with SELECT
4. If this question refers to previous context, preserve relevant filters
5. For improvement questions, add analytical elements (LAG, LEAD, growth calculations)

    You MUST respond with this format:

    SQL_QUERY:
    [Your SQL query here - start with SELECT]

    ANALYSIS:
    [Brief explanation]

    Generate SQL that answers: "{question}"
    """
        
        try:
            print(f"🔍 DEBUG: Sending fallback prompt to LLM")
            fallback_response = await self.ai_services.ask_intelligent_llm_async(simple_prompt)
            print(f"🔍 DEBUG: Fallback LLM response received: {len(fallback_response)} chars")
            
            if "SQL_QUERY:" in fallback_response:
                parts = fallback_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1)
                fallback_sql = Utils.clean_generated_sql(parts[0].strip())
                analysis = parts[1].strip() if len(parts) > 1 else "Fallback SQL generated"
                
                print(f"🔍 DEBUG: Extracted SQL: {fallback_sql[:100]}...")
                
                if fallback_sql and fallback_sql.upper().startswith("SELECT"):
                    print(f"🔍 DEBUG: Executing fallback SQL")
                    results = await self.execute_sql_query(fallback_sql)
                    
                    return {
                        "question": question,
                        "generated_sql": fallback_sql,
                        "analysis": analysis,
                        "result_count": len(results),
                        "sample_data": results[:5] if results else [],
                        "timestamp": datetime.now().isoformat(),
                        "session_id": session_id,
                        "ai_insights_enabled": enable_ai_insights,
                        "fallback_used": True,
                        "conversation_context_used": len(conversation_history) > 0
                    }
                else:
                    print(f"🔍 DEBUG: Invalid SQL generated: {fallback_sql}")
            else:
                print(f"🔍 DEBUG: No SQL_QUERY marker found in response")
        
        except Exception as e:
            logger.error("Fallback SQL generation failed", error=str(e))
        
        # Final fallback
        return self.create_error_response("Could not generate SQL for your question.",
                                    f"I'm having trouble creating a SQL query for: '{question}'",
                                    "Try rephrasing with more specific terms like 'Show me gross margin by year from Financial table'",
                                    session_id, [], enable_ai_insights)
    
    async def intelligent_analyze_with_context(self, question: str, session_id: str = None, 
                                         enable_ai_insights: bool = False, 
                                         conversation_history: List[Dict] = None) -> Dict[str, Any]:
        """Simple approach using natural LLM context handling"""
        
        start_total = time.time()
        actual_session_id = session_id if session_id else "default-session-1234567890"
        
        try:
            logger.info("Starting context analysis", 
                    question=question, 
                    session_id=actual_session_id,
                    has_provided_history=bool(conversation_history))
            
            # Handle casual greetings
            if self.is_casual_greeting(question):
                return await self.handle_casual_greeting(question, actual_session_id, conversation_history or [], enable_ai_insights)
            
            # ✅ Use provided conversation history or get fresh if None
            if conversation_history is None:
                conversation_history = await self.get_simple_conversation_history(actual_session_id)
                logger.info("Retrieved fresh conversation history", 
                        session_id=actual_session_id,
                        history_items=len(conversation_history))
            else:
                logger.info("Using provided conversation history", 
                        session_id=actual_session_id,
                        history_items=len(conversation_history))
            
            # Get tables info
            tables_info = await self.schema_manager.get_cached_tables_info()
            if not tables_info:
                return self.create_error_response("No accessible tables found.", 
                                                "The system couldn't find any tables in your database.",
                                                "Check database connection and permissions.",
                                                actual_session_id, conversation_history or [], enable_ai_insights)
                    
            is_data_question = self.is_data_question(question)
            is_contextual = self.is_contextual_question(question)
            
            print(f"🔍 ANALYSIS: is_data={is_data_question}, is_contextual={is_contextual}")
            
            if is_contextual and conversation_history:
    # If contextual and we have conversation history, check for previous data
                for msg in conversation_history:
                    if msg.get('role') == 'assistant' and 'SQL Query:' in msg.get('content', ''):
                        print(f"🔍 CONTEXTUAL DATA: Found previous SQL in history - forcing data analysis")
                        is_data_question = True
                        break
        
        
            if not is_data_question:
                # Handle as conversational
                return await self.handle_conversational_question(question, actual_session_id, conversation_history or [], enable_ai_insights)
            
            # ✅ Build prompt with conversation context
            enhanced_prompt = await self.build_prompt_with_conversation(question, tables_info, conversation_history)
            
            logger.info("Sending enhanced prompt to LLM", 
                prompt_length=len(enhanced_prompt),
                has_conversation_history=len(conversation_history) > 0,
                question=question)
                    
            try:
                llm_response = await self.ai_services.ask_intelligent_llm_async(enhanced_prompt)
                
                logger.info("LLM response received", 
                        response_length=len(llm_response),
                        has_sql_query_marker="SQL_QUERY:" in llm_response,
                        has_analysis_marker="ANALYSIS:" in llm_response,
                        response_preview=llm_response[:200])
                
                # ✅ ENHANCED: Multiple ways to extract SQL
                generated_sql = ""
                analysis = ""
                
                # Method 1: Standard format
                if "SQL_QUERY:" in llm_response and "ANALYSIS:" in llm_response:
                    parts = llm_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1)
                    generated_sql = Utils.clean_generated_sql(parts[0].strip())
                    analysis = parts[1].strip()
                    logger.info("SQL extracted using standard format")
                
                # Method 2: Look for SQL_QUERY: without ANALYSIS:
                elif "SQL_QUERY:" in llm_response:
                    parts = llm_response.split("SQL_QUERY:", 1)
                    generated_sql = Utils.clean_generated_sql(parts[1].strip())
                    analysis = "SQL query generated"
                    logger.info("SQL extracted using SQL_QUERY marker only")
                
                # Method 3: Look for SELECT statements
                elif "SELECT" in llm_response.upper():
                    lines = llm_response.split('\n')
                    sql_lines = []
                    in_sql_block = False
                    
                    for line in lines:
                        line_upper = line.strip().upper()
                        if line_upper.startswith('SELECT'):
                            in_sql_block = True
                            sql_lines = [line]
                        elif in_sql_block:
                            if line.strip() and not line.strip().startswith('--') and not line.strip().startswith('/*'):
                                # Check if this looks like SQL
                                if any(keyword in line_upper for keyword in ['FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'JOIN', 'AND', 'OR']):
                                    sql_lines.append(line)
                                elif line.strip().endswith(';'):
                                    sql_lines.append(line)
                                    break
                                elif not line.strip():
                                    break
                            elif not line.strip():
                                break
                    
                    if sql_lines:
                        generated_sql = Utils.clean_generated_sql(' '.join(sql_lines))
                        # Find analysis after SQL
                        sql_end_idx = llm_response.find(sql_lines[-1]) + len(sql_lines[-1])
                        remaining_text = llm_response[sql_end_idx:].strip()
                        analysis = remaining_text[:500] if remaining_text else "SQL extracted from response"
                        logger.info("SQL extracted by finding SELECT statements")
                
                # ✅ FALLBACK: If still no SQL for a data question, force generate
                if not generated_sql and is_data_question:
                    logger.warning("No SQL found in LLM response for data question, forcing generation")
                    return await self.force_sql_generation_simple(question, tables_info, conversation_history, actual_session_id, enable_ai_insights)
                
                # Execute SQL if we have it
                if generated_sql and generated_sql.upper().startswith("SELECT"):
                    logger.info("Executing generated SQL", sql_length=len(generated_sql))
                    
                    try:
                        results = await self.execute_sql_query(generated_sql)
                        logger.info("SQL execution successful", result_count=len(results))
                    except Exception as sql_error:
                        logger.error("SQL execution failed", error=str(sql_error), sql=generated_sql)
                        return self.create_error_response(f"SQL execution error: {str(sql_error)}", 
                                                    "The generated SQL query failed to execute.",
                                                    "There may be an issue with the query syntax or database schema.",
                                                    actual_session_id, [], enable_ai_insights)
                    
                    # Build response
                    response = {
                        "question": question,
                        "generated_sql": generated_sql,
                        "analysis": analysis,
                        "result_count": len(results),
                        "sample_data": results[:5] if results else [],
                        "timestamp": datetime.now().isoformat(),
                        "session_id": actual_session_id,
                        "ai_insights_enabled": enable_ai_insights,
                        "conversation_context_used": len(conversation_history) > 0
                    }
                    
                    # Add visualization if appropriate
                    await self.viz_manager.add_visualization_to_response(question, generated_sql, results, response)
                    
                    # Enhanced analysis
                    if results and enable_ai_insights:
                        await self.add_enhanced_analysis(question, generated_sql, results, {}, response, enable_ai_insights)
                    
                    logger.info("Simple analysis completed", 
                            duration=time.time() - start_total,
                            has_context=len(conversation_history) > 0,
                            result_count=len(results))
                    
                    return response
                else:
                    # No valid SQL generated for data question
                    logger.warning("No valid SQL generated for data question")
                    return {
                        "question": question,
                        "response_type": "error",
                        "analysis": f"I couldn't generate SQL for your question: '{question}'. This appears to be a data question but I wasn't able to create a valid query.",
                        "suggestion": "Try rephrasing your question more specifically, such as 'Show me gross margin by year' or 'Calculate gross margin for 2024 and 2025'",
                        "timestamp": datetime.now().isoformat(),
                        "session_id": actual_session_id,
                        "ai_insights_enabled": enable_ai_insights,
                        "debug_info": {
                            "llm_response_preview": llm_response[:300],
                            "has_sql_marker": "SQL_QUERY:" in llm_response,
                            "has_select": "SELECT" in llm_response.upper()
                        }
                    }
            
            except Exception as e:
                logger.error("LLM processing failed", error=str(e))
                return self.create_error_response(f"LLM error: {str(e)}", 
                                            "Failed to process your question with the AI model.",
                                            "Try rephrasing your question more clearly.",
                                            actual_session_id, [], enable_ai_insights)
                
        except Exception as e:
            logger.error("Simple analysis failed", error=str(e))
            return self.create_error_response(f"Analysis error: {str(e)}",
                                        "I encountered an error while analyzing your question.",
                                        "Try rephrasing your question with more specific details.",
                                        actual_session_id, [], enable_ai_insights)
    
    async def get_simple_conversation_history(self, session_id: str, limit: int = 3) -> List[Dict]:
        """Get conversation history - FIXED KustoResultRow access"""
        
        print(f"🔍 DEBUG: Getting history for session: '{session_id}'")
        
        # Clean session ID (same as your storage method)
        clean_session_id = str(session_id).strip()
        if clean_session_id.startswith('"') and clean_session_id.endswith('"'):
            clean_session_id = clean_session_id[1:-1]
        if clean_session_id.startswith("'") and clean_session_id.endswith("'"):
            clean_session_id = clean_session_id[1:-1]
        clean_session_id = clean_session_id.replace('"', '').replace("'", "")
        
        print(f"🔍 DEBUG: Cleaned session ID: '{clean_session_id}'")
        
        for attempt in range(3):
            conversation = []
            try:
                print(f"🔍 DEBUG: History retrieval attempt {attempt + 1}")
                
                # ✅ Use 'has' operator and try to decode base64
                history_query = f"""
                ChatHistory_CFO
                | where SessionID has "{clean_session_id}"
                | where Question != 'tables_info' and Question != 'schema_info'
                | where Question != ''
                | order by Timestamp desc
                | take {limit * 2}
                | order by Timestamp asc
                | extend 
                    Decoded_Response = case(
                        Response startswith "eyJ" or Response startswith "ew", base64_decode_tostring(Response),  // Looks like base64
                        Response  // Use as-is for old data
                    )
                | project Question, Decoded_Response, Timestamp
                """
                
                result = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self.db_manager.kusto_client.execute(self.db_manager.kusto_database, history_query)
                )
                
                raw_results = result.primary_results[0] if result.primary_results else []
                print(f"🔍 DEBUG: KQL returned {len(raw_results)} raw results on attempt {attempt + 1}")
                
                if len(raw_results) > 0:
                    # Process the results
                    for i, row in enumerate(raw_results):
                        try:
                            print(f"🔍 DEBUG: Processing row {i+1}: {row['Question'][:50]}...")
                            
                            # ✅ FIXED: Access KustoResultRow by column name directly
                            question = row["Question"]
                            decoded_response = row["Decoded_Response"]
                            
                            if not decoded_response:
                                print(f"❌ DEBUG: No response data for row {i+1}")
                                continue
                            
                            try:
                                response_data = json.loads(decoded_response)
                                print(f"✅ DEBUG: JSON parsing successful for row {i+1}")
                            except json.JSONDecodeError as e:
                                print(f"❌ DEBUG: JSON decode error for row {i+1}: {e}")
                                print(f"❌ DEBUG: Raw response preview: {decoded_response[:100]}...")
                                continue
                            
                            # Add user message
                            conversation.append({
                                "role": "user",
                                "content": question
                            })
                            
                            # Add assistant message with key info
                            generated_sql = response_data.get('generated_sql', '')
                            result_count = response_data.get('result_count', 0)
                            analysis = response_data.get('analysis', '')
                            
                            assistant_content = f"I analyzed your question and found {result_count} records.\n\n"
                            if generated_sql:
                                assistant_content += f"SQL Query: {generated_sql}\n\n"
                            assistant_content += f"Analysis: {analysis}"
                            
                            conversation.append({
                                "role": "assistant", 
                                "content": assistant_content
                            })
                            
                            print(f"🔍 DEBUG: Added Q&A pair {i+1}")
                            
                        except KeyError as e:
                            print(f"❌ DEBUG: Column access error for row {i+1}: {e}")
                            continue
                        except Exception as e:
                            print(f"❌ DEBUG: Error processing row {i+1}: {e}")
                            continue
                            
                    print(f"🔍 DEBUG: Final conversation length: {len(conversation)}")
                    return conversation  # Return immediately if we got results
                    
                elif attempt < 2:  # Don't wait after the last attempt
                    print(f"🔍 DEBUG: No results on attempt {attempt + 1}, waiting 3 seconds...")
                    await asyncio.sleep(3)
                    
            except Exception as e:
                print(f"❌ DEBUG: Failed to get conversation history on attempt {attempt + 1}: {e}")
                if attempt < 2:
                    await asyncio.sleep(3)
        
        print(f"🔍 DEBUG: All attempts failed, returning empty conversation")
        return conversation
   
    def is_casual_greeting(self, question: str) -> bool:
        """Check if question is a casual greeting"""
        vague_questions = ["hi", "hello", "hey", "greetings"]
        return question.lower().strip() in vague_questions
    
    async def handle_casual_greeting(self, question: str, session_id: str, conversation_history: List[Dict], enable_ai_insights: bool) -> Dict[str, Any]:
        """Handle casual greetings"""
        start_convo = time.time()
        conversational_prompt = f"""The user said: \"{question}\"

This is a casual greeting. Provide a friendly response that:
1. Acknowledges their greeting
2. Explains what this enhanced analytics tool can do
3. Mentions AI-powered insights and email capabilities if available
4. Suggests example questions
5. Invites a specific question"""
        conversational_response = await self.ai_services.ask_intelligent_llm_async(conversational_prompt)
        logger.info("Conversational LLM call", duration=time.time() - start_convo)
        return {
            "question": question,
            "response_type": "conversational",
            "analysis": conversational_response,
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "conversation_history": conversation_history,
            "ai_insights_enabled": enable_ai_insights,
            "ai_insights": None
        }
    
    def create_error_response(self, error: str, analysis: str, suggestion: str, session_id: str, conversation_history: List[Dict], enable_ai_insights: bool) -> Dict[str, Any]:
        """Create standardized error response"""
        return {
            "question": "",
            "error": error,
            "analysis": analysis,
            "suggestion": suggestion,
            "session_id": session_id,
            "conversation_history": conversation_history,
            "ai_insights_enabled": enable_ai_insights,
            "ai_insights": None
        }
    
    async def generate_sql_and_analysis(self, question: str, tables_info: List[Dict], conversation_history: List[Dict] = None) -> tuple:
        """✅ KEEP - Simplified version"""
        
        print(f"🔍 DEBUG: generate_sql_and_analysis called")
        print(f"🔍 DEBUG: Has conversation history: {bool(conversation_history and len(conversation_history) > 0)}")
        
        potential_sql = self.get_predefined_query(question)
        analysis = "Using predefined query for common question type"
        
        if not potential_sql:
            start_llm = time.time()
            # ✅ SIMPLIFIED: Use basic prompt without complex context
            base_prompt = await self.prompt_manager.build_chatgpt_system_prompt(question, tables_info, conversation_history or [])  # Empty conversation history
            logger.info("Sending prompt to LLM", prompt_length=len(base_prompt),
                        has_conversation_context=bool(conversation_history and len(conversation_history) > 0))
            llm_response = await self.ai_services.ask_intelligent_llm_async(base_prompt)
            logger.info("LLM call", duration=time.time() - start_llm)
            
            if "SQL_QUERY:" in llm_response and "ANALYSIS:" in llm_response:
                parts = llm_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1)
                potential_sql = Utils.clean_generated_sql(parts[0].strip())
                analysis = parts[1].strip() if len(parts) > 1 else "Analysis not found"
            else:
                parts = llm_response.split('\n\n', 1)
                potential_sql = Utils.clean_generated_sql(parts[0]) if len(parts) >= 1 else ""
                analysis = parts[1] if len(parts) > 1 else llm_response

        # Keep temporal query handling
        if potential_sql and any(keyword in question.lower() for keyword in ['trend', 'predict', 'likely', 'next month', 'based on past', 'future']):
            potential_sql, analysis = await self.improve_temporal_query(question, potential_sql, analysis, tables_info)
        
        # Validate and sanitize SQL
        if potential_sql:
            potential_sql, analysis = await self.validate_and_sanitize_sql(potential_sql, analysis, tables_info)
        
        return potential_sql, analysis
    
    def get_predefined_query(self, question: str) -> Optional[str]:
        """Get predefined queries for common questions"""
        return None
    
    async def improve_temporal_query(self, question: str, potential_sql: str, analysis: str, tables_info: List[Dict]) -> tuple:
        """Improve temporal queries with time filtering"""
        if 'DATEADD' not in potential_sql.upper():
            logger.warning("Temporal query missing time filtering", question=question)
            start_temporal_fix = time.time()
            temporal_prompt = f"""The previous SQL query for temporal analysis is missing time filtering:

Original Question: "{question}"
Generated SQL: {potential_sql}
Available Schema: {json.dumps(tables_info[:3], indent=2, default=Utils.safe_json_serialize)}

For predictive analysis:
1. Filter to recent periods (last 6 months): WHERE [date_column] >= DATEADD(MONTH, -6, GETDATE())
2. Use a date column from the schema (e.g., DetectionTime, IncidentDate, or similar)
3. Compare time periods using GROUP BY DATEPART(MONTH, [date_column])
4. Include devices with recent activity
5. Use JOINs based on foreign keys if needed
6. Ensure the query uses only SELECT, FROM, WHERE, JOIN, INNER JOIN, LEFT JOIN, RIGHT JOIN, GROUP BY, ORDER BY, HAVING, AND, OR, ON

Generate an improved SQL query.

Format:
SQL_QUERY:
[Improved SQL]

ANALYSIS:
[Explanation]"""
            try:
                temporal_response = await self.ai_services.ask_intelligent_llm_async(temporal_prompt)
                if "SQL_QUERY:" in temporal_response:
                    temporal_parts = temporal_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1)
                    improved_sql = Utils.clean_generated_sql(temporal_parts[0].strip())
                    if improved_sql and improved_sql.upper().startswith("SELECT") and "DATEADD" in improved_sql.upper():
                        potential_sql = improved_sql
                        analysis = temporal_parts[1].strip() if len(temporal_parts) > 1 else analysis + " (Query improved)"
                logger.info("Temporal query improvement", duration=time.time() - start_temporal_fix)
            except Exception as e:
                logger.warning("Temporal query improvement failed", error=str(e))
        
        return potential_sql, analysis
    
    async def validate_and_sanitize_sql(self, potential_sql: str, analysis: str, tables_info: List[Dict]) -> tuple:
        """Validate and sanitize SQL query"""
        try:
            potential_sql = Utils.sanitize_sql(potential_sql)
            table_pattern = r'\[([^\]]+)\]\.\[([^\]]+)\]'
            used_tables = re.findall(table_pattern, potential_sql)
            used_table_names = [f"[{schema}].[{table}]" for schema, table in used_tables]
            available_tables = [table_info['table'] for table_info in tables_info]
            invalid_tables = [t for t in used_table_names if not any(t.lower() == at.lower() for at in available_tables)]
            if invalid_tables:
                logger.error("SQL uses non-existent tables", invalid_tables=invalid_tables, available_tables=available_tables)
                raise ValueError(f"Invalid tables: {invalid_tables}")
        except ValueError as e:
            logger.error("SQL validation failed", error=str(e))
            raise
        
        return potential_sql, analysis
    
    async def handle_no_sql_generated(self, question: str, tables_info: List[Dict], conversation_history: List[Dict], session_id: str, enable_ai_insights: bool) -> Dict[str, Any]:
        """Handle cases where no valid SQL was generated - FIXED SIGNATURE"""
        
        logger.warning("No SQL generated, attempting fallback", question=question)
        
        # Try a more direct approach
        if any(keyword in question.lower() for keyword in ["show", "revenue", "brown", "client", "2024", "2025"]):
            
            # Force SQL generation with explicit instructions
            force_sql_prompt = f"""
    You are a SQL expert. Generate SQL for this question: "{question}"

    CRITICAL REQUIREMENTS:
    1. Use ONLY tables from the provided schema
    2. MUST respond with SQL_QUERY: and ANALYSIS: format
    3. For revenue questions, use Revenue column
    4. For client questions, use Client column  
    5. For year questions, use DATEPART(YEAR, [Date])

    Available tables:
    {json.dumps([t['table'] for t in tables_info[:3]], indent=2)}

    Schema details:
    {json.dumps(tables_info[:2], indent=2, default=Utils.safe_json_serialize)}

    EXAMPLE for Brown Ltd revenue:
    SQL_QUERY:
    SELECT [Client], DATEPART(YEAR, [Date]) AS [Year], SUM([Revenue]) AS [Total_Revenue]
    FROM [dbo].[Financial] 
    WHERE [Client] = 'Brown Ltd' 
    AND DATEPART(YEAR, [Date]) IN (2024, 2025)
    GROUP BY [Client], DATEPART(YEAR, [Date])
    ORDER BY [Year]

    ANALYSIS:
    Shows revenue totals for Brown Ltd across 2024 and 2025.

    Now generate SQL for: "{question}"
    """
            
            try:
                forced_response = await self.ai_services.ask_intelligent_llm_async(force_sql_prompt)
                
                if "SQL_QUERY:" in forced_response:
                    forced_parts = forced_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1)
                    forced_sql = Utils.clean_generated_sql(forced_parts[0].strip())
                    
                    if forced_sql and forced_sql.upper().startswith("SELECT") and "FROM" in forced_sql.upper():
                        logger.info("Fallback SQL generation successful", sql_length=len(forced_sql))
                        
                        try:
                            # Execute the fallback SQL
                            results = await self.execute_sql_query(forced_sql)
                            analysis = forced_parts[1].strip() if len(forced_parts) > 1 else "Fallback analysis provided"
                            
                            return {
                                "question": question,
                                "generated_sql": forced_sql,
                                "analysis": analysis,
                                "result_count": len(results),
                                "sample_data": results[:5] if results else [],
                                "timestamp": datetime.now().isoformat(),
                                "session_id": session_id,
                                "ai_insights_enabled": enable_ai_insights,
                                "fallback_used": True
                            }
                        except Exception as e:
                            logger.error("Fallback SQL execution failed", error=str(e))
                
            except Exception as e:
                logger.error("Fallback SQL generation failed", error=str(e))
        
        # Final fallback - conversational response
        return await self.handle_conversational_question(question, session_id, conversation_history, enable_ai_insights)

    
    async def force_sql_generation(self, question: str, tables_info: List[Dict], conversation_history: List[Dict], session_id: str, enable_ai_insights: bool) -> Dict[str, Any]:
        """Force SQL generation for data questions - FIXED SIGNATURE"""
        start_fallback = time.time()
        force_sql_prompt = f"""This is a data analysis question requiring SQL.

    Question: {question}

    Use ONLY these tables:
    {chr(10).join(['- ' + table_info['table'] for table_info in tables_info])}

    Available schema:
    {json.dumps(tables_info[:3], indent=2, default=Utils.safe_json_serialize)}

    Generate a comprehensive SQL query with JOINs.

    Format:
    SQL_QUERY:
    [Complete SQL]

    ANALYSIS:
    [Explanation]"""
        
        try:
            forced_response = await self.ai_services.ask_intelligent_llm_async(force_sql_prompt)
            if "SQL_QUERY:" in forced_response:
                forced_parts = forced_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1)
                forced_sql = Utils.clean_generated_sql(forced_parts[0].strip())
                if forced_sql and forced_sql.upper().startswith("SELECT") and "FROM" in forced_sql.upper():
                    potential_sql = Utils.sanitize_sql(forced_sql)
                    analysis = forced_parts[1].strip() if len(forced_parts) > 1 else "Analysis provided"
                    # Continue with normal processing
                    results = await self.execute_sql_query(potential_sql)
                    return await self.build_successful_response(question, potential_sql, analysis, results, session_id, enable_ai_insights)
                else:
                    raise Exception("Invalid SQL generated")
            else:
                raise Exception("No SQL generated")
            logger.info("Fallback LLM call", duration=time.time() - start_fallback)
        except Exception as e:
            return self.create_error_response("Could not generate SQL for this question.",
                                        "This appears to be a complex data question. There might be an issue with query complexity or data structure.",
                                        f"Try asking about specific aspects using these tables: {', '.join([table_info['table'] for table_info in tables_info[:5]])}",
                                        session_id, conversation_history, enable_ai_insights)

    
    async def handle_conversational_question(self, question: str, session_id: str, conversation_history: List[Dict], enable_ai_insights: bool) -> Dict[str, Any]:
        """Handle conversational questions"""
        start_convo = time.time()
        conversational_prompt = f"""The user asked: "{question}"

This doesn't require database analysis. Provide a conversational response that:
1. Addresses the question
2. Explains relevant concepts
3. Offers data analysis help with AI insights
4. Suggests data exploration"""
        conversational_response = await self.ai_services.ask_intelligent_llm_async(conversational_prompt)
        logger.info("Conversational LLM call", duration=time.time() - start_convo)
        return {
            "question": question,
            "response_type": "conversational",
            "analysis": conversational_response,
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "conversation_history": conversation_history,
            "ai_insights_enabled": enable_ai_insights,
            "ai_insights": None
        }
    
    
    
    async def enhance_contextual_question(self, question: str, conversation_history: List[Dict]) -> str:
        """Enhance vague contextual questions with previous context"""
        
        if not conversation_history:
            return question
            
        # Get previous question and results info
        last_user_question = ""
        last_data_summary = ""
        
        for item in reversed(conversation_history):
            if item.get('role') == 'user':
                last_user_question = item.get('content', '')
                break
        
        for item in reversed(conversation_history):
            if item.get('role') == 'assistant' and item.get('result_count', 0) > 0:
                result_count = item.get('result_count', 0)
                last_data_summary = f"Found {result_count} records"
                break
        
        # Create enhanced question
        enhanced_question = f"""
Based on the previous query: "{last_user_question}" which {last_data_summary}:

User is now asking: "{question}"

This is asking for analysis/explanation of the revenue data patterns. 
Generate SQL to analyze trends, patterns, or explanations for the revenue behavior.
Include year-over-year comparisons, growth rates, or trend analysis.
"""
        
        logger.info("Enhanced contextual question", 
                   original=question,
                   enhanced=enhanced_question[:200])
        
        return enhanced_question
    
    
    
    async def handle_new_analysis(self, question: str, tables_info: List[Dict], conversation_history: List[Dict], session_id: str, enable_ai_insights: bool) -> Dict[str, Any]:
        """Handle new analysis questions - FIXED SIGNATURE"""
        
        start_total = time.time()
        
        logger.info("Starting new analysis", 
                question=question,
                table_count=len(tables_info),
                session_id=session_id,
                has_conversation_history=bool(conversation_history))
        
        # Generate SQL and analysis (ignore conversation_history for now)
        try:
            potential_sql, analysis = await self.generate_sql_and_analysis(question, tables_info, [])
            
            logger.info("SQL generation result", 
                    has_sql=bool(potential_sql),
                    sql_length=len(potential_sql) if potential_sql else 0,
                    sql_starts_with_select=potential_sql.upper().startswith("SELECT") if potential_sql else False)
            
        except Exception as e:
            logger.error("SQL generation failed", error=str(e))
            return self.create_error_response(f"SQL generation error: {str(e)}",
                                        "Failed to generate SQL query for your question.",
                                        "Try rephrasing your question more specifically.",
                                        session_id, conversation_history, enable_ai_insights)
        
        if not potential_sql or not potential_sql.upper().startswith("SELECT"):
            logger.warning("No valid SQL generated", 
                        has_sql=bool(potential_sql),
                        sql_content=potential_sql[:100] if potential_sql else "None")
            return await self.handle_no_sql_generated(question, tables_info, conversation_history, session_id, enable_ai_insights)

        # Execute SQL query
        try:
            results = await self.execute_sql_query(potential_sql)
            logger.info("SQL execution result", result_count=len(results))
        except Exception as e:
            logger.error("SQL execution failed", error=str(e), sql=potential_sql)
            return self.create_error_response(f"SQL execution error: {str(e)}",
                                        "The generated SQL query failed to execute.",
                                        "There may be an issue with the database schema or query syntax.",
                                        session_id, conversation_history, enable_ai_insights)
        
        query_context = Utils.extract_context_from_results(results)
        
        # Build response
        response = {
            "question": question,
            "generated_sql": potential_sql,
            "analysis": analysis,
            "result_count": len(results),
            "sample_data": results[:5] if results else [],
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "ai_insights_enabled": enable_ai_insights
        }

        # Add visualization
        await self.viz_manager.add_visualization_to_response(question, potential_sql, results, response)

        # Enhanced analysis
        if results:
            await self.add_enhanced_analysis(question, potential_sql, results, query_context, response, enable_ai_insights)
        else:
            await self.handle_no_results(question, potential_sql, response)

        logger.info("Total processing time", duration=time.time() - start_total)
        return response
    
    
    
    async def execute_sql_query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query with proper error handling"""
        loop = asyncio.get_event_loop()
        start_query = time.time()
        results = await loop.run_in_executor(None, lambda: self.db_manager.execute_sql_query(sql))
        logger.info("Query execution", duration=time.time() - start_query)
        return results
    
        
    async def build_successful_response(self, question: str, sql: str, analysis: str, results: List[Dict], session_id: str, enable_ai_insights: bool) -> Dict[str, Any]:
        """Build successful response with all data - FIXED"""
        query_context = Utils.extract_context_from_results(results)
        conversation_history = []  # Empty for now since we're using KQL-based context
        
        response = {
            "question": question,
            "generated_sql": sql,
            "analysis": analysis,
            "result_count": len(results),
            "sample_data": results[:5] if results else [],
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "ai_insights_enabled": enable_ai_insights
        }

        # Add visualization
        await self.viz_manager.add_visualization_to_response(question, sql, results, response)

        # Enhanced analysis
        if results:
            await self.add_enhanced_analysis(question, sql, results, query_context, response, enable_ai_insights)
        else:
            await self.handle_no_results(question, sql, response)

        return response
    
    async def add_enhanced_analysis(self, question: str, sql: str, results: List[Dict], context: Dict, response: Dict, enable_ai_insights: bool):
        """Add enhanced analysis to response - FIXED VERSION"""
        start_enhanced = time.time()
        
        print(f"🔍 ENHANCED ANALYSIS DEBUG:")
        print(f"  Question: '{question}'")
        print(f"  Results count: {len(results)}")
        print(f"  AI insights enabled: {enable_ai_insights}")
        print(f"  AI Foundry available: {self.ai_services.ai_foundry_enabled}")
        print(f"  Intelligent agent available: {self.ai_services.intelligent_agent is not None}")
        
        # Standard LLM analysis
        query_feedback = ""
        if any(keyword in question.lower() for keyword in ['trend', 'predict', 'likely', 'next month', 'based on past']):
            query_feedback = f"\n\n**QUERY ANALYSIS NOTE**: {'Good - this query uses time filtering.' if 'DATEADD' in sql.upper() else 'This query lacks sufficient time filtering for predictive analysis.'}"
        
        enhanced_prompt = f"""
User Question: {question}

Query Results: {len(results)} records
Generated SQL: {sql}
{query_feedback}

Sample Data: {json.dumps(results[:10], default=Utils.safe_json_serialize)}

Provide a conversational response that:
1. Summarizes results
2. Explains business context
3. Identifies key patterns
4. Provides actionable recommendations
5. Suggests next steps

Use clear formatting with headers and bullet points. Include specific numbers and percentages from the data.

"""
        
        try:
            print(f"🔍 Generating standard analysis...")
            standard_analysis = await self.ai_services.ask_intelligent_llm_async(enhanced_prompt)
            print(f"✅ Standard analysis generated: {len(standard_analysis)} characters")
            print(f"✅ Preview: {standard_analysis[:200]}...")
        except Exception as e:
            print(f"❌ Standard analysis failed: {e}")
            standard_analysis = f"Analysis generation failed: {str(e)}"
        
        # ✅ FIX 3: Enhanced AI Agent analysis with proper None checks
        if enable_ai_insights and self.ai_services.ai_foundry_enabled and self.ai_services.intelligent_agent is not None:
            try:
                print(f"🤖 Attempting AI Foundry analysis...")
                print(f"🤖 Data size being sent: {len(results)} records")
                print(f"🤖 Question complexity: {len(question)} characters")
                
                start_ai_analysis = time.time()
                ai_insights = await self.ai_services.intelligent_agent.analyze_with_ai(
                    results, question, context
                )
                
                if ai_insights:
                    # Combine standard and AI analysis
                    response["ai_insights"] = ai_insights
                    response["enhanced_analysis"] = f"{standard_analysis}\n\n**🤖 AI-Enhanced Insights:**\n{ai_insights}"
                    logger.info("AI insights added", duration=time.time() - start_ai_analysis)
                    print(f"✅ Enhanced analysis field set successfully")
                else:
                    response["ai_insights"] = "AI insights could not be generated; using standard analysis."
                    response["enhanced_analysis"] = standard_analysis
                    logger.warning("AI insights generation failed - no response from agent")
                    
            except Exception as e:
                logger.error("AI insights generation error", error=str(e))
                response["enhanced_analysis"] = standard_analysis
                response["ai_insights"] = f"AI Foundry insights error: {str(e)}"
        else:
            response["enhanced_analysis"] = standard_analysis
            if not enable_ai_insights:
                response["ai_insights"] = "AI insights disabled by user."
            elif not self.ai_services.ai_foundry_enabled:
                response["ai_insights"] = "AI Foundry not enabled; using standard LLM analysis."
            elif self.ai_services.intelligent_agent is None:
                response["ai_insights"] = "AI agent not available; using standard LLM analysis."
            else:
                response["ai_insights"] = "AI Foundry not available; using standard analysis."
        
        logger.info("Enhanced analysis", duration=time.time() - start_enhanced)

    
    async def handle_no_results(self, question: str, sql: str, response: Dict):
        """Handle case when query returns no results"""
        start_no_data = time.time()
        no_data_prompt = f"""
The query for '{question}' returned no results.

SQL: {sql}

Explain:
1. Why no data was found
2. Business context
3. Alternative analysis approaches
4. Next steps"""
        response["analysis"] = await self.ai_services.ask_intelligent_llm_async(no_data_prompt)
        response["ai_insights"] = None
        logger.warning("No-data query", duration=time.time() - start_no_data)

    async def build_prompt_with_conversation(self, question: str, tables_info: List[Dict], conversation_history: List[Dict]) -> str:
        """Build prompt with natural conversation context"""
        
        base_prompt = self.prompt_manager.load_base_prompt()
        schema_section = self.prompt_manager.format_schema_for_prompt(tables_info)
        
        # Add conversation history if available
        conversation_section = ""
        if conversation_history:
            conversation_section = "\n\n📝 CONVERSATION HISTORY:\n"
            for msg in conversation_history[-6:]:  # Last 3 Q&A pairs
                role = "User" if msg["role"] == "user" else "Assistant"
                conversation_section += f"{role}: {msg['content']}\n\n"
            
            conversation_section += f"📋 CURRENT QUESTION: {question}\n"
            
            # 🆕 DIRECTLY USE EXISTING text_columns FROM tables_info
            is_contextual = self.is_contextual_question(question)
            question_lower = question.lower()
            
            # Check if question mentions any text columns from your schema
            has_business_entities = False
            matching_columns = []
            
            for table_info in tables_info:
                text_columns = table_info.get('text_columns', [])  # 🎯 Reuse existing variable
                for col in text_columns:
                    col_term = col.lower().replace('_', ' ')
                    if col_term in question_lower:
                        has_business_entities = True
                        matching_columns.append(col)
            
            print(f"🔍 CONTEXT CHECK: contextual={is_contextual}, has_entities={has_business_entities}")
            if matching_columns:
                print(f"🔍 MATCHING COLUMNS: {matching_columns}")
            
            if is_contextual or has_business_entities:
                # 🆕 CREATE MAPPING FROM EXISTING text_columns
                if matching_columns:
                    column_mapping = "\n".join([f'- "{col.lower().replace("_", " ")}" → GROUP BY [{col}]' 
                                            for col in matching_columns])
                else:
                    # Show available text columns from first table for reference
                    first_table_text_cols = tables_info[0].get('text_columns', []) if tables_info else []
                    column_mapping = "\n".join([f'- "{col.lower().replace("_", " ")}" → GROUP BY [{col}]' 
                                            for col in first_table_text_cols[:8]])
                
                conversation_section += f"""
    🔗 CONTEXTUAL DRILL-DOWN ANALYSIS INSTRUCTIONS:
    - This is a FOLLOW-UP question referring to previous data analysis
    - Generate SQL that builds upon the previous query results
    - Use the same filters and WHERE conditions from the previous successful query
    - Add the new GROUP BY dimension that the user is asking about

    AVAILABLE TEXT COLUMNS FOR GROUPING:
    {column_mapping}

    ANALYSIS PATTERNS:
    - For "why" questions: Include analytical elements (LAG, LEAD, growth calculations, comparisons)
    - For "which" questions: Add appropriate GROUP BY and ORDER BY to show top contributors
    - For drill-down: Preserve previous filters + add new grouping dimension

    CRITICAL: Even if the question seems conversational, treat it as a data analysis request.
    """
            else:
                conversation_section += """
    🆕 NEW QUESTION: This appears to be a new question, analyze independently.
    """
        
        # Enhanced response format for follow-ups
        is_followup = conversation_history and len(conversation_history) > 0
        is_contextual = self.is_contextual_question(question) if conversation_history else False
        
        # Quick check for any text column mentions
        has_text_col_refs = False
        if tables_info:
            for table_info in tables_info:
                text_columns = table_info.get('text_columns', [])  # 🎯 Reuse again
                if any(col.lower().replace('_', ' ') in question.lower() for col in text_columns):
                    has_text_col_refs = True
                    break
        
        if is_followup and (is_contextual or has_text_col_refs):
            # Get first table's text columns for instruction
            first_table_text_cols = tables_info[0].get('text_columns', []) if tables_info else []
            
            sql_instruction = f"""

    🎯 CONTEXTUAL DRILL-DOWN ANALYSIS: "{question}"

    Available grouping dimensions: {', '.join([f'[{col}]' for col in first_table_text_cols[:5]])}

    MANDATORY RESPONSE FORMAT FOR FOLLOW-UP:
    SQL_QUERY:
    [Enhanced SQL that builds on previous query with new grouping/filtering]

    ANALYSIS:
    **🔍 Drill-Down Analysis:**
    [Explain what specific aspect we're analyzing and why]

    **📊 Key Findings:**
    [Present the data findings in business context]

    **💡 Business Insights:**
    [Explain what these results mean for business decision-making]

    **🎯 Actionable Recommendations:**
    [Specific next steps based on this drill-down analysis]

    Generate enhanced SQL with deeper analysis for: "{question}"
    """
        else:
            sql_instruction = f"""

    🎯 ANALYZE THIS QUESTION: "{question}"

    MANDATORY RESPONSE FORMAT:
    SQL_QUERY:
    [Your SQL query here]

    ANALYSIS:
    [Your analysis here]

    Generate SQL that answers: "{question}"
    """
            
        return f"{base_prompt}\n\n{schema_section}\n{conversation_section}" + sql_instruction
            
    
    # ✅ NEW: Explain results based on actual previous data from KQL
    async def explain_previous_results(self, question: str, previous_question: str, 
                                     previous_data: List[Dict], previous_analysis: str,
                                     session_id: str, enable_ai_insights: bool) -> Dict[str, Any]:
        """Explain previous results using actual data from KQL"""
        
        if not previous_data:
            return await self.handle_conversational_question(question, session_id, [], enable_ai_insights)
        
        # Extract specific insights from actual data
        client_name = "Unknown"
        years_analyzed = set()
        revenue_values = []
        quarters_found = set()
        
        for record in previous_data:
            # Extract client
            if 'Client' in record and record['Client']:
                client_name = record['Client']
            
            # Extract years
            for key, value in record.items():
                if 'year' in key.lower() and value:
                    years_analyzed.add(str(value))
                if 'quarter' in key.lower() and value:
                    quarters_found.add(str(value))
            
            # Extract revenue values
            for key, value in record.items():
                if 'revenue' in key.lower() and isinstance(value, (int, float)):
                    revenue_values.append(value)
        
        # Calculate metrics from actual data
        total_revenue = sum(revenue_values) if revenue_values else 0
        avg_revenue = total_revenue / len(revenue_values) if revenue_values else 0
        max_revenue = max(revenue_values) if revenue_values else 0
        min_revenue = min(revenue_values) if revenue_values else 0
        
        explanation_prompt = f"""
EXPLAIN ACTUAL REVENUE BEHAVIOR USING REAL DATA:

Previous Question: "{previous_question}"
Current Question: "{question}"

ACTUAL DATA ANALYSIS FOR {client_name}:
- Years Analyzed: {', '.join(sorted(years_analyzed))}
- Total Data Points: {len(previous_data)}
- Revenue Records: {len(revenue_values)}
- Quarters: {', '.join(sorted(quarters_found)) if quarters_found else 'Not specified'}

REAL FINANCIAL METRICS:
- Total Revenue: ${total_revenue:,.2f}
- Average Revenue: ${avg_revenue:,.2f}
- Highest Revenue: ${max_revenue:,.2f}
- Lowest Revenue: ${min_revenue:,.2f}
- Revenue Range: ${max_revenue - min_revenue:,.2f}

ACTUAL DATA SAMPLE:
{json.dumps(previous_data[:5], indent=2, default=Utils.safe_json_serialize)}

INSTRUCTIONS:
1. Analyze {client_name}'s ACTUAL revenue behavior
2. Reference the SPECIFIC numbers and patterns from the real data
3. Explain business reasons for the revenue patterns shown
4. Provide actionable insights based on the ACTUAL data
5. Focus on {client_name}'s specific situation

DO NOT make generic statements. Use only the actual data provided above.
Explain what the numbers tell us about {client_name}'s business performance.
"""
        
        try:
            explanation = await self.ai_services.ask_intelligent_llm_async(explanation_prompt)
            
            return {
                "question": question,
                "response_type": "kql_based_explanation",
                "analysis": explanation,
                "kql_context_used": {
                    "previous_question": previous_question,
                    "client_analyzed": client_name,
                    "years_analyzed": list(years_analyzed),
                    "data_points": len(previous_data),
                    "revenue_metrics": {
                        "total": total_revenue,
                        "average": avg_revenue,
                        "max": max_revenue,
                        "min": min_revenue
                    }
                },
                "timestamp": datetime.now().isoformat(),
                "session_id": session_id,
                "ai_insights_enabled": enable_ai_insights
            }
            
        except Exception as e:
            logger.error("KQL-based explanation failed", error=str(e))
            return self.create_error_response(f"Explanation error: {str(e)}",
                                           "I couldn't generate an explanation using the previous data.",
                                           "Try asking a more specific question about the data.",
                                           session_id, [], enable_ai_insights)
# Initialize analytics engine
analytics_engine = AnalyticsEngine(db_manager, schema_manager, kql_storage, ai_services, viz_manager, prompt_manager)

# CONSOLIDATED: FastAPI application setup
app = FastAPI(
    title="Intelligent Microsoft Fabric SQL Analytics",
    description="Processes natural language questions to generate SQL queries, execute them, and provide insights with optional visualizations."
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"], expose_headers=["*"])

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(429, _rate_limit_exceeded_handler)

# MCP server
mcp = FastMCP("Intelligent Fabric Analytics", dependencies=["pyodbc", "fastapi", "python-dotenv", "pandas", "sqlalchemy", "slowapi", "sqlparse", "structlog", "azure-kusto-data"])

# CONSOLIDATED: API Endpoints
@mcp.tool("fabric_intelligent_analyze")
async def intelligent_analyze_tool(question: str, session_id: str = None, enable_ai_insights: bool = False) -> Dict[str, Any]:
    """MCP tool wrapper for intelligent analysis"""
    return await analytics_engine.intelligent_analyze(question, session_id, enable_ai_insights)

@app.post("/api/fabric/intelligent")
@limiter.limit("10/minute")
async def intelligent_analyze_endpoint(
    req: IntelligentRequest, 
    background_tasks: BackgroundTasks, 
    request: Request,
    session: Optional[str] = Query(None, description="Session ID")
):
    """Enhanced endpoint with AI insights and email notification"""
    try:
        session_id = SessionManager.get_session_id_from_request(session)
        # Process the question with enhanced capabilities
        result = await analytics_engine.cached_intelligent_analyze(
            req.question, 
            session_id, 
            req.enable_ai_insights
           
        )
        
        if "error" in result and result.get("response_type") != "conversational":
            raise HTTPException(status_code=400, detail=result["error"])
        
        # Send notification email if requested
        if req.enable_email_notification and req.email_recipients and ai_services.graph_client:
            async def send_notification():
                try:
                    subject = f"Analytics Result: {req.question[:50]}..."
                    
                    body = f"""
                    <h2>Analytics Notification</h2>
                    <p><strong>Question:</strong> {req.question}</p>
                    <p><strong>Results:</strong> {result.get('result_count', 0)} records found</p>
                    
                    {f"<p><strong>Analysis:</strong></p><p>{result.get('analysis', '')[:500]}...</p>" if result.get('analysis') else ""}
                    
                    {f"<p><strong>AI Insights:</strong></p><p>{result.get('ai_insights', '')[:500]}...</p>" if result.get('ai_insights') else ""}
                    
                    <p>For full details, please check the analytics dashboard.</p>
                    
                    <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    """
                    
                    await email_service.send_notification_email(
                        req.email_recipients, subject, body
                    )
                    logger.info("Notification email sent", recipients=req.email_recipients)
                    
                except Exception as e:
                    logger.error("Notification email failed", error=str(e))
            
            background_tasks.add_task(send_notification)
            result["email_notification_sent"] = True
        
        result["session_id"] = session_id
        result["features_enabled"] = {
            "ai_insights": req.enable_ai_insights and ai_services.ai_foundry_enabled,
            "email_notification": req.enable_email_notification and bool(ai_services.graph_client),
            "ai_foundry_available": ai_services.ai_foundry_enabled,
            "graph_api_available": bool(ai_services.graph_client),
            "chat_context": True 
        }
        
        return result
        
    except Exception as e:
        logger.error("Enhanced endpoint error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/intelligent-workflow")
@limiter.limit("5/minute")
async def intelligent_workflow_endpoint(
    req: ReportRequest,
    background_tasks: BackgroundTasks,
    request: Request
):
    """Complete intelligent workflow - analyze, generate report, and optionally send email - FIXED"""
    
    if not REPORT_LIBS_AVAILABLE:
        raise HTTPException(
            status_code=503, 
            detail="Report generation libraries not available. Install with: pip install reportlab xlsxwriter"
        )
    
    try:
        async def run_workflow():
            try:
                logger.info("Starting workflow", query=req.data_query)
                
                # Process the data query
                analysis_result = await analytics_engine.cached_intelligent_analyze(
                    req.data_query, 
                    enable_ai_insights=req.include_ai_analysis
                )
                
                if "error" not in analysis_result:
                    data = analysis_result.get("sample_data", [])
                    
                    # ✅ FIXED: Handle analysis data properly
                    if req.include_ai_analysis and "enhanced_analysis" in analysis_result:
                        analysis_text = analysis_result["enhanced_analysis"]
                    else:
                        analysis_text = analysis_result.get("analysis", "Analysis not available")
                    
                    logger.info("Analysis completed", data_count=len(data), has_analysis=bool(analysis_text))
                    
                    # Generate report with question context for AI intelligence
                    
                    report_data = None
                    filename = None
                    if req.report_format == "pdf":
                        print("📄 Generating PDF report...")
                        report_data = await report_generator.generate_pdf_report(
                            data, 
                            analysis_text,
                            f"{req.report_type.title()} Report",
                            req.data_query
                        )
                        
                        if report_data:
                            filename = f"analytics_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            print(f"✅ PDF generated: {len(report_data)} bytes, filename: {filename}")
                            
                            # DEBUG: Check SharePoint configuration
                            sp_config = SharePointConfig.get_config()
                            print(f"\n🔍 SHAREPOINT CONFIG DEBUG:")
                            print(f"   Site ID: {sp_config.get('site_id', 'NOT_SET')}")
                            print(f"   Library ID: {sp_config.get('document_library_id', 'NOT_SET')}")
                            print(f"   Tenant ID: {sp_config.get('tenant_id', 'NOT_SET')}")
                            print(f"   Client ID: {sp_config.get('client_id', 'NOT_SET')}")
                            print(f"   Has Secret: {bool(sp_config.get('client_secret'))}")
                            
                            # FIXED: Check SharePoint configuration properly
                            site_id = sp_config.get('site_id')
                            library_id = sp_config.get('document_library_id')
                            
                            print(f"🔍 Configuration check:")
                            print(f"   Site ID set: {bool(site_id and site_id.strip())}")
                            print(f"   Library ID set: {bool(library_id and library_id.strip())}")
                            
                            # Try SharePoint upload if both IDs are configured
                            if site_id and site_id.strip() and library_id and library_id.strip():
                                print("🌐 SharePoint is configured, attempting upload...")
                                
                                # Try SharePoint upload
                                print(f"📤 Uploading to SharePoint: {filename}.pdf")
                                try:
                                    success = sharepoint_uploader.upload_pdf_to_sharepoint(report_data, filename)
                                    print(f"📋 SharePoint upload result: {'✅ Success' if success else '❌ Failed'}")
                                    
                                    if success:
                                        print(f"🎉 Report uploaded to SharePoint: {filename}.pdf")
                                        logger.info(f"Report uploaded to SharePoint: {filename}.pdf")
                                        
                                        # Don't save locally if SharePoint upload succeeded
                                        uploaded_to_sharepoint = True
                                    else:
                                        print("❌ SharePoint upload failed, saving locally as fallback")
                                        logger.error("Failed to upload to SharePoint, saving locally as fallback")
                                        uploaded_to_sharepoint = False
                                        
                                except Exception as upload_error:
                                    print(f"💥 SharePoint upload exception: {upload_error}")
                                    logger.error("SharePoint upload exception", error=str(upload_error))
                                    uploaded_to_sharepoint = False
                                
                                # Only save locally if SharePoint upload failed
                                if not uploaded_to_sharepoint:
                                    local_filename = f"{filename}.pdf"
                                    with open(local_filename, "wb") as f:
                                        f.write(report_data)
                                    print(f"💾 Report saved locally as fallback: {local_filename}")
                                    logger.info(f"Report saved locally as fallback: {local_filename}")
                                    
                            else:
                                print("⚠️  SharePoint not fully configured, saving locally")
                                logger.warning("SharePoint not configured, saving locally")
                                
                                missing_configs = []
                                if not (site_id and site_id.strip()):
                                    missing_configs.append("SHAREPOINT_SITE_ID")
                                if not (library_id and library_id.strip()):
                                    missing_configs.append("SHAREPOINT_DOCUMENT_LIBRARY_ID")
                                
                                print(f"❌ Missing or empty: {', '.join(missing_configs)}")
                                
                                # Save locally if SharePoint not configured
                                local_filename = f"{filename}.pdf"
                                with open(local_filename, "wb") as f:
                                    f.write(report_data)
                                print(f"💾 Report saved locally: {local_filename}")
                                logger.info(f"Report saved locally: {local_filename}")
                        else:
                            print("❌ PDF generation returned None")
                            logger.error("PDF generation returned None")
                    
                    # Send email if Graph API is available and recipients are provided
                    if report_data and req.email_recipients and ai_services.graph_client:
                        try:
                            await email_service.send_email_with_report(
                                recipients=req.email_recipients,
                                subject=req.subject_hint or f"Analytics Report - {datetime.now().strftime('%Y-%m-%d')}",
                                body=f"<h2>Analytics Report</h2><p>Please find attached the requested {req.report_type} analytics report.</p><p>Query: {req.data_query}</p>",
                                report_data=report_data,
                                report_filename=filename,
                                report_type="pdf"
                            )
                            logger.info("Report emailed successfully", recipients=req.email_recipients)
                        except Exception as email_error:
                            logger.error("Email sending failed", error=str(email_error))
                    elif req.email_recipients and not ai_services.graph_client:
                        logger.info("Email not sent: Graph API credentials not configured")
                else:
                    logger.error("Analysis failed", error=analysis_result.get("error"))
                
                logger.info("Workflow completed successfully")
                
            except Exception as e:
                logger.error("Background workflow failed", error=str(e), traceback=traceback.format_exc())
        
        background_tasks.add_task(run_workflow)
        
        return {
            "status": "workflow_started",
            "workflow_id": str(uuid.uuid4()),
            "message": "Intelligent workflow initiated. Report will be generated and emailed if configured.",
            "timestamp": datetime.now().isoformat(),
            "expected_filename": f"analytics_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        }
        
    except Exception as e:
        logger.error("Workflow endpoint error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

# CONSOLIDATED: Chat management endpoints
@app.get("/api/chat/messages")
async def get_chat_messages(
    session: Optional[str] = Query(None, description="Session ID"),
    limit: Optional[int] = Query(10, description="Number of recent conversations to return")
):
    """Get chat messages for specified session with session validation"""
    
    session_id = SessionManager.get_session_id_from_request(session)
    
    try:
        # First check if this session exists
        check_query = f"""
        ChatHistory_CFO
        | where SessionID == '{session_id}'
        | count
        """
        
        check_result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: db_manager.kusto_client.execute(db_manager.kusto_database, check_query)
        )
        
        session_exists = check_result.primary_results[0][0]["Count"] > 0 if check_result.primary_results[0] else False
        
        history_query = f"""
        ChatHistory_CFO
        | where SessionID == '{session_id}'
        | where Question != 'tables_info' and Question != 'schema_info'
        | order by Timestamp desc
        | take {limit * 2}
        | order by Timestamp asc
        | project Timestamp, Question, Response
        """
         
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: db_manager.kusto_client.execute(db_manager.kusto_database, history_query)
        )
        
        messages = []
        for row in result.primary_results[0]:
            try:
                response_data = json.loads(row["Response"])
                
                messages.append({
                    "id": f"user_{len(messages)}",
                    "type": "user",
                    "content": row["Question"],
                    "timestamp": row["Timestamp"]
                })
                
                messages.append({
                    "id": f"assistant_{len(messages)}",
                    "type": "assistant", 
                    "content": response_data.get("analysis", "No analysis available"),
                    "sql": response_data.get("generated_sql"),
                    "result_count": response_data.get("result_count", 0),
                    "sample_data": response_data.get("sample_data", []),
                    "visualization": response_data.get("visualization"),
                    "timestamp": row["Timestamp"]
                })
                
            except json.JSONDecodeError:
                continue
            
        return {
            "status": "success",
            "session_id": session_id,
            "session_exists": session_exists,
            "messages": messages,
            "message_count": len(messages),
            "total_pairs": len(messages) // 2
        }
        
    except Exception as e:
        logger.error("Failed to retrieve chat messages", session_id=session_id, error=str(e))
        return {
            "status": "error",
            "session_id": session_id,
            "session_exists": False,
            "messages": [],
            "message_count": 0,
            "total_pairs": 0,
            "error": str(e)
        }

@app.post("/api/chat/clear")
async def clear_chat_and_start_new_session(
    session: Optional[str] = Query(None, description="Current Session ID"),
    create_new: Optional[bool] = Query(True, description="Create new session after clear")
):
    """Clear current session and optionally start a new one"""
    
    current_session_id = SessionManager.get_session_id_from_request(session)
    
    try:
        logger.info("Chat clear requested", session_id=current_session_id, create_new=create_new)
        
        if create_new:
            # Generate a new session ID
            new_session_id = SessionManager.generate_new_session_id()
            
            return {
                "status": "success",
                "message": "Chat cleared and new session started",
                "old_session_id": current_session_id,
                "new_session_id": new_session_id,
                "timestamp": datetime.now().isoformat(),
                "action": "new_session_created"
            }
        else:
            return {
                "status": "success", 
                "message": "Chat cleared successfully",
                "session_id": current_session_id,
                "timestamp": datetime.now().isoformat(),
                "action": "session_cleared"
            }
        
    except Exception as e:
        logger.error("Chat clear request failed", session_id=current_session_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to clear chat")

@app.get("/api/chat/sessions")
async def get_chat_sessions(
    date: Optional[str] = Query(None, description="Date in YYYYMMDD format, or 'all' for all sessions"),
    limit: Optional[int] = Query(50, description="Maximum number of sessions to return")
):
    """Get chat sessions with dynamic naming and full history support"""
    
    try:
        print(f"🔍 DEBUG: Sessions endpoint called with date='{date}', limit={limit}")
        
        if not date:
            date = "all"
            
        if date == "all":
            sessions_query = f"""
            ChatHistory_CFO
            | where SessionID startswith 'powerbi_'
            | where Question != 'tables_info' and Question != 'schema_info'
            | summarize 
                MessageCount = count(),
                FirstMessage = min(Timestamp),
                LastMessage = max(Timestamp),
                FirstQuestion = take_any(Question),
                LastQuestion = arg_max(Timestamp, Question)
            by SessionID
            | order by LastMessage desc
            | take {limit}
            """
        else:
            sessions_query = f"""
            ChatHistory_CFO
            | where SessionID contains 'powerbi_{date}'
            | where Question != 'tables_info' and Question != 'schema_info'
            | summarize 
                MessageCount = count(),
                FirstMessage = min(Timestamp),
                LastMessage = max(Timestamp),
                FirstQuestion = take_any(Question),
                LastQuestion = arg_max(Timestamp, Question)
            by SessionID
            | order by LastMessage desc
            | take {limit}
            """
        
        print(f"🔍 DEBUG: Executing query...")
        
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: db_manager.kusto_client.execute(db_manager.kusto_database, sessions_query)
        )
        
        raw_results = result.primary_results[0] if result.primary_results else []
        print(f"🔍 DEBUG: KQL returned {len(raw_results)} raw results")
        
        sessions = []
        for i, row in enumerate(raw_results):
            try:
                # Use dictionary-style access
                session_id = row["SessionID"]
                message_count = row["MessageCount"]
                first_message = row["FirstMessage"]
                last_message = row["LastMessage"]
                first_question = row["FirstQuestion"]
                last_question = row.get("LastQuestion", first_question) if hasattr(row, 'get') else row["LastQuestion"] if "LastQuestion" in row else first_question
                
                print(f"🔍 DEBUG: Processing row {i+1}: {session_id} ({message_count} messages)")
                
                # Use last question for better identification, fallback to first question
                display_question = last_question or first_question or "Unknown"
                
                # Clean and truncate the question for display
                display_question = str(display_question).strip()
                if len(display_question) > 45:
                    display_question = display_question[:45] + "..."
                
                # Extract date from session ID for grouping
                session_parts = session_id.split('_')
                session_date = "Unknown"
                
                if len(session_parts) >= 2:
                    date_part = session_parts[1]
                    if len(date_part) == 8:  # YYYYMMDD format
                        try:
                            parsed_date = datetime.strptime(date_part, "%Y%m%d")
                            session_date = parsed_date.strftime("%b %d, %Y")
                        except:
                            session_date = date_part
                
                session_info = {
                    "session_id": session_id,
                    "display_name": display_question,
                    "message_count": message_count,
                    "first_message": first_message,
                    "last_message": last_message,
                    "first_question": first_question,
                    "last_question": last_question,
                    "session_date": session_date,
                    "is_today": session_date == datetime.now().strftime("%b %d, %Y")
                }
                sessions.append(session_info)
                
            except Exception as e:
                print(f"❌ ERROR processing row {i+1}: {e}")
                print(f"❌ Row data: {row}")
                continue
        
        print(f"🔍 DEBUG: Successfully processed {len(sessions)} sessions")
        
        return {
            "status": "success",
            "query_type": "all" if date == "all" else f"date_{date}",
            "sessions": sessions,
            "total_sessions": len(sessions)
        }
        
    except Exception as e:
        print(f"❌ ERROR in get_chat_sessions: {str(e)}")
        print(f"❌ TRACEBACK: {traceback.format_exc()}")
        logger.error("Failed to retrieve sessions", date=date, error=str(e))
        return {
            "status": "error",
            "query_type": "all" if date == "all" else f"date_{date}",
            "sessions": [],
            "total_sessions": 0,
            "error": str(e)
        }

# CONSOLIDATED: Utility endpoints
@app.post("/api/schema/refresh")
async def refresh_schema_cache():
    """Manually refresh the schema cache"""
    try:
        logger.info("Manual schema refresh requested")
        
        # Clear current cache
        schema_manager.refresh_cache()
        
        # Fetch fresh schema
        tables_info = await schema_manager.get_cached_tables_info()
        
        return {
            "status": "success",
            "message": "Schema cache refreshed successfully",
            "table_count": len(tables_info),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error("Schema refresh failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Schema refresh failed: {str(e)}")

@app.delete("/api/admin/cache/clear")
async def admin_clear_kql_cache():
    """ADMIN ONLY: Clear the entire KQL ChatHistory_CFO table"""
    try:
        clear_query = ".drop table ChatHistory_CFO"
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: db_manager.kusto_client.execute(db_manager.kusto_database, clear_query)
        )
        await kql_storage.initialize_kql_table()
        
        logger.warning("ADMIN: KQL cache cleared completely")
        
        return {
            "status": "success",
            "message": "KQL cache cleared completely - ALL conversation history deleted",
            "timestamp": datetime.now().isoformat(),
            "warning": "This action cannot be undone"
        }
    except Exception as e:
        logger.error("Admin KQL cache clear failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to clear KQL cache: {str(e)}")

@app.get("/health")
async def health_check():
    """Enhanced health check with chat session info"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {},
        "schema_cache": {},
        "chat_session": {
            "session_id": "default-session-1234567890",
            "ready": True
        }
    }
    
    try:
        # Test SQL Database
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: db_manager.execute_sql_query("SELECT 1"))
        health_status["services"]["sql_database"] = "connected"
    except Exception as e:
        health_status["services"]["sql_database"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    try:
        # Test KQL Database
        test_result = await db_manager.test_kql_connection()
        health_status["services"]["kql_database"] = "connected" if test_result else "error"
        if not test_result:
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["services"]["kql_database"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Schema cache status
    if schema_manager.cached_tables_info is not None:
        cache_age = time.time() - (schema_manager.schema_cache_timestamp or 0)
        health_status["schema_cache"] = {
            "status": "loaded",
            "table_count": len(schema_manager.cached_tables_info),
            "cache_age_seconds": int(cache_age),
            "is_fresh": cache_age < schema_manager.schema_cache_duration
        }
    else:
        health_status["schema_cache"] = {
            "status": "not_loaded",
            "message": "Schema not cached yet"
        }
    
    # Quick chat history count
    try:
        count_query = f"""
        ChatHistory_CFO
        | where SessionID == 'default-session-1234567890'
        | where Question != 'tables_info' and Question != 'schema_info'
        | count
        """
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: db_manager.kusto_client.execute(db_manager.kusto_database, count_query)
        )
        
        if result.primary_results and len(result.primary_results[0]) > 0:
            chat_count = result.primary_results[0][0]["Count"]
            health_status["chat_session"]["stored_conversations"] = chat_count
    except:
        health_status["chat_session"]["stored_conversations"] = "unknown"
    
    health_status["features"] = [
        "Natural language processing", 
        "SQL analytics", 
        "Business insights", 
        "Smart visualization", 
        "KQL storage",
        "In-memory schema caching",
        "UI chat management"
    ]
    
    if health_status["status"] == "degraded":
        raise HTTPException(status_code=503, detail=health_status)
        
    return health_status

@app.get("/api/fabric/capabilities")
def get_capabilities():
    return {
        "capabilities": "Natural language query analysis with KQL persistence",
        "example_questions": [
            "What is the average cyber risk score?",
            "Show critical vulnerabilities (CVSS >= 7.0)",
            "Count unpatched devices by type",
            "Show login failure trends over time",
            "What are their departments?"
        ],
        "calculation_features": [
            "SQL-based stats",
            "Aggregations and percentages", 
            "Dynamic risk scores",
            "Trend analysis",
            "Group-based comparisons",
            "Real-time metrics"
        ],
        "intelligence_features": [
            "Natural language understanding",
            "Context-aware answers",
            "Proactive suggestions",
            "Detailed explanations",
            "Business insights"
        ],
        "visualization_features": [
            "Smart chart generation",
            "Bar charts for comparisons",
            "Line charts for trends",
            "Pie charts for distributions",
            "Stacked bars for grouped data"
        ],
        "supported_analysis": [
            "Cybersecurity risks",
            "Vulnerability tracking",
            "Patch monitoring",
            "Compliance checks",
            "Performance metrics",
            "Trends analysis",
            "Comparative studies",
            "Predictive insights"
        ]
    }

@app.get("/api/fabric/history")
async def get_history(session: Optional[str] = Query(None, description="Session ID")):
    """Retrieve the latest 10 conversation responses for the session"""
    try:
        session_id = SessionManager.get_session_id_from_request(session)
        responses = await kql_storage.get_latest_responses(session_id)
        return {
            "status": "success",
            "session_id": session_id,
            "history": responses
        }
    except Exception as e:
        logger.error("Failed to retrieve history", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve conversation history")

@app.delete("/api/cache/clear")
async def clear_kql_cache():
    """Clear the KQL ChatHistory_CFO table"""
    try:
        clear_query = ".drop table ChatHistory_CFO"
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: db_manager.kusto_client.execute(db_manager.kusto_database, clear_query)
        )
        await kql_storage.initialize_kql_table()
        return {
            "status": "success",
            "message": "KQL cache cleared successfully",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error("KQL cache clear failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to clear KQL cache: {str(e)}")

# CONSOLIDATED: Application startup
@app.on_event("startup")
async def startup_event():
    """Enhanced startup with AI Foundry and schema preloading"""
    try:
        logger.info("Starting enhanced application initialization...")
        
        # Test connections
        kql_ok = await db_manager.test_kql_connection()
        if not kql_ok:
            logger.warning("KQL connection failed during startup")
        else:
            logger.info("KQL connection test passed")
            
        await kql_storage.initialize_kql_table()
        
        schema_preloaded = await schema_manager.preload_schema()
        if schema_preloaded:
            print("✅ Schema preloaded - first query will be fast!")
        else:
            print("⚠️  Schema preload failed - first query may be slower")
        
        # Check additional services
        if ai_services.ai_foundry_enabled:
            print("✅ Azure AI Foundry agents initialized")
        else:
            print("⚠️  Azure AI Foundry not available - using standard OpenAI only")
        
        if ai_services.graph_client:
            print("✅ Microsoft Graph email service available")
        else:
            print("⚠️  Email service not available - configure Graph API for email features")
        
        if REPORT_LIBS_AVAILABLE:
            print("✅ Report generation libraries available")
        else:
            print("⚠️  Report generation not available - install reportlab and xlsxwriter")
        
        logger.info("Enhanced application startup completed successfully")
        
    except Exception as e:
        logger.error("Enhanced startup failed", error=str(e))
        print(f"❌ Startup Error: {e}")
        
@app.post("/api/debug/full-pipeline")
async def debug_full_pipeline(question: str = "Create a P&L report for 2025"):
    """Debug the complete analysis pipeline"""
    try:
        result = {
            "question": question,
            "steps": {},
            "errors": []
        }
        
        # Step 1: Schema
        tables_info = await schema_manager.get_cached_tables_info()
        balance_sheet = next((t for t in tables_info if 'balance' in t.get('table', '').lower()), None)
        result["steps"]["1_schema"] = {
            "balance_sheet_found": bool(balance_sheet),
            "column_count": len(balance_sheet.get('columns', [])) if balance_sheet else 0,
            "sample_columns": balance_sheet.get('columns', [])[:5] if balance_sheet else []
        }
        
        # Step 2: Prompt Generation
        prompt = await prompt_manager.build_chatgpt_system_prompt(question, tables_info, [])
        result["steps"]["2_prompt_length"] = len(prompt)
        
        # Step 3: LLM Response
        llm_response = await ai_services.ask_intelligent_llm_async(prompt)
        result["steps"]["3_llm_response"] = llm_response[:500] + "..." if len(llm_response) > 500 else llm_response
        
        # Step 4: SQL Extraction
        if "SQL_QUERY:" in llm_response:
            parts = llm_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1)
            raw_sql = parts[0].strip()
            result["steps"]["4_raw_sql"] = raw_sql
            
            # Step 5: SQL Cleaning
            cleaned_sql = Utils.clean_generated_sql(raw_sql)
            result["steps"]["5_cleaned_sql"] = cleaned_sql
            
            # Step 6: SQL Execution
            if cleaned_sql:
                try:
                    execution_result = await analytics_engine.execute_sql_query(cleaned_sql)
                    result["steps"]["6_execution"] = {
                        "success": True,
                        "row_count": len(execution_result),
                        "sample_data": execution_result[:2] if execution_result else []
                    }
                except Exception as e:
                    result["steps"]["6_execution"] = {
                        "success": False,
                        "error": str(e)
                    }
                    result["errors"].append(f"Execution error: {str(e)}")
            else:
                result["errors"].append("No cleaned SQL generated")
        else:
            result["errors"].append("No SQL_QUERY found in LLM response")
        
        return result
        
    except Exception as e:
        return {"error": str(e), "steps": result.get("steps", {})}
@app.get("/api/debug/schema-order")
async def debug_schema_order(question: str = "Create a P&L report for 2025"):
    tables_info = await schema_manager.get_cached_tables_info()
    
    # What does the AI see?
    relevant_tables = prompt_manager.filter_schema_for_question(question, tables_info)
    
    result = []
    for i, table in enumerate(relevant_tables[:5]):
        table_name = table.get('table', '')
        columns = table.get('columns', [])
        
        # Check for financial columns
        financial_cols = []
        for col in columns:
            col_text = col.lower()
            if any(term in col_text for term in ['revenue', 'profit', 'expense', 'income']):
                financial_cols.append(col.split()[0])  # Just column name
        
        result.append({
            "order": i + 1,
            "table": table_name,
            "total_columns": len(columns),
            "financial_columns": financial_cols,
            "is_balance_sheet": 'balance' in table_name.lower()
        })
    
    return {
        "question": question,
        "tables_in_order": result,
        "issue": "AI picks first table with financial columns"
    }

@app.post("/api/debug/enhanced-context-test")
async def enhanced_context_test(
    question1: str = "Show me all revenue for Brown Ltd in 2024 and 2025",
    question2: str = "Why does it behave this way?"
):
    """Test the enhanced context flow with comprehensive validation"""
    
    session_id = f"test_enhanced_{int(time.time())}"
    
    try:
        print(f"\n🧪 ENHANCED CONTEXT TEST")
        print(f"Q1: {question1}")
        
        result1 = await analytics_engine.cached_intelligent_analyze(question1, session_id, False)
        print(f"✅ Q1 completed - SQL: {bool(result1.get('generated_sql'))}, Results: {result1.get('result_count', 0)}")
        
        print("⏳ Waiting for KQL storage consistency...")
        await asyncio.sleep(10)
        
        stored_records = []
        for attempt in range(5):  # Try up to 5 times
            verify_query = f"""
            ChatHistory_CFO
            | where SessionID == '{session_id}'
            | project Question, Response
            """
            
            try:
                verify_result = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: db_manager.kusto_client.execute(db_manager.kusto_database, verify_query)
                )
                
                stored_records = verify_result.primary_results[0] if verify_result.primary_results else []
                print(f"🔍 Verification attempt {attempt + 1}: {len(stored_records)} records found")
                
                if len(stored_records) > 0:
                    break  # Found records, stop trying
                    
                if attempt < 4:  # Don't wait after the last attempt
                    await asyncio.sleep(2)  # Wait 2 seconds between attempts
                    
            except Exception as e:
                print(f"❌ Verification attempt {attempt + 1} failed: {e}")
                if attempt < 4:
                    await asyncio.sleep(2)
        
        print(f"🔍 Final verification: {len(stored_records)} records found in KQL for session {session_id}")
        
        
        if stored_records:
            for i, record in enumerate(stored_records):
                print(f"  Record {i+1}: {record['Question'][:50]}...")
        
        print(f"\nQ2: {question2}")
        
        # ✅ Test conversation history retrieval directly
        print(f"\n🔍 DIRECT HISTORY TEST:")
        direct_history = await analytics_engine.get_simple_conversation_history(session_id)
        print(f"Direct history retrieval: {len(direct_history)} items")
        
        # Second query
        result2 = await analytics_engine.cached_intelligent_analyze(question2, session_id, False)
        print(f"✅ Q2 completed - Type: {result2.get('response_type', 'unknown')}")
        
        # Extract analysis
        sql1 = result1.get("generated_sql", "")
        sql2 = result2.get("generated_sql", "")
        
        return {
            "test_info": {
                "session_id": session_id,
                "q1": question1,
                "q2": question2,
                "timestamp": datetime.now().isoformat(),
                "kql_consistency_wait_seconds": 10
            },
            "storage_verification": {
                "records_in_kql": len(stored_records),
                "sample_questions": [r['Question'] for r in stored_records[:3]],
                "verification_attempts": "Multiple attempts with backoff"
            },
            "conversation_debug": {
                "direct_history_items": len(direct_history),
                "direct_history_preview": [
                    {"role": item["role"], "content": item["content"][:100] + "..."} 
                    for item in direct_history[:2]
                ]
            },
            "first_query": {
                "sql": sql1,
                "result_count": result1.get("result_count", 0),
                "response_type": result1.get("response_type", "unknown"),
                "year_filter": "Found" if "2024" in sql1 and "2025" in sql1 else "Not found",
                "success": bool(sql1 and result1.get("result_count", 0) > 0)
            },
            "second_query": {
                "sql": sql2,
                "result_count": result2.get("result_count", 0),
                "response_type": result2.get("response_type", "unknown"),
                "has_conversation_history": bool(result2.get("conversation_context_used", False)),
                "year_filter": "Found" if sql2 and ("2024" in sql2 and "2025" in sql2) else "Not found",
                "has_analytical_elements": bool(sql2 and any(keyword in sql2.upper() for keyword in ['LAG', 'LEAD', 'GROWTH', 'TREND', 'OVER', 'PARTITION'])),
                "success": bool(sql2 and result2.get("result_count", 0) > 0)
            },
            "context_flow_analysis": {
                "kql_storage_working": len(stored_records) > 0,
                "history_retrieval_working": len(direct_history) > 0,
                "contextual_detection_working": result2.get("response_type") != "conversational",
                "overall_context_flow": len(stored_records) > 0 and len(direct_history) > 0 and sql2 != "",
                "kql_consistency_issue": len(stored_records) == 0,
                "recommended_wait_time": "10-15 seconds for KQL consistency"
            }
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "test_info": {
                "session_id": session_id,
                "q1": question1,
                "q2": question2
            }
        }
        
@app.get("/api/debug/test-classification")
async def test_classification():
    """Test question classification"""
    
    test_questions = [
        "show me revenue for 2024",
        "what is the gross profit for 2024 and 2025", 
        "How do I improve this value?",
        "Why does it behave this way?",
        "Calculate net income by quarter"
    ]
    
    results = []
    for q in test_questions:
        is_contextual = analytics_engine.is_contextual_question(q)
        is_data = analytics_engine.is_data_question(q)
        will_store = not is_contextual
        
        results.append({
            "question": q,
            "is_contextual": is_contextual,
            "is_data_question": is_data,
            "will_be_stored": will_store,
            "expected_path": "contextual (no storage)" if is_contextual else "non-contextual (will store)"
        })
    
    return {"test_results": results}

@app.get("/api/debug/kql-contents")
async def debug_kql_contents(session_id: str = None):
    """Check what's actually stored in KQL"""
    
    actual_session_id = session_id or "test_session"
    
    try:
        # Get all records for this session
        query = f"""
        ChatHistory_CFO
        | where SessionID == '{actual_session_id}'
        | project Timestamp, Question, Response
        | order by Timestamp desc
        | take 10
        """
        
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: db_manager.kusto_client.execute(db_manager.kusto_database, query)
        )
        
        records = result.primary_results[0] if result.primary_results else []
        
        return {
            "session_id": actual_session_id,
            "total_records": len(records),
            "records": [
                {
                    "timestamp": row["Timestamp"],
                    "question": row["Question"],
                    "response_preview": row["Response"][:200] + "..."
                }
                for row in records
            ]
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }

# CONSOLIDATED: Main application entry point
if __name__ == "__main__":
    print("🤖 Intelligent SQL Analytics Assistant")
    print("📊 Powered by Microsoft Fabric SQL Database and KQL Storage")
    print("🖥 Advanced analytics engine")
    print("📈 Smart visualization")
    print("")
    print("✨ Key Features:")
    print("• Natural language queries")
    print("• Automatic SQL generation")
    print("• Business-oriented insights")
    print("• Context-aware visualizations")
    print("• KQL-based conversation history")
    print("• AI-powered analysis")
    print("• Email notifications")
    print("• Professional report generation")
    print("")
    print("💡 Example Questions:")
    print("• 'What is the average cyber risk score?'")
    print("• 'Show critical vulnerabilities (CVSS ≥ 7.0)'")
    print("• 'How many unpatched devices by type?'")
    print("• 'Show trends in incidents over time'")
    print("• 'What are their departments?'")
    print("")
    print("🔗 API Endpoints:")
    print("• POST /api/fabric/intelligent - Main analytics endpoint")
    print("• POST /api/intelligent-workflow - Report generation workflow")
    print("• GET /api/chat/messages - Chat history")
    print("• GET /api/chat/sessions - Session management")
    print("• GET /health - Health check")
    print("• GET /api/fabric/capabilities - System capabilities")
    print("")
    try:
        ConfigManager.validate_environment()
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except Exception as e:
        print("❌ Failed to start application: {e}")
        exit(1)
