from mcp.server.fastmcp import FastMCP
import mysql.connector
from mysql.connector import Error
import time
import hashlib

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "ofxx_db"
}

mcp = FastMCP("wordpress-users")

# Cache storage
_cache = {}
CACHE_TTL = 300  # 5 minutes


def get_db():
    """Get database connection"""
    return mysql.connector.connect(**DB_CONFIG)


def get_cache_key(query):
    """Generate cache key from query"""
    return hashlib.md5(query.encode()).hexdigest()


def is_cache_valid(cache_entry, ttl=CACHE_TTL):
    """Check if cache entry is still valid"""
    if not cache_entry:
        return False
    return (time.time() - cache_entry["timestamp"]) < ttl


def get_cached_query(query):
    """Get cached query result if valid"""
    key = get_cache_key(query)
    entry = _cache.get(key)
    if is_cache_valid(entry):
        return entry["data"]
    return None


def set_cached_query(query, data):
    """Store query result in cache"""
    key = get_cache_key(query)
    _cache[key] = {
        "data": data,
        "timestamp": time.time()
    }


@mcp.tool()
def search_sql(
    search_term: str,
    table: str = None,
    columns: str = None,
    use_wildcard: bool = True,
    limit: int = 100,
    case_sensitive: bool = False
):
    """
    Search database using SQL with wildcard support. This tool can search across tables and columns.
    
    IMPORTANT: If the search parameters are ambiguous or unclear, ASK THE HUMAN FOR CLARIFICATION before executing.
    For example:
    - If table name is not provided and multiple tables might match, ask which table to search
    - If search_term is too vague (empty, single character, or very common), ask for more specific search criteria
    - If columns parameter is ambiguous, ask which specific columns to search
    - If the search might return too many results, ask if they want to refine the search
    
    Args:
        search_term: The term to search for. Wildcards (%) are automatically added if use_wildcard=True
        table: Table name to search in (e.g., 'wp_users', 'wp_posts'). If None, will search common WordPress tables.
               If unclear which table, ASK THE HUMAN for clarification.
        columns: Comma-separated column names to search (e.g., 'user_login,user_email,display_name').
                 If None, will search common text columns. If ambiguous, ASK THE HUMAN for clarification.
        use_wildcard: If True, automatically wraps search_term with % for partial matching (default: True)
        limit: Maximum number of results to return (default: 100). If results exceed limit, suggest refinement.
        case_sensitive: If True, uses case-sensitive search (default: False)
    
    Returns:
        Dictionary with search results, count, and suggestions for refinement if needed.
    
    Examples:
        search_sql("john", table="wp_users", columns="user_login,display_name")
        search_sql("admin", use_wildcard=True)
        search_sql("test@example.com", table="wp_users", columns="user_email", use_wildcard=False)
    """
    try:
        # Validation: Ask for clarification if search term is too vague
        if not search_term or len(search_term.strip()) == 0:
            return {
                "error": "Search term cannot be empty.",
                "needs_clarification": True,
                "message": "Please provide a search term to look for."
            }
        
        if len(search_term.strip()) == 1 and use_wildcard:
            return {
                "warning": "Single character search with wildcard may return many results.",
                "needs_clarification": True,
                "message": "Would you like to refine your search term? Single character searches can be very broad."
            }
        
        # Prepare search term with wildcards
        search_pattern = search_term.strip()
        if use_wildcard:
            search_pattern = f"%{search_pattern}%"
        
        # Determine which table(s) to search
        if not table:
            # Default to common WordPress tables - but ask if unclear
            table = "wp_users"
            return {
                "needs_clarification": True,
                "message": "No table specified. Which table would you like to search? Common options: wp_users, wp_posts, wp_comments, wp_options, etc.",
                "suggested_tables": ["wp_users", "wp_posts", "wp_comments", "wp_options", "wp_usermeta"]
            }
        
        # Determine which columns to search
        if not columns:
            # Try to infer common columns based on table
            if "user" in table.lower():
                columns = "user_login,user_email,display_name,user_nicename"
            elif "post" in table.lower():
                columns = "post_title,post_content,post_name"
            elif "comment" in table.lower():
                columns = "comment_content,comment_author,comment_author_email"
            elif "option" in table.lower():
                columns = "option_name,option_value"
            else:
                return {
                    "needs_clarification": True,
                    "message": f"Table '{table}' specified but no columns. Which columns would you like to search in this table?",
                    "suggestion": "You can specify columns as a comma-separated list, e.g., 'column1,column2,column3'"
                }
        
        # Build the WHERE clause for multiple columns
        column_list = [col.strip() for col in columns.split(",")]
        if not column_list:
            return {
                "error": "No valid columns specified.",
                "needs_clarification": True,
                "message": "Please specify which columns to search."
            }
        
        # Build SQL query with LIKE conditions
        like_operator = "LIKE BINARY" if case_sensitive else "LIKE"
        where_conditions = []
        params = []
        
        for col in column_list:
            where_conditions.append(f"{col} {like_operator} %s")
            params.append(search_pattern)
        
        where_clause = " OR ".join(where_conditions)
        
        # Construct the query
        query = f"SELECT * FROM {table} WHERE {where_clause} LIMIT %s"
        params.append(limit)
        
        # Execute query
        db = get_db()
        cursor = db.cursor(dictionary=True)
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        db.close()
        
        # Convert results to list of dicts
        data = []
        for row in results:
            clean_row = {}
            for key, value in row.items():
                clean_row[key] = str(value) if value is not None else None
            data.append(clean_row)
        
        # Check if we hit the limit
        result_count = len(data)
        warning = None
        if result_count >= limit:
            warning = f"Results limited to {limit} rows. Consider refining your search for more specific results."
        
        return {
            "data": data,
            "count": result_count,
            "table": table,
            "columns_searched": column_list,
            "search_pattern": search_pattern,
            "warning": warning,
            "message": f"Found {result_count} result(s) in {table} matching '{search_term}'"
        }
        
    except Error as e:
        error_msg = str(e)
        # Provide helpful error messages that might need clarification
        if "Table" in error_msg and "doesn't exist" in error_msg:
            return {
                "error": error_msg,
                "needs_clarification": True,
                "message": f"Table '{table}' not found. Please verify the table name or ask which table to search."
            }
        elif "Unknown column" in error_msg:
            return {
                "error": error_msg,
                "needs_clarification": True,
                "message": "One or more columns don't exist in this table. Please verify column names or ask which columns are available."
            }
        return {"error": f"Database error: {e}"}


def is_write_query(query: str):
    """Check if query is a write operation (INSERT, UPDATE, DELETE, etc.)"""
    query_upper = query.strip().upper()
    write_keywords = ['INSERT', 'UPDATE', 'DELETE', 'ALTER', 'DROP', 'CREATE', 'TRUNCATE', 'REPLACE']
    return any(query_upper.startswith(keyword) for keyword in write_keywords)


@mcp.tool()
def run_query(query: str, use_cache: bool = True, force_refresh: bool = False, confirm_write: bool = False):
    """
    Run SQL query with caching and validation. Write operations require confirmation.
    
    Args:
        query: SQL query to execute
        use_cache: Use cached result if available and valid (default: True, ignored for write queries)
        force_refresh: Force refresh cache even if valid (default: False)
        confirm_write: Must be True for write operations (INSERT, UPDATE, DELETE, etc.)
    
    Example:
        run_query("SELECT * FROM wp_users")  # Read query
        run_query("UPDATE wp_users SET display_name='Test' WHERE ID=1", confirm_write=True)  # Write query
    """
    try:
        is_write = is_write_query(query)
        
        # Safety check for write operations
        if is_write and not confirm_write:
            return {
                "error": "Write operation detected. This query will modify data.",
                "query_type": "WRITE",
                "message": "To execute this query, set confirm_write=True. Example: run_query(query, confirm_write=True)",
                "query_preview": query[:100] + "..." if len(query) > 100 else query
            }
        
        # Write operations don't use cache
        if is_write:
            use_cache = False
        
        # Check cache first (only for read queries)
        if use_cache and not force_refresh and not is_write:
            cached = get_cached_query(query)
            if cached:
                return {
                    "data": cached,
                    "cached": True,
                    "query_type": "READ",
                    "message": "Data retrieved from cache"
                }
        
        # Execute query
        db = get_db()
        cursor = db.cursor(dictionary=True)
        cursor.execute(query)
        
        # For write operations, commit and get affected rows
        if is_write:
            affected_rows = cursor.rowcount
            db.commit()
            cursor.close()
            db.close()
            
            # Clear cache after write operation
            _cache.clear()
            
            return {
                "success": True,
                "query_type": "WRITE",
                "affected_rows": affected_rows,
                "message": f"Write operation completed successfully. {affected_rows} row(s) affected."
            }
        
        # For read operations, fetch results
        results = cursor.fetchall()
        cursor.close()
        db.close()
        
        # Convert to list of dicts (handle datetime serialization)
        data = []
        for row in results:
            clean_row = {}
            for key, value in row.items():
                clean_row[key] = str(value) if value is not None else None
            data.append(clean_row)
        
        # Store in cache (only for read queries)
        if use_cache:
            set_cached_query(query, data)
        
        return {
            "data": data,
            "cached": False,
            "query_type": "READ",
            "count": len(data),
            "message": f"Query executed successfully. {len(data)} row(s) returned."
        }
        
    except Error as e:
        return {"error": f"Database error: {e}"}


@mcp.tool()
def clear_cache():
    """Clear all cached query results"""
    _cache.clear()
    return {"message": "Cache cleared successfully"}


@mcp.tool()
def get_cache_info():
    """Get information about cached queries"""
    info = {
        "total_cached_queries": len(_cache),
        "cache_entries": []
    }
    
    for key, entry in _cache.items():
        age = time.time() - entry["timestamp"]
        info["cache_entries"].append({
            "key": key[:16] + "...",
            "age_seconds": round(age, 2),
            "is_valid": is_cache_valid(entry),
            "row_count": len(entry["data"]) if entry["data"] else 0
        })
    
    return info


if __name__ == "__main__":
    mcp.run()
