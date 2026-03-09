import duckdb
from pathlib import Path

def create_warehouse(warehouse_path=None):
    """
    Create DuckDB warehouse with fact and dimension tables
    Tables:
    - fact_air_quality: raw air quality data
    - dim_city: city metadata
    - daily_city_metrics: aggregated daily metrics by city
    """
    if warehouse_path is None:
        warehouse_dir = Path(__file__).parent.parent.parent / "warehouse"
        warehouse_dir.mkdir(parents=True, exist_ok=True)
        warehouse_path = warehouse_dir / "air_quality.duckdb"
    else:
        warehouse_path = Path(warehouse_path)
        warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create DuckDB connection
    conn = duckdb.connect(str(warehouse_path))
    
    # Drop existing tables if they exist
    conn.execute("DROP TABLE IF EXISTS fact_air_quality")
    conn.execute("DROP TABLE IF EXISTS dim_city")
    conn.execute("DROP TABLE IF EXISTS daily_city_metrics")
    
    print(f"Created DuckDB warehouse: {warehouse_path}")
    return conn


def load_data_to_warehouse(conn, df):
    """Load cleaned data into fact_air_quality table"""
    
    # Load fact_air_quality
    conn.execute("CREATE TABLE fact_air_quality AS SELECT * FROM df")
    print(f"Loaded {len(df)} records into fact_air_quality")
    
    # Create dim_city
    conn.execute("""
        CREATE TABLE dim_city AS
        SELECT DISTINCT city FROM fact_air_quality
        ORDER BY city
    """)
    print(f"Created dim_city with {conn.execute('SELECT COUNT(*) FROM dim_city').fetchall()[0][0]} cities")
    
    # Get numeric columns for aggregation (exclude date, city, source_file)
    columns = df.columns.tolist()
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    
    # Build aggregation expressions for numeric columns
    agg_expressions = []
    for col in numeric_cols:
        safe_col = f'"{col}"' if col.lower() in ['date', 'city'] else col
        agg_expressions.append(f"ROUND(AVG({col}), 2) as avg_{col.lower().replace(' ', '_').replace('.', '_')}")
    
    agg_select = ",\n            ".join(agg_expressions)
    
    # Create daily_city_metrics with dynamic aggregations
    create_metrics_sql = f"""
        CREATE TABLE daily_city_metrics AS
        SELECT 
            city,
            CAST(strptime(date, '%Y-%m-%d') AS DATE) as date,
            {agg_select},
            COUNT(*) as record_count
        FROM fact_air_quality
        GROUP BY city, CAST(strptime(date, '%Y-%m-%d') AS DATE)
        ORDER BY city, date
    """
    
    conn.execute(create_metrics_sql)
    daily_count = conn.execute('SELECT COUNT(*) FROM daily_city_metrics').fetchall()[0][0]
    print(f"Created daily_city_metrics with {daily_count} daily aggregations")
    
    # Print schema info
    schema = conn.execute("SELECT * FROM fact_air_quality LIMIT 0").description
    print(f"Data columns: {[col[0] for col in schema]}")
    
    return conn


def query_warehouse(conn, sql):
    """Execute custom query on warehouse"""
    result = conn.execute(sql).fetchall()
    return result
