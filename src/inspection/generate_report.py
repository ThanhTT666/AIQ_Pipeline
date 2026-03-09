import sys
from pathlib import Path
import pandas as pd
import duckdb
from datetime import datetime
import json

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def generate_html_report():
    """Generate comprehensive HTML report of pipeline results"""
    
    project_root = Path(__file__).parent.parent.parent
    warehouse_path = project_root / "warehouse" / "air_quality.duckdb"
    data_lake_path = project_root / "data_lake"
    processed_path = project_root / "data" / "processed" / "clean_dataset.csv"
    reports_dir = project_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    # Load data
    conn = duckdb.connect(str(warehouse_path), read_only=True)
    df_clean = pd.read_csv(processed_path)
    
    # Get statistics
    fact_table = conn.execute("SELECT * FROM fact_air_quality").df()
    daily_metrics = conn.execute("SELECT * FROM daily_city_metrics").df()
    dim_city = conn.execute("SELECT * FROM dim_city").df()
    
    # Prepare data for HTML
    stats = {
        'total_records': len(df_clean),
        'total_cities': df_clean['city'].nunique(),
        'cities': sorted(df_clean['city'].unique().tolist()),
        'date_range': f"{df_clean['date'].min()} to {df_clean['date'].max()}",
        'numeric_columns': df_clean.select_dtypes(include=['number']).columns.tolist(),
        'column_count': len(df_clean.columns)
    }
    
    # Build HTML
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AIQ Pipeline Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .header p {{
            font-size: 1.1em;
            opacity: 0.9;
        }}
        
        .content {{
            padding: 40px;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        
        .stat-card .number {{
            font-size: 2em;
            font-weight: bold;
            margin: 10px 0;
        }}
        
        .stat-card .label {{
            font-size: 0.9em;
            opacity: 0.9;
        }}
        
        .section {{
            margin-bottom: 40px;
        }}
        
        .section-title {{
            font-size: 1.8em;
            color: #333;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        
        thead {{
            background: #667eea;
            color: white;
        }}
        
        th {{
            padding: 15px;
            text-align: left;
            font-weight: 600;
        }}
        
        td {{
            padding: 12px 15px;
            border-bottom: 1px solid #eee;
        }}
        
        tbody tr:hover {{
            background-color: #f8f9ff;
        }}
        
        .footer {{
            background: #f5f5f5;
            padding: 20px;
            text-align: center;
            color: #666;
            font-size: 0.9em;
        }}
        
        .badge {{
            display: inline-block;
            background: #e8f0ff;
            color: #667eea;
            padding: 5px 10px;
            border-radius: 20px;
            margin: 2px;
            font-size: 0.85em;
        }}
        
        .info-box {{
            background: #f0f4ff;
            border-left: 4px solid #667eea;
            padding: 15px;
            margin: 15px 0;
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌍 Air Quality Pipeline Report</h1>
            <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="content">
            <!-- Overview Stats -->
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="label">Total Records</div>
                    <div class="number">{stats['total_records']:,}</div>
                </div>
                <div class="stat-card">
                    <div class="label">Cities</div>
                    <div class="number">{stats['total_cities']}</div>
                </div>
                <div class="stat-card">
                    <div class="label">Columns</div>
                    <div class="number">{stats['column_count']}</div>
                </div>
            </div>
            
            <!-- Dataset Information -->
            <div class="section">
                <h2 class="section-title">📊 Dataset Information</h2>
                <div class="info-box">
                    <strong>Date Range:</strong> {stats['date_range']}
                </div>
                <div class="info-box">
                    <strong>Cities:</strong><br>
                    {' '.join([f'<span class="badge">{city}</span>' for city in stats['cities']])}
                </div>
            </div>
            
            <!-- Numeric Columns -->
            <div class="section">
                <h2 class="section-title">🔢 Numeric Columns</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Column</th>
                            <th>Min</th>
                            <th>Max</th>
                            <th>Mean</th>
                            <th>Missing Values</th>
                        </tr>
                    </thead>
                    <tbody>
"""
    
    # Add numeric column statistics
    for col in stats['numeric_columns']:
        min_val = df_clean[col].min()
        max_val = df_clean[col].max()
        mean_val = df_clean[col].mean()
        missing = df_clean[col].isna().sum()
        
        html_content += f"""
                        <tr>
                            <td><strong>{col}</strong></td>
                            <td>{min_val:.2f}</td>
                            <td>{max_val:.2f}</td>
                            <td>{mean_val:.2f}</td>
                            <td>{missing}</td>
                        </tr>
"""
    
    html_content += """
                    </tbody>
                </table>
            </div>
            
            <!-- City Statistics -->
            <div class="section">
                <h2 class="section-title">🏙️ City Statistics</h2>
                <table>
                    <thead>
                        <tr>
                            <th>City</th>
                            <th>Record Count</th>
                            <th>Date Range</th>
                        </tr>
                    </thead>
                    <tbody>
"""
    
    # Add city statistics
    for city in stats['cities']:
        city_data = df_clean[df_clean['city'] == city]
        count = len(city_data)
        
        # Handle NaN dates
        valid_dates = city_data['date'].dropna()
        if len(valid_dates) > 0:
            date_min = valid_dates.min()
            date_max = valid_dates.max()
            date_range_text = f"{date_min} to {date_max}"
            missing_note = f"({len(city_data) - len(valid_dates)} missing dates)"
        else:
            date_range_text = "No valid dates"
            missing_note = ""
        
        html_content += f"""
                        <tr>
                            <td><strong>{city}</strong></td>
                            <td>{count:,}</td>
                            <td>{date_range_text} {missing_note}</td>
                        </tr>
"""
    
    html_content += """
                    </tbody>
                </table>
            </div>
            
            <!-- Daily Metrics Sample -->
            <div class="section">
                <h2 class="section-title">📈 Daily City Metrics (Sample)</h2>
                <table>
                    <thead>
                        <tr>
"""
    
    # Add column headers
    for col in daily_metrics.columns:
        html_content += f"                            <th>{col}</th>\n"
    
    html_content += """
                        </tr>
                    </thead>
                    <tbody>
"""
    
    # Add sample rows
    for idx, row in daily_metrics.head(10).iterrows():
        html_content += "                        <tr>\n"
        for col in daily_metrics.columns:
            val = row[col]
            if isinstance(val, (int, float)):
                html_content += f"                            <td>{val:.2f}</td>\n"
            else:
                html_content += f"                            <td>{val}</td>\n"
        html_content += "                        </tr>\n"
    
    html_content += """
                    </tbody>
                </table>
                <div class="info-box" style="margin-top: 15px;">
                    Total daily aggregations: """ + str(len(daily_metrics)) + """
                </div>
            </div>
            
            <!-- Data Lake Info -->
            <div class="section">
                <h2 class="section-title">🗄️ Data Lake (Parquet)</h2>
                <div class="info-box">
                    <strong>Location:</strong> <code>data_lake/</code><br>
                    <strong>Format:</strong> Parquet (columnar, compressed)<br>
                    <strong>Partitioning:</strong> by City (city=Bangkok, city=Beijing, ...)<br>
                    <strong>Technology:</strong> Efficient for analytics and big data processing
                </div>
            </div>
            
            <!-- DuckDB Warehouse Info -->
            <div class="section">
                <h2 class="section-title">⚙️ DuckDB Warehouse</h2>
                <div class="info-box">
                    <strong>Location:</strong> <code>warehouse/air_quality.duckdb</code><br>
                    <strong>Tables:</strong>
                    <ul style="margin-top: 10px; margin-left: 20px;">
                        <li><strong>fact_air_quality</strong> - Raw air quality observations</li>
                        <li><strong>dim_city</strong> - City dimension</li>
                        <li><strong>daily_city_metrics</strong> - Daily aggregated metrics by city</li>
                    </ul>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>AIQ Pipeline Report | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>Data includes {stats['total_records']:,} records from {stats['total_cities']} cities</p>
        </div>
    </div>
    
    <script>
        // Optional: Add some interactivity
        document.addEventListener('DOMContentLoaded', function() {{
            console.log('Report loaded successfully!');
        }});
    </script>
</body>
</html>
"""
    
    # Save report
    report_path = reports_dir / "pipeline_report.html"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    conn.close()
    
    print(f"✅ HTML Report generated: {report_path}")
    print(f"📂 Open in browser: file:///{report_path.absolute()}")
    
    return report_path


if __name__ == "__main__":
    generate_html_report()
