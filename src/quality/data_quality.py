import json
from datetime import datetime
from pathlib import Path

def validate_data(df):
    """Generate comprehensive data quality report"""
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "columns": list(df.columns),
        "missing_values": df.isnull().sum().to_dict(),
        "duplicates": int(df.duplicated().sum()),
        "data_types": df.dtypes.to_dict()
    }

    return report


def save_quality_report(report, output_dir):
    """Save data quality report to JSON file"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    report_path = output_dir / "data_quality_report.json"
    
    # Convert non-serializable types to string
    report_clean = {}
    for key, value in report.items():
        if isinstance(value, dict):
            report_clean[key] = {str(k): (int(v) if isinstance(v, (int, float)) else str(v)) 
                                for k, v in value.items()}
        else:
            report_clean[key] = value
    
    with open(report_path, 'w') as f:
        json.dump(report_clean, f, indent=2, default=str)
    
    return report_path
