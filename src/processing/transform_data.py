
import pandas as pd

def create_analytics(df):

    numeric_cols = df.select_dtypes(include="number").columns

    if len(numeric_cols) == 0:
        return None

    analytics = df[numeric_cols].mean().to_frame(name="mean").reset_index()
    analytics.columns = ["metric","value"]

    return analytics
