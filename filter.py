import pandas as pd
from typing import Union, Optional, Dict, List, Literal
import os
from datetime import datetime

class FilterCondition:
    """
    Class to define filter conditions for Excel data.
    
    Attributes:
        column (str): Column name to filter on
        value (Union[str, int, float, List, datetime]): Value(s) to filter by
        condition (str): Type of condition to apply
        date_format (str): Format string for parsing date strings (e.g., '%d/%m/%Y')
    """
    def __init__(
        self,
        column: str,
        value: Union[str, int, float, List, datetime],
        condition: Literal['equals', 'contains', 'starts_with', 'in', 'greater_than', 'less_than', 'date_equals', 'date_greater', 'date_less'] = 'equals',
        date_format: str = '%d/%m/%Y'
    ):
        self.column = column
        self.value = value
        self.condition = condition
        self.date_format = date_format

class FilterGroup:
    """
    Class to group filter conditions with OR logic.
    Conditions within a group are combined with OR,
    while different groups are combined with AND.
    """
    def __init__(self, conditions: List[FilterCondition]):
        self.conditions = conditions

def apply_filter_condition(df: pd.DataFrame, condition: FilterCondition) -> pd.Series:
    """Helper function to apply a single filter condition and return a boolean mask"""
    if condition.condition.startswith('date_'):
        # Convert column to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df[condition.column]):
            df[condition.column] = pd.to_datetime(df[condition.column], format=condition.date_format, errors='coerce')
        
        # Convert value to datetime if it's a string
        if isinstance(condition.value, str):
            filter_date = pd.to_datetime(condition.value, format=condition.date_format)
        else:
            filter_date = condition.value
        
        if condition.condition == 'date_equals':
            return df[condition.column].dt.date == filter_date.date()
        elif condition.condition == 'date_greater':
            return df[condition.column].dt.date > filter_date.date()
        elif condition.condition == 'date_less':
            return df[condition.column].dt.date < filter_date.date()
    else:
        # Convert column to string if needed for string operations
        if condition.condition in ['contains', 'starts_with']:
            df[condition.column] = df[condition.column].astype(str)
            
        if condition.condition == 'equals':
            return df[condition.column] == condition.value
        elif condition.condition == 'contains':
            return df[condition.column].str.contains(str(condition.value), na=False)
        elif condition.condition == 'starts_with':
            return df[condition.column].str.startswith(str(condition.value))
        elif condition.condition == 'in':
            return df[condition.column].isin(condition.value)
        elif condition.condition == 'greater_than':
            return df[condition.column] > condition.value
        elif condition.condition == 'less_than':
            return df[condition.column] < condition.value
    
    return pd.Series(True, index=df.index)

def filter_excel_data(
    input_file: str,
    filter_groups: List[Union[FilterGroup, FilterCondition]],
    output_file: Optional[str] = None,
) -> pd.DataFrame:
    """
    Filter Excel file based on multiple conditions across different columns.
    
    Args:
        input_file (str): Path to the input Excel file
        filter_groups (List[Union[FilterGroup, FilterCondition]]): List of FilterGroups or FilterConditions
            - Conditions within a FilterGroup are combined with OR logic
            - Different FilterGroups or individual FilterConditions are combined with AND logic
        output_file (str, optional): Path to save filtered data. If None, data won't be saved
    
    Returns:
        pd.DataFrame: Filtered DataFrame containing only rows matching the conditions
    """
    # Validate input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Read the Excel file
    df = pd.read_excel(input_file)
    
    # Process each filter group or condition
    for group in filter_groups:
        if isinstance(group, FilterCondition):
            # Single condition - treat as AND
            if group.column not in df.columns:
                raise KeyError(f"Column '{group.column}' not found in the Excel file")
            mask = apply_filter_condition(df, group)
            df = df[mask]
        else:
            # Group of conditions - combine with OR
            group_mask = pd.Series(False, index=df.index)
            for condition in group.conditions:
                if condition.column not in df.columns:
                    raise KeyError(f"Column '{condition.column}' not found in the Excel file")
                condition_mask = apply_filter_condition(df, condition)
                group_mask = group_mask | condition_mask
            df = df[group_mask]
    
    if output_file:
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Save to a new Excel file
        df.to_excel(output_file, index=False)
        print(f"Filtered data saved to {output_file}")
    
    return df

def get_unique_values(df: pd.DataFrame, column_name: str) -> list:
    """
    Get unique values from a specific column in the DataFrame.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        column_name (str): Name of the column
    
    Returns:
        list: Sorted list of unique values
    """
    return sorted(df[column_name].unique().tolist())

if __name__ == "__main__": 
    input_file = "./spreadsheets/fse.xlsx"
    output_file = "./spreadsheets/filtered/filtered_data.xlsx"
    
    try: 
        filters = [ 
            FilterCondition(
                column='Zone/Zonal Council',
                value='6',
                condition='starts_with'
            ), 
            FilterGroup([
                FilterCondition(
                    column='Timestamp',
                    value='05/02/2025',
                    condition='date_equals',
                    date_format='%d/%m/%Y'
                ),
                FilterCondition(
                    column='Timestamp',
                    value='06/02/2025',
                    condition='date_equals',
                    date_format='%d/%m/%Y'
                ),
            ])
        ]
        
        # Filter and save the data
        filtered_data = filter_excel_data(
            input_file=input_file,
            filter_groups=filters,
            output_file=output_file
        )
        
        # Print information about the filtered data
        print(f"\nFound {len(filtered_data)} rows matching all conditions")
        print("\nFirst few rows of filtered data:")
        print(filtered_data.head())
        
        # Display unique values for specific columns
        print("\nUnique values in Zone/Zonal Council column:")
        print(get_unique_values(filtered_data, 'Zone/Zonal Council'))
        
        if 'Timestamp' in filtered_data.columns:
            print("\nUnique dates in Timestamp column:")
            print(sorted(filtered_data['Timestamp'].dt.date.unique().tolist()))
        
    except Exception as e:
        print(f"Error: {str(e)}")
