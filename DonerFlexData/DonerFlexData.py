import pandas as pd
import os
from pathlib import Path
from datetime import datetime

class DonationDataProcessor:
    def __init__(self, results_csv='donation_results.csv'):
        """
        Initialize the processor with a results CSV file.
        
        Args:
            results_csv: Path to the results CSV file
        """
        self.results_csv = results_csv
        self.results_df = self._load_or_create_results()
    
    def _load_or_create_results(self):
        """Load existing results CSV or create a new DataFrame."""
        if os.path.exists(self.results_csv):
            print(f"Loading existing results from {self.results_csv}")
            return pd.read_csv(self.results_csv)
        else:
            print(f"Creating new results file: {self.results_csv}")
            return pd.DataFrame(columns=[
                'Month_Year', 'Postcode', 'Donor Type', 
                'Total_Amount', 'Number_of_Donors'
            ])
    
    def process_excel_file(self, filepath):
        """
        Process a single Excel file and return aggregated data.
        
        Args:
            filepath: Path to the Excel file
            
        Returns:
            DataFrame with aggregated donation data
        """
        print(f"\nProcessing: {filepath}")
        
        # Read the Excel file (.xls format requires xlrd engine)
        df = pd.read_excel(filepath, engine='xlrd')
        
        # Convert Donation Date to datetime
        df['Donation Date'] = pd.to_datetime(df['Donation Date'], errors='coerce', dayfirst=True)
        
        # Create Month_Year column (format: MM/YYYY)
        df['Month_Year'] = df['Donation Date'].dt.strftime('%m/%Y')
        
        # Clean postcode (remove extra spaces)
        df['Postcode'] = df['Postcode'].str.strip()
        
        # Group by Month_Year, Postcode, and Donor Type
        grouped = df.groupby(['Month_Year', 'Postcode', 'Donor Type']).agg({
            'Donation Amount': 'sum',
            'Donor No': 'count'  # Count number of donors
        }).reset_index()
        
        source_map = df.groupby(['Month_Year', 'Postcode', 'Donor Type'])['Source'].first()
        application_map = df.groupby(['Month_Year', 'Postcode', 'Donor Type'])['Application'].first()

        grouped['Source'] = grouped.apply(lambda row: source_map.loc[(row['Month_Year'], row['Postcode'], row['Donor Type'])], axis=1)
        grouped['Application'] = grouped.apply(lambda row: application_map.loc[(row['Month_Year'], row['Postcode'], row['Donor Type'])], axis=1)
        
        # Rename columns to match results format
        grouped.columns = ['Month_Year', 'Postcode', 'Donor Type', 
                          'Total_Amount', 'Number_of_Donors', 'Source', 'Application']
        
        return grouped
    
    def merge_with_results(self, new_data):
        """
        Merge new data with existing results, combining duplicates.
        
        Args:
            new_data: DataFrame with new processed data
        """
        # Combine with existing results
        combined = pd.concat([self.results_df, new_data], ignore_index=True)
        
        # Group by Month_Year, Postcode, and Donor_Type to combine duplicates
        self.results_df = combined.groupby(
            ['Month_Year', 'Postcode', 'Donor Type'],  
            as_index=False
        ).agg({
            'Total_Amount': 'sum',
            'Number_of_Donors': 'sum',
            'Source': 'first',
            'Application': 'first'
        })

        
        # Sort by Month_Year and Postcode
        self.results_df = self.results_df.sort_values(
            ['Month_Year', 'Postcode', 'Donor Type']
        ).reset_index(drop=True)
    
    def save_results(self):
        """Save the results DataFrame to CSV."""
        self.results_df.to_csv(self.results_csv, index=False)
        print(f"\nResults saved to {self.results_csv}")
        print(f"Total records: {len(self.results_df)}")
    
    def process_multiple_files(self, file_list):
        """
        Process multiple Excel files.
        
        Args:
            file_list: List of file paths to process
        """
        for filepath in file_list:
            if not os.path.exists(filepath):
                print(f"Warning: File not found - {filepath}")
                continue
            
            try:
                new_data = self.process_excel_file(filepath)
                self.merge_with_results(new_data)
                print(f"Successfully processed {filepath}")
            except Exception as e:
                print(f"Error processing {filepath}: {str(e)}")
        
        self.save_results()
    
    def display_summary(self):
        """Display a summary of the results."""
        if len(self.results_df) == 0:
            print("\nNo data to display.")
            return
        
        print("\n" + "="*80)
        print("SUMMARY OF RESULTS")
        print("="*80)
        print(f"\nTotal unique combinations: {len(self.results_df)}")
        print(f"Date range: {self.results_df['Month_Year'].min()} to {self.results_df['Month_Year'].max()}")
        print(f"Unique postcodes: {self.results_df['Postcode'].nunique()}")
        print(f"Donor types: {', '.join(self.results_df['Donor Type'].unique())}")
        print(f"\nTotal donation amount: £{self.results_df['Total_Amount'].sum():,.2f}")
        print(f"Total number of donors: {self.results_df['Number_of_Donors'].sum():,.0f}")
        
        print("\n" + "-"*80)
        print("Sample of results (first 10 rows):")
        print("-"*80)
        print(self.results_df.head(10).to_string(index=False))


# Example usage
if __name__ == "__main__":
    # Set the directory where your data files are located
    data_directory = r"C:\Users\dirk.hoffman\OneDrive - ellenor\Documents\ellenor-catchment-area\DonerFlexData"
    # Change to the data directory
    os.chdir(data_directory)
    print(f"Working directory: {os.getcwd()}\n")
    
    # Initialize the processor (results CSV will be saved in the same directory)
    processor = DonationDataProcessor(results_csv='donation_results_2.csv')
    
    
    # Alternative: Generate file list programmatically
    # This will automatically find all matching files in the directory
    files_to_process = []
    for year in range(2021, 2026):  # 2015 to 2025
        for part in range(1, 4):  # parts 1-3
            filename = f"Donation Data {year} part {part}.xls"
            if os.path.exists(filename):
                files_to_process.append(filename)
                print(f"Found: {filename}")
    
    if not files_to_process:
        print("No files found! Please check the directory path and file names.")
    else:
        print(f"\nTotal files to process: {len(files_to_process)}\n")
        
        # Process all files
        processor.process_multiple_files(files_to_process)
        
        # Display summary
        processor.display_summary()
        
        print("\n" + "="*80)
        print("Processing complete!")
        print("="*80)