import pandas as pd
import os
import sys
import io
import shutil

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def main(session_id=None):
    """Main function to execute TCS schedule processing"""
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    from api.session_manager import session_manager
    
    # Get session information
    if session_id:
        session = session_manager.get_session(session_id)
        if not session:
            print(f"[ERROR] Session {session_id} not found")
            return
        session_dir = session["output_dir"]
    else:
        # Fallback to original paths if no session_id
        session_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'output')
    
    # Get the script's directory and build relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.join(script_dir, '..', '..')
    
    # Input file in session directory (look for TCS Schedule file)
    input_file = None
    for filename in os.listdir(session_dir):
        if 'tcs_schedule' in filename.lower():
            input_file = os.path.join(session_dir, filename)
            break
    
    if not input_file:
        print("[ERROR] No TCS Schedule file found in session directory")
        return
    
    # Template file in root/template folder
    template_file = os.path.join(root_dir, 'template', 'main_carriageway.xlsx')
    
    # Output directory and file in session directory
    output_dir = session_dir
    output_file = os.path.join(output_dir, f'main_carriageway_{session_id}.xlsx')
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Copy template file to output directory
    shutil.copy2(template_file, output_file)
    print(f"Template copied to: {output_file}")
    
    # Wait a moment to ensure file is fully copied
    import time
    time.sleep(0.5)
    
    # Read columns B to E from Excel file starting from 3rd row (row index 2)
    # header=None means don't treat any row as header, just read raw data
    try:
        df = pd.read_excel(input_file, sheet_name='TCS', skiprows=2, usecols='B:E', header=None)
    except Exception as e:
        print(f"[ERROR] Failed to read input file: {e}")
        return
    
    print(f"DataFrame shape: {df.shape}")
    print(f"DataFrame columns: {df.columns.tolist()}")
    print(f"\nRaw data preview:")
    print(df.head())
    
    # Check if dataframe is empty
    if df.empty:
        print("\nWarning: No data found after row 3 in columns B:E")
        return
    
    # Reset column names to ensure they are 0, 1, 2, 3
    df.columns = range(len(df.columns))
    
    # Convert numeric columns (first 3 columns are From, To, Length)
    df[0] = pd.to_numeric(df[0], errors='coerce')  # From (column B)
    df[1] = pd.to_numeric(df[1], errors='coerce')  # To (column C)
    df[2] = pd.to_numeric(df[2], errors='coerce')  # Length (column D)
    # df[3] is C/S Type (column E) - keep as string
    
    # Remove any rows where all values are NaN
    df_output = df.dropna(how='all')
    
    print(f"\nData after cleaning:")
    print(df_output.head())
    print(f"Total rows to write: {len(df_output)}")
    
    # Load copied workbook and write to it with retry mechanism
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Wait a moment before attempting to write
            time.sleep(0.2)
            
            with pd.ExcelWriter(output_file, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                # Write to 'Quantity' sheet starting from row 7, column A (0-indexed row 6, col 0)
                # header=False and index=False ensure only data is written
                df_output.to_excel(writer, sheet_name='Quantity', startrow=6, startcol=0, index=False, header=False)
            break
        except Exception as e:
            print(f"[WARNING] Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                print(f"[ERROR] Failed to write to output file after {max_retries} attempts")
                return
            time.sleep(0.5)
    
    print(f"\nSuccessfully wrote data to {output_file}")
    print(f"Sheet: Quantity")
    print(f"Starting from row: 7, column: A")
    print(f"Total data rows written: {len(df_output)}")

if __name__ == "__main__":
    main()
