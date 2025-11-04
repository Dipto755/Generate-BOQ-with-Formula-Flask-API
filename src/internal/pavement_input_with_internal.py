"""
Pavement Input with Internal Processor
Reads Pavement_Input.xlsx and applies internal pavement calculations
Processes geogrid and other internal pavement-related calculations
"""

import pandas as pd
import os
import sys
import io
import time
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def wait_for_file_ready(file_path, max_wait_seconds=15):
    """Wait for file to be ready for access"""
    import os
    start_time = time.time()
    
    print(f"[INFO] Waiting for file to be ready: {os.path.basename(file_path)}")
    
    while time.time() - start_time < max_wait_seconds:
        try:
            # Try to open file in read mode to check if it's ready
            with open(file_path, 'rb') as f:
                pass
            # Additional check: try to read first few bytes
            with open(file_path, 'rb') as f:
                f.read(1024)  # Try to read first 1KB
            print(f"[OK] File is ready for access: {os.path.basename(file_path)}")
            return True
        except (IOError, PermissionError) as e:
            print(f"[WAIT] File not ready, waiting... ({time.time() - start_time:.1f}s elapsed)")
            time.sleep(1.0)
        except Exception as e:
            print(f"[ERROR] Unexpected error waiting for file: {e}")
            return False
    
    print(f"[ERROR] Timeout waiting for file to be ready: {file_path}")
    return False

def safe_workbook_operation(file_path, operation_func, max_retries=5):
    """Safely perform workbook operations with enhanced retry logic"""
    import os
    from openpyxl import load_workbook
    
    for attempt in range(max_retries):
        try:
            # Wait for file to be ready
            if not wait_for_file_ready(file_path, max_wait_seconds=10):
                raise IOError(f"File not ready for access: {file_path}")
            
            print(f"[INFO] Attempting workbook operation (attempt {attempt + 1}/{max_retries})")
            
            wb = load_workbook(file_path)
            result = operation_func(wb)
            
            # Save and close with additional delay
            wb.save(file_path)
            wb.close()
            
            # Wait longer after save to ensure file system completion
            time.sleep(2.0)
            
            print(f"[OK] Workbook operation completed successfully")
            return result
            
        except Exception as e:
            print(f"[WARNING] Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                print(f"[ERROR] All attempts failed for {os.path.basename(file_path)}")
                raise e
            
            # Exponential backoff with jitter
            wait_time = (2 ** attempt) * 0.5 + (attempt * 0.1)
            print(f"[INFO] Waiting {wait_time:.1f}s before retry...")
            time.sleep(wait_time)

def main(session_id=None):
    """Main function to execute pavement input with internal processing"""
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
        session_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data')
    
    # Get the script's directory and build relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.join(script_dir, '..', '..')
    
    # Input files in session directory (look for Pavement Input file)
    pavement_input_file = None
    for filename in os.listdir(session_dir):
        if 'pavement input' in filename.lower():
            pavement_input_file = os.path.join(session_dir, filename)
            break
    
    if not pavement_input_file:
        print("[ERROR] No Pavement Input file found in session directory")
        return
    
    # Main carriageway file in session directory with session_id suffix
    main_carriageway_file = os.path.join(session_dir, f'main_carriageway_{session_id}.xlsx')
    output_file = os.path.join(session_dir, f'main_carriageway_{session_id}.xlsx')
    
    try:
        # Step 1: Process pavement input with internal calculations
        process_pavement_input_internal(pavement_input_file, main_carriageway_file, output_file)
        
        print("\n" + "="*80)
        print("SUCCESS! [OK]")
        print("="*80)
        print("Output file:", output_file)
        print("Pavement internal calculations completed")
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()

def process_pavement_input_internal(pavement_input_file, main_carriageway_file, output_file):
    """
    Reads Pavement_Input.xlsx and processes internal pavement calculations
    Updates main_carriageway.xlsx with internal pavement data
    """
    print("="*80)
    print("PAVEMENT INPUT WITH INTERNAL PROCESSOR")
    print("="*80)
    
    # Step 1: Read pavement input data
    print("\nSTEP 1: Reading Pavement Input Data")
    print("-" * 40)
    
    try:
        # Wait for pavement input file to be ready
        if not wait_for_file_ready(pavement_input_file, max_wait_seconds=10):
            raise IOError(f"Pavement input file not ready: {pavement_input_file}")
        
        # Read pavement data from Excel file
        # Assuming data is in a specific format - adjust as needed
        df_pavement = pd.read_excel(pavement_input_file, sheet_name='Pavement', header=None)
        
        print(f"[OK] Read pavement input file: {pavement_input_file}")
        print(f"[OK] Data shape: {df_pavement.shape}")
        
        # Extract layer information based on expected format
        # This is a placeholder - adjust based on actual file structure
        pavement_layers = extract_pavement_layers_internal(df_pavement)
        
    except Exception as e:
        print(f"[ERROR] Failed to read pavement input file: {e}")
        raise
    
    # Step 2: Load and update main carriageway with internal calculations
    print("\nSTEP 2: Applying Internal Pavement Calculations")
    print("-" * 40)
    
    def process_workbook(wb):
        """Inner function to process the workbook"""
        from openpyxl import load_workbook
        
        ws = wb['Quantity']
        
        print(f"[OK] Loaded workbook: {main_carriageway_file}")
        print(f"  Sheet: Quantity")
        print(f"  Max row: {ws.max_row}, Max column: {ws.max_column}")
        
        # Define column indices for internal pavement calculations
        # These are example column positions - adjust based on actual requirements
        LENGTH_COL = 3   # Column C
        
        # Example internal pavement columns (adjust as needed)
        INTERNAL_PAVEMENT_COLS = {
            'geogrid_area': 70,     # Column BR - Example geogrid area
            'geogrid_weight': 71,    # Column BS - Example geogrid weight
            'subbase_extension': 72,  # Column BT - Example subbase extension
            'base_extension': 73,      # Column BU - Example base extension
            # Add more internal columns as needed
        }
        
        # Data starts from row 7
        start_row = 7
        
        # Find last row with data
        last_row = 0
        for row_idx in range(start_row, ws.max_row + 1):
            cell_value = ws.cell(row_idx, LENGTH_COL).value
            if cell_value is not None and cell_value != '':
                last_row = row_idx
        
        if last_row == 0:
            print("[WARNING] No data found in column C")
            return 0
        
        print(f"[OK] Processing rows {start_row} to {last_row}")
        
        # Process each row
        processed_rows = 0
        for row_idx in range(start_row, last_row + 1):
            # Get length value
            length = ws.cell(row_idx, LENGTH_COL).value
            
            # Skip empty rows
            if length is None or length == 0:
                continue
            
            try:
                length = float(length)
            except (ValueError, TypeError):
                continue
            
            # Calculate internal pavement quantities
            # This is example logic - adjust based on actual requirements
            internal_quantities = calculate_internal_pavement_quantities(pavement_layers, length)
            
            # Update internal pavement columns
            for calc_type, col_idx in INTERNAL_PAVEMENT_COLS.items():
                if calc_type in internal_quantities:
                    ws.cell(row_idx, col_idx).value = internal_quantities[calc_type]
                else:
                    ws.cell(row_idx, col_idx).value = 0
            
            processed_rows += 1
            
            # Progress indicator
            if processed_rows % 200 == 0:
                print(f"  Processed {processed_rows} rows...")
        
        print(f"[OK] Processed {processed_rows} rows")
        return processed_rows
    
    # Use safe workbook operation
    try:
        return safe_workbook_operation(main_carriageway_file, process_workbook)
    except Exception as e:
        print(f"[ERROR] Failed to process main carriageway workbook: {e}")
        raise

def extract_pavement_layers_internal(df_pavement):
    """
    Extract pavement layer information from dataframe for internal calculations
    This is a placeholder function - adjust based on actual file structure
    """
    # Example implementation - replace with actual logic based on file format
    layers = {}
    
    # Example: Extract layer data for internal calculations
    # This would need to be adjusted based on your actual file structure
    
    if not df_pavement.empty:
        # Example: Look for internal calculation parameters in specific rows/columns
        try:
            # This is placeholder logic - adjust based on actual requirements
            layers['geogrid_width'] = 4.0      # meters
            layers['geogrid_overlap'] = 0.3      # meters
            layers['subbase_extension_width'] = 0.5  # meters
            layers['base_extension_width'] = 0.3      # meters
            layers['shoulder_width'] = 1.5            # meters
            
            print(f"[OK] Extracted internal pavement parameters:")
            for param, value in layers.items():
                print(f"  {param}: {value}")
                
        except Exception as e:
            print(f"[WARNING] Could not extract internal parameters: {e}")
            # Set default values
            layers = {
                'geogrid_width': 4.0,
                'geogrid_overlap': 0.3,
                'subbase_extension_width': 0.5,
                'base_extension_width': 0.3,
                'shoulder_width': 1.5
            }
    
    return layers

def calculate_internal_pavement_quantities(layers, length):
    """
    Calculate internal pavement quantities based on layer data and length
    """
    quantities = {}
    
    # Example calculation - adjust based on actual requirements
    # This assumes various widths and overlaps for internal calculations
    
    # Calculate geogrid area and weight
    if 'geogrid_width' in layers:
        geogrid_area = length * layers['geogrid_width']
        # Assuming geogrid weight of 300 g/m²
        geogrid_weight = geogrid_area * 0.3  # kg
        quantities['geogrid_area'] = geogrid_area
        quantities['geogrid_weight'] = geogrid_weight
    
    # Calculate subbase extension
    if 'subbase_extension_width' in layers:
        subbase_extension_area = length * layers['subbase_extension_width']
        quantities['subbase_extension'] = subbase_extension_area
    
    # Calculate base extension
    if 'base_extension_width' in layers:
        base_extension_area = length * layers['base_extension_width']
        quantities['base_extension'] = base_extension_area
    
    return quantities

if __name__ == "__main__":
    main()
