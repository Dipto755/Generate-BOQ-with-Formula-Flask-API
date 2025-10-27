from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from pymongo import MongoClient
import traceback
import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime, timezone
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
import openpyxl
import uuid
import re

load_dotenv()

# Configure logging
LOG_FOLDER = "logs"
SESSIONS_LOG_FOLDER = os.path.join(LOG_FOLDER, "sessions")
os.makedirs(LOG_FOLDER, exist_ok=True)
os.makedirs(SESSIONS_LOG_FOLDER, exist_ok=True)

def setup_session_logger():
    """Set up a new logger for each session with unique log file"""
    # Create session ID based on datetime
    session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create logger
    logger = logging.getLogger(f'boq_calculator_{session_id}')
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Session-specific log file
    session_log_file = os.path.join(SESSIONS_LOG_FOLDER, f'session_{session_id}.log')
    file_handler = RotatingFileHandler(
        session_log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=3
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger, session_id

# Initialize logger for the current session
logger, current_session_id = setup_session_logger()

app = Flask(__name__)
CORS(app)

# Add logger to Flask app
app.logger = logger

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[os.getenv("MONGO_DB_NAME")]

# Collections for Main Carriageway formulas
main_carriageway_formulas_collection = db["formulas"]

# Collections for input file values (separate for each file type)
tcs_input_values_collection = db["tcs_input_values"]
pavement_input_values_collection = db["pavement_input_values"]
emb_height_values_collection = db["emb_height_values"]
tcs_schedule_values_collection = db["tcs_schedule_values"]

# Collections for different types of sessions and templates
app_sessions_collection = db["app_sessions"]  # For application runtime sessions
file_sessions_collection = db["file_sessions"]  # For file upload sessions
boq_templates_collection = db["boq_templates"]
calculated_main_carriageway_collection = db["calculated_main_carriageway_results"]  # For storing main carriageway calculations

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
ALLOWED_EXTENSIONS = {"xlsx", "xls", "xlsm", "xlsb", "odf", "ods", "odt"}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def get_collection_for_file(file_key):
    """Get the appropriate MongoDB collection for a file type"""
    collections = {
        "tcs_input": tcs_input_values_collection,
        "pavement_input": pavement_input_values_collection,
        "emb_height": emb_height_values_collection,
        "tcs_schedule": tcs_schedule_values_collection
    }
    return collections.get(file_key)


def parse_cell_address(cell_address):
    """Parse Excel cell address like 'AY5' into column and row"""
    match = re.match(r'([A-Z]+)(\d+)', cell_address.upper())
    if match:
        col_letter = match.group(1)
        row_num = int(match.group(2))
        return col_letter, row_num
    return None, None


def expand_range(range_str, current_sheet=None):
    """
    Expand Excel range like 'Q73:S73' to ['Q73', 'R73', 'S73']
    """
    try:
        if ':' not in range_str:
            return [range_str]
        
        # Remove sheet prefix if present: Sheet!Q73:S73
        if '!' in range_str:
            sheet_part, range_part = range_str.split('!', 1)
            prefix = sheet_part + '!'
        else:
            range_part = range_str
            prefix = ''
        
        # Parse start and end cells
        start_cell, end_cell = range_part.split(':')
        
        # Remove $ signs
        start_cell = start_cell.replace('$', '')
        end_cell = end_cell.replace('$', '')
        
        # Extract column and row
        start_col = ''.join(filter(str.isalpha, start_cell))
        start_row = int(''.join(filter(str.isdigit, start_cell)))
        end_col = ''.join(filter(str.isalpha, end_cell))
        end_row = int(''.join(filter(str.isdigit, end_cell)))
        
        # Convert column letters to numbers
        def col_to_num(col):
            num = 0
            for char in col:
                num = num * 26 + (ord(char.upper()) - ord('A') + 1)
            return num
        
        def num_to_col(num):
            col = ''
            while num > 0:
                num -= 1
                col = chr(num % 26 + ord('A')) + col
                num //= 26
            return col
        
        start_col_num = col_to_num(start_col)
        end_col_num = col_to_num(end_col)
        
        # Generate all cells in range
        cells = []
        for row in range(start_row, end_row + 1):
            for col_num in range(start_col_num, end_col_num + 1):
                col = num_to_col(col_num)
                cells.append(f"{prefix}{col}{row}")
        
        return cells
        
    except Exception as e:
        logger.error(f"Error expanding range '{range_str}': {e}")
        return [range_str]


def get_cell_value_from_db(session_id, file_name, sheet_name, cell_address, collection):
    """Retrieve cell value from MongoDB"""
    try:
        cell_doc = collection.find_one({
            "session_id": session_id,
            "file_name": file_name,
            "sheet": sheet_name,
            "cell": cell_address
        })
        
        if cell_doc:
            return cell_doc.get("value")
        return 0
    except Exception as e:
        print(f"Error retrieving cell value: {e}")
        return None


def col_letter_to_index(col):
    idx = 0
    for c in col:
        idx = idx * 26 + (ord(c) - ord('A') + 1)
    return idx


def index_to_col_letter(index):
    letters = ""
    while index > 0:
        index, rem = divmod(index - 1, 26)
        letters = chr(rem + ord('A')) + letters
    return letters


def generate_cells_in_range(start_cell, end_cell):
    """Generate cell coordinates from start to end (e.g., C3 -> C44 or A1 -> D4)"""
    start_col, start_row = parse_cell_address(start_cell)
    end_col, end_row = parse_cell_address(end_cell)
    if not start_col or not end_col:
        return []
    start_idx = col_letter_to_index(start_col)
    end_idx = col_letter_to_index(end_col)
    cells = []
    for col_idx in range(start_idx, end_idx + 1):
        col = index_to_col_letter(col_idx)
        for row in range(start_row, end_row + 1):
            cells.append(f"{col}{row}")
    return cells


def get_collection_and_filename_from_name(file_name):
    """Map a file name string to the collection and canonical file_name used in DB"""
    name = file_name.lower()
    if "pavement" in name:
        return pavement_input_values_collection, "Pavement Input.xlsx"
    if "tcs" in name and "schedule" not in name:
        return tcs_input_values_collection, "TCS Input.xlsx"
    if "emb" in name or "height" in name:
        return emb_height_values_collection, "Emb Height.xlsx"
    if "schedule" in name:
        return tcs_schedule_values_collection, "TCS Schedule.xlsx"
    return None, None


def get_range_values_from_db(session_id, file_name, sheet_name, start_cell, end_cell):
    """Retrieve a list of values for cells in a range from the appropriate collection"""
    collection, file_key = get_collection_and_filename_from_name(file_name)
    if collection is None:
        logger.debug(f"No collection mapping for file: {file_name}")
        return []
    cells = generate_cells_in_range(start_cell, end_cell)
    values = []
    for c in cells:
        v = get_cell_value_from_db(session_id, file_key, sheet_name, c, collection)
        values.append(v)
    return values


def evaluate_lookup_function(formula, session_id, current_sheet=None):
    """
    Very small implementation for LOOKUP(lookup_value, lookup_range, result_range)
    Supports cross-file ranges and local cell refs for lookup_value (prefixed with current_sheet if needed).
    Performs exact-match lookup returning corresponding result_range value.
    """
    try:
        # Extract content inside LOOKUP(...)
        match = re.match(r'LOOKUP\((.*)\)', formula, re.IGNORECASE)
        if not match:
            logger.debug("LOOKUP pattern not matched")
            return None
        content = match.group(1)
        parts = split_formula_parts(content)
        if len(parts) < 3:
            logger.debug("LOOKUP requires 3 arguments")
            return None

        lookup_value_str = parts[0].strip()
        lookup_range_str = parts[1].strip()
        result_range_str = parts[2].strip()
        
        print(f"LOOKUP args --> lookup value str: {lookup_value_str}, lookup range str: {lookup_range_str}, result range str: {result_range_str}")

        # Resolve lookup value (could be a cell reference or literal)
        # If it's a simple cell like $D7 or D7 and no sheet provided, prefix with current_sheet
        if re.match(r'^\$?[A-Z]{1,3}\$?\d+$', lookup_value_str) and '!' not in lookup_value_str:
            if not current_sheet:
                logger.debug("No current_sheet provided for relative cell ref in LOOKUP")
                return None
            lookup_value = resolve_cell_reference(f"{current_sheet}!{lookup_value_str}", session_id)
            print(f"``````LOOKUP relative cell ref resolved to: {lookup_value}")
        else:
            # Use existing resolver to handle quoted strings or cross-file refs
            lookup_value = (
                resolve_value(lookup_value_str, session_id)
                if '!' not in lookup_value_str
                else resolve_cell_reference(lookup_value_str, session_id)
            )

        logger.debug(f"LOOKUP value resolved to: {lookup_value}")
        print("_________________________ Lookup value:", lookup_value)

        # Helper to parse a range like '[TCS Input.xlsx]Input!$C$3:$C$44' or 'Input!C3:C44'
        def parse_range_str(range_str):
            # Cross-file?
            if range_str.startswith("'[") or range_str.startswith("["):
                m = re.match(r"'?\[([^\]]+)\]'?(.+)", range_str)
                if not m:
                    return None
                file_name = m.group(1)
                rest = m.group(2)
                if '!' not in rest:
                    return None
                sheet_part, cells_part = rest.split('!', 1)
                sheet_name = sheet_part.strip("'")
            else:
                # local sheet included?
                if '!' in range_str:
                    sheet_part, cells_part = range_str.split('!', 1)
                    sheet_name = sheet_part.strip("'")
                    file_name = None
                else:
                    # No sheet/file — not supported for ranges
                    return None
            cells_part = cells_part.replace('$', '')
            if ':' not in cells_part:
                return None
            start_cell, end_cell = cells_part.split(':', 1)
            return file_name, sheet_name, start_cell.strip(), end_cell.strip()

        lookup_parsed = parse_range_str(lookup_range_str)
        result_parsed = parse_range_str(result_range_str)
        if not lookup_parsed or not result_parsed:
            logger.debug("Failed to parse LOOKUP ranges")
            return None

        lookup_file, lookup_sheet, lookup_start, lookup_end = lookup_parsed
        result_file, result_sheet, result_start, result_end = result_parsed

        # If ranges come from files, use DB; otherwise fail
        if lookup_file:
            lookup_values = get_range_values_from_db(session_id, lookup_file, lookup_sheet, lookup_start, lookup_end)
        else:
            logger.debug("Local range lookups not implemented")
            lookup_values = []

        if result_file:
            result_values = get_range_values_from_db(session_id, result_file, result_sheet, result_start, result_end)
        else:
            logger.debug("Local result ranges not implemented")
            result_values = []

        # Perform exact match lookup
        for idx, lv in enumerate(lookup_values):
            if lv == lookup_value:
                # Return corresponding result value if exists
                if idx < len(result_values):
                    logger.debug(f"LOOKUP matched at index {idx}: returning {result_values[idx]}")
                    return result_values[idx]
                break

        logger.debug("LOOKUP did not find a match")
        return None
    except Exception as e:
        logger.error(f"Error evaluating LOOKUP: {str(e)}", exc_info=True)
        return None


def evaluate_excel_formula(formula, session_id, current_sheet=None):
    """
    Evaluate Excel formula by resolving cell references and functions
    
    Supports:
    - Direct cell references: =Quantity!$GY$5091
    - Cross-file references: ='[Pavement input.xlsx]'Summary!$E$9
    - IF functions: =IF(condition, true_value, false_value)
    - LOOKUP: =LOOKUP(lookup_value, lookup_range, result_range)
    - Basic arithmetic: +, -, *, /
    """
    try:
        logger.info(f"Starting formula evaluation. Formula: {formula}, Session ID: {session_id}, Sheet: {current_sheet}")
        print(f"📝 Processing formula: {formula}")

        # Remove leading '=' if present
        formula = formula.strip()
        if formula.startswith('='):
            formula = formula[1:]
            logger.debug("Removed leading '=' from formula")
        
        # Handle IF function
        # But first check if there's arithmetic after the IF (like IF(...)/1000)
        if formula.upper().startswith('IF('):
            # Check if there are arithmetic operations outside the IF function
            # Find the closing parenthesis of the IF function
            try:
                paren_count = 0
                if_end_index = -1
                for i, char in enumerate(formula):
                    if char == '(':
                        paren_count += 1
                    elif char == ')':
                        paren_count -= 1
                        if paren_count == 0:
                            if_end_index = i
                            break
                
                # Check if there's anything after the IF function closes
                if if_end_index > 0 and if_end_index < len(formula) - 1:
                    remaining = formula[if_end_index + 1:].strip()
                    if remaining and re.search(r'^[+\-*/]', remaining):
                        # There's arithmetic after the IF: IF(...)/1000
                        print(f"🔄 IF function has arithmetic after it: {remaining}")
                        logger.info("IF function has arithmetic operations after it, evaluating IF first")
                        
                        # Extract just the IF part
                        if_part = formula[:if_end_index + 1]
                        
                        # Evaluate IF function first
                        if_result = evaluate_if_function(if_part, session_id, current_sheet=current_sheet)
                        
                        if if_result is not None:
                            # Now do the arithmetic on the result
                            try:
                                result = eval(f"{if_result}{remaining}", {"__builtins__": {}}, {})
                                logger.info(f"IF with arithmetic evaluated. Result: {result}")
                                print(f"✅ IF with arithmetic result: {result}")
                                return result
                            except Exception as e:
                                logger.error(f"Error in arithmetic after IF: {e}")
                                return None
                        else:
                            logger.warning("IF function returned None, cannot do arithmetic")
                            return None
                    else:
                        # Pure IF function without trailing arithmetic
                        logger.info("Detected IF function, delegating to evaluate_if_function")
                        print("🔄 Processing IF function...")
                        result = evaluate_if_function(formula, session_id, current_sheet=current_sheet)
                        if result is not None:
                            logger.info(f"IF function evaluated successfully. Result: {result}")
                            print(f"✅ IF function result: {result}")
                        else:
                            logger.warning("IF function evaluation returned None")
                            print("⚠️ IF function evaluation failed")
                        return result
                else:
                    # Pure IF function
                    logger.info("Detected IF function, delegating to evaluate_if_function")
                    print("🔄 Processing IF function...")
                    result = evaluate_if_function(formula, session_id, current_sheet=current_sheet)
                    if result is not None:
                        logger.info(f"IF function evaluated successfully. Result: {result}")
                        print(f"✅ IF function result: {result}")
                    else:
                        logger.warning("IF function evaluation returned None")
                        print("⚠️ IF function evaluation failed")
                    return result
            except Exception as e:
                logger.error(f"Error parsing IF function: {e}")
                # Fall through to arithmetic handling

        # Handle LOOKUP function
        if formula.upper().startswith('LOOKUP('):
            logger.info("Detected LOOKUP function, delegating to evaluate_lookup_function")
            print("🔄 Processing LOOKUP function...")
            result = evaluate_lookup_function(formula, session_id, current_sheet=current_sheet)
            if result is not None:
                logger.info(f"LOOKUP function result: {result}")
                print(f"✅ LOOKUP result: {result}")
            else:
                logger.warning("LOOKUP evaluation returned None")
                print("⚠️ LOOKUP evaluation failed")
            return result
        
        # Handle ROUNDUP function
        if formula.upper().startswith('ROUNDUP('):
            logger.info("Detected ROUNDUP function")
            print("🔄 Processing ROUNDUP function...")
            result = evaluate_roundup_function(formula, session_id, current_sheet=current_sheet)
            if result is not None:
                logger.info(f"ROUNDUP function result: {result}")
                print(f"✅ ROUNDUP result: {result}")
            return result

        # Handle SQRT function
        if formula.upper().startswith('SQRT('):
            logger.info("Detected SQRT function")
            print("🔄 Processing SQRT function...")
            result = evaluate_sqrt_function(formula, session_id, current_sheet=current_sheet)
            if result is not None:
                logger.info(f"SQRT function result: {result}")
                print(f"✅ SQRT result: {result}")
            return result
        
        
        # Handle complex expressions with SUM/AVERAGE and arithmetic
        # Replace SUM(...) and AVERAGE(...) calls with their evaluated results before arithmetic
        temp_formula = formula
        sum_pattern = r'SUM\([^)]+\)'
        avg_pattern = r'AVERAGE\([^)]+\)'

        def replace_sum(match):
            sum_result = evaluate_sum_function(match.group(0), session_id, current_sheet=current_sheet)
            return str(sum_result) if sum_result is not None else match.group(0)

        def replace_avg(match):
            avg_result = evaluate_average_function(match.group(0), session_id, current_sheet=current_sheet)
            return str(avg_result) if avg_result is not None else match.group(0)

        # Replace SUM functions
        import re as regex_module
        temp_formula = regex_module.sub(sum_pattern, replace_sum, temp_formula, flags=regex_module.IGNORECASE)

        # Replace AVERAGE functions
        temp_formula = regex_module.sub(avg_pattern, replace_avg, temp_formula, flags=regex_module.IGNORECASE)

        # If formula was modified, use the modified version for further processing
        if temp_formula != formula:
            formula = temp_formula
            logger.info(f"Formula after SUM/AVERAGE evaluation: {formula}")
            print(f"🔄 Formula after function evaluation: {formula}")
        
        # Handle direct cell reference (e.g., Quantity!$GY$5091)
        if '!' in formula and not any(op in formula for op in ['+', '-', '*', '/', '(', ')']):
            logger.info(f"Detected direct cell reference: {formula}")
            print(f"🔄 Resolving cell reference: {formula}")
            result = resolve_cell_reference(formula, session_id)
            if result is not None:
                logger.info(f"Cell reference resolved. Value: {result}")
                print(f"✅ Cell value retrieved: {result}")
            else:
                logger.warning(f"Failed to resolve cell reference: {formula}")
                print("⚠️ Cell reference resolution failed")
            return result
        
        # Handle arithmetic expressions
        # Replace cell references with their values
        logger.info("Processing arithmetic expression")
        print("🔄 Processing arithmetic expression...")
        
        # Resolve all cell references in the formula
        # Pass current_sheet so plain cell refs like $D7 get prefixed correctly
        # But cross-file/sheet refs like '[File]Sheet'!A1 won't be modified
        resolved_formula = resolve_all_cell_references(formula, session_id, current_sheet=current_sheet)
        logger.debug(f"Resolved formula after replacing cell references: {resolved_formula}")
        print(f"🔍 Final resolved formula: {resolved_formula}")
        
        # Evaluate the expression safely
        try:
            result = safe_eval(resolved_formula)
            if result is not None:
                logger.info(f"Formula evaluated successfully. Result: {result}")
                print(f"✅ Formula result: {result}")
            else:
                logger.warning("Formula evaluation returned None")
                print("⚠️ Formula evaluation failed")
            return result
        except Exception as e:
            logger.error(f"Error in safe_eval: {str(e)}", exc_info=True)
            print(f"❌ Error evaluating expression: {str(e)}")
            return None
            
    except Exception as e:
        logger.error(f"Error evaluating formula '{formula}': {str(e)}", exc_info=True)
        print(f"❌ Formula evaluation failed: {str(e)}")
        return None


def evaluate_if_function(formula, session_id, current_sheet=None):
    """Evaluate Excel IF function: IF(condition, true_value, false_value)

    Accepts optional current_sheet so relative references inside the IF (including OR/AND)
    are resolved against the correct sheet.
    """
    try:
        # Extract the content inside IF()
        match = re.match(r'IF\((.*)\)', formula, re.IGNORECASE)
        if not match:
            return None
        
        content = match.group(1)
        print("***************************** IF function content:", content)
        
        # Split by commas (careful with nested functions)
        parts = split_formula_parts(content)
        
        if len(parts) != 3:
            return None
        
        condition_str, true_value_str, false_value_str = parts
        
        print("---------------- IF parts ----------------")
        print(f"Condition str --> {condition_str}")
        print(f"True value str --> {true_value_str}")
        print(f"False value str --> {false_value_str}")
        
        # Resolve condition
        condition_str = resolve_all_cell_references(condition_str.strip(), session_id, current_sheet=current_sheet)

        # Check if condition contains OR function
        if condition_str.upper().startswith('OR('):
            condition_result = evaluate_or_function(condition_str, session_id)
        else:
            # Evaluate condition
            condition_result = evaluate_condition(condition_str)
        
        # Choose branch
        branch_raw = true_value_str.strip() if condition_result else false_value_str.strip()
        
        # Remove surrounding parentheses if present
        if branch_raw.startswith('(') and branch_raw.endswith(')'):
            branch_raw = branch_raw[1:-1].strip()
        
        # Check if branch contains IF or other Excel functions with arithmetic operations
        # Example: IF(...)/1000 or IF(...)+5 or (IF(...)*2)
        if re.search(r'\b(IF|OR|AND|LOOKUP)\s*\(', branch_raw, re.IGNORECASE):
            # Check if there are arithmetic operations after the function
            # This handles cases like: IF(...)/1000
            if re.search(r'[+\-*/]', branch_raw):
                print("🔄 Branch contains Excel functions with arithmetic, evaluating through evaluate_excel_formula")
                return evaluate_excel_formula(branch_raw, session_id, current_sheet=current_sheet)
            # If branch is just a nested IF without arithmetic
            elif branch_raw.upper().startswith('IF('):
                print("🔄 Branch is nested IF, evaluating recursively")
                return evaluate_if_function(branch_raw, session_id, current_sheet=current_sheet)
            # Other functions (OR, AND, LOOKUP) without arithmetic
            else:
                print("🔄 Branch contains Excel functions, evaluating through evaluate_excel_formula")
                return evaluate_excel_formula(branch_raw, session_id, current_sheet=current_sheet)
        
        # Resolve any cell references inside the branch (with sheet context)
        branch_expr = resolve_all_cell_references(branch_raw, session_id, current_sheet=current_sheet).strip()
        
        # After resolving cell references, check again if there are still Excel functions
        # (this handles cases where functions are constructed after resolution)
        if re.search(r'\b(IF|OR|AND|LOOKUP)\s*\(', branch_expr, re.IGNORECASE):
            print("🔄 Resolved branch still contains Excel functions, evaluating recursively")
            return evaluate_excel_formula(branch_expr, session_id, current_sheet=current_sheet)
        
        # If branch contains arithmetic or parentheses, try safe_eval
        if re.search(r'[+\-*/()]', branch_expr):
            try:
                val = safe_eval(branch_expr)
                if val is not None:
                    return val
            except Exception:
                logger.debug(f"safe_eval failed for branch expression: {branch_expr}")
            # fallback to resolve_value of original branch (in case of strings)
            return resolve_value(branch_expr, session_id, current_sheet)
        
        # No arithmetic — resolve as value (literal, cell ref resolved already)
        return resolve_value(branch_expr, session_id, current_sheet)
            
    except Exception as e:
        print(f"Error evaluating IF function: {e}")
        logger.error(f"Error evaluating IF function: {e}", exc_info=True)
        return None
    
def evaluate_or_function(formula, session_id):
    """Evaluate Excel OR function: OR(condition1, condition2, ...)"""
    try:
        match = re.match(r'OR\((.*)\)', formula, re.IGNORECASE)
        if not match:
            return None
        
        content = match.group(1)
        parts = split_formula_parts(content)
        
        # Evaluate each condition
        for part in parts:
            condition_str = resolve_all_cell_references(part.strip(), session_id)
            if evaluate_condition(condition_str):
                return True
        
        return False
    except Exception as e:
        logger.error(f"Error evaluating OR function: {e}")
        return False
    

def evaluate_sum_function(formula, session_id, current_sheet=None):
    """Evaluate Excel SUM function: SUM(range) or SUM(val1, val2, ...)"""
    try:
        match = re.match(r'SUM\(((?:[^()]+|\([^()]*\))*)\)', formula, re.IGNORECASE)
        if not match:
            return None

        content = match.group(1)  # Now group(1) exists!
        
        print("############################################ SUM function content:", content)
        
        # Check if it's a range or individual values
        parts = split_formula_parts(content)
        
        total = 0
        for part in parts:
            part = part.strip()
            
            # Check if it's a range
            if ':' in part:
                # Expand range
                cells = expand_range(part, current_sheet)
                
                # Sum all cells in range
                for cell in cells:
                    # Resolve cell reference
                    value = resolve_cell_reference(cell, session_id, current_sheet)
                    if value is not None:
                        try:
                            total += float(value)
                        except (ValueError, TypeError):
                            pass
            else:
                # Single cell or value
                resolved = resolve_all_cell_references(part, session_id, current_sheet)
                try:
                    total += float(resolved)
                except (ValueError, TypeError):
                    pass
        
        return total
        
    except Exception as e:
        logger.error(f"Error evaluating SUM function: {e}")
        return None
    
    
def evaluate_average_function(formula, session_id, current_sheet=None):
    """Evaluate Excel AVERAGE function: AVERAGE(range) or AVERAGE(val1, val2, ...)"""
    try:
        match = re.match(r'AVERAGE\((.*)\)', formula, re.IGNORECASE)
        if not match:
            return None
        
        content = match.group(1)
        parts = split_formula_parts(content)
        
        values = []
        for part in parts:
            part = part.strip()
            
            # Check if it's a range
            if ':' in part:
                cells = expand_range(part, current_sheet)
                
                for cell in cells:
                    value = resolve_cell_reference(cell, session_id, current_sheet)
                    if value is not None:
                        try:
                            values.append(float(value))
                        except (ValueError, TypeError):
                            pass
            else:
                # Single cell or value
                resolved = resolve_all_cell_references(part, session_id, current_sheet)
                try:
                    values.append(float(resolved))
                except (ValueError, TypeError):
                    pass
        
        if len(values) == 0:
            return 0
        
        return sum(values) / len(values)
        
    except Exception as e:
        logger.error(f"Error evaluating AVERAGE function: {e}")
        return None


def evaluate_roundup_function(formula, session_id, current_sheet=None):
    """Evaluate Excel ROUNDUP function: ROUNDUP(number, num_digits)"""
    try:
        match = re.match(r'ROUNDUP\((.*)\)', formula, re.IGNORECASE)
        if not match:
            return None
        
        content = match.group(1)
        parts = split_formula_parts(content)
        
        print(f"^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ROUNDUP function got {len(parts)}  <<<<----- parts:", parts)
        
        if len(parts) != 2:
            return None
        
        number_str, digits_str = parts
        
        # Resolve and evaluate the number part (might contain nested formulas)
        number_resolved = resolve_all_cell_references(number_str.strip(), session_id, current_sheet)
        
        # Check if it contains nested functions
        if any(func in number_resolved.upper() for func in ['IF(', 'SQRT(', 'SUM(', 'AVERAGE(']):
            number = evaluate_excel_formula(number_resolved, session_id, current_sheet)
        else:
            number = safe_eval(number_resolved)
        
        # Resolve digits
        digits_resolved = resolve_all_cell_references(digits_str.strip(), session_id, current_sheet)
        digits = safe_eval(digits_resolved)
        
        if number is None or digits is None:
            return None
        
        import math
        multiplier = 10 ** int(digits)
        return math.ceil(float(number) * multiplier) / multiplier
        
    except Exception as e:
        logger.error(f"Error evaluating ROUNDUP function: {e}")
        return None


def evaluate_sqrt_function(formula, session_id, current_sheet=None):
    """Evaluate Excel SQRT function: SQRT(number)"""
    try:
        match = re.match(r'SQRT\((.*)\)', formula, re.IGNORECASE)
        if not match:
            return None
        
        content = match.group(1)
        
        # Resolve cell references
        resolved = resolve_all_cell_references(content.strip(), session_id, current_sheet)
        
        # Evaluate the expression
        number = safe_eval(resolved)
        
        if number is None:
            return None
        
        import math
        return math.sqrt(float(number))
        
    except Exception as e:
        logger.error(f"Error evaluating SQRT function: {e}")
        return None


def split_formula_parts(content):
    """Split formula by commas, respecting nested parentheses and quotes"""
    parts = []
    current_part = ""
    paren_depth = 0
    in_quotes = False
    quote_char = None
    
    for char in content:
        if char in ['"', "'"]:
            if not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char:
                in_quotes = False
                quote_char = None
            current_part += char
        elif char == '(' and not in_quotes:
            paren_depth += 1
            current_part += char
        elif char == ')' and not in_quotes:
            paren_depth -= 1
            current_part += char
        elif char == ',' and paren_depth == 0 and not in_quotes:
            parts.append(current_part)
            current_part = ""
        else:
            current_part += char
    
    if current_part:
        parts.append(current_part)
    
    return parts


def evaluate_condition(condition_str):
    """Evaluate a condition string (supports OR(...), AND(...), basic comparisons).

    Assumes any cell references have been resolved beforehand (strings or numbers).
    Supports nested OR/AND calls and simple comparisons for strings and numbers.
    """
    try:
        s = condition_str.strip()

        # Support OR(...) and AND(...)
        upper = s.upper()
        if upper.startswith('OR(') and s.endswith(')'):
            inner = s[s.find('(') + 1:-1]
            parts = split_formula_parts(inner)
            for part in parts:
                if evaluate_condition(part.strip()):
                    return True
            return False

        if upper.startswith('AND(') and s.endswith(')'):
            inner = s[s.find('(') + 1:-1]
            parts = split_formula_parts(inner)
            for part in parts:
                if not evaluate_condition(part.strip()):
                    return False
            return True

        # Unwrap surrounding parentheses
        if s.startswith('(') and s.endswith(')'):
            return evaluate_condition(s[1:-1].strip())

        # Handle string equality using single '=' as in Excel
        if '=' in s and '==' not in s:
            parts = s.split('=', 1)
            if len(parts) == 2:
                left = parts[0].strip().strip('"').strip("'")
                right = parts[1].strip().strip('"').strip("'")
                return left == right

        # Handle numeric comparisons in order to avoid prefix issues
        for op in ['>=', '<=', '!=', '>', '<']:
            if op in s:
                parts = s.split(op, 1)
                if len(parts) == 2:
                    left_str = parts[0].strip().strip('"').strip("'")
                    right_str = parts[1].strip().strip('"').strip("'")
                    try:
                        left = float(left_str)
                        right = float(right_str)
                        if op == '>=':
                            return left >= right
                        if op == '<=':
                            return left <= right
                        if op == '>':
                            return left > right
                        if op == '<':
                            return left < right
                        if op == '!=':
                            return left != right
                    except Exception:
                        logger.debug(f"Could not convert to float in condition: '{left_str}' or '{right_str}'")
                        return False

        # Boolean literals
        if s.upper() in ('TRUE', 'FALSE'):
            return s.upper() == 'TRUE'

        # Numeric truthiness (non-zero is True)
        try:
            num = float(s)
            return num != 0
        except Exception:
            logger.debug(f"Unable to evaluate condition: '{s}'")
            return False

    except Exception as e:
        logger.error(f"Error evaluating condition: {e}", exc_info=True)
        return False


def resolve_value(value_str, session_id, current_sheet=None):
    """Resolve a value (could be string literal, cell reference, or number)"""
    value_str = value_str.strip()
    
    # String literal
    if (value_str.startswith('"') and value_str.endswith('"')) or \
       (value_str.startswith("'") and value_str.endswith("'")):
        return value_str[1:-1]
    
    # Cell reference
    if '!' in value_str:
        return resolve_cell_reference(value_str, session_id, current_sheet)
    
    # Number
    try:
        return float(value_str)
    except ValueError as e:
        logger.debug(f"Value {value_str} could not be converted to float: {str(e)}")
        return value_str


def resolve_cell_reference(cell_ref, session_id, current_sheet=None):
    """
    Resolve a cell reference to its value
    Formats supported:
    - Quantity!$GY$5091 (same file, different sheet)
    - '[Pavement input.xlsx]'Summary!$E$9 (cross-file reference)
    """
    
    # If it's a plain cell reference (no sheet specified) and current_sheet is provided
    if '!' not in cell_ref and current_sheet:
        cell_ref = f"{current_sheet}!{cell_ref}"
    
    try:
        cell_ref = cell_ref.strip()
        
        # Check if it's a cross-file reference
        if cell_ref.startswith("'[") or cell_ref.startswith("["):
            # Extract file name
            match = re.match(r"'?\[([^\]]+)\]'?(.+)", cell_ref)
            if match:
                file_name = match.group(1)
                rest = match.group(2)
                print("---------------------------", file_name, rest)
                
                # Parse sheet and cell
                if '!' in rest:
                    sheet_and_cell = rest.split('!', 1)
                    sheet_name = sheet_and_cell[0].strip("'")
                    cell_address = sheet_and_cell[1].strip().replace('$', '')
                    
                    # Map file name to collection
                    collection = None
                    file_key = None
                    if "pavement" in file_name.lower():
                        collection = pavement_input_values_collection
                        file_key = "Pavement Input.xlsx"
                    elif "tcs" in file_name.lower() and "schedule" not in file_name.lower():
                        collection = tcs_input_values_collection
                        file_key = "TCS Input.xlsx"
                    elif "emb" in file_name.lower() or "height" in file_name.lower():
                        collection = emb_height_values_collection
                        file_key = "Emb Height.xlsx"
                    elif "schedule" in file_name.lower():
                        collection = tcs_schedule_values_collection
                        file_key = "TCS Schedule.xlsx"
                    
                    print("=====================", collection, file_key)
                    if collection is not None:
                        value = get_cell_value_from_db(session_id, file_key, sheet_name, cell_address, collection)
                        return value
        else:
            # Same-file reference (Main Carriageway)
            if '!' in cell_ref:
                parts = cell_ref.split('!', 1)
                sheet_name = parts[0].strip("'")
                cell_address = parts[1].strip().replace('$', '')
                
                # Look up in Main Carriageway formulas collection
                formula_doc = main_carriageway_formulas_collection.find_one({
                    "file_name": "Main Carriageway.xlsx",
                    "sheet": sheet_name,
                    "cell": cell_address
                })

                if formula_doc:
                    # Check if it's a formula or a value
                    if formula_doc.get("is_formula"):
                        # It's a formula - evaluate it recursively
                        formula = formula_doc.get("formula")
                        if formula:
                            logger.debug(f"Cell {cell_address} contains formula: {formula}")
                            print(f"Formula doc found for {current_sheet}!{cell_address}: {formula_doc}")
                            return evaluate_excel_formula(formula, session_id, current_sheet=sheet_name)
                    else:
                        # It's a value - return directly
                        value = formula_doc.get("value")
                        logger.debug(f"Cell {cell_address} contains value: {value}")
                        print(f"Value doc found for {current_sheet}!{cell_address}: value={value}")
                        return value
                
        return None
    except Exception as e:
        print(f"Error resolving cell reference '{cell_ref}': {e}")
        return None


def resolve_all_cell_references(formula, session_id, current_sheet=None):
    """Replace all cell references in a formula with their values.

    Supports:
      - '[File.xlsx]Sheet'!$A$1 (cross-file reference)
      - 'Sheet Name'!A1 (cross-sheet reference)
      - Sheet!A1 (same-file cross-sheet)
      - plain A1 or $A$1 (will be prefixed with current_sheet if provided)
    """
    # Match cell references with proper context awareness
    # This pattern matches:
    # 1. Cross-file: '[File.xlsx]Sheet'!A1 or '[File.xlsx]'Sheet'!A1
    # 2. Cross-sheet: 'Sheet Name'!A1 or Sheet!A1
    # 3. Plain cells: A1 or $A$1
    pattern = r"(?:'?\[[^\]]+\]'?)?(?:'[^']+'|[A-Za-z0-9_ ]+)?!\$?[A-Z]{1,3}\$?\d+|\$?[A-Z]{1,3}\$?\d+"

    def replacer(match):
        token = match.group(0)
        ref = token
        
        # Check if this is already a qualified reference (contains '!')
        if '!' in token:
            # It's already qualified (cross-file or cross-sheet), use as-is
            ref = token
            print(f"  📎 Qualified reference: {token}")
        elif current_sheet:
            # Plain cell reference - prefix with current_sheet
            ref = f"{current_sheet}!{token}"
            print(f"  📎 Plain cell {token} -> prefixed as {ref}")
        else:
            # Plain cell but no current_sheet - can't resolve
            print(f"  ⚠️ Plain cell {token} but no current_sheet")
            return token
        
        # Try to resolve the reference
        value = resolve_cell_reference(ref, session_id)
        if value is not None:
            print(f"  ✅ Resolved {ref} = {value}")
            # Keep strings quoted so subsequent parsing works
            if isinstance(value, str):
                return f'"{value}"'
            return str(value)
        
        # If not resolvable, return original token unchanged
        print(f"  ❌ Could not resolve {ref}")
        return token

    print(f"🔍 Resolving references in: {formula}")
    resolved = re.sub(pattern, replacer, formula)
    print(f"🔍 After resolution: {resolved}")
    logger.debug(f"resolve_all_cell_references: '{formula}' -> '{resolved}'")
    # Check if resolved formula contains SUM or AVERAGE
    if 'SUM(' in resolved.upper():
        # Extract and evaluate SUM
        sum_pattern = r'SUM\([^)]+\)'
        def replace_sum(match):
            result = evaluate_sum_function(match.group(0), session_id, current_sheet)
            return str(result) if result is not None else match.group(0)
        resolved = re.sub(sum_pattern, replace_sum, resolved, flags=re.IGNORECASE)

    if 'AVERAGE(' in resolved.upper():
        # Extract and evaluate AVERAGE
        avg_pattern = r'AVERAGE\([^)]+\)'
        def replace_avg(match):
            result = evaluate_average_function(match.group(0), session_id, current_sheet)
            return str(result) if result is not None else match.group(0)
        resolved = re.sub(avg_pattern, replace_avg, resolved, flags=re.IGNORECASE)
        
    # Handle ROUNDUP
    if 'ROUNDUP(' in resolved.upper():
        def replace_roundup(match):
            result = evaluate_roundup_function(match.group(0), session_id, current_sheet)
            return str(result) if result is not None else match.group(0)
        resolved = re.sub(r'ROUNDUP\((?:[^()]+|\([^()]*\))*\)', replace_roundup, resolved, flags=re.IGNORECASE)

    # Handle SQRT (with nested parentheses support)
    if 'SQRT(' in resolved.upper():
        def replace_sqrt(match):
            result = evaluate_sqrt_function(match.group(0), session_id, current_sheet)
            return str(result) if result is not None else match.group(0)
        resolved = re.sub(r'SQRT\((?:[^()]+|\([^()]*\))*\)', replace_sqrt, resolved, flags=re.IGNORECASE)
        
    # Handle IF functions embedded in expressions
    if 'IF(' in resolved.upper():
        def replace_if(match):
            if_formula = match.group(0)
            print(f"  🔢 Found nested IF function: {if_formula}")
            result = evaluate_if_function(if_formula, session_id, current_sheet)
            return str(result) if result is not None else if_formula
        # Pattern to match IF with nested content
        resolved = re.sub(r'IF\((?:[^()]+|\((?:[^()]+|\([^()]*\))*\))*\)', replace_if, resolved, flags=re.IGNORECASE)
        print(f"🔍 After IF replacement: {resolved}")

    # Handle OR functions embedded in expressions  
    if 'OR(' in resolved.upper():
        def replace_or(match):
            or_formula = match.group(0)
            print(f"  🔢 Found nested OR function: {or_formula}")
            result = evaluate_or_function(or_formula, session_id, current_sheet)
            return str(result) if result is not None else or_formula
        resolved = re.sub(r'OR\((?:[^()]+|\([^()]*\))*\)', replace_or, resolved, flags=re.IGNORECASE)
        print(f"🔍 After OR replacement: {resolved}")


    return resolved


def safe_eval(expression):
    """Safely evaluate a mathematical expression"""
    try:
        # Remove any quotes around strings
        expression = expression.strip()
        
        # Convert Excel exponentiation (^) to Python (**)
        expression = expression.replace('^', '**')
        
        # Don't process if it contains Excel functions
        if any(func in expression.upper() for func in ['IF(', 'OR(', 'AND(']):
            return None
        
        # Remove quotes from the expression for numeric evaluation
        expression = expression.replace('"', '')
        
        logger.debug(f"safe_eval input: {expression}")
        print(f"🔢 Evaluating: {expression}")
        
        # Only allow basic math operations and numbers
        allowed_chars = set('0123456789+-*/()., ')
        if not all(c in allowed_chars or c.isspace() for c in expression):
            logger.warning(f"Expression contains invalid characters: {expression}")
            print("⚠️ Invalid characters in expression")
            return None
        
        # Evaluate
        result = eval(expression, {"__builtins__": {}}, {})
        logger.debug(f"safe_eval result: {result}")
        print(f"🔢 Result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error in safe_eval for expression '{expression}': {str(e)}", exc_info=True)
        print(f"❌ Evaluation error: {str(e)}")
        return None


@app.route("/api/upload-boq-template", methods=["POST"])
def upload_boq_template():
    """Upload BOQ template and identify the BOQ sheet"""
    try:
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files["file"]
        if file.filename == "":
            return jsonify({"error": "Empty filename"}), 400

        if not allowed_file(file.filename):
            return jsonify({"error": "Invalid file type"}), 400

        template_id = str(uuid.uuid4())
        filename = secure_filename(file.filename)
        file_extension = os.path.splitext(filename)[1]
        
        temp_filepath = os.path.join(UPLOAD_FOLDER, f"boq_template_{template_id}{file_extension}")
        file.save(temp_filepath)

        # Load workbook
        wb = openpyxl.load_workbook(temp_filepath, data_only=False)
        
        # Find BOQ sheet
        identified_sheet = None
        for sheet_name in wb.sheetnames:
            if 'BOQ' in sheet_name.upper():
                identified_sheet = sheet_name
                break
        
        if not identified_sheet:
            identified_sheet = wb.sheetnames[0]
        
        ws = wb[identified_sheet]
        
        # Extract BOQ items
        boq_items = []
        headers = []
        
        # Find header row (usually row 1)
        for col in range(1, ws.max_column + 1):
            cell_value = ws.cell(row=1, column=col).value
            if cell_value:
                headers.append(str(cell_value))
            else:
                headers.append(f"Column_{col}")
        
        # Extract data rows
        for row in range(2, ws.max_row + 1):
            row_data = {}
            has_data = False
            
            for col in range(1, len(headers) + 1):
                cell_value = ws.cell(row=row, column=col).value
                row_data[headers[col-1]] = cell_value
                if cell_value:
                    has_data = True
            
            if has_data:
                boq_items.append(row_data)
        
        # Save to MongoDB
        template_doc = {
            "_id": template_id,
            "filename": filename,
            "file_extension": file_extension,
            "identified_sheet": identified_sheet,
            "headers": headers,
            "boq_items": boq_items,
            "uploaded_at": datetime.now(timezone.utc),
            "item_count": len(boq_items)
        }
        
        boq_templates_collection.insert_one(template_doc)

        # Save original file
        output_path = os.path.join(OUTPUT_FOLDER, f"original_boq_template{file_extension}")
        file.seek(0)
        with open(output_path, 'wb') as f:
            f.write(file.read())

        print(f"✅ BOQ Template uploaded: {filename}")
        print(f"✅ Identified sheet: {identified_sheet}")
        print(f"✅ BOQ items extracted: {len(boq_items)}")

        return jsonify({
            "message": "BOQ template uploaded successfully",
            "template_id": template_id,
            "filename": filename,
            "identified_sheet": identified_sheet,
            "item_count": len(boq_items),
            "headers": headers
        }), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/extract-main-carriageway-formulas", methods=["POST"])
def extract_main_carriageway_formulas():
    """Extract formulas from Main Carriageway template and save to MongoDB"""
    try:
        # Check if main carriageway template exists
        template_path = os.path.join(OUTPUT_FOLDER, "main_carriageway_template.xlsx")
        if not os.path.exists(template_path):
            return jsonify({"error": "Main Carriageway template not found. Please upload it first."}), 404

        # Load workbook with formulas
        wb = openpyxl.load_workbook(template_path, data_only=False)
        
        formula_count = 0
        processed_sheets = []
        
        # Extract formulas from all sheets
        for sheet_name in wb.sheetnames:
            sheet_formula_count = 0
            ws = wb[sheet_name]
            
            for row in ws.iter_rows():
                for cell in row:
                    # Check if cell contains a formula
                    if cell.value and isinstance(cell.value, str) and cell.value.startswith('='):
                        formula_doc = {
                            "file_name": "Main Carriageway.xlsx",
                            "sheet": sheet_name,
                            "cell": cell.coordinate,
                            "formula": cell.value,
                            "uploaded_at": datetime.now(timezone.utc)
                        }
                        
                        # Update or insert
                        main_carriageway_formulas_collection.update_one(
                            {
                                "file_name": "Main Carriageway.xlsx",
                                "sheet": sheet_name,
                                "cell": cell.coordinate
                            },
                            {"$set": formula_doc},
                            upsert=True
                        )
                        formula_count += 1
                        sheet_formula_count += 1
            
            processed_sheets.append({
                "name": sheet_name,
                "formula_count": sheet_formula_count
            })

        logger.info(f"Extracted {formula_count} formulas from Main Carriageway template")
        print(f"✅ Formulas extracted: {formula_count}")

        return jsonify({
            "message": "Formulas extracted successfully",
            "total_formula_count": formula_count,
            "sheets": processed_sheets
        }), 200

    except Exception as e:
        logger.error(f"Error extracting formulas: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/upload-main-carriageway-template", methods=["POST"])
def upload_main_carriageway_template():
    """Upload Main Carriageway template"""
    try:
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files["file"]
        if file.filename == "":
            return jsonify({"error": "Empty filename"}), 400

        if not allowed_file(file.filename):
            return jsonify({"error": "Invalid file type"}), 400

        filename = secure_filename(file.filename)
        file_extension = os.path.splitext(filename)[1]

        # Save to uploads folder (source template)
        upload_path = os.path.join(UPLOAD_FOLDER, f"main_carriageway_template{file_extension}")
        file.seek(0)
        with open(upload_path, 'wb') as f:
            f.write(file.read())

        # Save to outputs folder (for modifications)
        output_path = os.path.join(OUTPUT_FOLDER, f"main_carriageway_template{file_extension}")
        file.seek(0)
        with open(output_path, 'wb') as f:
            f.write(file.read())

        logger.info(f"Main Carriageway Template uploaded: {filename}")
        print(f"✅ Main Carriageway Template uploaded: {filename}")
        print("✅ Saved to uploads and outputs folders")

        # Load workbook to get sheet names for response
        wb = openpyxl.load_workbook(upload_path)
        
        return jsonify({
            "message": "Main Carriageway template uploaded successfully",
            "filename": filename,
            "sheets": wb.sheetnames
        }), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/upload-input-files", methods=["POST"])
def upload_input_files():
    """Upload four input files and save cell values to MongoDB"""
    try:
        # Generate session ID
        session_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        
        # File mappings
        file_mappings = {
            "pavement_input": {
                "collection": pavement_input_values_collection,
                "filename": "Pavement Input.xlsx"
            },
            "tcs_input": {
                "collection": tcs_input_values_collection,
                "filename": "TCS Input.xlsx"
            },
            "emb_height": {
                "collection": emb_height_values_collection,
                "filename": "Emb Height.xlsx"
            },
            "tcs_schedule": {
                "collection": tcs_schedule_values_collection,
                "filename": "TCS Schedule.xlsx"
            }
        }
        
        uploaded_files = {}
        total_cells = 0
        
        for file_key, mapping in file_mappings.items():
            if file_key not in request.files:
                return jsonify({"error": f"Missing file: {file_key}"}), 400
            
            file = request.files[file_key]
            if file.filename == "":
                return jsonify({"error": f"Empty filename for {file_key}"}), 400
            
            if not allowed_file(file.filename):
                return jsonify({"error": f"Invalid file type for {file_key}"}), 400
            
            # Save temporarily
            temp_filepath = os.path.join(UPLOAD_FOLDER, f"{file_key}_{session_id}.xlsx")
            file.save(temp_filepath)
            
            # Load workbook
            wb = openpyxl.load_workbook(temp_filepath, data_only=True)
            
            collection = mapping["collection"]
            filename = mapping["filename"]
            cell_count = 0
            
            # Extract all cell values from all sheets
            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                
                for row in ws.iter_rows():
                    for cell in row:
                        if cell.value is not None:
                            cell_doc = {
                                "session_id": session_id,
                                "file_name": filename,
                                "sheet": sheet_name,
                                "cell": cell.coordinate,
                                "value": cell.value,
                                "uploaded_at": datetime.now(timezone.utc)
                            }
                            
                            collection.insert_one(cell_doc)
                            cell_count += 1
            
            uploaded_files[file_key] = {
                "filename": filename,
                "sheets": wb.sheetnames,
                "cell_count": cell_count
            }
            
            total_cells += cell_count
        
        # Create file session document
        session_doc = {
            "session_id": session_id,  # Store session_id as a field
            "created_at": datetime.now(timezone.utc),
            "type": "file_upload",
            "uploaded_files": uploaded_files,
            "total_cells": total_cells
        }
        
        # Insert and get the MongoDB generated _id
        result = file_sessions_collection.insert_one(session_doc)
        mongo_id = result.inserted_id

        logger.info(f"Created file upload session with ID: {session_id}")
        print(f"✅ Session created: {session_id}")
        print(f"✅ Total cells stored: {total_cells}")
        print(f"✅ MongoDB document ID: {mongo_id}")

        return jsonify({
            "message": "Input files uploaded successfully",
            "session_id": session_id,
            "mongo_id": str(mongo_id),
            "uploaded_files": uploaded_files,
            "total_cells": total_cells
        }), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/calculate-boq", methods=["POST"])
def calculate_boq():
    """Calculate BOQ values by evaluating Main Carriageway formulas"""
    try:
        logger.info("Starting BOQ calculation")
        data = request.json
        session_id = data.get("session_id")
        
        if not session_id:
            logger.error("Missing session_id in request")
            return jsonify({"error": "session_id is required"}), 400

        # Verify file upload session exists
        session = file_sessions_collection.find_one({"session_id": session_id})
        if not session:
            return jsonify({"error": "Session not found"}), 404
        
        # Get BOQ template
        boq_template = boq_templates_collection.find_one(sort=[("uploaded_at", -1)])
        if not boq_template:
            return jsonify({"error": "No BOQ template found"}), 404
        
        boq_items = boq_template.get("boq_items", [])
        
        # Removed unused query for Main Carriageway formulas
        
        calculated_results = []
        
        # For each BOQ item, find corresponding formula and calculate
        for idx, item in enumerate(boq_items):
            # Find sub_bill_id or item identifier
            sub_bill_id = None
            for key, value in item.items():
                if value and (isinstance(value, str) or isinstance(value, (int, float))):
                    sub_bill_id = str(value)
                    break
            
            if not sub_bill_id:
                continue
            
            # Try to find matching formula in Abstract sheet
            # Usually Abstract sheet formulas are in sequential rows
            row_num = idx + 2  # Assuming row 1 is headers, data starts at row 2
            
            # Look for formulas in common result columns (like D, E, F, etc.)
            main_carriageway_value = None
            service_road_value = None
            calculation_notes = ""
            
            # Try to find formula for this row
            for col_letter in ['D', 'E', 'F', 'G', 'H']:  # Common result columns
                cell_address = f"{col_letter}{row_num}"
                
                formula_doc = main_carriageway_formulas_collection.find_one({
                    "file_name": "Main Carriageway.xlsx",
                    "sheet": "Abstract",
                    "cell": cell_address
                })
                
                if formula_doc:
                    formula = formula_doc.get("formula")
                    if formula:
                        result = evaluate_excel_formula(formula, session_id, current_sheet=formula_doc.get("sheet"))
                        
                        if main_carriageway_value is None:
                            main_carriageway_value = result
                            calculation_notes += f"Cell {cell_address}: {formula} = {result}; "
                        elif service_road_value is None:
                            service_road_value = result
                            calculation_notes += f"Cell {cell_address}: {formula} = {result}; "
            
            result_doc = {
                "sub_bill_id": sub_bill_id,
                "item": item,
                "main_carriageway": main_carriageway_value,
                "service_road": service_road_value,
                "calculation_notes": calculation_notes.strip()
            }
            
            calculated_results.append(result_doc)
        
        # Update session with results
        file_sessions_collection.update_one(
            {"session_id": session_id},
            {"$set": {
                "calculated_results": calculated_results,
                "calculated_at": datetime.now(timezone.utc),
                "status": "calculated"  # Add status to track calculation state
            }}
        )
        
        print(f"✅ Calculated {len(calculated_results)} items")

        return jsonify({
            "message": "Calculation completed",
            "session_id": session_id,
            "calculated_items": len(calculated_results),
            "results": calculated_results[:10]  # Return first 10 for preview
        }), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/save-in-boq-template", methods=["POST"])
def save_in_boq_template():
    """Update the BOQ template Excel file with calculated values"""
    try:
        data = request.json
        session_id = data.get("session_id")

        if not session_id:
            return jsonify({"error": "session_id is required"}), 400

        # Get session data
        session = file_sessions_collection.find_one({"session_id": session_id})
        if not session:
            return jsonify({"error": "Session not found"}), 404

        calculated_results = session.get("calculated_results", [])
        if not calculated_results:
            return jsonify({"error": "No calculated results found. Run calculations first."}), 400

        # Get BOQ template
        boq_template = boq_templates_collection.find_one(sort=[("uploaded_at", -1)])
        if not boq_template:
            return jsonify({"error": "BOQ template not found"}), 404

        identified_sheet = boq_template["identified_sheet"]
        file_extension = boq_template["file_extension"]
        # original_filename not needed, skipping

        # Load original BOQ file
        original_boq_path = os.path.join(OUTPUT_FOLDER, f"original_boq_template{file_extension}")

        if not os.path.exists(original_boq_path):
            return jsonify({"error": "Original BOQ Excel file not found"}), 404

        wb = openpyxl.load_workbook(original_boq_path)
        
        if identified_sheet not in wb.sheetnames:
            return jsonify({"error": f"Sheet '{identified_sheet}' not found"}), 404
        
        ws = wb[identified_sheet]

        # Find or create columns
        header_row = 1
        headers = {}

        for col_idx in range(1, ws.max_column + 1):
            cell_value = ws.cell(row=header_row, column=col_idx).value
            if cell_value:
                headers[str(cell_value).lower()] = col_idx

        # Find or create result columns
        main_col = None
        service_col = None
        notes_col = None

        for header_text, col_idx in headers.items():
            if "main" in header_text and "carriageway" in header_text:
                main_col = col_idx
            if "service" in header_text and "road" in header_text:
                service_col = col_idx
            if "calculation" in header_text and "note" in header_text:
                notes_col = col_idx

        if main_col is None:
            main_col = ws.max_column + 1
            ws.cell(row=header_row, column=main_col).value = "Main Carriageway"

        if service_col is None:
            service_col = ws.max_column + 1
            ws.cell(row=header_row, column=service_col).value = "Service Road"

        if notes_col is None:
            notes_col = ws.max_column + 1
            ws.cell(row=header_row, column=notes_col).value = "Calculation Notes"

        # Update rows with calculated values
        updated_count = 0

        for idx, result in enumerate(calculated_results):
            row_idx = idx + 2  # Row 1 is header, data starts at row 2
            
            if row_idx <= ws.max_row:
                ws.cell(row=row_idx, column=main_col).value = result.get("main_carriageway")
                ws.cell(row=row_idx, column=service_col).value = result.get("service_road")
                ws.cell(row=row_idx, column=notes_col).value = result.get("calculation_notes")
                updated_count += 1

        # Save files
        wb.save(original_boq_path)
        
        session_output_filename = f"BOQ_Updated_{session_id}{file_extension}"
        session_output_path = os.path.join(OUTPUT_FOLDER, session_output_filename)
        wb.save(session_output_path)

        print(f"✅ Updated {updated_count} rows")

        mime_types = {
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.xlsm': 'application/vnd.ms-excel.sheet.macroEnabled.12',
            '.xlsb': 'application/vnd.ms-excel.sheet.binary.macroEnabled.12',
        }

        mime_type = mime_types.get(file_extension, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        return send_file(
            session_output_path,
            as_attachment=True,
            download_name=session_output_filename,
            mimetype=mime_type,
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/sessions/<session_id>", methods=["GET"])
def get_session(session_id):
    """Get session details"""
    try:
        # Try to find in app sessions first
        session = app_sessions_collection.find_one({"_id": session_id})
        if not session:
            # If not found, try file sessions
            session = file_sessions_collection.find_one({"_id": session_id})
        
        if not session:
            return jsonify({"error": "Session not found"}), 404
        
        # Convert to JSON-serializable format
        if "_id" in session:
            session["_id"] = str(session["_id"])
            
        logger.info(f"Retrieved {session.get('type', 'unknown')} session: {session_id}")
        
        return jsonify(session), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/calculate-main-carriageway", methods=["POST"])
def calculate_main_carriageway():
    """Calculate values using Main Carriageway formulas"""
    try:
        data = request.json
        session_id = data.get("session_id")
        calculation_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")  # Unique ID for this calculation

        if not session_id:
            return jsonify({"error": "Session ID is required"}), 400

        # Verify file upload session exists
        session = file_sessions_collection.find_one({"session_id": session_id})
        if not session:
            return jsonify({"error": f"No file upload session found with ID: {session_id}"}), 404

        # Get formulas from database
        formulas = list(main_carriageway_formulas_collection.find({}))
        if not formulas:
            return jsonify({"error": "No formulas found in database"}), 404

        results = []
        errors = []
        
        # Calculate values for each formula/value
        for formula_doc in formulas:
            try:
                cell = formula_doc.get("cell")
                sheet = formula_doc.get("sheet")
                is_formula = formula_doc.get("is_formula")
                
                if not cell:
                    errors.append({
                        "cell": "unknown",
                        "error": "Missing cell in database document"
                    })
                    continue
                
                # Check if it's a formula or direct value
                if is_formula:
                    # It's a formula - evaluate it
                    formula = formula_doc.get("formula")
                    if not formula:
                        errors.append({
                            "cell": cell,
                            "sheet": sheet,
                            "error": "Formula field is null but is_formula is true"
                        })
                        continue
                    
                    value = evaluate_excel_formula(formula, session_id, current_sheet=sheet)
                    
                    results.append({
                        "cell": cell,
                        "sheet": sheet,
                        "is_formula": True,
                        "formula": formula,
                        "value": value,
                        "success": value is not None
                    })
                else:
                    # It's a direct value - no calculation needed
                    value = formula_doc.get("value")
                    
                    results.append({
                        "cell": cell,
                        "sheet": sheet,
                        "is_formula": False,
                        "formula": None,
                        "value": value,
                        "success": True
                    })
                
            except Exception as calc_error:
                errors.append({
                    "cell": cell or "unknown",
                    "error": str(calc_error)
                })
                logger.error(f"Error calculating cell {cell}: {str(calc_error)}", exc_info=True)
        
        # Store results in database
        result_doc = {
            "calculation_id": calculation_id,
            "session_id": session_id,
            "timestamp": datetime.now(timezone.utc),
            "results": results,
            "errors": errors,
            "total_formulas": len(formulas),
            "successful_calculations": len([r for r in results if r["success"]]),
            "failed_calculations": len(errors)
        }
        
        calculated_main_carriageway_collection.insert_one(result_doc)
        
        response = {
            "calculation_id": calculation_id,
            "results": results,
            "errors": errors,
            "summary": {
                "total_formulas": len(formulas),
                "successful_calculations": len([r for r in results if r["success"]]),
                "failed_calculations": len(errors)
            }
        }
        
        return jsonify(response)
    except Exception as e:
        error_msg = f"Error calculating Main Carriageway values: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return jsonify({"error": error_msg}), 500



@app.route("/api/calculate-main-carriageway-single-cell", methods=["POST"])
def calculate_main_carriageway_single_cell():
    """Calculate value for a specific cell in Main Carriageway using its cell number"""
    try:
        data = request.json
        session_id = data.get("session_id")
        cell = data.get("cell")  # Example: "A84" or "BY8"
        sheet_name = data.get("sheet_name")  # Sheet name where the cell is located

        if not all([session_id, cell, sheet_name]):
            return jsonify({"error": "session_id, cell, and sheet_name are required"}), 400

        # Verify file upload session exists
        session = file_sessions_collection.find_one({"session_id": session_id})
        if not session:
            return jsonify({"error": f"No file upload session found with ID: {session_id}"}), 404
            
        # Get formula from database for this cell and sheet
        formula_doc = main_carriageway_formulas_collection.find_one({
            "cell": cell,
            "sheet": sheet_name
        })
        
        if not formula_doc:
            return jsonify({"error": f"No document found for cell {cell} in sheet {sheet_name}"}), 404

        # Check if it's a formula or direct value
        if formula_doc.get("is_formula"):
            # It's a formula - evaluate it
            formula = formula_doc.get("formula")
            if not formula:
                return jsonify({"error": f"Formula field is null for cell {cell}"}), 500
            
            # Calculate the value using the formula
            value = evaluate_excel_formula(formula, session_id, current_sheet=sheet_name)
            
            # Format the response
            result = {
                "cell": cell,
                "sheet": sheet_name,
                "is_formula": True,
                "formula": formula,
                "value": value,
                "success": value is not None
            }
        else:
            # It's a direct value - no calculation needed
            value = formula_doc.get("value")
            
            # Format the response
            result = {
                "cell": cell,
                "sheet": sheet_name,
                "is_formula": False,
                "formula": None,
                "value": value,
                "success": True
            }
        
        return jsonify(result)
        
    except Exception as e:
        error_msg = f"Error calculating cell {cell}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return jsonify({
            "error": error_msg
        }), 500


@app.route("/api/save-in-main-carriageway", methods=["POST"])
def save_in_main_carriageway():
    """Update the Main Carriageway Excel file with calculated values"""
    try:
        data = request.json
        session_id = data.get("session_id")

        if not session_id:
            return jsonify({"error": "session_id is required"}), 400

        # Get session data
        # Try to find in app sessions first
        session = app_sessions_collection.find_one({"_id": session_id})
        if not session:
            # If not found, try file sessions
            session = file_sessions_collection.find_one({"_id": session_id})
        if not session:
            return jsonify({"error": "Session not found"}), 404

        calculated_results = session.get("calculated_results", [])
        if not calculated_results:
            return jsonify({"error": "No calculated results found. Run calculations first."}), 400

        # Load template from uploads folder
        template_path = os.path.join(UPLOAD_FOLDER, "main_carriageway_template.xlsx")

        if not os.path.exists(template_path):
            return jsonify({"error": "Main Carriageway template not found in uploads folder"}), 404

        wb = openpyxl.load_workbook(template_path)
        
        # Update Abstract sheet with calculated values
        if "Abstract" not in wb.sheetnames:
            return jsonify({"error": "Abstract sheet not found in Main Carriageway file"}), 404
        
        ws = wb["Abstract"]
        updated_count = 0

        # For each calculated result, update the corresponding cells in Abstract sheet
        for idx, result in enumerate(calculated_results):
            row_num = idx + 2  # Assuming row 1 is headers, data starts at row 2
            
            # Update main carriageway value
            main_carriageway_value = result.get("main_carriageway")
            if main_carriageway_value is not None:
                for col_letter in ['D', 'E', 'F', 'G', 'H']:  # Common result columns
                    cell_address = f"{col_letter}{row_num}"
                    formula_doc = main_carriageway_formulas_collection.find_one({
                        "file_name": "Main Carriageway.xlsx",
                        "sheet": "Abstract",
                        "cell": cell_address
                    })
                    if formula_doc:
                        ws[cell_address].value = main_carriageway_value
                        updated_count += 1
                        break

        # Save updated template back to original location
        wb.save(template_path)
        logger.info(f"Updated main carriageway template in uploads folder: {updated_count} cells updated")

        # Save session-specific copy to outputs folder
        session_output_filename = f"Main_Carriageway_Updated_{session_id}.xlsx"
        session_output_path = os.path.join(OUTPUT_FOLDER, session_output_filename)
        wb.save(session_output_path)
        
        print(f"✅ Updated {updated_count} cells in Main Carriageway file")
        print("✅ Saved updated template and session-specific copy")

        return send_file(
            session_output_path,
            as_attachment=True,
            download_name=session_output_filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )

    except Exception as e:
        print(f"Error saving to Main Carriageway file: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/", methods=["GET"])
@app.route("/api", methods=["GET"])
def root():
    """Get API information and list of available endpoints"""
    api_info = {
        "name": "BOQ Calculator API",
        "version": "2.1.0",
        "description": "API for calculating Bill of Quantities (BOQ) with Excel formula support",
        "endpoints": [
            {
                "path": "/api",
                "method": "GET",
                "description": "Get API information and list of endpoints"
            },
            {
                "path": "/api/health",
                "method": "GET",
                "description": "Check API and database health status"
            },
            {
                "path": "/api/upload-boq-template",
                "method": "POST",
                "description": "Upload BOQ template Excel file"
            },
            {
                "path": "/api/upload-main-carriageway-template",
                "method": "POST",
                "description": "Upload Main Carriageway template"
            },
            {
                "path": "/api/extract-main-carriageway-formulas",
                "method": "POST",
                "description": "Extract and save formulas from uploaded Main Carriageway template"
            },
            {
                "path": "/api/upload-input-files",
                "method": "POST",
                "description": "Upload input files (TCS, Pavement, Emb Height, TCS Schedule)"
            },
            {
                "path": "/api/calculate-boq",
                "method": "POST",
                "description": "Calculate BOQ values using Main Carriageway formulas"
            },
            {
                "path": "/api/save-in-boq-template",
                "method": "POST",
                "description": "Save calculated values back to BOQ template"
            },
            {
                "path": "/api/calculate-main-carriageway",
                "method": "POST",
                "description": "Calculate values using Main Carriageway formulas"
            },
            {
                "path": "/api/calculate-main-carriageway-single-cell",
                "method": "POST",
                "description": "Calculate value for a specific cell in Main Carriageway. Required parameters: session_id, cell, sheet_name"
            },
            {
                "path": "/api/save-in-main-carriageway",
                "method": "POST",
                "description": "Save calculated values back to Main Carriageway file"
            },
            {
                "path": "/api/sessions/{session_id}",
                "method": "GET",
                "description": "Get details of a specific calculation session"
            }
        ],
        "documentation": "For more details on request/response formats, please refer to the API documentation"
    }
    return jsonify(api_info), 200


@app.route("/api/health", methods=["GET"])
def health_check():
    """Check if API and database are running"""
    try:
        # Test MongoDB connection
        mongo_client.admin.command('ping')
        db_status = "connected"
        
        # Count documents in collections
        stats = {
            "main_carriageway_formulas": main_carriageway_formulas_collection.count_documents({}),
            "tcs_input_values": tcs_input_values_collection.count_documents({}),
            "pavement_input_values": pavement_input_values_collection.count_documents({}),
            "emb_height_values": emb_height_values_collection.count_documents({}),
            "tcs_schedule_values": tcs_schedule_values_collection.count_documents({}),
            "app_sessions": app_sessions_collection.count_documents({}),
            "file_sessions": file_sessions_collection.count_documents({}),
            "boq_templates": boq_templates_collection.count_documents({})
        }
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}", exc_info=True)
        db_status = "disconnected"
        stats = {}
    
    return jsonify({
        "status": "healthy",
        "service": "BOQ Calculator API with MongoDB",
        "version": "2.1.0",
        "database": db_status,
        "statistics": stats
    }), 200


if __name__ == "__main__":
    # Log startup information
    logger.info(f"Starting BOQ Calculator API... Session ID: {current_session_id}")
    
    # Check database connection
    try:
        mongo_client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        
        # Log collection statistics
        stats = {
            "main_carriageway_formulas": main_carriageway_formulas_collection.count_documents({}),
            "tcs_input_values": tcs_input_values_collection.count_documents({}),
            "pavement_input_values": pavement_input_values_collection.count_documents({}),
            "emb_height_values": emb_height_values_collection.count_documents({}),
            "tcs_schedule_values": tcs_schedule_values_collection.count_documents({}),
            "app_sessions": app_sessions_collection.count_documents({}),
            "file_sessions": file_sessions_collection.count_documents({}),
            "boq_templates": boq_templates_collection.count_documents({})
        }
        logger.info(f"Database statistics: {stats}")
        
        # Save application session info to database
        session_info = {
            "_id": current_session_id,
            "type": "application_session",
            "start_time": datetime.now(),
            "log_file": os.path.join(SESSIONS_LOG_FOLDER, f'session_{current_session_id}.log'),
            "database_stats": stats
        }
        app_sessions_collection.insert_one(session_info)
        logger.info(f"Application session info saved to database with ID: {current_session_id}")
        
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}", exc_info=True)
        raise SystemExit("Could not establish database connection. Exiting...")
    
    # Log server configuration
    logger.info("Server starting on http://0.0.0.0:5000")
    logger.info(f"Debug mode: {app.debug}")
    logger.info(f"Upload folder: {os.path.abspath(UPLOAD_FOLDER)}")
    logger.info(f"Output folder: {os.path.abspath(OUTPUT_FOLDER)}")
    logger.info(f"Log folder: {os.path.abspath(LOG_FOLDER)}")
    logger.info(f"Session log folder: {os.path.abspath(SESSIONS_LOG_FOLDER)}")
    
    # Start the server
    app.run(debug=True, host="0.0.0.0", port=5000)