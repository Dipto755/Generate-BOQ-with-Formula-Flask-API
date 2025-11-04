"""
Flask API for BOQ Generation
============================
Accepts 4 Excel file uploads and generates populated main_carriageway.xlsx
"""

import os
import sys
import subprocess
from pathlib import Path
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import traceback

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configuration
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB per file

# Get project root directory
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / 'data'
OUTPUT_DIR = PROJECT_ROOT / 'output'
TEMPLATE_DIR = PROJECT_ROOT / 'template'
SRC_DIR = PROJECT_ROOT / 'src'

# Required file names (as expected by processors)
REQUIRED_FILES = {
    'tcs_schedule': 'TCS Schedule.xlsx',
    'tcs_input': 'TCS Input.xlsx',
    'emb_height': 'Emb Height.xlsx',
    'pavement_input': 'Pavement Input.xlsx'
}

# Template file
TEMPLATE_FILE = TEMPLATE_DIR / 'main_carriageway.xlsx'
OUTPUT_FILE = OUTPUT_DIR / 'main_carriageway.xlsx'

# Sequential script path
SEQUENTIAL_SCRIPT = SRC_DIR / 'sequential.py'


def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def ensure_directories():
    """Ensure required directories exist"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)


def validate_template():
    """Validate that template file exists"""
    if not TEMPLATE_FILE.exists():
        raise FileNotFoundError(
            f"Template file not found: {TEMPLATE_FILE}\n"
            "Please ensure template/main_carriageway.xlsx exists."
        )


def save_uploaded_file(file, target_name):
    """Save uploaded file to data directory with specific name"""
    if file.filename == '':
        raise ValueError(f"File '{target_name}' is empty")
    
    if not allowed_file(file.filename):
        raise ValueError(
            f"Invalid file type for '{target_name}'. "
            "Only .xlsx and .xls files are allowed."
        )
    
    # Check file size
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(0)
    
    if file_size > MAX_FILE_SIZE:
        raise ValueError(
            f"File '{target_name}' is too large. "
            f"Maximum size: {MAX_FILE_SIZE / (1024*1024):.1f} MB"
        )
    
    # Save to data directory with exact name expected by processors
    target_path = DATA_DIR / target_name
    file.save(str(target_path))
    
    # Verify file was saved
    if not target_path.exists():
        raise IOError(f"Failed to save file: {target_name}")
    
    return target_path


def run_sequential_processing():
    """Run the sequential.py processing pipeline"""
    print(f"Running sequential processing from: {SEQUENTIAL_SCRIPT}")
    
    if not SEQUENTIAL_SCRIPT.exists():
        raise FileNotFoundError(
            f"Sequential script not found: {SEQUENTIAL_SCRIPT}"
        )
    
    # Change to project root to ensure relative imports work
    original_cwd = os.getcwd()
    
    try:
        os.chdir(PROJECT_ROOT)  # Change to project root
        
        # Run sequential.py as a subprocess
        # Set PYTHONPATH to include project root for imports
        env = os.environ.copy()
        env['PYTHONPATH'] = str(PROJECT_ROOT)
        
        result = subprocess.run(
            [sys.executable, str(SEQUENTIAL_SCRIPT)],
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout
            env=env,
            cwd=PROJECT_ROOT
        )
        
        if result.returncode != 0:
            error_msg = f"Processing failed with exit code {result.returncode}\n"
            if result.stdout:
                error_msg += f"STDOUT:\n{result.stdout}\n"
            if result.stderr:
                error_msg += f"STDERR:\n{result.stderr}\n"
            raise RuntimeError(error_msg)
        
        return result.stdout, result.stderr
        
    finally:
        os.chdir(original_cwd)


@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'message': 'BOQ Generation API is running',
        'version': '1.0.0'
    }), 200


@app.route('/api/generate-boq', methods=['POST'])
def generate_boq():
    """
    Generate BOQ endpoint
    Accepts 4 Excel files and returns populated main_carriageway.xlsx
    
    Two ways to upload:
    1. Use specific form field names: tcs_schedule, tcs_input, emb_height, pavement_input
    2. Upload all files with field name 'files' (will try to match by filename)
    """
    try:
        # Check if request has files
        if not request.files:
            return jsonify({
                'error': 'No files provided',
                'message': 'Please upload 4 Excel files',
                'option_1': 'Use form fields: tcs_schedule, tcs_input, emb_height, pavement_input',
                'option_2': 'Upload all files with field name "files" (will match by filename)'
            }), 400
        
        # Try to get files by specific field names first (preferred method)
        file_map = {}
        
        # Method 1: Try specific field names
        for key in REQUIRED_FILES.keys():
            if key in request.files:
                file = request.files[key]
                if file and file.filename:
                    file_map[key] = file
        
        # Method 2: If not all files found, try 'files' field
        if len(file_map) < 4 and 'files' in request.files:
            files = request.files.getlist('files')
            
            # Check if exactly 4 files are provided
            if len(files) != 4:
                return jsonify({
                    'error': 'Incorrect number of files',
                    'message': f'Expected 4 files, received {len(files)}',
                    'required_files': list(REQUIRED_FILES.keys())
                }), 400
            
            # Try to match files by filename
            unmatched_files = []
            
            for file in files:
                if file.filename:
                    matched = False
                    filename_lower = file.filename.lower()
                    
                    # Direct filename match
                    for key, expected_name in REQUIRED_FILES.items():
                        if key not in file_map:
                            expected_lower = expected_name.lower()
                            if expected_lower in filename_lower or filename_lower in expected_lower:
                                file_map[key] = file
                                matched = True
                                break
                    
                    # Pattern matching: try to match key words
                    if not matched:
                        for key in REQUIRED_FILES.keys():
                            if key not in file_map:
                                key_words = key.split('_')
                                # Check if all key words appear in filename
                                if all(word in filename_lower for word in key_words if len(word) > 2):
                                    file_map[key] = file
                                    matched = True
                                    break
                    
                    if not matched:
                        unmatched_files.append(file.filename)
        
        # Ensure directories exist
        ensure_directories()
        
        # Validate template exists
        validate_template()
        
        # Verify we have all required files
        missing_files = [k for k in REQUIRED_FILES.keys() if k not in file_map]
        if missing_files:
            return jsonify({
                'error': 'Could not identify all required files',
                'message': 'Please ensure your files are named clearly or use form field names matching the required keys',
                'missing_files': missing_files,
                'matched_files': list(file_map.keys()),
                'unmatched_files': unmatched_files if 'unmatched_files' in locals() else [],
                'hint': 'Use form field names: tcs_schedule, tcs_input, emb_height, pavement_input'
            }), 400
        
        # Save uploaded files
        saved_files = {}
        
        # Save files with correct names
        for key, file in file_map.items():
            target_name = REQUIRED_FILES[key]
            try:
                saved_path = save_uploaded_file(file, target_name)
                saved_files[key] = str(saved_path)
                print(f"Saved {key}: {target_name}")
            except Exception as e:
                return jsonify({
                    'error': f'Error saving file {key}',
                    'message': str(e)
                }), 400
        
        # Run sequential processing
        try:
            stdout, stderr = run_sequential_processing()
            print("Sequential processing completed successfully")
        except Exception as e:
            return jsonify({
                'error': 'Processing failed',
                'message': str(e),
                'details': traceback.format_exc()
            }), 500
        
        # Check if output file was created
        if not OUTPUT_FILE.exists():
            return jsonify({
                'error': 'Output file not generated',
                'message': 'Processing completed but output file was not created',
                'expected_path': str(OUTPUT_FILE)
            }), 500
        
        # Return the generated file
        return send_file(
            str(OUTPUT_FILE),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='main_carriageway.xlsx'
        )
        
    except ValueError as e:
        return jsonify({
            'error': 'Validation error',
            'message': str(e)
        }), 400
    
    except FileNotFoundError as e:
        return jsonify({
            'error': 'File not found',
            'message': str(e)
        }), 404
    
    except Exception as e:
        return jsonify({
            'error': 'Internal server error',
            'message': str(e),
            'details': traceback.format_exc()
        }), 500


@app.route('/api/generate-boq', methods=['GET'])
def generate_boq_info():
    """Get information about the generate-boq endpoint"""
    return jsonify({
        'endpoint': '/api/generate-boq',
        'method': 'POST',
        'description': 'Upload 4 Excel files to generate BOQ',
        'required_files': {
            'tcs_schedule': 'TCS Schedule.xlsx - Technical specification schedule',
            'tcs_input': 'TCS Input.xlsx - Technical specification input data',
            'emb_height': 'Emb Height.xlsx - Embankment height data',
            'pavement_input': 'Pavement Input.xlsx - Pavement layer specifications'
        },
        'max_file_size_mb': MAX_FILE_SIZE / (1024 * 1024),
        'allowed_formats': list(ALLOWED_EXTENSIONS),
        'output': 'main_carriageway.xlsx - Populated BOQ file'
    }), 200


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        'error': 'Not found',
        'message': 'The requested endpoint does not exist'
    }), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    return jsonify({
        'error': 'Internal server error',
        'message': 'An unexpected error occurred'
    }), 500


if __name__ == '__main__':
    # Ensure directories exist on startup
    ensure_directories()
    
    # Validate template exists
    try:
        validate_template()
        print(f"✓ Template file found: {TEMPLATE_FILE}")
    except FileNotFoundError as e:
        print(f"⚠ WARNING: {e}")
        print("The API will start but processing will fail until template is available.")
    
    # Print startup information
    print("\n" + "="*80)
    print("BOQ Generation Flask API")
    print("="*80)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Template directory: {TEMPLATE_DIR}")
    print("="*80)
    print("\nStarting Flask server...")
    print("API Endpoints:")
    print("  GET  /                 - Health check")
    print("  GET  /api/generate-boq  - API information")
    print("  POST /api/generate-boq  - Generate BOQ (upload 4 Excel files)")
    print("="*80 + "\n")
    
    # Run Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)

