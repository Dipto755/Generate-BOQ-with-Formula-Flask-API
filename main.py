"""
Flask API for BOQ Generation with Session Management
"""
import os
import sys
import subprocess
import threading
from pathlib import Path
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import traceback
import shutil
from datetime import datetime
import zipfile
from dotenv import load_dotenv
# Import session manager
from api.session_manager import SessionManager

load_dotenv()
# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Configuration
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB per file

# Get project root directory
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / 'data'
OUTPUT_DIR = PROJECT_ROOT / 'output'
TEMPLATE_DIR = PROJECT_ROOT / 'template'
SRC_DIR = PROJECT_ROOT / 'src'

# Required file names
REQUIRED_FILES = {
    'tcs_schedule': 'TCS Schedule.xlsx',
    'tcs_input': 'TCS Input.xlsx',
    'emb_height': 'Emb Height.xlsx',
    'pavement_input': 'Pavement Input.xlsx'
}

# Template file
TEMPLATE_FILE = TEMPLATE_DIR / 'main_carriageway_and_boq.xlsx'

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
        raise FileNotFoundError(f"Template file not found: {TEMPLATE_FILE}")

def run_session_processing(session_id, session_data_dir, session_output_file):
    """
    Run the sequential processing for a specific session with progress tracking
    """
    session_manager = SessionManager()
    
    try:
        # Update session status to processing IMMEDIATELY
        session_manager.update_session_status(session_id, 'processing')
        session_manager.set_processing_started(session_id)
        
        # Define processing steps with user-friendly names
        processing_steps = [
            {'script': 'tcs_schedule', 'name': 'TCS Schedule Processing', 'message': 'Processing TCS schedule data...'},
            {'script': 'tcs_input', 'name': 'TCS Input Processing', 'message': 'Processing TCS input specifications...'},
            {'script': 'emb_height', 'name': 'Embankment Height Processing', 'message': 'Processing embankment height data...'},
            {'script': 'pavement_input', 'name': 'Pavement Input Processing', 'message': 'Processing pavement layer specifications...'},
            {'script': 'constant_fill', 'name': 'Constant Values Processing', 'message': 'Applying constant values...'},
            {'script': 'formula_applier', 'name': 'Formula Application', 'message': 'Applying calculation formulas...'},
            {'script': 'pavement_input_with_internal', 'name': 'Geogrid Calculation', 'message': 'Calculating geogrid requirements...'},
            {'script': 'final_sum_applier', 'name': 'Final Summary', 'message': 'Generating final summary...'},
            # {'script': 'calculator', 'name': 'Formula Calculation', 'message': 'Calculating formula values...'},
            {'script': 'boq_populator', 'name': 'BOQ Generation', 'message': 'Generating BOQ template...'},
        ]
        total_steps = len(processing_steps)
        
        # Initialize progress - starting first step (0% complete)
        session_manager.update_progress(session_id, {
            'current_step': processing_steps[0]['name'],
            'current_step_number': 1,
            'total_steps': total_steps,
            'percentage': 0,
            'message': processing_steps[0]['message'],
            'completed_steps': 0,
            'started_at': datetime.now()
        })
        
        # Change to project root to ensure relative imports work
        original_cwd = os.getcwd()
        
        try:
            os.chdir(PROJECT_ROOT)
            
            # Set environment variables for session-specific paths
            env = os.environ.copy()
            env['SESSION_DATA_DIR'] = str(session_data_dir)
            env['SESSION_OUTPUT_FILE'] = str(session_output_file)
            env['SESSION_ID'] = session_id
            
            # Run each script sequentially with proper progress tracking
            for step_num, step_info in enumerate(processing_steps, 1):
                script_name = step_info['script']
                step_name = step_info['name']
                step_message = step_info['message']
                
                print(f"Executing step {step_num}/{total_steps}: {step_name}")
                
                # Update progress to show current step (before execution)
                if step_num > 1:  # For steps 2-8, update current step
                    session_manager.update_progress(session_id, {
                        'current_step': step_name,
                        'current_step_number': step_num,
                        'total_steps': total_steps,
                        'percentage': int(((step_num - 1) / total_steps) * 100),  # Previous step completed
                        'message': step_message,
                        'completed_steps': step_num - 1,
                        'last_completed_at': datetime.now()
                    })
                
                # Determine script path based on name
                if script_name in ['tcs_schedule', 'tcs_input', 'emb_height', 'pavement_input', 'constant_fill', 'calculator', 'boq_populator']:
                    script_path = SRC_DIR / 'processor' / f'{script_name}.py'
                else:
                    script_path = SRC_DIR / 'internal' / f'{script_name}.py'
                
                # Run the script
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=300,  # 5 minute timeout per script
                    env=env,
                    cwd=PROJECT_ROOT
                )
                
                if result.returncode != 0:
                    error_msg = f"Script {step_name} failed with exit code {result.returncode}\n"
                    if result.stdout:
                        error_msg += f"STDOUT:\n{result.stdout}\n"
                    if result.stderr:
                        error_msg += f"STDERR:\n{result.stderr}\n"
                    raise RuntimeError(error_msg)
                
                # Update progress AFTER step completion
                progress_percentage = int((step_num / total_steps) * 100)
                completion_message = f'Completed: {step_name}'
                
                # If this is the last step, show final message
                if step_num == total_steps:
                    completion_message = 'All calculations completed successfully!'
                
                session_manager.update_progress(session_id, {
                    'current_step': step_name if step_num == total_steps else processing_steps[step_num]['name'] if step_num < total_steps else step_name,
                    'current_step_number': step_num if step_num == total_steps else step_num + 1,
                    'total_steps': total_steps,
                    'percentage': progress_percentage,
                    'message': completion_message,
                    'completed_steps': step_num,
                    'last_completed_at': datetime.now()
                })
                
                print(f"✓ Completed step {step_num}/{total_steps}: {step_name}")
            
            # Final completion update
            session_manager.update_progress(session_id, {
                'current_step': 'All steps completed',
                'current_step_number': total_steps,
                'total_steps': total_steps,
                'percentage': 100,
                'message': 'All calculations completed successfully!',
                'completed_steps': total_steps,
                'completed_at': datetime.now()
            })
            
            # Calculate execution time and update session
            session = session_manager.get_session(session_id)
            if session and session['processing_info']['started_at']:
                started_at = session['processing_info']['started_at']
                execution_time = (datetime.now() - started_at).total_seconds()
                
                # Update session with output file info
                output_info = {
                    'filename': session_output_file.name,
                    'file_path': str(session_output_file),
                    'generated_at': datetime.now(),
                    'download_count': 0
                }
                
                session_manager.set_output_file(session_id, output_info)
                session_manager.sessions.update_one(
                    {'session_id': session_id},
                    {'$set': {'processing_info.execution_time_seconds': execution_time}}
                )
                
                # Add BOQ file info to session
                session_output_dir = session_output_file.parent
                boq_output_path = session_output_dir / f"{session_id}_BOQ.xlsx"
                if boq_output_path.exists():
                    boq_info = {
                        'filename': f"{session_id}_BOQ.xlsx",
                        'file_path': str(boq_output_path),
                        'generated_at': datetime.now(),
                        'download_count': 0
                    }
                    session_manager.sessions.update_one(
                        {'session_id': session_id},
                        {'$set': {'boq_file': boq_info}}
                    )
            
            print(f"✓ Session {session_id} processing completed successfully")
            
        finally:
            os.chdir(original_cwd)
            
    except Exception as e:
        print(f"✗ Session {session_id} processing failed: {str(e)}")
        session_manager.set_error(session_id, str(e), traceback.format_exc(), "sequential_processing")

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'message': 'BOQ Generation API is running',
        'version': '1.0.0'
    }), 200

@app.route('/api/upload-files', methods=['POST'])
def upload_files():
    """Upload 4 Excel files and create session"""
    try:
        session_manager = SessionManager()
        
        # Generate unique session ID
        session_id = session_manager.generate_session_id()
        
        # Create session directories
        session_data_dir = DATA_DIR / 'sessions' / session_id
        session_output_dir = OUTPUT_DIR / 'sessions' / session_id
        session_data_dir.mkdir(parents=True, exist_ok=True)
        session_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create session in MongoDB
        session_manager.create_session(session_id)
        
        # Debug: Check what files we received
        print(f"DEBUG: Request files: {list(request.files.keys())}")
        
        # File validation
        if not request.files:
            return jsonify({'error': 'No files provided'}), 400
        
        # Try to get files by specific field names first
        file_map = {}
        file_debug_info = {}
        
        for key in REQUIRED_FILES.keys():
            if key in request.files:
                file = request.files[key]
                file_debug_info[key] = {
                    'file_object': str(type(file)),
                    'has_filename': hasattr(file, 'filename'),
                    'filename': file.filename if hasattr(file, 'filename') else 'NO_FILENAME',
                    'filename_type': str(type(file.filename)) if hasattr(file, 'filename') else 'NO_FILENAME_ATTR'
                }
                
                # More robust check for valid file object with filename
                if (file and 
                    hasattr(file, 'filename') and 
                    file.filename is not None and 
                    str(file.filename).strip() != ''):
                    file_map[key] = file
                    print(f"DEBUG: Added file for {key}: {file.filename}")
                else:
                    print(f"DEBUG: Skipped file for {key}: {file_debug_info[key]}")
        
        print(f"DEBUG: Files mapped: {list(file_map.keys())}")
        
        # If not all files found, try 'files' field
        if len(file_map) < 4 and 'files' in request.files:
            files = request.files.getlist('files')
            print(f"DEBUG: Found {len(files)} files in 'files' field")
            
            if len(files) != 4:
                return jsonify({
                    'error': 'Incorrect number of files',
                    'message': f'Expected 4 files, received {len(files)}',
                    'debug_info': file_debug_info
                }), 400
            
            # Simple filename matching
            for file in files:
                if file and hasattr(file, 'filename') and file.filename:
                    filename_lower = file.filename.lower()
                    for key, expected_name in REQUIRED_FILES.items():
                        if key not in file_map:
                            expected_lower = expected_name.lower()
                            if expected_lower in filename_lower or filename_lower in expected_lower:
                                file_map[key] = file
                                print(f"DEBUG: Matched {file.filename} to {key}")
                                break
        
        # Verify we have all required files
        missing_files = [k for k in REQUIRED_FILES.keys() if k not in file_map]
        if missing_files:
            return jsonify({
                'error': 'Missing required files',
                'missing_files': missing_files,
                'required_files': list(REQUIRED_FILES.keys()),
                'debug_info': file_debug_info,
                'files_received': list(request.files.keys())
            }), 400
        
        # Save uploaded files to session directory
        saved_files = {}
        for key, file in file_map.items():
            target_name = REQUIRED_FILES[key]
            target_path = session_data_dir / target_name
            
            # Additional validation for file object
            if not file or not hasattr(file, 'filename') or not file.filename:
                return jsonify({
                    'error': f'Invalid file for: {key}',
                    'message': 'File object is invalid or has no filename',
                    'debug_info': file_debug_info.get(key, 'NO_DEBUG_INFO')
                }), 400
            
            try:
                # Check file size and type
                file.seek(0, os.SEEK_END)
                file_size = file.tell()
                file.seek(0)
                
                if file_size > MAX_FILE_SIZE:
                    return jsonify({
                        'error': f'File too large: {target_name}',
                        'message': f'Maximum size: {MAX_FILE_SIZE / (1024*1024):.1f} MB'
                    }), 400
                
                if not allowed_file(file.filename):
                    return jsonify({
                        'error': f'Invalid file type: {target_name}',
                        'message': 'Only .xlsx and .xls files are allowed'
                    }), 400
                
                # Save file
                file.save(str(target_path))
                print(f"DEBUG: Saved file for {key} to {target_path}")
                
                # Add to session in MongoDB
                file_info = {
                    'field_name': key,
                    'original_filename': file.filename,
                    'saved_filename': target_name,
                    'file_path': str(target_path),
                    'uploaded_at': datetime.now(),
                    'file_size_bytes': file_size
                }
                session_manager.add_input_file(session_id, file_info)
                saved_files[key] = str(target_path)
                
            except Exception as file_error:
                return jsonify({
                    'error': f'Error processing file: {key}',
                    'message': str(file_error),
                    'file_info': {
                        'filename': file.filename,
                        'content_type': file.content_type if hasattr(file, 'content_type') else 'NO_CONTENT_TYPE'
                    }
                }), 400
        
        return jsonify({
            'status': 'success',
            'session_id': session_id,
            'message': 'Files uploaded successfully',
            'uploaded_files': list(saved_files.keys())
        }), 200
        
    except Exception as e:
        print(f"DEBUG: Upload failed with error: {str(e)}")
        print(f"DEBUG: Traceback: {traceback.format_exc()}")
        return jsonify({
            'error': 'Upload failed',
            'message': str(e),
            'traceback': traceback.format_exc()
        }), 500

@app.route('/api/execute-calculation', methods=['POST'])
def execute_calculation():
    """Execute calculations for a session (starts background thread)"""
    try:
        data = request.get_json()
        session_id = data.get('session_id')
        
        if not session_id:
            return jsonify({'error': 'session_id is required'}), 400
        
        session_manager = SessionManager()
        session = session_manager.get_session(session_id)
        
        if not session:
            return jsonify({'error': 'Session not found'}), 404
        
        if session['status'] != 'uploaded':
            return jsonify({
                'error': f"Cannot start calculation",
                'message': f"Session status is '{session['status']}', must be 'uploaded'"
            }), 400
        
        # Create session output file
        session_output_dir = OUTPUT_DIR / 'sessions' / session_id
        session_output_dir.mkdir(parents=True, exist_ok=True)
        output_filename = f"{session_id}_main_carriageway_and_boq.xlsx"
        session_output_file = session_output_dir / output_filename
        
        # Copy template to session output directory
        shutil.copy2(TEMPLATE_FILE, session_output_file)
        
        # Update status to processing immediately
        session_manager.update_session_status(session_id, 'processing')
        
        # Start processing in background thread
        thread = threading.Thread(
            target=run_session_processing,
            args=(session_id, DATA_DIR / 'sessions' / session_id, session_output_file)
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'success',
            'session_id': session_id,
            'message': 'Calculation started in background',
            'output_file': output_filename
        }), 200
        
    except Exception as e:
        return jsonify({
            'error': 'Execution failed',
            'message': str(e)
        }), 500

@app.route('/api/execute-calculation-sync', methods=['POST'])
def execute_calculation_sync():
    """Execute calculations for a session (synchronous - waits for completion)"""
    try:
        data = request.get_json()
        session_id = data.get('session_id')
        
        if not session_id:
            return jsonify({'error': 'session_id is required'}), 400
        
        session_manager = SessionManager()
        session = session_manager.get_session(session_id)
        
        if not session:
            return jsonify({'error': 'Session not found'}), 404
        
        if session['status'] != 'uploaded':
            return jsonify({
                'error': f"Cannot start calculation",
                'message': f"Session status is '{session['status']}', must be 'uploaded'"
            }), 400
        
        # Create session output file
        session_output_dir = OUTPUT_DIR / 'sessions' / session_id
        session_output_dir.mkdir(parents=True, exist_ok=True)
        output_filename = f"{session_id}_main_carriageway_and_boq.xlsx"
        session_output_file = session_output_dir / output_filename
        
        # Copy template to session output directory
        shutil.copy2(TEMPLATE_FILE, session_output_file)
        
        # Run processing synchronously (blocks until completion)
        try:
            run_session_processing(session_id, DATA_DIR / 'sessions' / session_id, session_output_file)
            
            # Check if processing was successful
            updated_session = session_manager.get_session(session_id)
            if updated_session['status'] == 'completed':
                return jsonify({
                    'status': 'success',
                    'session_id': session_id,
                    'message': 'Calculation completed successfully',
                    'output_file': output_filename,
                    'execution_time_seconds': updated_session['processing_info'].get('execution_time_seconds')
                }), 200
            else:
                return jsonify({
                    'error': 'Calculation failed',
                    'session_id': session_id,
                    'message': updated_session['error_info'].get('error_message', 'Unknown error'),
                    'failed_script': updated_session['error_info'].get('failed_script')
                }), 500
                
        except Exception as processing_error:
            return jsonify({
                'error': 'Processing error',
                'session_id': session_id,
                'message': str(processing_error)
            }), 500
        
    except Exception as e:
        return jsonify({
            'error': 'Execution failed',
            'message': str(e)
        }), 500

@app.route('/api/session-status/<session_id>', methods=['GET'])
def get_session_status(session_id):
    """Get session status with progress"""
    try:
        session_manager = SessionManager()
        session = session_manager.get_session(session_id)
        
        if not session:
            return jsonify({'error': 'Session not found'}), 404
        
        response_data = {
            'session_id': session_id,
            'status': session['status'],
            'created_at': session['created_at'].isoformat() if session['created_at'] else None,
            'updated_at': session['updated_at'].isoformat() if session['updated_at'] else None,
            'has_error': session['error_info']['has_error'],
            'error_message': session['error_info']['error_message'],
            'input_files_count': len(session['input_files']),
            'output_file': session['output_file'],
            'boq_file': session.get('boq_file'),
            'zip_available': True,  # Add this
            'files_in_zip': [
                f"{session_id}_main_carriageway.xlsx",
                f"{session_id}_BOQ.xlsx"
            ],
            'progress': session.get('progress', {})  # Make sure this line includes progress
        }
        
        # Add processing info if available
        if session['processing_info']:
            response_data['processing_info'] = session['processing_info']
        
        return jsonify(response_data), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download-file/<session_id>', methods=['GET'])
def download_file(session_id):
    """Download output file for session"""
    try:
        session_manager = SessionManager()
        session = session_manager.get_session(session_id)
        
        if not session:
            return jsonify({'error': 'Session not found'}), 404
        
        if session['status'] != 'completed':
            return jsonify({
                'error': f"Calculation not completed", 
                'message': f"Current status: {session['status']}"
            }), 400
        
        output_file_path = session['output_file']['file_path']
        
        if not os.path.exists(output_file_path):
            return jsonify({'error': 'Output file not found'}), 404
        
        # Increment download count
        session_manager.sessions.update_one(
            {'session_id': session_id},
            {'$inc': {'output_file.download_count': 1}}
        )
        
        return send_file(
            output_file_path,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=session['output_file']['filename']
        )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/download-boq/<session_id>', methods=['GET'])
def download_boq(session_id):
    """Download BOQ file for session"""
    try:
        session_manager = SessionManager()
        session = session_manager.get_session(session_id)
        
        if not session:
            return jsonify({'error': 'Session not found'}), 404
        
        if not session.get('boq_file'):
            return jsonify({
                'error': 'BOQ file not generated',
                'message': 'BOQ file has not been generated for this session'
            }), 404
        
        boq_file_path = session['boq_file']['file_path']
        
        if not os.path.exists(boq_file_path):
            return jsonify({'error': 'BOQ file not found'}), 404
        
        # Increment download count
        session_manager.sessions.update_one(
            {'session_id': session_id},
            {'$inc': {'boq_file.download_count': 1}}
        )
        
        return send_file(
            boq_file_path,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=session['boq_file']['filename']
        )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/download-session/<session_id>', methods=['GET'])
def download_session_zip(session_id):
    """Download both files as ZIP"""
    try:
        session_manager = SessionManager()
        session = session_manager.get_session(session_id)
        
        if not session:
            return jsonify({'error': 'Session not found'}), 404
        
        if session['status'] != 'completed':
            return jsonify({
                'error': 'Calculation not completed', 
                'message': f"Current status: {session['status']}"
            }), 400
        
        session_output_dir = OUTPUT_DIR / 'sessions' / session_id
        
        # Check if both files exist
        main_file = session_output_dir / f"{session_id}_main_carriageway.xlsx"
        boq_file = session_output_dir / f"{session_id}_BOQ.xlsx"
        
        if not main_file.exists() or not boq_file.exists():
            return jsonify({'error': 'Required files not found'}), 404
        
        # Create ZIP file
        zip_path = session_output_dir / f"{session_id}_files.zip"
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(main_file, main_file.name)
            zipf.write(boq_file, boq_file.name)
        
        # Update download counts
        session_manager.sessions.update_one(
            {'session_id': session_id},
            {'$inc': {
                'output_file.download_count': 1,
                'boq_file.download_count': 1,
                'zip_download_count': 1
            }}
        )
        
        return send_file(
            zip_path,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f"{session_id}_BOQ_Files.zip"
        )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sessions', methods=['GET'])
def get_all_sessions():
    """Get all sessions with pagination"""
    try:
        session_manager = SessionManager()
        
        # Get query parameters with defaults
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 10, type=int)
        
        # Validate pagination
        if page < 1:
            page = 1
        if limit < 1 or limit > 100:
            limit = 10
        
        # Calculate skip for pagination
        skip = (page - 1) * limit
        
        # Get sessions with pagination (newest first)
        sessions_cursor = session_manager.sessions.find().sort('created_at', -1).skip(skip).limit(limit)
        
        sessions = []
        for session in sessions_cursor:
            session_data = {
                'session_id': session['session_id'],
                'status': session['status'],
                'created_at': session['created_at'].isoformat(),
                'updated_at': session['updated_at'].isoformat(),
                'input_files_count': len(session['input_files']),
                'has_error': session['error_info']['has_error']
            }
            
            # Add output file info if exists
            if session.get('output_file'):
                session_data['output_file'] = session['output_file']
            
            sessions.append(session_data)
        
        total_sessions = session_manager.sessions.count_documents({})

        return jsonify({
            'sessions': sessions,
            'total_sessions': total_sessions
        }), 200
        
    except Exception as e:
        return jsonify({
            'error': 'Failed to fetch sessions',
            'message': str(e)
        }), 500

@app.route('/', methods=['GET'])
@app.route('/api', methods=['GET'])
def root():
    """API Root - Get available endpoints and basic usage information"""
    return jsonify({
        'title': 'BOQ Generation API',
        'version': '1.0.0',
        'description': 'Flask API for BOQ (Bill of Quantities) Generation with Session Management',
        'endpoints': {
            'health': {
                'method': 'GET',
                'path': '/health',
                'description': 'Health check endpoint',
                'usage': 'curl -X GET http://localhost:5000/health'
            },
            'upload_files': {
                'method': 'POST',
                'path': '/api/upload-files',
                'description': 'Upload 4 Excel files and create a new session',
                'required_files': [
                    'tcs_schedule (TCS Schedule.xlsx)',
                    'tcs_input (TCS Input.xlsx)',
                    'emb_height (Emb Height.xlsx)',
                    'pavement_input (Pavement Input.xlsx)'
                ],
                'usage': 'curl -X POST -F "tcs_schedule=@TCS_Schedule.xlsx" -F "tcs_input=@TCS_Input.xlsx" -F "emb_height=@Emb_Height.xlsx" -F "pavement_input=@Pavement_Input.xlsx" http://localhost:5000/api/upload-files'
            },
            'execute_calculation': {
                'method': 'POST',
                'path': '/api/execute-calculation',
                'description': 'Start BOQ calculation for a session (runs in background)',
                'request_body': {
                    'session_id': 'string (required)'
                },
                'usage': 'curl -X POST -H "Content-Type: application/json" -d \'{"session_id": "your_session_id"}\' http://localhost:5000/api/execute-calculation'
            },
            'execute_calculation_sync': {
                'method': 'POST',
                'path': '/api/execute-calculation-sync',
                'description': 'Start BOQ calculation for a session (waits for completion)',
                'request_body': {
                    'session_id': 'string (required)'
                },
                'usage': 'curl -X POST -H "Content-Type: application/json" -d \'{"session_id": "your_session_id"}\' http://localhost:5000/api/execute-calculation-sync'
            },
            'session_list': {
                'method': 'GET',
                'path': '/api/sessions',
                'description': 'Get list of all sessions (paginated)',
                'query_parameters': {
                    'page': 'integer (optional, default: 1)',
                    'limit': 'integer (optional, default: 10, max: 100)'
                },
                'usage': 'curl -X GET "http://localhost:5000/api/sessions?page=1&limit=10"'
            },
            'session_status': {
                'method': 'GET',
                'path': '/api/session-status/<session_id>',
                'description': 'Check the status of a processing session',
                'usage': 'curl -X GET http://localhost:5000/api/session-status/your_session_id'
            },
            'download_file': {
                'method': 'GET',
                'path': '/api/download-file/<session_id>',
                'description': 'Download the generated BOQ file (only available after completion)',
                'usage': 'curl -X GET -O http://localhost:5000/api/download-file/your_session_id'
            },
            'download_boq_file': {
                'method': 'GET',
                'path': '/api/download-boq/<session_id>',
                'description': 'Download the generated BOQ file',
                'usage': 'curl -X GET -O http://localhost:5000/api/download-boq/your_session_id'
            },
            'download_session_zip': {
                'method': 'GET',
                'path': '/api/download-session/<session_id>',
                'description': 'Download both main carriageway and BOQ files as ZIP',
                'usage': 'curl -X GET -O http://localhost:5000/api/download-session/your_session_id'
            }
        },
        'workflow': {
            'steps': [
                '1. Upload 4 required Excel files using /api/upload-files',
                '2. Get session_id from upload response',
                '3. Start calculation using /api/execute-calculation with session_id',
                '4. Monitor progress using /api/session-status/<session_id>',
                '5. Download result using /api/download-file/<session_id> when status is "completed"',
                '6. Download BOQ file using /api/download-boq/<session_id>',
                '7. Download session ZIP file using /api/download-session/<session_id>'
            ]
        },
        'session_states': {
            'uploaded': 'Files uploaded, ready for calculation',
            'processing': 'Calculation in progress',
            'completed': 'Calculation completed, file ready for download',
            'error': 'Processing failed, check error message'
        },
        'notes': [
            'Maximum file size: 100 MB per file',
            'Allowed file types: .xlsx, .xls',
            'Session timeout: 10 minutes for processing',
            'All files are stored temporarily and cleaned up automatically',
            'For best results, download the ZIP file containing both files',
            'Extract both files to the same folder before opening the BOQ file',
            'Excel may show security prompts about external links - click "Enable"',
            'Keep both files in the same directory for formulas to work properly'
        ]
    }), 200

if __name__ == '__main__':
    # Load environment variables
    
    
    # Print MongoDB configuration
    mongodb_uri = os.getenv('MONGO_URI')
    db_name = os.getenv('MONGO_DB_NAME')
    print(f"✓ MongoDB URI: {mongodb_uri}")
    print(f"✓ Database: {db_name}")
    
    # Test MongoDB connection
    try:
        session_manager = SessionManager()
        # Ping the MongoDB server
        session_manager.client.admin.command('ping')
        print("✓ MongoDB connection test: SUCCESS")
    except Exception as e:
        print(f"⚠ MongoDB connection test: FAILED - {str(e)}")
    
    # Ensure directories exist on startup
    ensure_directories()
    
    # Validate template exists
    try:
        validate_template()
        print(f"✓ Template file found: {TEMPLATE_FILE}")
    except FileNotFoundError as e:
        print(f"⚠ WARNING: {e}")
    
    # Print startup information
    print("\n" + "="*80)
    print("BOQ Generation Flask API with Session Management")
    print("="*80)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Template directory: {TEMPLATE_DIR}")
    print("="*80)
    print("\nStarting Flask server...")
    print("API Endpoints:")
    print("  GET  /                                    - API root")
    print("  GET  /health                              - Health check")
    print("  POST /api/upload-files                    - Upload files & create session")
    print("  POST /api/execute-calculation             - Start calculation")
    print("  POST /api/execute-calculation-sync        - Start calculation (synchronous)")
    print("  GET  /api/session-status/<id>             - Check session status")
    print("  GET  /api/download-file/<id>              - Download result")
    print("  GET  /api/download-boq/<id>               - Download BOQ file")
    print("  GET  /api/download-session/<id>           - Download session ZIP file")
    print("="*80 + "\n")
    
    # Run Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)
