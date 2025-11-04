import os
import sys
import traceback
from flask import Blueprint, request, jsonify, current_app
from werkzeug.utils import secure_filename
from .session_manager import session_manager

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

bp = Blueprint('api', __name__)

# Allowed file extensions
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv', 'txt', 'json'}

def allowed_file(filename):
    """Check if the file has an allowed extension"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@bp.route('/upload-inputs', methods=['POST'])
def upload_and_create_session():
    """Upload files and automatically create a session"""
    try:
        # Check if files are provided
        if 'files' not in request.files:
            return jsonify({
                "success": False,
                "error": "No files provided",
                "message": "Please provide files to upload"
            }), 400
        
        files = request.files.getlist('files')
        if not files or files[0].filename == '':
            return jsonify({
                "success": False,
                "error": "No files selected",
                "message": "Please select files to upload"
            }), 400
        
        # Limit to 4 files
        if len(files) > 4:
            return jsonify({
                "success": False,
                "error": "Too many files",
                "message": "Maximum 4 files allowed per session"
            }), 400
        
        # Create a new session
        session_id = session_manager.create_session()
        session = session_manager.get_session(session_id)
        session_dir = session["output_dir"]
        
        uploaded_files = []
        
        for i, file in enumerate(files):
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file_path = os.path.join(session_dir, filename)
                
                # Save file
                file.save(file_path)
                
                # Add to session with file type based on order or filename
                file_type = f"file_{i+1}"
                if 'pavement' in filename.lower():
                    file_type = "pavement"
                elif 'tcs' in filename.lower():
                    file_type = "tcs"
                elif 'embankment' in filename.lower():
                    file_type = "embankment"
                elif 'constant' in filename.lower():
                    file_type = "constant"
                
                session_manager.add_file_to_session(session_id, file_type, filename, file_path)
                uploaded_files.append({
                    "file_type": file_type,
                    "filename": filename,
                    "path": file_path
                })
        
        # Update session status
        session_manager.update_session_status(session_id, "files_uploaded")
        
        return jsonify({
            "success": True,
            "session_id": session_id,
            "uploaded_files": uploaded_files,
            "message": f"Successfully created session {session_id} and uploaded {len(uploaded_files)} files"
        }), 201
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "message": "Failed to upload files and create session"
        }), 500

@bp.route('/session/<session_id>/upload', methods=['POST'])
def upload_files(session_id):
    """Upload additional files to an existing session"""
    try:
        # Check if session exists
        session = session_manager.get_session(session_id)
        if not session:
            return jsonify({
                "success": False,
                "error": "Session not found",
                "message": "Invalid session ID"
            }), 404
        
        # Check if files are provided
        if 'files' not in request.files:
            return jsonify({
                "success": False,
                "error": "No files provided",
                "message": "Please provide files to upload"
            }), 400
        
        files = request.files.getlist('files')
        if not files or files[0].filename == '':
            return jsonify({
                "success": False,
                "error": "No files selected",
                "message": "Please select files to upload"
            }), 400
        
        # Check total file count (existing + new)
        existing_file_count = len(session.get("files", {}))
        if existing_file_count + len(files) > 4:
            return jsonify({
                "success": False,
                "error": "Too many files",
                "message": f"Maximum 4 files allowed per session. Already have {existing_file_count} files."
            }), 400
        
        uploaded_files = []
        session_dir = session["output_dir"]
        
        for i, file in enumerate(files):
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file_path = os.path.join(session_dir, filename)
                
                # Save file
                file.save(file_path)
                
                # Add to session with file type based on order or filename
                file_type = f"file_{existing_file_count + i + 1}"
                if 'pavement' in filename.lower():
                    file_type = "pavement"
                elif 'tcs' in filename.lower():
                    file_type = "tcs"
                elif 'embankment' in filename.lower():
                    file_type = "embankment"
                elif 'constant' in filename.lower():
                    file_type = "constant"
                
                session_manager.add_file_to_session(session_id, file_type, filename, file_path)
                uploaded_files.append({
                    "file_type": file_type,
                    "filename": filename,
                    "path": file_path
                })
        
        # Update session status
        session_manager.update_session_status(session_id, "files_uploaded")
        
        return jsonify({
            "success": True,
            "session_id": session_id,
            "uploaded_files": uploaded_files,
            "message": f"Successfully uploaded {len(uploaded_files)} files to session {session_id}"
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "message": "Failed to upload files"
        }), 500

@bp.route('/execute-calculation', methods=['POST'])
def execute_scripts():
    """Execute all scripts in src/processors and src/internal directories"""
    try:
        # Get session_id from request payload
        data = request.get_json()
        if not data or 'session_id' not in data:
            return jsonify({
                "success": False,
                "error": "Session ID required",
                "message": "Please provide session_id in request payload"
            }), 400
        
        session_id = data['session_id']
        
        # Check if session exists
        session = session_manager.get_session(session_id)
        if not session:
            return jsonify({
                "success": False,
                "error": "Session not found",
                "message": "Invalid session ID"
            }), 404
        
        # Check if files are uploaded
        if not session.get("files"):
            return jsonify({
                "success": False,
                "error": "No files found",
                "message": "Please upload files before executing scripts"
            }), 400
        
        # Update session status
        session_manager.update_session_status(session_id, "executing")
        
        # Execute processor scripts in specified order
        processor_results = []
        processor_scripts = [
            'src.processor.tcs_schedule',
            'src.processor.tcs_input',
            'src.processor.emb_height',
            'src.processor.pavement_input',
            'src.processor.constant_fill'
        ]
        
        for script_name in processor_scripts:
            try:
                module = __import__(script_name, fromlist=[''])
                if hasattr(module, 'main') or hasattr(module, 'process'):
                    result = {
                        "script": script_name,
                        "status": "executed",
                        "message": "Script executed successfully"
                    }
                    # Call main or process function if available with session_id
                    if hasattr(module, 'main') and callable(getattr(module, 'main')):
                        module.main(session_id)
                    elif hasattr(module, 'process') and callable(getattr(module, 'process')):
                        module.process(session_id)
                    processor_results.append(result)
                else:
                    processor_results.append({
                        "script": script_name,
                        "status": "skipped",
                        "message": "No main or process function found"
                    })
            except Exception as e:
                processor_results.append({
                    "script": script_name,
                    "status": "error",
                    "error": str(e),
                    "message": "Script execution failed"
                })
        
        # Execute internal scripts in specified order (excluding recalc)
        internal_results = []
        internal_scripts = [
            'src.internal.formula_applier',
            'src.internal.pavement_input_with_internal',
            'src.internal.final_sum_applier'
        ]
        
        for script_name in internal_scripts:
            try:
                module = __import__(script_name, fromlist=[''])
                if hasattr(module, 'main') or hasattr(module, 'process'):
                    result = {
                        "script": script_name,
                        "status": "executed",
                        "message": "Script executed successfully"
                    }
                    # Call main or process function if available
                    if hasattr(module, 'main') and callable(getattr(module, 'main')):
                        module.main(session_id)
                    elif hasattr(module, 'process') and callable(getattr(module, 'process')):
                        module.process(session_id)
                    internal_results.append(result)
                else:
                    internal_results.append({
                        "script": script_name,
                        "status": "skipped",
                        "message": "No main or process function found"
                    })
            except Exception as e:
                internal_results.append({
                    "script": script_name,
                    "status": "error",
                    "error": str(e),
                    "message": "Script execution failed"
                })
        
        # Update session status
        session_manager.update_session_status(session_id, "completed")
        
        return jsonify({
            "success": True,
            "session_id": session_id,
            "processor_results": processor_results,
            "internal_results": internal_results,
            "message": "Script execution completed"
        }), 200
        
    except Exception as e:
        # Update session status to error
        session_manager.update_session_status(session_id, "error")
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "message": "Failed to execute scripts"
        }), 500

@bp.route('/session/<session_id>/status', methods=['GET'])
def get_session_status(session_id):
    """Get session status and information"""
    try:
        session = session_manager.get_session(session_id)
        if not session:
            return jsonify({
                "success": False,
                "error": "Session not found",
                "message": "Invalid session ID"
            }), 404
        
        return jsonify({
            "success": True,
            "session": session,
            "message": "Session status retrieved successfully"
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "message": "Failed to get session status"
        }), 500

@bp.route('/sessions', methods=['GET'])
def list_sessions():
    """List all sessions"""
    try:
        sessions = session_manager.list_sessions()
        return jsonify({
            "success": True,
            "sessions": sessions,
            "count": len(sessions),
            "message": "Sessions retrieved successfully"
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "message": "Failed to retrieve sessions"
        }), 500

@bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "success": True,
        "status": "healthy",
        "message": "API is running"
    }), 200
