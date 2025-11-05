"""
Session Manager for MongoDB
"""
from pymongo import MongoClient
import os
from datetime import datetime
import traceback

class SessionManager:
    def __init__(self):
        self.client = MongoClient(os.getenv('MONGODB_URI'))
        self.db = self.client[os.getenv('MONGO_DB_NAME')]
        self.sessions = self.db[os.getenv('SESSION_COLLECTION', 'sessions')]
    
    def generate_session_id(self):
        """Generate session ID in format YYYYMMDD_HHMMSS"""
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def create_session(self, session_id):
        """Create new session"""
        session_data = {
            'session_id': session_id,
            'status': 'uploaded',
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
            'input_files': [],
            'output_file': None,
            'processing_info': {
                'started_at': None,
                'completed_at': None,
                'execution_time_seconds': None
            },
            'error_info': {
                'has_error': False,
                'error_message': None,
                'error_traceback': None,
                'failed_script': None
            },
            'metadata': {},
            'progress': {}  # Ensure this field exists
        }
        self.sessions.insert_one(session_data)
        return session_data
    
    def update_session_status(self, session_id, status, **updates):
        """Update session status and other fields"""
        update_data = {
            'status': status,
            'updated_at': datetime.now(),
            **updates
        }
        self.sessions.update_one(
            {'session_id': session_id},
            {'$set': update_data}
        )
    
    def update_progress(self, session_id, progress_data):
        """Update processing progress"""
        self.sessions.update_one(
            {'session_id': session_id},
            {'$set': {
                'progress': progress_data,
                'updated_at': datetime.now()
            }}
        )
    
    def add_input_file(self, session_id, file_info):
        """Add input file info to session"""
        self.sessions.update_one(
            {'session_id': session_id},
            {'$push': {'input_files': file_info}, '$set': {'updated_at': datetime.now()}}
        )
    
    def set_error(self, session_id, error_message, traceback_str, failed_script):
        """Set error information for session"""
        self.sessions.update_one(
            {'session_id': session_id},
            {'$set': {
                'status': 'failed',
                'updated_at': datetime.now(),
                'error_info': {
                    'has_error': True,
                    'error_message': error_message,
                    'error_traceback': traceback_str,
                    'failed_script': failed_script
                }
            }}
        )
    
    def set_output_file(self, session_id, output_info):
        """Set output file information"""
        self.sessions.update_one(
            {'session_id': session_id},
            {'$set': {
                'output_file': output_info,
                'status': 'completed',
                'updated_at': datetime.now(),
                'processing_info.completed_at': datetime.now()
            }}
        )
    
    def set_processing_started(self, session_id):
        """Set processing start time"""
        self.sessions.update_one(
            {'session_id': session_id},
            {'$set': {
                'processing_info.started_at': datetime.now(),
                'updated_at': datetime.now()
            }}
        )
    
    def get_session(self, session_id):
        """Get session by ID"""
        return self.sessions.find_one({'session_id': session_id})
    
    def session_exists(self, session_id):
        """Check if session exists"""
        return self.sessions.find_one({'session_id': session_id}) is not None