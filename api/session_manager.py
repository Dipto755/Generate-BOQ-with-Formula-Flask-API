import os
from datetime import datetime
from typing import Dict, Optional
import json

class SessionManager:
    def __init__(self, sessions_dir: str = "uploads"):
        self.sessions_dir = sessions_dir
        self.sessions: Dict[str, Dict] = {}
        self._ensure_sessions_dir()
    
    def _ensure_sessions_dir(self):
        """Ensure the sessions directory exists"""
        if not os.path.exists(self.sessions_dir):
            os.makedirs(self.sessions_dir)
    
    def create_session(self) -> str:
        """Create a new session with datetime-based ID"""
        now = datetime.now()
        session_id = now.strftime("%Y%m%d_%H%M%S")
        
        # Create session directory
        session_path = os.path.join(self.sessions_dir, session_id)
        os.makedirs(session_path, exist_ok=True)
        
        # Initialize session data
        self.sessions[session_id] = {
            "id": session_id,
            "created_at": now.isoformat(),
            "files": {},
            "status": "created",
            "output_dir": session_path
        }
        
        # Save session metadata
        self._save_session_metadata(session_id)
        
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Dict]:
        """Get session information"""
        if session_id in self.sessions:
            return self.sessions[session_id]
        
        # Try to load from disk if not in memory
        self._load_session_metadata(session_id)
        return self.sessions.get(session_id)
    
    def add_file_to_session(self, session_id: str, file_type: str, filename: str, file_path: str) -> bool:
        """Add a file to a session"""
        session = self.get_session(session_id)
        if not session:
            return False
        
        session["files"][file_type] = {
            "filename": filename,
            "path": file_path,
            "uploaded_at": datetime.now().isoformat()
        }
        
        self._save_session_metadata(session_id)
        return True
    
    def update_session_status(self, session_id: str, status: str) -> bool:
        """Update session status"""
        session = self.get_session(session_id)
        if not session:
            return False
        
        session["status"] = status
        session["last_updated"] = datetime.now().isoformat()
        
        self._save_session_metadata(session_id)
        return True
    
    def get_session_file_path(self, session_id: str, file_type: str) -> Optional[str]:
        """Get the path of a specific file in a session"""
        session = self.get_session(session_id)
        if not session or file_type not in session["files"]:
            return None
        
        return session["files"][file_type]["path"]
    
    def _save_session_metadata(self, session_id: str):
        """Save session metadata to disk"""
        session = self.sessions.get(session_id)
        if not session:
            return
        
        metadata_path = os.path.join(self.sessions[session_id]["output_dir"], "session_metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(session, f, indent=2)
    
    def _load_session_metadata(self, session_id: str):
        """Load session metadata from disk"""
        session_path = os.path.join(self.sessions_dir, session_id)
        metadata_path = os.path.join(session_path, "session_metadata.json")
        
        if not os.path.exists(metadata_path):
            return
        
        try:
            with open(metadata_path, 'r') as f:
                session_data = json.load(f)
                self.sessions[session_id] = session_data
        except (json.JSONDecodeError, IOError):
            pass
    
    def list_sessions(self) -> list:
        """List all available sessions"""
        # Load all sessions from disk
        if os.path.exists(self.sessions_dir):
            for session_id in os.listdir(self.sessions_dir):
                session_path = os.path.join(self.sessions_dir, session_id)
                if os.path.isdir(session_path):
                    self._load_session_metadata(session_id)
        
        return list(self.sessions.values())

# Global session manager instance
session_manager = SessionManager()
