from flask import Flask
from .routes import bp

def create_app(config_name='development'):
    """Create and configure the Flask application"""
    app = Flask(__name__)
    
    # Configuration
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
    app.config['UPLOAD_FOLDER'] = 'uploads'
    app.config['SECRET_KEY'] = 'your-secret-key-here'  # Change in production
    
    # Register blueprints
    app.register_blueprint(bp, url_prefix='/api')
    
    # Basic error handlers
    @app.errorhandler(413)
    def too_large(e):
        return {
            "success": False,
            "error": "File too large",
            "message": "File size exceeds maximum limit (16MB)"
        }, 413
    
    @app.errorhandler(404)
    def not_found(e):
        return {
            "success": False,
            "error": "Not found",
            "message": "The requested resource was not found"
        }, 404
    
    @app.errorhandler(500)
    def internal_error(e):
        return {
            "success": False,
            "error": "Internal server error",
            "message": "An unexpected error occurred"
        }, 500
    
    @app.route('/')
    def index():
        """Root endpoint"""
        return {
            "success": True,
            "message": "BOQ Generation API",
            "version": "1.0.0",
            "endpoints": {
                "upload_and_create_session": "POST /api/upload-inputs",
                "upload_files_to_session": "POST /api/session/<session_id>/upload",
                "execute_scripts": "POST /api/execute-calculation",
                "get_session_status": "GET /api/session/<session_id>/status",
                "list_sessions": "GET /api/sessions",
                "health_check": "GET /api/health"
            }
        }
    
    return app
