import os
from api.app import create_app

if __name__ == '__main__':
    # Create the Flask app
    app = create_app()
    
    # Run the application
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'True').lower() == 'true'
    
    print(f"Starting BOQ Generation API on port {port}")
    print(f"Debug mode: {debug}")
    print(f"Available endpoints:")
    print("  POST /api/upload-inputs - Upload files and create session automatically")
    print("  POST /api/session/<session_id>/upload - Upload additional files to existing session")
    print("  POST /api/execute-calculation - Execute scripts")
    print("  GET /api/session/<session_id>/status - Get session status")
    print("  GET /api/sessions - List all sessions")
    print("  GET /api/health - Health check")
    print("  GET / - API information")
    
    app.run(host='0.0.0.0', port=port, debug=debug)
