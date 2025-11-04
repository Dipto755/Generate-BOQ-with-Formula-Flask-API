# BOQ Generation Flask API

A Flask-based API for generating Bill of Quantities (BOQ) with formula-based calculations for civil engineering and construction applications.

## Features

- Session-based file management
- Upload up to 4 input files per session
- Execute processing scripts from `src/processor` and `src/internal` directories
- Real-time session status tracking
- RESTful API design

## API Endpoints

### Session Management

#### Upload Files and Create Session (Primary Method)
```
POST /api/upload
Content-Type: multipart/form-data
```
Upload up to 4 files and automatically create a session. Files are automatically categorized based on filename.

**Request:**
```
files: [file1, file2, file3, file4]  (multipart form data)
```

**Response:**
```json
{
    "success": true,
    "session_id": "20251104_153310",
    "uploaded_files": [
        {
            "file_type": "pavement",
            "filename": "pavement_data.xlsx",
            "path": "uploads/20251104_153310/pavement_data.xlsx"
        }
    ],
    "message": "Successfully created session 20251104_153310 and uploaded 1 files"
}
```

#### Upload Additional Files to Existing Session
```
POST /api/session/<session_id>/upload
Content-Type: multipart/form-data
```
Upload additional files to an existing session (up to 4 files total per session).

**Request:**
```
files: [file1, file2, file3, file4]  (multipart form data)
```

**Response:**
```json
{
    "success": true,
    "session_id": "20251104_153310",
    "uploaded_files": [
        {
            "file_type": "tcs",
            "filename": "tcs_data.xlsx",
            "path": "uploads/20251104_153310/tcs_data.xlsx"
        }
    ],
    "message": "Successfully uploaded 1 files to session 20251104_153310"
}
```

#### Execute Scripts
```
POST /api/session/<session_id>/execute
```
Executes all scripts in `src/processor` and `src/internal` directories.

**Response:**
```json
{
    "success": true,
    "session_id": "20251104_153310",
    "processor_results": [
        {
            "script": "src.processor.constant_fill",
            "status": "executed",
            "message": "Script executed successfully"
        }
    ],
    "internal_results": [
        {
            "script": "src.internal.final_sum_applier",
            "status": "executed",
            "message": "Script executed successfully"
        }
    ],
    "message": "Script execution completed"
}
```

#### Get Session Status
```
GET /api/session/<session_id>/status
```
Retrieves session information and current status.

**Response:**
```json
{
    "success": true,
    "session": {
        "id": "20251104_153310",
        "created_at": "2025-11-04T15:33:10.123456",
        "files": {
            "pavement": {
                "filename": "pavement_data.xlsx",
                "path": "uploads/20251104_153310/pavement_data.xlsx",
                "uploaded_at": "2025-11-04T15:33:15.123456"
            }
        },
        "status": "completed",
        "output_dir": "uploads/20251104_153310"
    },
    "message": "Session status retrieved successfully"
}
```

#### List All Sessions
```
GET /api/sessions
```
Lists all available sessions.

**Response:**
```json
{
    "success": true,
    "sessions": [...],
    "count": 5,
    "message": "Sessions retrieved successfully"
}
```

#### Health Check
```
GET /api/health
```
Checks if the API is running.

**Response:**
```json
{
    "success": true,
    "status": "healthy",
    "message": "API is running"
}
```

### Root Endpoint
```
GET /
```
Returns API information and available endpoints.

## File Upload Details

### Allowed File Types
- Excel files: `.xlsx`, `.xls`
- CSV files: `.csv`
- Text files: `.txt`
- JSON files: `.json`

### File Categorization
Files are automatically categorized based on filename patterns:
- Files containing "pavement" → `pavement`
- Files containing "tcs" → `tcs`
- Files containing "embankment" → `embankment`
- Files containing "constant" → `constant`
- Others → `file_1`, `file_2`, etc.

### File Size Limit
Maximum file size: 16MB per request

## Session Management

### Session ID Format
Session IDs are generated using datetime only:
```
YYYYMMDD_HHMMSS
Example: 20251104_153310
```

### Session States
- `created` - Session initialized
- `files_uploaded` - Files uploaded successfully
- `executing` - Scripts are running
- `completed` - All scripts executed successfully
- `error` - An error occurred during execution

### Session Storage
- Sessions are stored in the `uploads/` directory
- Each session has its own subdirectory
- Session metadata is saved as `session_metadata.json`

## Installation and Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
python main.py
```

The API will be available at `http://localhost:5000`

## Usage Example

1. **Upload files and create session (recommended):**
```bash
curl -X POST \
  http://localhost:5000/api/upload \
  -F "files=@pavement_data.xlsx" \
  -F "files=@tcs_data.xlsx" \
  -F "files=@embankment_data.xlsx" \
  -F "files=@constant_data.xlsx"
```

2. **Execute scripts:**
```bash
curl -X POST http://localhost:5000/api/session/20251104_153310/execute
```

3. **Check status:**
```bash
curl http://localhost:5000/api/session/20251104_153310/status
```

4. **Alternative: Upload additional files to existing session:**
```bash
curl -X POST \
  http://localhost:5000/api/session/20251104_153310/upload \
  -F "files=@additional_file.xlsx"
```

## Project Structure

```
Generate-BOQ-with-Formula-Flask-API/
├── api/
│   ├── __init__.py
│   ├── app.py              # Flask app factory
│   ├── routes.py           # API endpoints
│   └── session_manager.py  # Session management
├── src/
│   ├── internal/           # Internal processing scripts
│   └── processor/          # Data processing scripts
├── uploads/                # Session storage
├── main.py                 # Application entry point
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## Environment Variables

- `PORT` - Server port (default: 5000)
- `FLASK_DEBUG` - Debug mode (default: True)

## Error Handling

The API includes comprehensive error handling:
- File upload validation
- Session validation
- Script execution error tracking
- Detailed error messages and tracebacks
