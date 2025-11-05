# Postman Collection Usage Guide

## Overview
This document explains how to use the BOQ Generation API Postman collection to test the Flask API endpoints.

## Import the Collection

1. Open Postman
2. Click **Import** in the top left
3. Select **File** tab
4. Choose `BOQ_Generation_API.postman_collection.json`
5. Click **Import**

## Prerequisites

1. **Start the Flask API server:**
   ```bash
   python main.py
   ```
   The server should start on `http://localhost:5000`

2. **Prepare your Excel files:**
   - TCS Schedule.xlsx
   - TCS Input.xlsx
   - Emb Height.xlsx
   - Pavement Input.xlsx

## Available Requests

### 1. Health Check
- **Method**: GET
- **URL**: `http://localhost:5000/`
- **Purpose**: Verify the API server is running
- **Expected Response**: JSON with status "healthy"

### 2. API Information
- **Method**: GET
- **URL**: `http://localhost:5000/api/generate-boq`
- **Purpose**: Get detailed information about the API endpoints
- **Expected Response**: JSON with API documentation

### 3. Generate BOQ - Specific Form Fields (Recommended)
- **Method**: POST
- **URL**: `http://localhost:5000/api/generate-boq`
- **Body Type**: form-data
- **Fields**:
  - `tcs_schedule`: TCS Schedule.xlsx file
  - `tcs_input`: TCS Input.xlsx file
  - `emb_height`: Emb Height.xlsx file
  - `pavement_input`: Pavement Input.xlsx file
- **Expected Response**: Downloads `main_carriageway.xlsx` file

#### How to Use:
1. Click on "Generate BOQ - Specific Form Fields"
2. Go to **Body** tab → **form-data**
3. For each key, click **Select Files** and choose the corresponding Excel file
4. Click **Send**
5. The response will download the generated BOQ file

### 4. Generate BOQ - Generic Files Upload (Alternative)
- **Method**: POST
- **URL**: `http://localhost:5000/api/generate-boq`
- **Body Type**: form-data
- **Fields**:
  - `files`: All 4 Excel files (select multiple files)
- **Expected Response**: Downloads `main_carriageway.xlsx` file

#### How to Use:
1. Click on "Generate BOQ - Generic Files Upload"
2. Go to **Body** tab → **form-data**
3. Click **Select Files** next to `files` key
4. Choose all 4 Excel files (must be exactly 4 files)
5. Click **Send**

### 5. Error Test - No Files
- **Method**: POST
- **URL**: `http://localhost:5000/api/generate-boq`
- **Purpose**: Test error handling when no files are provided
- **Expected Response**: JSON error with status 400

### 6. Error Test - Invalid File Type
- **Method**: POST
- **URL**: `http://localhost:5000/api/generate-boq`
- **Purpose**: Test error handling when invalid file types are uploaded
- **Expected Response**: JSON error with status 400

## File Requirements

- **Format**: Only `.xlsx` and `.xls` files are accepted
- **Size**: Maximum 100MB per file
- **Count**: Exactly 4 files required
- **Names**: 
  - For specific fields: any filename is accepted
  - For generic upload: filenames should contain recognizable patterns for automatic matching

## Environment Variables

The collection includes one environment variable:
- `baseUrl`: `http://localhost:5000` (automatically set)

You can modify this if your API is running on a different host or port.

## Testing Workflow

1. **Start with Health Check** to verify the server is running
2. **Check API Information** to understand the requirements
3. **Test the main functionality** using either:
   - "Generate BOQ - Specific Form Fields" (recommended)
   - "Generate BOQ - Generic Files Upload" (alternative)
4. **Test error scenarios** to understand validation:
   - "Error Test - No Files"
   - "Error Test - Invalid File Type"

## Expected Success Response

When the BOQ generation is successful, you'll receive:
- **Status**: 200 OK
- **Content-Type**: `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`
- **Body**: Binary file download named `main_carriageway.xlsx`

## Common Error Responses

### 400 Bad Request
```json
{
    "error": "Validation error",
    "message": "Specific error details"
}
```

### 404 Not Found
```json
{
    "error": "File not found", 
    "message": "Template file not found"
}
```

### 500 Internal Server Error
```json
{
    "error": "Processing failed",
    "message": "Error details",
    "details": "Stack trace"
}
```

## Troubleshooting

### Issues with File Upload
- Ensure all files are Excel format (.xlsx or .xls)
- Check file sizes are under 100MB
- Verify you have exactly 4 files
- For generic upload, ensure filenames contain recognizable patterns

### Server Connection Issues
- Verify the Flask server is running on port 5000
- Check if another application is using port 5000
- Ensure `python main.py` was executed successfully

### Processing Issues
- Check that the `template/main_carriageway.xlsx` file exists
- Verify all required Python packages are installed
- Check the server console for detailed error messages

## Automated Tests

The collection includes basic test scripts for error scenarios:
- Verifies HTTP status codes (400 for errors)
- Validates error response structure (contains 'error' and 'message' fields)

You can extend these tests or add new ones using Postman's test scripting features.
