# BOQ Generation API Usage Guide

## Overview
This Flask API accepts 4 Excel files and generates a populated `main_carriageway_and_boq.xlsx` file with calculated BOQ (Bill of Quantities) data.

## Endpoints

### 1. Health Check
```
GET /
```
Returns API status and version information.

### 2. API Information
```
GET /api/generate-boq
```
Returns information about the generate-boq endpoint, including required files and formats.

### 3. Generate BOQ
```
POST /api/generate-boq
```
Main endpoint for generating BOQ. Accepts 4 Excel files and returns populated `main_carriageway.xlsx`.

## Required Files

You need to upload exactly 4 Excel files:

1. **tcs_schedule** → `TCS Schedule.xlsx` - Technical specification schedule
2. **tcs_input** → `TCS Input.xlsx` - Technical specification input data
3. **emb_height** → `Emb Height.xlsx` - Embankment height data
4. **pavement_input** → `Pavement Input.xlsx` - Pavement layer specifications

## Upload Methods

### Method 1: Using Specific Form Field Names (Recommended)

Use form fields with exact names:

```bash
curl -X POST http://localhost:5000/api/generate-boq \
  -F "tcs_schedule=@TCS Schedule.xlsx" \
  -F "tcs_input=@TCS Input.xlsx" \
  -F "emb_height=@Emb Height.xlsx" \
  -F "pavement_input=@Pavement Input.xlsx" \
  --output main_carriageway_and_boq.xlsx
```

### Method 2: Using 'files' Field (Auto-Matching)

Upload all files with field name 'files'. The API will try to match by filename:

```bash
curl -X POST http://localhost:5000/api/generate-boq \
  -F "files=@TCS Schedule.xlsx" \
  -F "files=@TCS Input.xlsx" \
  -F "files=@Emb Height.xlsx" \
  -F "files=@Pavement Input.xlsx" \
  --output main_carriageway_and_boq.xlsx
```

**Note:** For Method 2, ensure your files are named clearly (containing keywords like "tcs", "schedule", "input", "emb", "height", "pavement").

## Python Example

```python
import requests

url = "http://localhost:5000/api/generate-boq"

files = {
    'tcs_schedule': open('TCS Schedule.xlsx', 'rb'),
    'tcs_input': open('TCS Input.xlsx', 'rb'),
    'emb_height': open('Emb Height.xlsx', 'rb'),
    'pavement_input': open('Pavement Input.xlsx', 'rb')
}

response = requests.post(url, files=files)

if response.status_code == 200:
    with open('main_carriageway_and_boq.xlsx', 'wb') as f:
        f.write(response.content)
    print("BOQ generated successfully!")
else:
    print(f"Error: {response.json()}")
```

## JavaScript/Fetch Example

```javascript
const formData = new FormData();

formData.append('tcs_schedule', fileInput1.files[0]);
formData.append('tcs_input', fileInput2.files[0]);
formData.append('emb_height', fileInput3.files[0]);
formData.append('pavement_input', fileInput4.files[0]);

fetch('http://localhost:5000/api/generate-boq', {
    method: 'POST',
    body: formData
})
.then(response => {
    if (response.ok) {
        return response.blob();
    }
    return response.json().then(err => Promise.reject(err));
})
.then(blob => {
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'main_carriageway_and_boq.xlsx';
    a.click();
})
.catch(error => {
    console.error('Error:', error);
});
```

## HTML Form Example

```html
<form action="http://localhost:5000/api/generate-boq" method="post" enctype="multipart/form-data">
    <label>TCS Schedule:</label>
    <input type="file" name="tcs_schedule" accept=".xlsx,.xls" required><br>
    
    <label>TCS Input:</label>
    <input type="file" name="tcs_input" accept=".xlsx,.xls" required><br>
    
    <label>Emb Height:</label>
    <input type="file" name="emb_height" accept=".xlsx,.xls" required><br>
    
    <label>Pavement Input:</label>
    <input type="file" name="pavement_input" accept=".xlsx,.xls" required><br>
    
    <button type="submit">Generate BOQ</button>
</form>
```

## Response Format

### Success Response (200 OK)
- Content-Type: `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`
- Body: Binary Excel file (`main_carriageway_and_boq.xlsx`)
- Content-Disposition: `attachment; filename=main_carriageway_and_boq.xlsx`

### Error Response (400/500)
- Content-Type: `application/json`
- Body: JSON object with error details

Example error response:
```json
{
    "error": "Could not identify all required files",
    "message": "Please ensure your files are named clearly...",
    "missing_files": ["tcs_input"],
    "matched_files": ["tcs_schedule", "emb_height", "pavement_input"]
}
```

## File Size Limits

- Maximum file size per file: **100 MB**
- Allowed formats: `.xlsx`, `.xls`

## Processing Pipeline

The API runs the following processing steps in sequence:

1. **TCS Schedule Processing** - Reads basic chainage data
2. **TCS Input Processing** - Adds technical specifications
3. **Embankment Height Processing** - Adds embankment heights
4. **Pavement Input Processing** - Adds pavement thicknesses
5. **Constant Fill Processing** - Fills constant values
6. **Formula Applier Processing** - Applies calculation formulas
7. **Pavement Input with Internal Processing** - Calculates geogrid
8. **Final Sum Applier Processing** - Adds final sum formulas

## Notes

- The API creates/overwrites files in the `data/` and `output/` directories
- Processing time depends on data size (typically 30 seconds to 2 minutes)
- The template file (`template/main_carriageway.xlsx`) must exist before processing
- Formula templates (`formula_template.json`, `formula_final_sum_template.json`) must exist

## Troubleshooting

### Error: "Template file not found"
- Ensure `template/main_carriageway_and_boq.xlsx` exists in the project root

### Error: "Could not identify all required files"
- Use specific form field names (Method 1) for reliable matching
- Ensure file names contain recognizable keywords

### Error: "Processing failed"
- Check that all required JSON template files exist
- Verify input Excel files have correct structure
- Check server logs for detailed error messages

### Error: "Output file not generated"
- Processing may have completed but output file was not created
- Check that `output/` directory exists and is writable
- Review processing logs for errors

