# Generate BOQ with Formula Flask API - File Structure

## Project Overview
This is a Flask API application designed to generate Bill of Quantities (BOQ) with formula-based calculations for construction/civil engineering projects. The application processes Excel files, applies complex formulas, and generates automated quantity calculations.

## Directory Structure

```
Generate-BOQ-with-Formula-Flask-API/
├── .gitignore                      # Git ignore file
├── main.py                         # Main Flask application entry point
├── requirements.txt                # Python dependencies
├── extractor.py                    # Formula extraction utility from Excel files
├── formula_template.json           # Extracted formula templates and mappings
├── FILE_STRUCTURE.md               # This file - project documentation
│
├── data/                           # Data directory containing Excel files
│   ├── .~lock.Main Carriageway.xlsx# Excel lock file (temporary)
│   ├── Emb Height.xlsx            # Embankment height data
│   ├── Pavement Input.xlsx        # Pavement input data
│   ├── TCS Input.xlsx             # TCS (Technical Specifications) input
│   ├── TCS Schedule.xlsx          # TCS schedule data
│   └── tcs_specifications.json    # TCS specifications configuration
│
├── template/                       # Template directory for cleaned Excel files
│   └── main_carriageway.xlsx      # Cleaned main carriageway template
│
└── src/                           # Source code directory
    ├── __init__.py                # Package initialization
    ├── main_carriageway_template.py # Excel template cleaning utility
    ├── sequential.py              # Master orchestration script (replaces run_all.py)
    │
    ├── internal/                  # Internal utilities and core logic
    │   ├── __init__.py           # Internal package initialization
    │   ├── formula_applier.py    # Core formula application engine
    │   └── recalc.py             # Recalculation utilities
    │
    └── processor/                # Data processing modules
        ├── __init__.py           # Processor package initialization
        ├── constant_fill.py      # Constant value filling processor
        ├── emb_height.py         # Embankment height processing
        ├── pavement_input.py     # Pavement input data processing
        ├── tcs_input.py          # TCS input data processing
        └── tcs_schedule.py       # TCS schedule data processing
```

## File Descriptions

### Root Level Files

#### `main.py`
- **Purpose**: Main Flask application entry point
- **Functionality**: API endpoints, request handling, response formatting
- **Dependencies**: Flask, internal modules

#### `requirements.txt`
- **Purpose**: Python package dependencies
- **Key Dependencies**: 
  - Flask (web framework)
  - openpyxl (Excel file processing)
  - pandas (data manipulation)
  - pymongo (MongoDB integration)
  - redis (caching)

#### `extractor.py`
- **Purpose**: Utility to extract formulas from Excel files
- **Functionality**:
  - Reads Excel formulas from specific cells
  - Generalizes formulas by replacing row numbers with placeholders
  - Creates reusable formula templates
- **Output**: Generates `formula_template.json`

#### `formula_template.json`
- **Purpose**: Contains extracted formula templates
- **Structure**:
  - Template metadata (name, source file, sheet, row)
  - Column-to-formula mappings
  - Generalized formulas with `{row}` placeholders

### Data Directory (`data/`)

#### Excel Files
- **Main Carriageway.xlsx**: Primary quantity calculation data
- **Emb Height.xlsx**: Embankment height specifications
- **Pavement Input.xlsx**: Pavement construction parameters
- **TCS Input.xlsx**: Technical specifications input
- **TCS Schedule.xlsx**: Technical specifications schedule

#### Configuration Files
- **tcs_specifications.json**: Technical specifications configuration

### Template Directory (`template/`)

#### Excel Templates
- **main_carriageway.xlsx**: Cleaned template file for main carriageway calculations

### Source Code (`src/`)

#### Root Level Files

##### `main_carriageway_template.py`
- **Purpose**: Excel template cleaning utility
- **Functionality**:
  - Cleans and prepares Excel templates for processing
  - Removes data while preserving structure and formulas
  - Unmerges cells and clears content from specified rows
  - Creates reusable templates from working Excel files

##### `sequential.py`
- **Purpose**: Master orchestration script (replaces `run_all.py`)
- **Functionality**:
  - Executes all processing scripts in correct order
  - Error handling and reporting
  - Progress tracking and status reporting
  - Comprehensive execution summary

#### Internal Module (`src/internal/`)

##### `formula_applier.py`
- **Purpose**: Core engine for applying formulas to data
- **Functionality**:
  - Applies generalized formulas to specific rows
  - Handles complex Excel formula translations
  - Manages calculation dependencies
  - Validates formula inputs

##### `recalc.py`
- **Purpose**: Recalculation utilities
- **Functionality**:
  - Batch recalculation of quantities
  - Dependency resolution
  - Error handling for circular references

#### Processor Module (`src/processor/`)

##### Individual Processors
- **`constant_fill.py`**: Fills specific columns with constant values (e.g., subgrade thickness)
- **`emb_height.py`**: Processes embankment height calculations
- **`pavement_input.py`**: Handles pavement quantity calculations
- **`tcs_input.py`**: Processes technical specification inputs
- **`tcs_schedule.py`**: Manages TCS schedule-based calculations

## Data Flow

1. **Input**: Excel files with raw data and formulas
2. **Template Preparation**: `main_carriageway_template.py` cleans and prepares Excel templates
3. **Extraction**: `extractor.py` pulls formulas from Excel → `formula_template.json`
4. **Processing**: `sequential.py` orchestrates individual processors in correct order
5. **Calculation**: `formula_applier.py` applies formulas to data
6. **Recalculation**: `recalc.py` performs final recalculations and validations
7. **Output**: Generated BOQ with calculated quantities

## Current Issues and Recommendations

### ⚠️ Identified Issues

1. **Missing Main Carriageway Excel File**: 
   - Expected: `data/Main Carriageway.xlsx`
   - Status: File not found (only lock file exists)
   - Impact: `constant_fill.py` processor will fail

2. **File Name Inconsistency**:
   - `constant_fill.py` references `main_carriageway.xlsx` (lowercase)
   - Documentation shows `Main Carriageway.xlsx` (title case)
   - Impact: File path mismatches

3. **Empty main.py**:
   - File exists but contains 0 bytes
   - Impact: Flask API application cannot start

### 🔧 Recommended Fixes

1. **Restore Missing Excel File**:
   ```bash
   # Copy from template if available, or restore from backup
   cp template/main_carriageway.xlsx data/Main\ Carriageway.xlsx
   ```

2. **Standardize File Names**:
   - Update `constant_fill.py` to use consistent naming
   - Either use lowercase everywhere or title case everywhere

3. **Implement main.py Flask Application**:
   - Create basic Flask API structure
   - Add endpoints for BOQ generation
   - Integrate with existing processing pipeline

4. **Add Error Handling**:
   - Add file existence checks in processors
   - Implement graceful fallbacks for missing files

## Key Features

- **Formula Templates**: Reusable Excel formula extraction and application
- **Batch Processing**: Automated processing of multiple data types
- **Error Handling**: Comprehensive error management and reporting
- **Modular Design**: Separated concerns for maintainability
- **Excel Integration**: Direct processing of Excel files and formulas

## Dependencies

### Core Framework
- **Flask**: Web framework and API server
- **Flask-CORS**: Cross-origin resource sharing

### Data Processing
- **pandas**: Data manipulation and analysis
- **openpyxl**: Excel file reading/writing
- **xlrd**: Legacy Excel file support
- **numpy**: Numerical computations

### Storage & Caching
- **pymongo**: MongoDB database integration
- **redis**: In-memory caching

### Utilities
- **python-dotenv**: Environment variable management
- **click**: Command line interface
- **python-dateutil**: Date/time utilities

## Development Notes

- The project follows a modular architecture with clear separation of concerns
- Formula extraction is designed to be reusable across different Excel templates
- Processing scripts are designed to be run independently or as a batch
- Error handling is comprehensive with detailed logging
- The system supports both manual and automated processing workflows
