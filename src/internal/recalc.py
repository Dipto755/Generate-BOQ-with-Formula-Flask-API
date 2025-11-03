#!/usr/bin/env python3
"""
Excel Formula Recalculation Script for main_carriageway.xlsx

This script recalculates all formulas in data/main_carriageway.xlsx using LibreOffice Calc
and scans for formula errors.

Usage:
    python recalc.py [timeout_seconds]
"""

import sys
import json
import subprocess
import time
import os
import tempfile
from pathlib import Path
import platform


class ExcelRecalculator:
    """Recalculate Excel formulas using LibreOffice and detect errors."""
    
    # Excel error types to detect
    ERROR_TYPES = ['#REF!', '#DIV/0!', '#VALUE!', '#N/A', '#NAME?', '#NULL!', '#NUM!']
    
    def __init__(self):
        self.system = platform.system()
        self.libreoffice_path = self._find_libreoffice()
        
        # Fix Excel path - go up two levels from src/internal to project root, then into data/
        current_dir = Path(__file__).parent
        self.excel_path = current_dir.parent.parent / "data" / "main_carriageway.xlsx"
    
    def _find_libreoffice(self):
        """Find LibreOffice installation path."""
        if self.system == 'Linux':
            return '/usr/bin/libreoffice'
        elif self.system == 'Darwin':  # macOS
            return '/Applications/LibreOffice.app/Contents/MacOS/soffice'
        elif self.system == 'Windows':
            return 'C:\\Program Files\\LibreOffice\\program\\soffice.exe'
        return None
    
    def _check_libreoffice(self):
        """Check if LibreOffice is available."""
        if not self.libreoffice_path or not os.path.exists(self.libreoffice_path):
            raise RuntimeError(
                "LibreOffice not found. Please install LibreOffice:\n"
                "  Ubuntu/Debian: sudo apt-get install libreoffice\n"
                "  macOS: brew install --cask libreoffice\n"
                "  Or download from: https://www.libreoffice.org/download/"
            )
    
    def recalculate(self, timeout=60):
        """
        Recalculate all formulas in data/main_carriageway.xlsx.
        
        Args:
            timeout: Timeout in seconds (default: 60)
        
        Returns:
            Dictionary with recalculation results
        """
        self._check_libreoffice()
        
        print(f"Looking for Excel file at: {self.excel_path}")
        print(f"Excel file exists: {self.excel_path.exists()}")
        
        if not self.excel_path.exists():
            # Try alternative path - one level up from src/internal to src/data/
            alternative_path = Path(__file__).parent.parent / "data" / "main_carriageway.xlsx"
            print(f"Trying alternative path: {alternative_path}")
            print(f"Alternative path exists: {alternative_path.exists()}")
            
            if alternative_path.exists():
                self.excel_path = alternative_path
                print(f"Using alternative path: {self.excel_path}")
            else:
                raise FileNotFoundError(f"Excel file not found at: {self.excel_path}\nAlso tried: {alternative_path}")
        
        print(f"Recalculating formulas in: {self.excel_path}")
        
        try:
            # Convert file using LibreOffice headless mode (this recalculates formulas)
            cmd = [
                self.libreoffice_path,
                '--headless',
                '--calc',
                '--convert-to', 'xlsx',
                '--outdir', str(self.excel_path.parent),
                str(self.excel_path)
            ]
            
            print("Running LibreOffice...")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            if result.returncode != 0:
                print(f"Warning: LibreOffice returned code {result.returncode}")
                if result.stderr:
                    print(f"stderr: {result.stderr}")
            
            print("✓ Formulas recalculated")
            
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"LibreOffice timed out after {timeout} seconds")
        
        except Exception as e:
            raise RuntimeError(f"Error running LibreOffice: {str(e)}")
        
        # Scan for errors
        error_report = self.scan_errors()
        
        return error_report
    
    def scan_errors(self):
        """
        Scan Excel file for formula errors.
        
        Returns:
            Dictionary with error report
        """
        from openpyxl import load_workbook
        
        print("Scanning for formula errors...")
        
        wb = load_workbook(self.excel_path, data_only=True)
        
        error_summary = {}
        total_errors = 0
        total_formulas = 0
        
        for sheet_name in wb.sheetnames:
            sheet = wb[sheet_name]
            
            for row in sheet.iter_rows():
                for cell in row:
                    if cell.value is not None:
                        cell_value = str(cell.value)
                        
                        # Check if it's an error
                        for error_type in self.ERROR_TYPES:
                            if error_type in cell_value:
                                total_errors += 1
                                
                                if error_type not in error_summary:
                                    error_summary[error_type] = {
                                        'count': 0,
                                        'locations': []
                                    }
                                
                                error_summary[error_type]['count'] += 1
                                error_summary[error_type]['locations'].append(
                                    f"{sheet_name}!{cell.coordinate}"
                                )
        
        # Count formulas
        wb_formulas = load_workbook(self.excel_path, data_only=False)
        for sheet_name in wb_formulas.sheetnames:
            sheet = wb_formulas[sheet_name]
            for row in sheet.iter_rows():
                for cell in row:
                    if cell.value and isinstance(cell.value, str) and cell.value.startswith('='):
                        total_formulas += 1
        
        wb.close()
        wb_formulas.close()
        
        # Build report
        if total_errors > 0:
            status = "errors_found"
            print(f"⚠ Found {total_errors} formula errors")
        else:
            status = "success"
            print("✓ No formula errors found")
        
        report = {
            "status": status,
            "total_errors": total_errors,
            "total_formulas": total_formulas,
            "file": str(self.excel_path)
        }
        
        if total_errors > 0:
            report["error_summary"] = error_summary
        
        return report


def main():
    """Main entry point."""
    timeout = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    
    try:
        recalculator = ExcelRecalculator()
        result = recalculator.recalculate(timeout=timeout)
        
        # Print JSON report
        print("\nRecalculation Report:")
        print(json.dumps(result, indent=2))
        
        # Return appropriate exit code
        return 0 if result['status'] == 'success' else 1
    
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())