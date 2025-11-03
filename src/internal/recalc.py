#!/usr/bin/env python3
"""
Excel Formula Recalculation Script using LibreOffice

This script recalculates all formulas in an Excel file using LibreOffice Calc
and scans for formula errors.

Usage:
    python recalc.py <excel_file> [timeout_seconds]

Example:
    python recalc.py output.xlsx 30
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
    
    def _find_libreoffice(self):
        """Find LibreOffice installation path."""
        if self.system == 'Linux':
            # Common Linux paths
            possible_paths = [
                '/usr/bin/libreoffice',
                '/usr/bin/soffice',
                '/usr/local/bin/libreoffice',
                '/snap/bin/libreoffice'
            ]
        elif self.system == 'Darwin':  # macOS
            possible_paths = [
                '/Applications/LibreOffice.app/Contents/MacOS/soffice'
            ]
        elif self.system == 'Windows':
            possible_paths = [
                'C:\\Program Files\\LibreOffice\\program\\soffice.exe',
                'C:\\Program Files (x86)\\LibreOffice\\program\\soffice.exe'
            ]
        else:
            return None
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        return None
    
    def _check_libreoffice(self):
        """Check if LibreOffice is available."""
        if not self.libreoffice_path:
            raise RuntimeError(
                "LibreOffice not found. Please install LibreOffice:\n"
                "  Ubuntu/Debian: sudo apt-get install libreoffice\n"
                "  macOS: brew install --cask libreoffice\n"
                "  Or download from: https://www.libreoffice.org/download/"
            )
    
    def recalculate(self, excel_path, timeout=60):
        """
        Recalculate all formulas in the Excel file.
        
        Args:
            excel_path: Path to Excel file
            timeout: Timeout in seconds (default: 60)
        
        Returns:
            Dictionary with recalculation results
        """
        self._check_libreoffice()
        
        excel_path = Path(excel_path).resolve()
        
        if not excel_path.exists():
            raise FileNotFoundError(f"Excel file not found: {excel_path}")
        
        print(f"Recalculating formulas in: {excel_path}")
        
        try:
            # Convert file using LibreOffice headless mode
            cmd = [
                self.libreoffice_path,
                '--headless',
                '--calc',
                '--convert-to', 'xlsx',
                '--outdir', str(excel_path.parent),
                str(excel_path)
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
                print(f"stderr: {result.stderr}")
            
            print("✓ Formulas recalculated")
            
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"LibreOffice timed out after {timeout} seconds")
        
        except Exception as e:
            raise RuntimeError(f"Error running LibreOffice: {str(e)}")
        
        finally:
            # Clean up temporary macro file
            if os.path.exists(macro_path):
                os.remove(macro_path)
        
        # Scan for errors
        error_report = self.scan_errors(excel_path)
        
        return error_report
    
    def _create_recalc_macro(self):
        """Create LibreOffice Basic macro for recalculation."""
        return """
Sub RecalculateAll
    Dim oDoc As Object
    oDoc = ThisComponent
    oDoc.calculateAll()
    oDoc.store()
End Sub
"""
    
    def scan_errors(self, excel_path):
        """
        Scan Excel file for formula errors.
        
        Args:
            excel_path: Path to Excel file
        
        Returns:
            Dictionary with error report
        """
        from openpyxl import load_workbook
        
        print("Scanning for formula errors...")
        
        wb = load_workbook(excel_path, data_only=True)
        
        error_summary = {}
        total_errors = 0
        total_formulas = 0
        
        for sheet_name in wb.sheetnames:
            sheet = wb[sheet_name]
            
            for row in sheet.iter_rows():
                for cell in row:
                    # Check if cell has a value
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
        wb_formulas = load_workbook(excel_path, data_only=False)
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
            "total_formulas": total_formulas
        }
        
        if total_errors > 0:
            report["error_summary"] = error_summary
        
        return report


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python recalc.py <excel_file> [timeout_seconds]")
        print("\nExample:")
        print("  python recalc.py output.xlsx 30")
        return 1
    
    excel_file = sys.argv[1]
    timeout = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    
    try:
        recalculator = ExcelRecalculator()
        result = recalculator.recalculate(excel_file, timeout=timeout)
        
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