import json
from pathlib import Path
from openpyxl import load_workbook

class FormulaApplier:
    """Apply formula templates to data/main_carriageway.xlsx by replacing {row} placeholders."""
    
    def __init__(self, template_path=None, excel_path=None):
        if template_path is None:
            # Look for formula_template.json in project root
            current_dir = Path(__file__).parent
            template_path = current_dir.parent.parent / "formula_template.json"
        
        self.template_path = Path(template_path)
        self.template = self._load_template()
        
        # Handle Excel file path - look in root/data/ directory
        if excel_path is None:
            current_dir = Path(__file__).parent
            # Go up two levels from src/internal to reach project root, then into data/
            self.excel_path = current_dir.parent.parent / "data" / "main_carriageway.xlsx"
        else:
            self.excel_path = Path(excel_path)
    
    def _load_template(self):
        """Load the formula template from JSON file."""
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template file not found: {self.template_path}")
        
        with open(self.template_path, 'r') as f:
            return json.load(f)
    
    def apply_formulas_to_all_data_rows(self, reference_column='D'):
        """
        Automatically detect data rows based on a reference column and apply formulas
        to the 'Main Carriageway' sheet.
        
        Args:
            reference_column: Column to check for data (default: 'D')
        
        Returns:
            Dictionary with statistics about the operation
        """
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
        
        wb = load_workbook(self.excel_path)
        sheet_name = "Main Carriageway"
        
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"Sheet '{sheet_name}' not found in {self.excel_path}")
        
        sheet = wb[sheet_name]
        formulas = self.template.get("formulas", {})
        
        # Find data rows
        data_rows = []
        for row_num in range(1, sheet.max_row + 1):
            cell_value = sheet[f'{reference_column}{row_num}'].value
            if cell_value is not None and str(cell_value).strip():
                data_rows.append(row_num)
        
        if not data_rows:
            raise ValueError(f"No data found in column {reference_column}")
        
        # Apply formulas to all data rows
        total_count = 0
        for row_num in data_rows:
            for col_letter, formula_template in formulas.items():
                if formula_template:
                    formula = formula_template.replace("{row}", str(row_num))
                    sheet[f"{col_letter}{row_num}"] = formula
                    total_count += 1
        
        wb.save(self.excel_path)
        wb.close()
        
        return {
            "first_row": min(data_rows),
            "last_row": max(data_rows),
            "total_rows": len(data_rows),
            "formulas_per_row": len(formulas),
            "total_formulas": total_count
        }
    
    def get_template_info(self):
        """Get information about the loaded template."""
        formulas = self.template.get("formulas", {})
        return {
            "template_name": self.template.get("template_name"),
            "source_file": self.template.get("source_file"),
            "sheet_name": self.template.get("sheet_name"),
            "source_row": self.template.get("source_row"),
            "column_range": self.template.get("column_range"),
            "total_formulas": len(formulas)
        }


def main():
    """Command line interface for applying formulas."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Apply formula templates to main_carriageway.xlsx")
    parser.add_argument("--ref-column", default="D", help="Reference column for auto detection")
    parser.add_argument("--template", help="Path to formula template JSON")
    parser.add_argument("--excel", help="Path to main_carriageway.xlsx file")
    
    args = parser.parse_args()
    
    applier = FormulaApplier(template_path=args.template, excel_path=args.excel)
    info = applier.get_template_info()
    print(f"Loaded template: {info['template_name']}")
    print(f"Total formulas: {info['total_formulas']}")
    print(f"Column range: {info['column_range']}\n")
    
    print(f"Auto-detecting data rows from column {args.ref_column}...")
    result = applier.apply_formulas_to_all_data_rows(args.ref_column)
    print(f"✓ Detected data rows: {result['first_row']} to {result['last_row']}")
    print(f"✓ Applied {result['total_formulas']} formulas to {result['total_rows']} rows")
    print(f"\nFormulas written to: {applier.excel_path} (Sheet: Main Carriageway)")
    
    return 0


if __name__ == "__main__":
    exit(main())