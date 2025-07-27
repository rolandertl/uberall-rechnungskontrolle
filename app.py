import streamlit as st
import pandas as pd
import chardet
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import re

class FileProcessor:
    """Handles file loading and preprocessing for uberall billing control"""
    
    @staticmethod
    def detect_encoding(file_bytes: bytes) -> str:
        """Detect encoding of uploaded file"""
        detected = chardet.detect(file_bytes)
        encoding = detected.get('encoding', 'utf-8')
        # Common encodings for German CSV files
        if encoding in ['Windows-1252', 'cp1252', 'iso-8859-1']:
            return 'cp1252'
        return 'utf-8'
    
    @staticmethod
    def load_crm_file(uploaded_file) -> pd.DataFrame:
        """Load and process CRM export CSV file"""
        try:
            # Read file bytes for encoding detection
            file_bytes = uploaded_file.read()
            uploaded_file.seek(0)  # Reset file pointer
            
            # Detect encoding
            encoding = FileProcessor.detect_encoding(file_bytes)
            
            # Try different delimiters (semicolon first for German CSV)
            delimiters = [';', ',', '\t']
            df = None
            
            for delimiter in delimiters:
                try:
                    uploaded_file.seek(0)
                    df = pd.read_csv(uploaded_file, 
                                   delimiter=delimiter, 
                                   encoding=encoding,
                                   dtype=str)  # Keep all as strings initially
                    
                    # Check if we have the required column
                    if 'uberall-Location-ID' in df.columns:
                        break
                    
                except Exception:
                    continue
            
            if df is None:
                raise ValueError("Could not parse CRM file with any delimiter")
            
            # Validate required columns
            required_columns = ['uberall-Location-ID', 'Projektname', 'Workflow-Status']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                st.error(f"CRM-Datei fehlt erforderliche Spalten: {missing_columns}")
                return pd.DataFrame()
            
            # Filter out empty uberall-Location-ID entries
            df = df[df['uberall-Location-ID'].notna()]
            df = df[df['uberall-Location-ID'].str.strip() != '']
            
            return df
            
        except Exception as e:
            st.error(f"Fehler beim Laden der CRM-Datei: {str(e)}")
            return pd.DataFrame()
    
    @staticmethod
    def load_uberall_file(uploaded_file) -> pd.DataFrame:
        """Load and process uberall billing XLSX file"""
        try:
            # Read Excel file
            df = pd.read_excel(uploaded_file, dtype={'location id': str})
            
            # Validate required columns
            required_columns = ['location id', 'salespartner name', 'location state', 'name']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                st.error(f"uberall-Datei fehlt erforderliche Spalten: {missing_columns}")
                return pd.DataFrame()
            
            # Filter by relevant salespartners
            relevant_partners = ["Edelweiss Digital GmbH", "Edelweiss (Russmedia)"]
            df_filtered = df[df['salespartner name'].isin(relevant_partners)].copy()
            
            st.info(f"📊 Gefiltert nach relevanten Salespartnern: {len(df_filtered)} von {len(df)} Einträgen")
            
            return df_filtered
            
        except Exception as e:
            st.error(f"Fehler beim Laden der uberall-Datei: {str(e)}")
            return pd.DataFrame()
    
    @staticmethod
    def categorize_service_type(df: pd.DataFrame) -> pd.DataFrame:
        """Categorize uberall services into product types based on plan column"""
        def get_product_type(row):
            if 'plan' in row and pd.notna(row['plan']):
                plan = str(row['plan']).lower()
                if 'basic' in plan:
                    return 'Firmendaten Manager Basic'
                elif 'plus' in plan or 'manger plus' in plan:
                    return 'Firmendaten Manager Plus'
                elif 'pro' in plan:
                    return 'Firmendaten Manager PRO'
                else:
                    return 'Sonstige'
            return 'Unbekannt'
        
        df['Produkttyp'] = df.apply(get_product_type, axis=1)
        return df

class DataAnalyzer:
    """Analyzes data and identifies billing discrepancies"""
    
    def __init__(self, uberall_df: pd.DataFrame, crm_df: pd.DataFrame):
        self.uberall_df = uberall_df.copy()
        self.crm_df = crm_df.copy()
        self.issues = []
        
    def is_status_combination_ok(self, billing_state: str, workflow_status: str) -> Tuple[bool, str]:
        """
        Check if billing state and workflow status combination is correct
        Returns: (is_ok, reason)
        """
        if pd.isna(workflow_status):
            return False, "Workflow-Status ist leer"
        
        workflow = str(workflow_status).strip()
        billing = str(billing_state).strip().upper()
        
        # Check for STORNO (should not be billed)
        if 'STORNO' in workflow.upper():
            return False, "STORNO-Status sollte nicht verrechnet werden"
        
        # Check for ACTIVE billing
        if billing == 'ACTIVE':
            # Must have "abgeschlossen" without "gekündigt"
            if 'Firmendaten Manager Fulfillment abgeschlossen.' in workflow:
                if 'gekündigt' not in workflow.lower():
                    return True, "OK"
                else:
                    return False, "ACTIVE aber gekündigt im Workflow-Status"
            else:
                return False, "ACTIVE aber nicht abgeschlossen"
        
        # Check for CANCELLED/INACTIVE billing
        elif billing in ['CANCELLED', 'INACTIVE']:
            # Must contain "gekündigt"
            if 'gekündigt' in workflow.lower():
                return True, "OK"
            else:
                return False, f"{billing} aber nicht gekündigt im Workflow-Status"
        
        return False, f"Unbekannter Billing-Status: {billing}"
    
    def analyze_billing_discrepancies(self) -> Dict:
        """Main analysis function to identify billing issues"""
        results = {
            'total_billed': len(self.uberall_df),
            'ok_count': 0,
            'issues_count': 0,
            'issues_by_type': {},
            'product_breakdown': {},
            'location_state_breakdown': {},
            'problematic_entries': []
        }
        
        # Product type breakdown
        if 'Produkttyp' in self.uberall_df.columns:
            results['product_breakdown'] = self.uberall_df['Produkttyp'].value_counts().to_dict()
        
        # Location state breakdown
        if 'location state' in self.uberall_df.columns:
            results['location_state_breakdown'] = self.uberall_df['location state'].value_counts().to_dict()
        
        # Analyze each billing entry
        for _, billing_row in self.uberall_df.iterrows():
            location_id = str(billing_row['location id']) if pd.notna(billing_row['location id']) else ''
            location_state = billing_row.get('location state', '')
            location_name = billing_row.get('name', '')
            
            if not location_id:
                continue
                
            # Find matching CRM entry
            crm_match = self.crm_df[self.crm_df['uberall-Location-ID'] == location_id]
            
            if crm_match.empty:
                # Location not found in CRM
                issue = {
                    'location_id': location_id,
                    'location_name': location_name,
                    'location_state': location_state,
                    'problem_type': 'Location nicht im CRM',
                    'problem_detail': 'Location ID wurde im CRM nicht gefunden',
                    'billing_data': billing_row.to_dict(),
                    'crm_data': None,
                    'workflow_status': 'N/A',
                    'projektname': 'N/A'
                }
                self.issues.append(issue)
                results['problematic_entries'].append(issue)
                
            else:
                # Check workflow status combination
                crm_row = crm_match.iloc[0]
                workflow_status = crm_row.get('Workflow-Status', '')
                projektname = crm_row.get('Projektname', '')
                
                is_ok, reason = self.is_status_combination_ok(location_state, workflow_status)
                
                if is_ok:
                    results['ok_count'] += 1
                else:
                    issue = {
                        'location_id': location_id,
                        'location_name': location_name,
                        'location_state': location_state,
                        'problem_type': 'Status-Kombination Problem',
                        'problem_detail': reason,
                        'billing_data': billing_row.to_dict(),
                        'crm_data': crm_row.to_dict(),
                        'workflow_status': workflow_status,
                        'projektname': projektname
                    }
                    self.issues.append(issue)
                    results['problematic_entries'].append(issue)
        
        # Count issues by type
        for issue in results['problematic_entries']:
            issue_type = issue['problem_type']
            results['issues_by_type'][issue_type] = results['issues_by_type'].get(issue_type, 0) + 1
        
        results['issues_count'] = len(results['problematic_entries'])
        
        return results

class ReportGenerator:
    """Generates CSV reports from analysis results"""
    
    @staticmethod
    def generate_csv_report(results: Dict) -> str:
        """Generate CSV report content"""
        lines = []
        
        # Header with summary
        lines.append("# uberall Rechnungskontrolle - Bericht")
        lines.append(f"# Erstellt am: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
        lines.append("#")
        lines.append(f"# Gesamt verrechnet (gefiltert): {results['total_billed']}")
        lines.append(f"# OK (korrekte Status-Kombination): {results['ok_count']}")
        lines.append(f"# Manuelle Kontrolle nötig: {results['issues_count']}")
        if results['total_billed'] > 0:
            problem_rate = (results['issues_count'] / results['total_billed']) * 100
            lines.append(f"# Problemrate: {problem_rate:.1f}%")
        lines.append("#")
        
        # Product breakdown
        if results['product_breakdown']:
            lines.append("# Produkttyp-Breakdown:")
            for product, count in results['product_breakdown'].items():
                lines.append(f"# {product}: {count}")
            lines.append("#")
        
        # Location state breakdown
        if results['location_state_breakdown']:
            lines.append("# Location State-Breakdown:")
            for state, count in results['location_state_breakdown'].items():
                lines.append(f"# {state}: {count}")
            lines.append("#")
        
        # Issues by type
        if results['issues_by_type']:
            lines.append("# Probleme nach Typ:")
            for issue_type, count in results['issues_by_type'].items():
                lines.append(f"# {issue_type}: {count}")
            lines.append("#")
        
        # Problematic entries
        lines.append("")
        lines.append("Location ID,Location Name,Location State,Problem Typ,Problem Detail,Workflow Status,Projektname")
        
        for issue in results['problematic_entries']:
            location_id = issue['location_id']
            location_name = str(issue['location_name']).replace(',', ';')
            location_state = issue['location_state']
            problem_type = issue['problem_type']
            problem_detail = str(issue['problem_detail']).replace(',', ';')
            workflow_status = str(issue['workflow_status']).replace(',', ';')
            projektname = str(issue['projektname']).replace(',', ';')
            
            lines.append(f"{location_id},{location_name},{location_state},{problem_type},{problem_detail},{workflow_status},{projektname}")
        
        return '\n'.join(lines)

def display_results(results: Dict):
    """Display analysis results in Streamlit UI"""
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Gesamt verrechnet", results['total_billed'])
    
    with col2:
        st.metric("✅ OK (korrekt)", results['ok_count'], 
                 delta=None, delta_color="normal")
    
    with col3:
        delta_color = "inverse" if results['issues_count'] > 0 else "normal"
        st.metric("⚠️ Manuelle Kontrolle", results['issues_count'], 
                 delta=f"{results['issues_count']} Probleme" if results['issues_count'] > 0 else None,
                 delta_color=delta_color)
    
    with col4:
        if results['total_billed'] > 0:
            problem_rate = (results['issues_count'] / results['total_billed']) * 100
            st.metric("Problemrate", f"{problem_rate:.1f}%")
        else:
            st.metric("Problemrate", "0%")
    
    # Breakdowns in columns
    col1, col2 = st.columns(2)
    
    # Product breakdown
    with col1:
        if results['product_breakdown']:
            st.subheader("📊 Produkttyp-Breakdown")
            breakdown_df = pd.DataFrame(list(results['product_breakdown'].items()), 
                                      columns=['Produkttyp', 'Anzahl'])
            st.dataframe(breakdown_df, use_container_width=True, hide_index=True)
    
    # Location state breakdown
    with col2:
        if results['location_state_breakdown']:
            st.subheader("📈 Location State-Breakdown")
            state_df = pd.DataFrame(list(results['location_state_breakdown'].items()), 
                                   columns=['Location State', 'Anzahl'])
            st.dataframe(state_df, use_container_width=True, hide_index=True)
    
    # Problematic entries
    if results['problematic_entries']:
        st.subheader("⚠️ Problematische Einträge")
        
        # Filter dropdown
        problem_types = ['Alle'] + list(results['issues_by_type'].keys())
        selected_filter = st.selectbox("Filter nach Problemtyp:", problem_types)
        
        # Filter entries
        filtered_entries = results['problematic_entries']
        if selected_filter != 'Alle':
            filtered_entries = [e for e in filtered_entries if e['problem_type'] == selected_filter]
        
        # Display table
        if filtered_entries:
            display_data = []
            for entry in filtered_entries:
                row = {
                    'Location ID': entry['location_id'],
                    'Location Name': entry['location_name'],
                    'Location State': entry['location_state'],
                    'Problem': entry['problem_detail'],
                    'Workflow Status': entry['workflow_status'],
                    'Projektname': entry['projektname']
                }
                
                # Add uberall dashboard link
                location_id = entry['location_id']
                if location_id and location_id != 'N/A':
                    # Generic uberall dashboard URL structure (may need adjustment)
                    dashboard_url = f"https://app.uberall.com/locations/{location_id}"
                    row['uberall Link'] = f"[Dashboard]({dashboard_url})"
                else:
                    row['uberall Link'] = 'N/A'
                
                display_data.append(row)
            
            df_display = pd.DataFrame(display_data)
            st.dataframe(df_display, use_container_width=True, hide_index=True)
            
            st.info(f"💡 Zeige {len(filtered_entries)} von {len(results['problematic_entries'])} problematischen Einträgen")
        else:
            st.info("Keine Einträge für den gewählten Filter.")
    
    else:
        st.success("🎉 Alle Einträge sind korrekt! Keine manuellen Kontrollen nötig.")
    
    # CSV Export
    if results['problematic_entries'] or results['total_billed'] > 0:
        st.subheader("📥 Export")
        csv_content = ReportGenerator.generate_csv_report(results)
        
        filename = f"uberall_kontrolle_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        st.download_button(
            label="📁 CSV-Bericht herunterladen",
            data=csv_content,
            file_name=filename,
            mime="text/csv"
        )

def main():
    """Main Streamlit application"""
    
    # Page config
    st.set_page_config(
        page_title="uberall Rechnungskontrolle",
        page_icon="💰",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Header
    st.title("💰 uberall Rechnungskontrolle")
    st.markdown("*Automatische Kontrolle der uberall-Rechnungen gegen CRM-Workflow-Status*")
    st.markdown("---")
    
    # Sidebar for file uploads
    with st.sidebar:
        st.header("📁 Datei-Upload")
        
        # uberall billing file upload
        st.subheader("uberall Billing-File")
        uberall_file = st.file_uploader(
            "XLSX-Datei mit Location IDs hochladen",
            type=['xlsx'],
            key="uberall_upload",
            help="Excel-Datei von uberall mit Abrechnungsdaten"
        )
        
        # CRM export upload  
        st.subheader("CRM Export")
        crm_file = st.file_uploader(
            "CSV-Datei mit Projekt-Daten hochladen", 
            type=['csv'],
            key="crm_upload",
            help="CSV-Export aus dem CRM mit uberall-Location-ID Spalte"
        )
        
        # Info section
        with st.expander("ℹ️ Hinweise"):
            st.markdown("""
            **Erforderliche Spalten:**
            
            **uberall Billing-File (XLSX):**
            - `location id` (für Matching)
            - `salespartner name` (für Filterung)
            - `location state` (ACTIVE/CANCELLED/INACTIVE)
            - `name` (Firmenname)
            
            **CRM-Datei (CSV):**
            - `uberall-Location-ID` (für Matching)
            - `Workflow-Status` (für Kontrolle)
            - `Projektname` (für Übersicht)
            
            **Salespartner-Filter:**
            - Nur "Edelweiss Digital GmbH" und "Edelweiss (Russmedia)"
            
            **Status-Kontrolle:**
            - ACTIVE → muss "...abgeschlossen." ohne "gekündigt" haben
            - CANCELLED/INACTIVE → muss "gekündigt" enthalten
            """)
        
        # Version info
        st.markdown("---")
        st.markdown("🔄 **Version:** v1.0")
        st.markdown("*Erstellt: 27.07.2025*")
    
    # Main processing
    if uberall_file is not None and crm_file is not None:
        
        with st.spinner("Lade und verarbeite Dateien..."):
            # Load files
            uberall_df = FileProcessor.load_uberall_file(uberall_file)
            crm_df = FileProcessor.load_crm_file(crm_file)
            
            if uberall_df.empty or crm_df.empty:
                st.error("Fehler beim Laden der Dateien. Bitte überprüfen Sie das Format.")
                return
            
            # Add product categorization
            uberall_df = FileProcessor.categorize_service_type(uberall_df)
            
            # Show file stats
            st.success(f"✅ Dateien erfolgreich geladen:")
            col1, col2 = st.columns(2)
            with col1:
                st.info(f"📊 uberall Billing: {len(uberall_df)} Einträge (nach Salespartner-Filter)")
            with col2:
                st.info(f"📋 CRM Export: {len(crm_df)} Einträge")
            
            # Analyze data
            analyzer = DataAnalyzer(uberall_df, crm_df)
            results = analyzer.analyze_billing_discrepancies()
            
        # Display results
        display_results(results)
        
    else:
        # Welcome message
        st.info("👆 Bitte laden Sie beide Dateien in der Sidebar hoch, um die Analyse zu starten.")
        
        # Instructions
        st.markdown("""
        ### 🚀 So funktioniert's:
        
        1. **uberall Billing-File** (XLSX) hochladen
        2. **CRM Export** (CSV) hochladen  
        3. **Automatische Filterung** nach relevanten Salespartnern
        4. **Automatische Analyse** der Status-Kombinationen
        5. **Ergebnisse ansehen** und problematische Einträge identifizieren
        6. **CSV-Bericht herunterladen** für weitere Bearbeitung
        
        ### 🔍 Was wird geprüft:
        
        - **Salespartner-Filter**: Nur "Edelweiss Digital GmbH" und "Edelweiss (Russmedia)"
        - **Location ID Matching**: Abgleich zwischen uberall `location id` und CRM `uberall-Location-ID`
        - **Status-Kombinationen**:
          - ACTIVE → muss "Firmendaten Manager Fulfillment abgeschlossen." ohne "gekündigt" haben
          - CANCELLED/INACTIVE → muss "gekündigt" enthalten
        - **STORNO-Check**: STORNO-Status sollten nicht verrechnet werden
        
        ### 📊 Ergebnisse:
        
        - **Zusammenfassung** mit Kennzahlen und Problemrate
        - **Produkttyp & Location State-Breakdown**
        - **Problematische Einträge** mit detaillierter Problemanalyse
        - **Filter-Funktionen** nach Problemtyp
        - **CSV-Export** für weitere Bearbeitung
        """)

if __name__ == "__main__":
    main()
