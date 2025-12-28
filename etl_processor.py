"""
ETL Processor - Core transformation logic for all prestataires
"""

import pandas as pd
import numpy as np
import re
from io import BytesIO

# Supported prestataires
SUPPORTED_PRESTATAIRES = [
    "Veolia",
    "Suez", 
    "Apeyron",
    "Les Alchimistes",
    "Paprec",
    "Elise",
    "Screlec",
    "Trackdechet"
]

# Prestataire-specific configurations
PRESTATAIRE_CONFIG = {
    "Veolia": {
        "pattern": "veolia",
        "weight_unit": "tonnes",
        "columns": {
            "date": ["date de réalisation", "date"],
            "site": ["lieu de collecte", "site", "adresse", "lieu"],
            "waste": ["matière", "déchet", "type de déchet"],
            "weight": ["poids", "masse", "quantité", "kg", "tonnage"],
            "treatment_code": ["code de traitement", "code traitement", "code r/d"],
            "waste_code": ["cod ced", "code déchet"],
            "container": ["matériel", "contenant", "bac"],
            "quantity": ["quantité facturée", "nombre", "nb"],
            "exutoire": ["exutoire", "destination", "installation"],
            "transporter": ["transporteur"],
            "bsd": ["n° de bon d'enlèvement", "bsd", "bordereau"],
        }
    },
    "Suez": {
        "pattern": "suez",
        "weight_unit": "tonnes",
        "sheet_patterns": ["registre", "déchets", "data"],
        "header_row_detection": True,
        "columns": {
            "date": ["date de l'expédition", "date", "date collecte"],
            "site": ["nom du site", "site", "adresse"],
            "waste": ["matière", "déchet", "type"],
            "weight": ["qté pesée", "poids", "masse", "quantité pesée"],
            "treatment_code": ["code r/d", "code de traitement"],
            "treatment_name": ["libellé r/d", "traitement"],
            "waste_code": ["code déchet", "code ced"],
            "container_qty": ["qté de matériel", "nombre"],
            "container_vol": ["volume du matériel", "volume"],
            "exutoire": ["nom de l'installation", "exutoire"],
            "transporter": ["nom.1", "transporteur"],
            "bsd": ["n° du bsd", "bsd"],
        }
    },
    "Apeyron": {
        "pattern": "apeyron",
        "weight_unit": "kg",
        "columns": {
            "date": ["date", "date collecte", "jour"],
            "site": ["nom etablissement", "etablissement", "site", "client"],
            "weight": ["poids total kg", "poids", "kg", "masse"],
            "container_code": ["volume bac", "bac", "contenant"],
            "container_qty": ["nb bac", "nombre bac", "nb"],
        }
    },
    "Les Alchimistes": {
        "pattern": "alchimiste",
        "weight_unit": "kg",
        "header_row_detection": True,
        "columns": {
            "date": ["date", "date collecte", "jour"],
            "site": ["site", "client", "établissement", "etablissement", "nom"],
            "waste": ["matière", "déchet", "type", "flux"],
            "weight": ["poids", "kg", "masse", "quantité", "collecte"],
            "treatment_code": ["code", "traitement", "r/d"],
        }
    },
    "Paprec": {
        "pattern": "paprec",
        "weight_unit": "kg",
        "columns": {
            "site_id": ["numéro contrat", "contrat", "numero contrat"],
            "date": ["date prestation", "date"],
            "waste": ["libellé qualité", "qualité", "matière"],
            "container": ["libellé matériel", "matériel"],
            "quantity": ["quantité", "nombre"],
            "weight": ["poids", "masse"],
            "bsd": ["numerobe", "n° be"],
            "month": ["mois"],
            "year": ["année"],
        }
    },
    "Elise": {
        "pattern": "elise",
        "weight_unit": "kg",
        "file_type": "csv",
        "csv_separator": ";",
        "columns": {
            "site_name": ["nom"],
            "date": ["date collecte", "date"],
            "waste": ["gisement", "matière"],
            "waste_code": ["code nomenclature"],
            "weight": ["quantité", "kg"],
            "treatment_code_intermediate": ["code d/r"],
            "exutoire": ["installation"],
        }
    },
    "Screlec": {
        "pattern": "screlec",
        "weight_unit": "tonnes",
        "bsdd_format": True,
        "columns": {
            "bsd": ["n° de bordereau"],
            "date": ["date de réalisation", "date d'expédition"],
            "waste_name": ["dénomination usuelle"],
            "waste_code": ["code du déchet"],
            "weight": ["quantité réceptionnée", "quantité acceptée"],
            "site_name": ["expéditeur nom usuel"],
            "treatment_code": ["code opération réalisé"],
            "transporter": ["transporteur raison sociale"],
            "destination": ["destination raison sociale"],
        }
    },
    "Trackdechet": {
        "pattern": "trackdechet|td-registre",
        "weight_unit": "tonnes",
        "bsdd_format": True,
        "columns": {
            "bsd": ["n° de bordereau"],
            "date": ["date de réalisation", "date d'expédition"],
            "waste_name": ["dénomination usuelle"],
            "waste_code": ["code du déchet"],
            "weight": ["quantité réceptionnée", "quantité acceptée"],
            "site_name": ["expéditeur nom usuel"],
            "treatment_code": ["code opération réalisé"],
            "transporter": ["transporteur raison sociale"],
            "destination": ["destination raison sociale"],
        }
    },
}

# Paprec container mappings
PAPREC_CONTAINER_VOLUMES = {
    'bac roulant 660l': 660, 'bac roulant 340l': 340, 'bac roulant 770l': 770,
    'bac roulant 240l': 240, 'bac roulant 120l': 120, 'caisse palette 600l': 600,
    'palette': 1000, 'balle': 1000, 'box': 1000, 'sac': 110,
}

PAPREC_CONTAINER_TYPES = {
    'bac roulant 660l': 'Bac 4 roues - 660 L', 'bac roulant 340l': 'Bac 2 roues - 340 L',
    'bac roulant 770l': 'Bac 4 roues - 770 L', 'bac roulant 240l': 'Bac 2 roues - 240 L',
    'bac roulant 120l': 'Bac 2 roues - 120 L', 'caisse palette 600l': 'Caisse Palette - 600 L',
    'palette': 'Palette - 1 m3', 'balle': 'Balle - 1 m3', 'box': 'Box - 1 m3',
    'sac': 'Sac - 110 L', 'vrac': 'Equipement inconnu',
}


class ETLProcessor:
    """Main processor class for ETL transformations"""
    
    def __init__(self, etl_file, template_file):
        """Initialize with ETL and template files"""
        self.etl_file = etl_file
        self.template_file = template_file
        self.warnings = []
        
        # Load template
        self.template_df = pd.read_excel(template_file, sheet_name=0, header=8)
        
        # Load ETL mappings
        self._load_etl_mappings()
    
    def _load_etl_mappings(self):
        """Load all ETL mappings from file"""
        # Déchet sheet
        self.dechet_df = pd.read_excel(self.etl_file, sheet_name='Déchet')
        
        # Paramètres sheet (Déchet fin → Déchets agrégé)
        param_df = pd.read_excel(self.etl_file, sheet_name='Paramètres')
        self.dechet_to_agrege = {}
        for _, row in param_df[['Category', 'Name']].dropna().iterrows():
            self.dechet_to_agrege[str(row['Name']).strip().lower()] = str(row['Category']).strip()
        
        # Traitement générique sheet
        trait_df = pd.read_excel(self.etl_file, sheet_name='Traitement générique')
        self.traitement_lookup = {}
        for _, row in trait_df.iterrows():
            key = str(row.get('Concatener déchet & code de traitement prestataire', '')).strip()
            if key:
                self.traitement_lookup[key] = {
                    'code': row.get('Code traitement retraité'),
                    'traitement': row.get('Traitement')
                }
        
        # Site sheet
        self.site_df = pd.read_excel(self.etl_file, sheet_name='Site')
    
    def detect_prestataire(self, filename):
        """Auto-detect prestataire from filename"""
        filename_lower = filename.lower()
        
        for name, config in PRESTATAIRE_CONFIG.items():
            pattern = config.get("pattern", name.lower())
            if re.search(pattern, filename_lower):
                return name
        
        return None
    
    def _get_dechet_mapping(self, prestataire_pattern):
        """Get waste mappings for a prestataire"""
        mask = self.dechet_df['Nom prestataire (FORMULE)'].str.contains(
            prestataire_pattern, case=False, na=False
        )
        df_filtered = self.dechet_df[mask]
        
        lookup = {}
        default = None
        
        for _, row in df_filtered.iterrows():
            prest = row.get('Nom des déchets prestataire')
            urbyn = row.get('Nom des déchets Urbyn')
            
            if pd.isna(prest):
                if pd.notna(urbyn):
                    default = urbyn
            elif pd.notna(urbyn):
                lookup[str(prest).strip().lower()] = urbyn
        
        return lookup, default
    
    def _get_site_mapping(self, prestataire_pattern):
        """Get site mappings for a prestataire"""
        mask = self.site_df['Nom prestataire (FORMULE)'].str.contains(
            prestataire_pattern, case=False, na=False
        )
        df_filtered = self.site_df[mask]
        
        lookup = {}
        for _, row in df_filtered.iterrows():
            site_prest = row.get('Nom site prestataire')
            if pd.notna(site_prest):
                key = str(site_prest).strip().lower()
                lookup[key] = {
                    'nom_site': row.get('Nom site Urbyn'),
                    'code_prestation': row.get('Code de la prestation'),
                    'prestataire': row.get('Nom prestataire (FORMULE)')
                }
        
        return lookup
    
    def _find_column(self, df, possible_names):
        """Find a column by trying multiple possible names"""
        for name in possible_names:
            for col in df.columns:
                if name.lower() in str(col).lower():
                    return col
        return None
    
    def _safe_get(self, row, column, default=None):
        """Safely get a value from a row"""
        if column is None:
            return default
        try:
            value = row.get(column)
            return default if pd.isna(value) else value
        except:
            return default
    
    def _extract_code_site(self, nom_site):
        """Extract code site from nom site"""
        if pd.isna(nom_site):
            return None
        nom_str = str(nom_site)
        return nom_str.split(' - ')[0] if ' - ' in nom_str else None
    
    def _map_dechet(self, dechet_prest, lookup, default):
        """Map waste type"""
        if pd.isna(dechet_prest) or str(dechet_prest).strip() == '':
            return default
        key = str(dechet_prest).strip().lower()
        return lookup.get(key, default)
    
    def _map_agrege(self, dechet_fin):
        """Map déchet fin to agrégé"""
        if pd.isna(dechet_fin):
            return None
        return self.dechet_to_agrege.get(str(dechet_fin).strip().lower())
    
    def _map_traitement(self, agrege, code=None):
        """Map treatment"""
        if pd.isna(agrege):
            return None, None
        
        if pd.notna(code) and str(code).strip():
            key = f"{agrege}{str(code).strip()}"
            if key in self.traitement_lookup:
                r = self.traitement_lookup[key]
                return r['code'], r['traitement']
        
        if agrege in self.traitement_lookup:
            r = self.traitement_lookup[agrege]
            return r['code'], r['traitement']
        
        return None, None
    
    def _map_site(self, site_name, site_lookup):
        """Map site with partial matching"""
        if pd.isna(site_name):
            return None
        
        key = str(site_name).strip().lower()
        
        # Exact match
        if key in site_lookup:
            return site_lookup[key]
        
        # Partial match
        for lookup_key, value in site_lookup.items():
            if key in lookup_key or lookup_key in key:
                return value
        
        return None
    
    def _read_file(self, file, prestataire):
        """Read input file based on prestataire config"""
        config = PRESTATAIRE_CONFIG.get(prestataire, {})
        
        # Check if CSV
        if config.get("file_type") == "csv" or file.name.endswith('.csv'):
            sep = config.get("csv_separator", ",")
            # Try to detect separator
            content = file.read()
            file.seek(0)
            
            try:
                # Try semicolon first for Elise
                df = pd.read_csv(BytesIO(content), sep=';', header=1, encoding='utf-8-sig')
                if len(df.columns) > 5:
                    return df
            except:
                pass
            
            # Try comma
            df = pd.read_csv(BytesIO(content), encoding='utf-8-sig')
            return df
        
        # Excel file
        xls = pd.ExcelFile(file)
        
        # Find the right sheet
        sheet_name = 0
        if 'sheet_patterns' in config:
            for pattern in config['sheet_patterns']:
                for sheet in xls.sheet_names:
                    if pattern.lower() in sheet.lower():
                        sheet_name = sheet
                        break
        elif 'registre' in [s.lower() for s in xls.sheet_names]:
            sheet_name = 'registre'
        
        # Detect header row if needed
        header_row = 0
        if config.get("header_row_detection"):
            df_raw = pd.read_excel(file, sheet_name=sheet_name, header=None, nrows=15)
            for i, row in df_raw.iterrows():
                row_text = ' '.join([str(v).lower() for v in row.values if pd.notna(v)])
                if 'date' in row_text and ('poids' in row_text or 'kg' in row_text or 'matière' in row_text):
                    header_row = i
                    break
            file.seek(0)
        
        df = pd.read_excel(file, sheet_name=sheet_name, header=header_row)
        return df
    
    def process(self, files, prestataire):
        """Process files for a given prestataire"""
        self.warnings = []
        
        try:
            config = PRESTATAIRE_CONFIG.get(prestataire, {})
            pattern = config.get("pattern", prestataire.lower())
            
            # Get mappings
            dechet_lookup, default_dechet = self._get_dechet_mapping(pattern)
            site_lookup = self._get_site_mapping(pattern)
            
            # Read and combine files
            dfs = []
            for f in files:
                df = self._read_file(f, prestataire)
                df['_source_file'] = f.name
                dfs.append(df)
            
            df_input = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
            
            # Detect columns
            columns = config.get("columns", {})
            cols = {}
            for key, patterns in columns.items():
                cols[key] = self._find_column(df_input, patterns)
            
            # Process rows
            output_rows = []
            skipped = 0
            
            for _, row in df_input.iterrows():
                result = self._process_row(row, cols, prestataire, config, 
                                          dechet_lookup, default_dechet, site_lookup)
                if result:
                    output_rows.append(result)
                else:
                    skipped += 1
            
            # Create output DataFrame
            df_output = pd.DataFrame(output_rows)
            
            # Align with template columns
            for col in self.template_df.columns:
                if col not in df_output.columns:
                    df_output[col] = None
            df_output = df_output[self.template_df.columns]
            
            return {
                'success': True,
                'data': df_output,
                'rows_processed': len(df_output),
                'rows_skipped': skipped,
                'warnings': self.warnings
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'data': None,
                'rows_processed': 0,
                'rows_skipped': 0,
                'warnings': self.warnings
            }
    
    def _process_row(self, row, cols, prestataire, config, dechet_lookup, default_dechet, site_lookup):
        """Process a single row"""
        
        # Get site
        site_col = cols.get('site') or cols.get('site_name') or cols.get('site_id')
        site_name = self._safe_get(row, site_col, '')
        site_info = self._map_site(site_name, site_lookup)
        
        if site_info is None:
            self.warnings.append(f"No site mapping for: '{site_name}'")
            site_info = {'nom_site': 'Site inconnu', 'code_prestation': None, 'prestataire': None}
        
        # Get waste
        waste_col = cols.get('waste') or cols.get('waste_name')
        dechets_prest = self._safe_get(row, waste_col, '')
        dechet_fin = self._map_dechet(dechets_prest, dechet_lookup, default_dechet)
        dechets_agrege = self._map_agrege(dechet_fin)
        
        # Get treatment
        code_col = cols.get('treatment_code') or cols.get('treatment_code_done')
        code_prest = self._safe_get(row, code_col, '')
        code_final, traitement = self._map_traitement(dechets_agrege, code_prest)
        if code_final is None:
            code_final = code_prest
        
        # Get weight
        weight_col = cols.get('weight')
        weight = self._safe_get(row, weight_col, 0)
        
        # Skip if no weight (for BSDD formats)
        if config.get('bsdd_format') and (pd.isna(weight) or weight == 0):
            return None
        
        # Convert weight
        if weight:
            weight = float(weight)
            if config.get('weight_unit') == 'tonnes':
                weight = weight * 1000
        else:
            weight = 0
        
        # Get date
        date_col = cols.get('date')
        date_val = self._safe_get(row, date_col)
        
        # Get other fields
        bsd = self._safe_get(row, cols.get('bsd'))
        waste_code = self._safe_get(row, cols.get('waste_code'))
        transporter = self._safe_get(row, cols.get('transporter'))
        exutoire = self._safe_get(row, cols.get('exutoire') or cols.get('destination'))
        
        # Container info (for Paprec)
        container = self._safe_get(row, cols.get('container'), '')
        quantity = self._safe_get(row, cols.get('quantity') or cols.get('container_qty'), 1)
        
        volume_contenant = None
        type_contenant = None
        if container and prestataire == "Paprec":
            container_lower = container.lower().strip()
            volume_contenant = PAPREC_CONTAINER_VOLUMES.get(container_lower)
            type_contenant = PAPREC_CONTAINER_TYPES.get(container_lower, 'Equipement inconnu')
        
        # Determine prestataire name
        prestataire_name = site_info.get('prestataire') or transporter or prestataire
        
        return {
            'Libellé': None,
            'Groupe': 'Capgemini',
            'Code site': self._extract_code_site(site_info['nom_site']),
            'Nom du site': site_info['nom_site'],
            'Nom du client': 'CAPGEMINI TECHNOLOGY SERVICES',
            'Type de porteur': 'FM',
            'Commentaire mouvement': None,
            'Code de la prestation': site_info.get('code_prestation'),
            'Prestataire': prestataire_name,
            'Groupe de Prestataire': prestataire,
            'Type de prestataire': 'Privé',
            'Périodicité': 'Jour',
            'Date début registre': date_val,
            'Date fin registre': date_val,
            'Code déchet prestataire': waste_code,
            'Déchet fin': dechet_fin,
            'Déchets agrégé': dechets_agrege,
            'Déchets prestataire': dechets_prest if dechets_prest else dechet_fin,
            'Masse totale (kg)': weight,
            'Nombre de contenants': quantity,
            'Volume contenant (L)': volume_contenant,
            'Type de contenant': type_contenant,
            'Volume total (L)': volume_contenant * quantity if volume_contenant and quantity else None,
            'Nature de quantités collectées': 'Masse',
            'Qualité quantités': 'Document prestataire',
            'Précision estimations des quantités': None,
            'Traitement': traitement,
            'Traitement prestataire': None,
            'Code traitement': code_final,
            'Code traitement prestataire': code_prest,
            'Qualité du Traitement': 'Document prestataire',
            'N° de BSD/BSDD': bsd,
            'N° de recépissé': None,
            'Transporteur': transporter,
            'Transporteur prestataire': transporter,
            "Plaque d'immatriculation": None,
            'Exutoire intermédiaire': None,
            'Exutoire intermédiaire prestataire': None,
            "Qualité de l'exutoire intermédiaire": None,
            'Exutoire final': exutoire,
            'Exutoire final prestataire': exutoire,
            "Qualité de l'exutoire final": 'Document prestataire',
            'Période de clôture': None,
            'Statut du mouvement': 'Réalisée',
            'Commentaire': None,
        }
