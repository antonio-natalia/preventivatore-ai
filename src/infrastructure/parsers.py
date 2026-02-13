import xml.etree.ElementTree as ET
import pandas as pd
import os
import json
import hashlib
from typing import List, Dict, Any

def _clean_tag(tag: str) -> str:
    """Rimuove namespace XML."""
    if '}' in tag:
        return tag.split('}', 1)[1]
    return tag

def _generate_synthetic_sku(description: str, prefix: str = "XLS") -> str:
    """Genera uno SKU deterministico basato sul contenuto."""
    clean_desc = description.strip().lower().encode('utf-8')
    hash_md5 = hashlib.md5(clean_desc).hexdigest()[:12].upper()
    return f"{prefix}_{hash_md5}"

def load_json_input(file_path: str) -> List[Dict[str, Any]]:
    """
    Carica un file JSON (es. output digitalizzazione o richiesta preventivo).
    Ripristinato per compatibilità con il resto del sistema.
    """
    print(f"📂 Caricamento JSON da: {os.path.basename(file_path)}")
    
    if not os.path.exists(file_path):
        print(f"❌ File non trovato: {file_path}")
        return []

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Gestione formati diversi (Lista piatta vs Oggetto wrapper)
        if isinstance(data, dict):
            if "items" in data: return data["items"]
            return [data]
        elif isinstance(data, list):
            return data
        else:
            return []
            
    except Exception as e:
        print(f"❌ Errore lettura JSON: {e}")
        return []

def parse_six_xml_topology(file_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parser SIX XML Completo per Ingestion Deterministica.
    Estrae Topologia (Relazioni), Anagrafiche complete e Prezzi Dichiarati.
    """
    print(f"🔄 Estrazione Topologia XML da: {os.path.basename(file_path)}")
    
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except Exception as e:
        print(f"❌ Errore critico XML: {e}")
        return {"items": [], "relations": []}

    uom_map = {}
    specie_map = {}
    category_map = {}
    
    MAN_KEYWORDS = ['operaio', 'manodopera', 'assistente', 'specializzato', 'qualificato', 'comune']

    for elem in root.iter():
        tag = _clean_tag(elem.tag)
        if tag == 'unitaDiMisura':
            uom_map[elem.get('unitaDiMisuraId')] = elem.get('simbolo')
        elif tag == 'specie':
            sid = elem.get('specieId')
            desc = elem.get('descrizione', '').strip()
            tipo = elem.get('tipoMerceologico', '').lower()
            is_man = any(k in desc.lower() for k in MAN_KEYWORDS) or any(k in tipo for k in MAN_KEYWORDS)
            specie_map[sid] = 'MAN' if is_man else 'MAT'
            category_map[sid] = desc

    extracted_items = []
    extracted_relations = []
    PRICE_PRIORITY = ['316', '318', '001', '1']

    for prod in root.iter():
        if _clean_tag(prod.tag) != 'prodotto': continue
        
        internal_id = prod.get('prodottoId')
        sku = prod.get('prdId')
        if not sku: continue

        d_breve = ""
        d_estesa = ""
        for child in prod:
            if _clean_tag(child.tag) == 'prdDescrizione':
                d_breve = child.get('breve', "").strip()
                d_estesa = child.get('estesa', "").strip()
        
        full_desc = f"{d_breve}\n{d_estesa}".strip()
        if not full_desc: full_desc = sku 

        found_prices = {}
        for child in prod:
            if _clean_tag(child.tag) == 'prdQuotazione':
                lid = child.get('listaQuotazioneId')
                try: val = float(child.get('valore', 0))
                except: val = 0.0
                found_prices[lid] = val
        
        declared_price = 0.0
        for list_id in PRICE_PRIORITY:
            if list_id in found_prices and found_prices[list_id] > 0:
                declared_price = found_prices[list_id]
                break
        if declared_price == 0 and found_prices:
            declared_price = list(found_prices.values())[0]

        specie_id = prod.get('specieId')
        cost_type = specie_map.get(specie_id, 'MAT')
        category_tag = category_map.get(specie_id, 'Generale')

        has_children = False
        for child in prod:
            if _clean_tag(child.tag) == 'analisi':
                for comp in child:
                    if _clean_tag(comp.tag) == 'componente':
                        child_int_id = comp.get('prodottoId')
                        try: qty = float(comp.get('quantita', 0) or 0)
                        except: qty = 0.0
                        
                        extracted_relations.append({
                            'parent_internal_id': internal_id,
                            'parent_sku': sku,
                            'child_internal_id': child_int_id,
                            'quantity': qty
                        })
                        has_children = True

        strategy = 'SUM_CHILDREN' if has_children else 'USE_DECLARED_PRICE'

        extracted_items.append({
            'sku': sku,
            'external_ref_id': internal_id,
            'description_short': d_breve,
            'description_long': d_estesa,
            'description_full': full_desc,
            'unit_of_measure': uom_map.get(prod.get('unitaDiMisuraId'), 'pz'),
            'category_tag': category_tag,
            'declared_price': declared_price,
            'cost_type': cost_type,
            'pricing_strategy': strategy
        })

    return {"items": extracted_items, "relations": extracted_relations}

def parse_complex_excel_topology(file_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parser Excel Avanzato.
    Supporta la struttura gerarchica tramite posizionamento righe.
    """
    print(f"📊 Parsing Excel Gerarchico da: {os.path.basename(file_path)}")
    
    IDX = {
        "ARTICOLO": 0, "DESCRIZIONE": 1, "UM": 2, "Q_COMP": 3,
        "Q_ART": 4, "Q_MAN": 5, "P_COMP": 8, "P_ART": 9,
        "P_MAN": 10, "IMPORTO_TOT": 14
    }

    try:
        df = pd.read_excel(file_path, header=None, dtype=str)
    except Exception as e:
        print(f"❌ Errore lettura Excel: {e}")
        return {"items": [], "relations": []}

    extracted_items = []
    extracted_relations = []
    current_parent_sku = None
    
    def clean_float(val):
        if pd.isna(val): return None
        s = str(val).strip().replace('€','').replace('.','').replace(',','.')
        try: return float(s)
        except: return None

    for _, row in df.iterrows():
        def get_col(idx): return row[idx] if idx < len(row) else None
        
        raw_code = get_col(IDX["ARTICOLO"])
        raw_desc = get_col(IDX["DESCRIZIONE"])
        raw_um = get_col(IDX["UM"])
        
        val_tot = clean_float(get_col(IDX["IMPORTO_TOT"]))
        val_p_comp = clean_float(get_col(IDX["P_COMP"]))
        val_q_comp = clean_float(get_col(IDX["Q_COMP"]))

        has_code = pd.notna(raw_code) and str(raw_code).strip() != ""
        has_desc = pd.notna(raw_desc) and str(raw_desc).strip() != ""
        
        # CASO 1: PADRE
        if has_code and has_desc and val_tot is None:
            sku = str(raw_code).strip()
            desc = str(raw_desc).strip()
            uom = str(raw_um).strip() if pd.notna(raw_um) else "pz"
            current_parent_sku = sku
            
            extracted_items.append({
                'sku': sku,
                'external_ref_id': None,
                'description_short': desc[:100],
                'description_long': desc,
                'description_full': desc,
                'unit_of_measure': uom,
                'category_tag': 'Excel Import',
                'declared_price': 0.0,
                'cost_type': 'MAT',
                'pricing_strategy': 'SUM_CHILDREN'
            })
            continue

        # CASO 2: FIGLIO
        if current_parent_sku and has_desc and val_tot is None and (val_p_comp is not None or val_q_comp is not None):
            desc = str(raw_desc).strip()
            qty = val_q_comp if val_q_comp is not None else 1.0
            price = val_p_comp if val_p_comp is not None else 0.0
            
            comp_sku = _generate_synthetic_sku(desc, prefix="XLS_COMP")
            is_man = "operaio" in desc.lower() or "manodopera" in desc.lower()
            cost_type = 'MAN' if is_man else 'MAT'
            
            extracted_items.append({
                'sku': comp_sku,
                'external_ref_id': None,
                'description_short': desc[:100],
                'description_long': desc,
                'description_full': desc,
                'unit_of_measure': 'pz',
                'category_tag': 'Excel Component',
                'declared_price': price,
                'cost_type': cost_type,
                'pricing_strategy': 'USE_DECLARED_PRICE'
            })
            
            extracted_relations.append({
                'parent_sku': current_parent_sku,
                'child_internal_id': comp_sku,
                'quantity': qty
            })
            continue

        if current_parent_sku and val_tot is not None:
            current_parent_sku = None
            continue
    
    return {"items": extracted_items, "relations": extracted_relations}