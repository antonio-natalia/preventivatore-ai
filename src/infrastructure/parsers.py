import json
import pandas as pd
import xml.etree.ElementTree as ET
import os
from typing import List, Dict, Any

def clean_xml_tag(tag: str) -> str:
    if '}' in tag:
        return tag.split('}', 1)[1]
    return tag

def load_json_input(file_path: str) -> List[Dict[str, Any]]:
    """
    Carica il JSON normalizzato prodotto da normalize_input.py.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File JSON non trovato: {file_path}")
        
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    if not isinstance(data, list):
        raise ValueError("Il file JSON deve contenere una lista di oggetti.")
        
    return data

def parse_excel(file_path: str) -> List[Dict[str, Any]]:
    """Legge Excel e normalizza in lista di dizionari."""
    df = pd.read_excel(file_path)
    
    # Riconoscimento colonne base
    col_desc = next((c for c in df.columns if 'desc' in c.lower()), None)
    col_price = next((c for c in df.columns if 'prezzo' in c.lower() or 'imp' in c.lower()), None)
    
    if not col_desc or not col_price:
        raise ValueError(f"Colonne Descrizione/Prezzo non trovate in {os.path.basename(file_path)}")

    products = []
    for _, row in df.iterrows():
        raw_desc = str(row[col_desc]).strip()
        try:
            raw_price = float(row[col_price])
        except: continue
        
        if len(raw_desc) < 3 or raw_price <= 0: continue
        
        products.append({
            'description': raw_desc,
            'price': raw_price,
            'components': [] # Excel base non ha componenti
        })
    return products

def parse_six_xml(file_path: str) -> List[Dict[str, Any]]:
    """Logica di parsing XML (STR/SIX) identica a ingest_xml_listino.py"""
    tree = ET.parse(file_path)
    root = tree.getroot()
    products = []
    
    for prod_elem in root.iter():
        if clean_xml_tag(prod_elem.tag) != 'prodotto': continue
        
        # Descrizione
        desc_elem = next((c for c in prod_elem if clean_xml_tag(c.tag) == 'prdDescrizione'), None)
        if desc_elem is None: continue
        desc = desc_elem.get('estesa') or desc_elem.get('breve')
        
        # Prezzo
        quot_elem = next((c for c in prod_elem if clean_xml_tag(c.tag) == 'prdQuotazione'), None)
        price = 0.0
        if quot_elem is not None:
            try: price = float(quot_elem.get('valore', 0.0))
            except: pass
        if price <= 0: continue

        # Componenti
        components = []
        for anl in prod_elem.iter():
            if clean_xml_tag(anl.tag) == 'analisi':
                ad = next((c for c in anl if clean_xml_tag(c.tag) == 'anlDescrizione'), None)
                ai = next((c for c in anl if clean_xml_tag(c.tag) == 'anlImporto'), None)
                aq = next((c for c in anl if clean_xml_tag(c.tag) == 'anlQuantita'), None)
                
                if ad is not None and ai is not None and aq is not None:
                    try:
                        c_desc = ad.get('breve', "N/D")
                        c_price = float(ai.get('valore', 0.0))
                        c_qty = float(aq.get('valore', 0.0))
                        c_type = 'MAN' if 'operaio' in c_desc.lower() else 'MAT'
                        components.append({
                            'description': c_desc, 'unit_price': c_price, 
                            'qty_coefficient': c_qty, 'type': c_type
                        })
                    except: continue

        products.append({'description': desc, 'price': price, 'components': components})
    
    return products