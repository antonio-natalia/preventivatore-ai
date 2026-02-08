import sqlite3
import pandas as pd
import struct
import json
import os
import csv
import time
import sys
import sqlite_vec
import numpy as np
import argparse
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv, find_dotenv
from tqdm import tqdm

# --- PATH SETUP INTELLIGENTE ---
dotenv_path = find_dotenv()
if not dotenv_path:
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
else:
    load_dotenv(dotenv_path)
    PROJECT_ROOT = os.path.dirname(dotenv_path)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- CONFIGURAZIONE & THRESHOLDS ---
DB_FILE = os.path.join(PROJECT_ROOT, "db", "preventivatore_v3_smart.db")

# Cartelle Output (Come BASE)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "preventivi")
if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

TMP_DIR = os.path.join(PROJECT_ROOT, "tmp")
if not os.path.exists(TMP_DIR): os.makedirs(TMP_DIR)

# Soglie di similarità (Come BASE)
THRESHOLD_GREEN = 0.85  # Sopra questo è MATCH probabile
THRESHOLD_YELLOW = 0.60 # Sopra questo è WARNING (Check)
THRESHOLD_AUTO = 0.96   # Sopra questo saltiamo GPT (Auto-Accept)

# --- PROMPT MANAGEMENT (Simulazione File Esterno) ---
# In produzione, puoi leggere questo testo da un file 'prompts/validation.txt'
PROMPT_VALIDATION_TEXT = """
    Sei un Senior Quantity Surveyor ed esperto in computi metrici MEP.
    
    OBIETTIVO: Identificare la voce del database tecnicamente equivalente alla RDO.
    
    INPUT:
    Voce RDO: "{user_query}"
    Opzioni DATABASE:
    {cand_str}
    
    ISTRUZIONI CRITICHE (NORMALIZZAZIONE & LOGICA):
    1. NORMALIZZAZIONE UNITÀ: Converti sempre mentalmente le unità (es. 120mm = 12cm = 0.12m). Se le dimensioni fisiche coincidono, È UN MATCH.
    2. TOLLERANZA SINTATTICA: "3x1.5" equivale a "3G1,5" (G = Giallo/Verde).
    3. ANALISI FUNZIONALE: Chiediti "Posso installare l'articolo del DB al posto di quello richiesto senza varianti sostanziali?".
    
    OUTPUT JSON:
    Rispondi esclusivamente con questo formato JSON:
    {{
      "selected_index": <int o -1 se nessuno>,
    "status": "<MATCH | CHECK | NOMATCH>",
      "reason": "Spiegazione sintetica. DEVI esplicitare le conversioni fatte (es. 'Trovato 120mm che corrisponde ai 12cm richiesti')."
    }}
"""

# --- UTILS DB & VETTORI ---
def serialize_f32(vector: list[float]) -> bytes:
    return struct.pack(f"<{len(vector)}f", *vector)

def get_embedding(text: str) -> list[float]:
    response = client.embeddings.create(input=text, model="text-embedding-3-small")
    return response.data[0].embedding

def search_pure_vector(cursor, query_vec, limit=5):
    """
    Cerca nel DB vettoriale.
    AGGIUNTO: Recupero colonna 'source_file' per tracciare la sorgente.
    """
    query_blob = serialize_f32(query_vec)
    
    cursor.execute("""
        SELECT 
            r.id, 
            vec_distance_cosine(v.embedding, ?) as distance,
            r.description,
            r.unit_material_price,
            r.unit_manpower_price,
            r.source_file
        FROM vec_recipes v
        JOIN recipes r ON v.rowid = r.id
        ORDER BY distance ASC
        LIMIT ?
    """, (query_blob, limit))
    return cursor.fetchall()

def validate_match_with_gpt(user_query, candidates):
    """
    Logica Ibrida: Thresholds + GPT.
    Ritorna: best_idx, status, reason
    """
    if not candidates:
        return -1, "NOMATCH", "Nessun candidato nel DB"

    top_candidate = candidates[0]
    similarity = 1 - top_candidate[1]  # cosine distance to similarity

    # 1. FAST PATH: Auto-Accept (Risparmio API)
    if similarity >= THRESHOLD_AUTO:
        return 0, "MATCH", f"Auto-Match per alta similarità ({similarity:.2f})"

    # 2. FAST PATH: Rejection immediata (Troppo diversi)
    if similarity < THRESHOLD_YELLOW:
        return -1, "NOMATCH", f"Similarità troppo bassa ({similarity:.2f})"

    # 3. GPT JUDGE (Zona Grigia o Verde Bassa)
    cand_str = ""
    for i, c in enumerate(candidates):
        sim = 1 - c[1]
        cand_str += f"[{i}] {c[2]} (Sim: {sim:.2f}) | Src: {c[5]} | p_mat: {c[3]} | p_man: {c[4]}\n"

    try:
        formatted_prompt = PROMPT_VALIDATION_TEXT.format(user_query=user_query, cand_str=cand_str)
        
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": formatted_prompt}],
            response_format={"type": "json_object"},
            temperature=0.0
        )
        
        ai_resp = json.loads(resp.choices[0].message.content)
        idx = ai_resp.get("selected_index", -1)
        status = ai_resp.get("status", "CHECK").upper()
        reason = ai_resp.get("reason", "GPT Decision")
        
        # Validazione indici
        if not isinstance(idx, int) or idx < 0 or idx >= len(candidates):
            return -1, "NOMATCH", "GPT ha scartato tutti i candidati"
            
        return idx, status, reason

    except Exception as e:
        # Fallback euristico se GPT fallisce
        if similarity >= THRESHOLD_GREEN:
            return 0, "WARNING", f"GPT Error, fallback su Top1 ({similarity:.2f})"
        return -1, "NOMATCH", f"GPT Error: {str(e)}"

def get_components(cursor, recipe_id):
    """Recupera i figli (componenti) usando la tabella relazionale."""
    try:
        cursor.execute("""
            SELECT description, qty_coefficient, unit_price, type 
            FROM components 
            WHERE recipe_id = ?
        """, (recipe_id,))
        rows = cursor.fetchall()
        comps = []
        for r in rows:
            comps.append({
                "description": r[0],
                "quantity": r[1], # Coefficiente
                "price": r[2],
                "type": r[3]
            })
        return comps
    except Exception:
        return []

# --- MAIN PROCESS ---

def main():
    
    parser = argparse.ArgumentParser(description="Generatore Preventivi AI")
    parser.add_argument("--solo-manodopera", action="store_true", help="Quota solo la manodopera (Azzera i costi materiali da DB)")
    args = parser.parse_args()
    
    if args.solo_manodopera:
        print("👷 MODALITÀ SOLO MANODOPERA ATTIVA: I prezzi dei materiali DB saranno impostati a 0.")
    
    # 1. Setup Input
    WORK_DIR = os.path.join(PROJECT_ROOT, "richieste_ordine")
    try:
        json_files = [f for f in os.listdir(WORK_DIR) if f.endswith("_clean.json")]
        if not json_files:
            print("⚠️ Nessun file JSON trovato.")
            return
        FILE_INPUT_JSON = max([os.path.join(WORK_DIR, f) for f in json_files], key=os.path.getmtime)
    except Exception as e:
        print(f"❌ Errore input: {e}")
        return

    # Nomi file Output
    base_name = os.path.splitext(os.path.basename(FILE_INPUT_JSON))[0].replace("_clean", "")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    
    # CSV Temporaneo per scrittura real-time (Recovery)
    FILE_STREAM_CSV = os.path.join(TMP_DIR, f"stream_{base_name}.csv")
    # Excel Finale
    FILE_OUTPUT_XLSX = os.path.join(OUTPUT_DIR, f"[PREVENTIVO] {base_name}_{timestamp}.xlsx")

    print(f"📂 Input: {os.path.basename(FILE_INPUT_JSON)}")
    print(f"🛡️ Stream: {os.path.basename(FILE_STREAM_CSV)}")

    # 2. Connessione DB
    conn = sqlite3.connect(DB_FILE)
    try:
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
    except Exception as e:
        print(f"❌ Errore sqlite-vec: {e}"); sys.exit(1)
    cursor = conn.cursor()

    # 3. Caricamento Dati
    with open(FILE_INPUT_JSON, 'r', encoding='utf-8') as f:
        data_input = json.load(f)

    # 4. Inizializzazione CSV Writer
    # Aggiunti campi richiesti: FAB, SORGENTE, P_MAT_RDO...
    csv_columns = [
        "TIPO", "CODICE", "DESCRIZIONE", "QTA", "UM", "FAB", # FAB aggiunto
        "SORGENTE", # SORGENTE aggiunta
        "DESC_DB", "P_MAT_DB", "P_MAN_DB", "P_TOT_DB", "TOTALE_RIGA",
        "P_MAT_RDO", "P_MAN_RDO", # Split Originali
        "DELTA_PREZZO", "STATO", "REASONING"
    ]
    
    stream_file = open(FILE_STREAM_CSV, 'w', newline='', encoding='utf-8')
    writer = csv.DictWriter(stream_file, fieldnames=csv_columns)
    writer.writeheader()

    stats = {
        "processed": 0, "match": 0, "warning": 0, "nomatch": 0, 
        "gpt_calls": 0, "start_time": time.time()
    }

    print(f"🚀 Elaborazione {len(data_input)} voci...")

    for item in tqdm(data_input):
        stats["processed"] += 1
        
        # Parsing Input
        codice = item.get("codice_originale", "")
        desc = item.get("descrizione_completa", "")
        try: qta = float(item.get("quantita", 0))
        except: qta = 0.0
        um = item.get("unita_misura", "")
        
        # Gestione Prezzi Originali (Split o Unico)
        # Se il normalizzatore v3 fornisce solo 'prezzo_unitario', lo mettiamo in MAT o MAN a seconda del contesto?
        # Per ora lo mettiamo in MAT come default se non specificato.
        p_orig = float(item.get("prezzo_unitario", 0))
        p_mat_rdo = p_orig 
        p_man_rdo = float(item.get("prezzo_manodopera", 0)) # Se esiste

        # 1. Ricerca
        query_vec = get_embedding(desc)
        candidates = search_pure_vector(cursor, query_vec)

        # 2. Validazione (Thresholds + GPT)
        best_idx, status, reason = validate_match_with_gpt(desc, candidates)

        # --- COSTRUZIONE RIGA PADRE ---
        row_padre = {
            "TIPO": "PADRE",
            "CODICE": codice,
            "DESCRIZIONE": desc,
            "QTA": qta,
            "UM": um,
            "FAB": "", # Padre non ha fabbisogno
            "SORGENTE": "",
            "DESC_DB": "",
            "P_MAT_DB": 0.0, "P_MAN_DB": 0.0, "P_TOT_DB": 0.0,
            "TOTALE_RIGA": 0.0,
            "P_MAT_RDO": p_mat_rdo, "P_MAN_RDO": p_man_rdo,
            "DELTA_PREZZO": 0.0,
            "STATO": "NOMATCH",
            "REASONING": reason
        }

        children_to_write = []

        if best_idx >= 0:
            # MATCH o WARNING
            match = candidates[best_idx]
            match_id = match[0]
            desc_db = match[2]
            p_mat_db = float(match[3] or 0)
            p_man_db = float(match[4] or 0)
            source_file = match[5] # Colonna SORGENTE recuperata

            # Override per modalità solo manodopera
            if args.solo_manodopera:
                p_mat_db = 0.0

            if status == "MATCH": stats["match"] += 1
            else: stats["warning"] += 1

            p_tot_db = p_mat_db + p_man_db
            
            row_padre.update({
                "SORGENTE": source_file,
                "DESC_DB": desc_db,
                "P_MAT_DB": p_mat_db,
                "P_MAN_DB": p_man_db,
                "P_TOT_DB": p_tot_db,
                "TOTALE_RIGA": p_tot_db * qta,
                "DELTA_PREZZO": p_tot_db - (p_mat_rdo + p_man_rdo),
                "STATO": status
            })

            # Esplosione Figli
            comps = get_components(cursor, match_id)
            for c in comps:
                c_coeff = float(c["quantity"])
                c_price = float(c["price"])
                c_type = c.get("type", "MAT")

                # Se siamo in modalità solo manodopera, azzeriamo i prezzi dei materiali
                if args.solo_manodopera and c_type.upper() != "MAN":
                    c_price = 0.0

                children_to_write.append({
                    "TIPO": "FIGLIO",
                    "CODICE": "",
                    "DESCRIZIONE": f"↳ {c['description']}",
                    "QTA": qta, # Qta Padre
                    "UM": "",
                    "FAB": c_coeff, # Fabbisogno unitario
                    "SORGENTE": source_file,
                    "DESC_DB": "",
                    "P_MAT_DB": c_price, "P_MAN_DB": 0.0,
                    "P_TOT_DB": c_price,
                    "TOTALE_RIGA": (c_price * qta * c_coeff),
                    "P_MAT_RDO": 0, "P_MAN_RDO": 0,
                    "DELTA_PREZZO": 0,
                    "STATO": "",
                    "REASONING": ""
                })
        else:
            stats["nomatch"] += 1
            # Placeholder per calcoli
            row_padre["TOTALE_RIGA"] = (p_mat_rdo + p_man_rdo) * qta

        # Scrittura su CSV (Recovery immediata)
        writer.writerow(row_padre)
        for child in children_to_write:
            writer.writerow(child)
        
        # Flush per sicurezza
        stream_file.flush()

    stream_file.close()
    conn.close()
    
    print(f"✅ Elaborazione completata. Generazione Excel da CSV...")
    create_excel_from_csv(FILE_STREAM_CSV, FILE_OUTPUT_XLSX, stats)

def create_excel_from_csv(csv_path, xlsx_path, stats):
    """
    Legge il CSV generato e applica lo styling 'BASE' (Colorazione Celle).
    """
    df = pd.read_csv(csv_path)
    
    writer = pd.ExcelWriter(xlsx_path, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Preventivo')
    
    wb = writer.book
    ws = writer.sheets['Preventivo']
    
    # --- STILI COME BASE ---
    fmt_currency = wb.add_format({'num_format': '#,##0.00 €'})
    fmt_number = wb.add_format({'num_format': '#,##0.00'})
    fmt_delta = wb.add_format({'num_format': '[Red]#,##0.00 €;[Green]-#,##0.00 €'})
    
    # Stati (Colorazione Cella come BASE)
    fmt_green = wb.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'bold': True})
    fmt_yellow = wb.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C5700', 'bold': True})
    fmt_red = wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'bold': True})
    
    # Stili Strutturali
    fmt_header = wb.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
    fmt_child = wb.add_format({'font_color': '#555555', 'italic': True, 'indent': 1})
    
    # Larghezza Colonne (Ottimizzata)
    ws.set_column('C:C', 50) # Descrizione
    ws.set_column('G:G', 15) # Sorgente
    ws.set_column('H:H', 40) # Desc DB
    ws.set_column('I:N', 12, fmt_currency) # Prezzi
    ws.set_column('O:O', 12, fmt_delta) # Delta
    
    # Mapping Indici Colonne (0-based)
    # TIPO, CODICE, DESC, QTA, UM, FAB, SORG, DESC_DB, P_MAT_DB, P_MAN_DB, P_TOT_DB, TOT_RIGA, RDO_MAT, RDO_MAN, DELTA, STATO, REASONING
    col_stato = df.columns.get_loc("STATO")
    col_tipo = df.columns.get_loc("TIPO")
    
    for i, row in df.iterrows():
        xls_row = i + 1
        row_type = str(row['TIPO'])
        status = str(row['STATO'])
        
        if row_type == "PADRE":
            # Colorazione Cella STATO (come BASE)
            if status == "MATCH":
                ws.write(xls_row, col_stato, status, fmt_green)
            elif status == "WARNING" or status == "CHECK":
                ws.write(xls_row, col_stato, status, fmt_yellow)
            elif status == "NOMATCH":
                ws.write(xls_row, col_stato, status, fmt_red)
                
        elif row_type == "FIGLIO":
            # Formattazione intera riga figlio
            ws.set_row(xls_row, None, fmt_child)

    # Foglio Metriche
    ws_stats = wb.add_worksheet("Metriche")
    ws_stats.write(0, 0, "Metriche Processo", fmt_header)
    ws_stats.write(1, 0, "Voci Totali")
    ws_stats.write(1, 1, stats["processed"])
    ws_stats.write(2, 0, "Match (Verde)")
    ws_stats.write(2, 1, stats["match"])
    ws_stats.write(3, 0, "Warning (Giallo)")
    ws_stats.write(3, 1, stats["warning"])
    ws_stats.write(4, 0, "No Match (Rosso)")
    ws_stats.write(4, 1, stats["nomatch"])
    ws_stats.write(5, 0, "Tempo (s)")
    ws_stats.write(5, 1, round(time.time() - stats["start_time"], 2))

    writer.close()
    print(f"✅ File Excel salvato: {xlsx_path}")

if __name__ == "__main__":
    main()