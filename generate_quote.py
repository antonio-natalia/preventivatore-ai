import sqlite3
import pandas as pd
import struct
import json
import os
import csv
import time
import sys
import sqlite_vec
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv, find_dotenv

# --- PATH SETUP INTELLIGENTE ---
dotenv_path = find_dotenv()

if not dotenv_path:
    # Fallback: se non lo trova, usa la cartella dello script
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
else:
    # Carica il .env trovato e definisce la ROOT
    load_dotenv(dotenv_path)
    PROJECT_ROOT = os.path.dirname(dotenv_path)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- CONFIGURAZIONE ---
DB_FILE = os.path.join(PROJECT_ROOT, "db", "preventivatore_v2_bulk.db")
FILE_INPUT_RDO = os.path.join(PROJECT_ROOT, "richieste_ordine", "input_cliente_clean.xlsx")

# Generazione nome file output dinamico
base_name = os.path.splitext(os.path.basename(FILE_INPUT_RDO))[0]
client_filename = base_name.replace("_clean", "").strip()
timestamp = datetime.now().strftime("%Y-%m-%d %H-%M")
FILE_FINAL_XLSX = os.path.join(
    PROJECT_ROOT, 
    "preventivi", 
    f"[PREVENTIVO - {timestamp}] {client_filename}.xlsx"
)

# HEADER RDO (Input)
HEADER_RDO = ["DESCRIZIONE", "QUANTITA", "UNITA_MISURA"]

# SOGLIE CONFIGURABILI
SIMILARITY_THRESHOLD_STRICT = 0.90 

# --- UTILS DATABASE ---

def get_db_connection():
    """Connette al DB e carica l'estensione vettoriale."""
    conn = sqlite3.connect(DB_FILE)
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    return conn

def get_embedding(text):
    """Genera embedding usando il modello OpenAI configurato."""
    text = text.replace("\n", " ").strip()
    return client.embeddings.create(input=[text], model="text-embedding-3-small").data[0].embedding

def serialize_f32(vector):
    """Serializza il vettore per sqlite-vec."""
    return struct.pack(f"<{len(vector)}f", *vector)

# --- CORE SEARCH & MATCHING ---

def search_similar_candidates(description, limit=5):
    """
    Cerca nel DB vettoriale i candidati più simili.
    Include recupero metriche di volatilità (Smart Pricing).
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Embedding della query
    query_embedding = get_embedding(description)
    
    # 2. Query Vettoriale + Metadati Statistici
    # Aggiornato per estrarre anche volatility_index e is_complex_assembly
    sql = """
        SELECT 
            r.id, r.code, r.description, 
            r.unit_material_price, r.unit_manpower_price, 
            r.source_file, 
            r.volatility_index, r.is_complex_assembly,
            v.distance
        FROM vec_recipes v
        JOIN recipes r ON v.rowid = r.id
        WHERE v.embedding MATCH ? AND k = ?
        ORDER BY v.distance ASC
    """
    
    try:
        results = cursor.execute(sql, (serialize_f32(query_embedding), limit)).fetchall()
    except Exception as e:
        print(f"Errore ricerca vettoriale: {e}")
        conn.close()
        return []

    candidates = []
    for row in results:
        # Calcolo similarità (1 / 1+distance)
        similarity = 1 / (1 + row[8]) 
        
        candidates.append({
            "id": row[0],
            "code": row[1],
            "desc": row[2],
            "price_mat": row[3],
            "price_man": row[4],
            "source_file": row[5],
            "volatility": row[6] if row[6] is not None else 0.0,   # Campo Nuovo
            "is_complex": row[7] if row[7] is not None else 0,     # Campo Nuovo
            "similarity": similarity
        })
    
    conn.close()
    return candidates

def validate_match_with_gpt(rdo_desc, options):
    """
    Usa GPT-4o per selezionare il miglior match tecnico con ragionamento CoT.
    Gestisce normalizzazione unità e analisi funzionale.
    """
    if not options:
        return {"selected_index": 0, "status": "NO MATCH", "reason": "Nessuna opzione fornita"}

    options_text = ""
    for idx, opt in enumerate(options):
        options_text += f"Opzione {idx+1}:\n- Descrizione: {opt['desc']}\n- Prezzo Mat: {opt['price_mat']}\n- ID: {opt['id']}\n\n"

    # PROMPT AGGIORNATO (SMART PRICING V2 - Senior Quantity Surveyor)
    prompt = f"""
    Sei un Senior Quantity Surveyor ed esperto in computi metrici MEP.
    
    OBIETTIVO: Identificare la voce del database tecnicamente equivalente alla RDO.
    
    INPUT:
    Voce RDO: "{rdo_desc}"
    Opzioni DATABASE:
    {options_text}
    
    ISTRUZIONI CRITICHE (NORMALIZZAZIONE & LOGICA):
    1. NORMALIZZAZIONE UNITÀ: Converti sempre mentalmente le unità (es. 120mm = 12cm = 0.12m). Se le dimensioni fisiche coincidono, È UN MATCH.
    2. TOLLERANZA SINTATTICA: "3x1.5" equivale a "3G1,5" (G = Giallo/Verde).
    3. ANALISI FUNZIONALE: Chiediti "Posso installare l'articolo del DB al posto di quello richiesto senza varianti sostanziali?".
    
    OUTPUT JSON:
    Rispondi esclusivamente con questo formato JSON:
    {{
      "selected_index": [numero intero 1-based, o 0 se nessun match valido],
      "status": "OK" | "CHECK" | "NO MATCH",
      "reason": "Spiegazione sintetica. DEVI esplicitare le conversioni fatte (es. 'Trovato 120mm che corrisponde ai 12cm richiesti')."
    }}
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o", 
            messages=[
                {"role": "system", "content": "Sei un assistente JSON rigoroso."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0
        )
        
        content = response.choices[0].message.content
        result = json.loads(content)
        return result
        
    except Exception as e:
        print(f"Errore GPT: {e}")
        return {"selected_index": 0, "status": "ERROR", "reason": str(e)}

def get_recipe_details(recipe_id):
    """Ottiene dettagli ricetta e componenti per l'output finale."""
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    recipe = cur.execute("SELECT * FROM recipes WHERE id = ?", (recipe_id,)).fetchall()
    components = cur.execute("SELECT * FROM components WHERE recipe_id = ?", (recipe_id,)).fetchall()
    
    conn.close()
    return recipe, components

# --- MAIN ENGINE ---

def main():
    print("🚀 AVVIO GENERATORE PREVENTIVI (SMART PRICING ENABLED)...")
    print(f"📂 Input: {FILE_INPUT_RDO}")
    print(f"💾 Output: {FILE_FINAL_XLSX}")

    if not os.path.exists(FILE_INPUT_RDO):
        print("❌ File di input non trovato!")
        return

    # Lettura Excel Input
    try:
        df_input = pd.read_excel(FILE_INPUT_RDO)
    except Exception as e:
        print(f"❌ Errore lettura Excel: {e}")
        return
        
    # Verifica colonne minime
    if not all(col in df_input.columns for col in HEADER_RDO):
        print(f"❌ Colonne mancanti! Richieste: {HEADER_RDO}")
        return

    # Inizializzazione Excel Writer (XlsxWriter per formattazione avanzata)
    import xlsxwriter
    workbook = xlsxwriter.Workbook(FILE_FINAL_XLSX)
    worksheet = workbook.add_worksheet("Preventivo")

    # Formattazioni Excel
    cell_format_header = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
    cell_format_currency = workbook.add_format({'num_format': '€ #,##0.00', 'border': 1})
    cell_format_text = workbook.add_format({'border': 1, 'text_wrap': True})
    cell_format_status_ok = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'border': 1, 'bold': True})
    cell_format_status_check = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C5700', 'border': 1, 'bold': True})
    cell_format_status_no_match = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'border': 1, 'bold': True})
    
    # Headers Output
    headers = ["DESCRIZIONE RDO", "QTA", "UM", "DESCRIZIONE DB", "PREZZO MAT", "PREZZO MAN", "TOTALE", "STATO", "NOTE AI"]
    for col_num, header in enumerate(headers):
        worksheet.write(0, col_num, header, cell_format_header)
    
    # Imposta larghezza colonne
    worksheet.set_column('A:A', 50) # Desc RDO
    worksheet.set_column('D:D', 50) # Desc DB
    worksheet.set_column('E:G', 15) # Prezzi
    worksheet.set_column('I:I', 40) # Note

    row_num = 1
    total_quote = 0.0

    # --- LOOP RIGHE ---
    for index, row in df_input.iterrows():
        rdo_desc = str(row['DESCRIZIONE']).strip()
        rdo_qty = float(row['QUANTITA']) if pd.notna(row['QUANTITA']) else 0.0
        rdo_um = str(row['UNITA_MISURA']) if pd.notna(row['UNITA_MISURA']) else ""
        
        print(f"\n🔹 Processing Riga {index+1}: {rdo_desc[:50]}...")

        # 1. Ricerca Candidati
        candidates = search_similar_candidates(rdo_desc, limit=5)
        
        # 2. Validazione GPT
        best_match = None
        validation_result = {}
        
        if candidates:
            # Filtro preliminare di sicurezza (se il primo è > 99% simile, saltiamo GPT per risparmiare, opzionale)
            if candidates[0]['similarity'] > SIMILARITY_THRESHOLD_STRICT:
                best_match = candidates[0]
                validation_result = {"status": "OK", "reason": "Match vettoriale esatto (>99%)"}
            else:
                validation_result = validate_match_with_gpt(rdo_desc, candidates)
                sel_idx = validation_result.get("selected_index", 0)
                
                if sel_idx > 0 and sel_idx <= len(candidates):
                    best_match = candidates[sel_idx - 1]
        
        # 3. Determinazione Dati Finali (Smart Pricing Logic)
        final_mat = 0.0
        final_man = 0.0
        db_desc = ""
        status = "NO MATCH"
        ai_note = ""

        if best_match:
            # Controllo Safety Mechanism (Volatilità)
            is_complex = best_match.get('is_complex', 0)
            volatility = best_match.get('volatility', 0.0)

            if is_complex:
                # CASO 1: ALTA VOLATILITÀ -> MANUAL
                final_mat = 0.00
                final_man = 0.00
                status = "MANUAL_ESTIMATION"
                ai_note = f"⚠️ ALTA VOLATILITÀ (CV: {volatility:.2f}). Richiede stima manuale specifica."
                db_desc = best_match['desc']
                print(f"   -> ⚠️  MANUAL CHECK (Volatilità {volatility:.2f})")
            
            else:
                # CASO 2: MATCH VALIDO
                final_mat = best_match['price_mat'] or 0.0
                final_man = best_match['price_man'] or 0.0
                db_desc = best_match['desc']
                
                # Mapping status GPT -> Status Excel
                gpt_status = validation_result.get("status", "CHECK")
                if gpt_status == "OK": status = "MATCH"
                elif gpt_status == "CHECK": status = "CHECK"
                else: status = "NO MATCH" # Fallback
                
                ai_note = validation_result.get("reason", "")
                print(f"   -> ✅ MATCH: {db_desc[:40]}... (€ {final_mat:.2f})")
        else:
            ai_note = validation_result.get("reason", "Nessun candidato trovato")
            print("   -> ❌ NO MATCH")

        # 4. Calcoli Totali
        line_total = (final_mat + final_man) * rdo_qty
        if status in ["MATCH", "CHECK"]: 
            total_quote += line_total

        # 5. Scrittura Excel
        worksheet.write(row_num, 0, rdo_desc, cell_format_text)
        worksheet.write(row_num, 1, rdo_qty, cell_format_text)
        worksheet.write(row_num, 2, rdo_um, cell_format_text)
        worksheet.write(row_num, 3, db_desc, cell_format_text)
        worksheet.write(row_num, 4, final_mat, cell_format_currency)
        worksheet.write(row_num, 5, final_man, cell_format_currency)
        worksheet.write(row_num, 6, line_total, cell_format_currency)
        
        # Formattazione condizionale Stato
        fmt_status = cell_format_status_no_match
        if status == "MATCH": fmt_status = cell_format_status_ok
        elif status in ["CHECK", "MANUAL_ESTIMATION"]: fmt_status = cell_format_status_check
        
        worksheet.write(row_num, 7, status, fmt_status)
        worksheet.write(row_num, 8, ai_note, cell_format_text)
        
        row_num += 1

    # Footer Totali
    row_num += 1
    worksheet.write(row_num, 5, "TOTALE STIMATO", cell_format_header)
    worksheet.write(row_num, 6, total_quote, cell_format_currency)

    workbook.close()
    print(f"\n✅ Preventivo generato con successo: {FILE_FINAL_XLSX}")

if __name__ == "__main__":
    main()