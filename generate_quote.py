import sqlite3
import pandas as pd
import struct
import json
import os
import csv
import time
import sys
import sqlite_vec
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- CONFIGURAZIONE ---
DB_FILE = "./db/preventivatore_v2_bulk.db"
FILE_INPUT_RDO = "./richieste_ordine/input_cliente_clean.xlsx"
FILE_STREAM_CSV = "./tmp/preventivo_stream_final.csv"
FILE_FINAL_XLSX = "./preventivi/[PREVENTIVO] Sacco Computo CF03.xlsx"

HEADER_RDO = 0  # Riga di header nel file RDO (0-based)
COL_RDO_DESC = "DESCRIZIONE"
COL_RDO_QTA = "QUANTITA"

# SOGLIE
THRESHOLD_GREEN = 0.80
THRESHOLD_YELLOW = 0.6 

# --- METRICHE GLOBALI ---
METRICS = {
    "match": 0,
    "no_match": 0,
    "warning": 0,
    "calls_embeddings": 0,
    "calls_gpt": 0,
    "scores": [], # Raccoglie tutti gli score trovati a DB
    "start_time": 0,
    "end_time": 0
}

def serialize_f32(vector):
    return struct.pack(f"<{len(vector)}f", *vector)

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    return conn

def get_embedding(text):
    # Incrementa contatore chiamate embeddings
    METRICS["calls_embeddings"] += 1
    
    text = str(text).replace("\n", " ")
    return client.embeddings.create(input=[text], model="text-embedding-3-small").data[0].embedding

def search_pure_vector(query_text, limit=3):
    conn = get_db_connection()
    query_vec = get_embedding(query_text)
    query_bin = serialize_f32(query_vec)
    
    sql = """
        SELECT r.id, r.code, r.description, r.unit_material_price, r.unit_manpower_price, r.source_file, v.distance
        FROM vec_recipes v
        JOIN recipes r ON v.rowid = r.id
        WHERE v.embedding MATCH ? AND k = ?
        ORDER BY v.distance ASC
    """
    rows = conn.execute(sql, (query_bin, limit)).fetchall()
    conn.close()
    
    candidates = []
    for row in rows:
        sim = 1 / (1 + row[6])  # Convert distance to similarity
        
        # Colleziona lo score per le statistiche
        METRICS["scores"].append(sim)
        
        if sim >= THRESHOLD_YELLOW:
            candidates.append({
                "id": row[0], 
                "code": row[1], 
                "desc": row[2],
                "p_art": row[3],
                "p_man": row[4],
                "source_file": row[5],
                "score": sim
            })
    return candidates

def get_components(recipe_id):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    comps = conn.execute("SELECT * FROM components WHERE recipe_id = ?", (recipe_id,)).fetchall()
    conn.close()
    return comps

def validate_match_with_gpt(rdo_desc, candidates):
    if not candidates:
        return None, "NO MATCH", "Nessun candidato sopra soglia"

    # Incrementa contatore chiamate GPT (solo se ci sono candidati da valutare)
    METRICS["calls_gpt"] += 1

    # Auto-confirm se score molto alto
    if candidates[0]['score'] > 0.94:
        # Nota: Qui NON incrementiamo calls_gpt perché usiamo una scorciatoia, 
        # ma se volessimo contare l'intenzione di validazione potremmo lasciarlo sopra.
        # Per precisione sui costi API, decrementiamo se saltiamo la chiamata.
        METRICS["calls_gpt"] -= 1 
        return candidates[0], "MATCH", "High Confidence Vector"

    options_text = ""
    for i, c in enumerate(candidates):
        options_text += f"Opzione {i+1}: {c['desc']} (Score: {c['score']:.2f} | P_ARTICOLO: {c['p_art']}, P_MANODOPERA: {c['p_man']})\n"

    prompt = f"""
    Sei un assistente esperto nella stesura di offerte per impianti meccanici ed elettrici.
    Ti fornirò la descrizione di una voce di computo metrico fornita dal cliente che dobbiamo valutare, ed una lista di possibili voci di costo proveniente dal nostro database di offerte storiche.
    Il tuo compito è selezionare la corrispondenza tecnica migliore in base alle specifiche indicate nella descrizione.
    Valuta attentamente le differenze in termini di misure, potenza, materiali e altre caratteristiche tecniche.
    Se nessuna delle opzioni corrisponde adeguatamente, indica che non c'è corrispondenza.
    è accettabile selezionare una voce simile solo se le differenze non influenzano la funzionalità per cui è richiesto l'elemento ma devi segnalare le differenze.
    Nello scegliere dai priorità alle voci di costo che esplicitano almeno uno tra prezzo articolo e prezzo manodopera.
    Voce di computo metrico: "{rdo_desc}"
    
    Voci di costo trovate:
    {options_text}

    Rispondi JSON: {{ "selected_index": 1, "status": "OK" se la voce di costo trovata la inseriresti nel preventivo finale per il cliente, "CHECK" se la voce è adatta ma vorresti una seconda verifica prima di inserirla nel preventivo finale, "NO MATCH" se non inseriresti nessuna delle opzioni nel preventivo finale, "reason": "..." }}
    """
    
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0
        )
        content = json.loads(res.choices[0].message.content)
        idx = content.get("selected_index", -1)
        gpt_status = content.get("status", "OK")
        reason = content.get("reason", "")
        
        if idx > 0 and idx <= len(candidates):
            chosen = candidates[idx-1]
            if gpt_status == "OK":
                return chosen, "MATCH", f"OK: {reason}"
            if gpt_status == "CHECK":
                return chosen, "WARNING", f"Voce Da Verificare: {reason}"
            else:
                return chosen, "NO MATCH", f"Scartata: {reason}"
        else:
            if candidates[0]['score'] >= 0.85:
                 return candidates[0], "WARNING", f"GPT Rejected but High Vector: {reason}"
            return None, "NO MATCH", f"GPT Rejected: {reason}"

    except:
        return candidates[0], "WARNING", "GPT Error"

def print_progress(current, total, bar_length=40, status="", last_item=""):
    percent = float(current) * 100 / total
    arrow = '-' * int(percent/100 * bar_length - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))
    
    # Pulisce la riga corrente e stampa la barra
    sys.stdout.write(f"\rProgress: [{arrow}{spaces}] {percent:.1f}% ({current}/{total}) {status}")
    sys.stdout.flush()

def process_production():
    print(f"🚀 AVVIO PREVENTIVATORE")
    METRICS["start_time"] = time.time()
    
    try:
        df_rdo = pd.read_excel(FILE_INPUT_RDO, header=HEADER_RDO)
        df_rdo.columns = [str(c).strip() for c in df_rdo.columns]
    except: 
        print(f"❌ ERRORE: Impossibile leggere il file RDO: {FILE_INPUT_RDO}")
        return

    f_csv = open(FILE_STREAM_CSV, 'w', newline='', encoding='utf-8')
    writer = csv.writer(f_csv, delimiter=';')
    
    cols = [
        "TIPO_RIGA", "CODICE RDO", "DESCRIZIONE RDO", "DESCRIZIONE TROVATA", "SORGENTE", "U.M.", 
        "QUANTITA COMP.", "QUANTITA ART.", "FAB.", 
        "PREZZO COMP.", "PREZZO ART.", "PREZZO MAN.", 
        "IMPORTO TOT.", 
        "STATO", "CONFIDENZA", "NOTE AI"
    ]
    writer.writerow(cols)
    
    total_rows = len(df_rdo)
    
    for i, row in df_rdo.iterrows():
        raw_desc = row.get(COL_RDO_DESC)
        raw_codice = row.get("CODICE")
        
        # Filtro righe vuote o non valide
        if pd.isna(raw_codice) or len(str(raw_codice)) < 3: 
            print_progress(i + 1, total_rows, status="Skipped (No Code)")
            continue
        
        desc_req = str(raw_desc).strip()
        qta_req = float(row.get(COL_RDO_QTA, 1) or 1)
        
        # Logica Core
        candidates = search_pure_vector(desc_req, limit=3)
        match_data, status, note = validate_match_with_gpt(desc_req, candidates)
        
        # Aggiornamento Metriche
        if status == "MATCH": METRICS["match"] += 1
        elif status == "WARNING": METRICS["warning"] += 1
        else: METRICS["no_match"] += 1
        
        score_fmt = f"{match_data['score']:.2f}" if match_data else "0.00"
        
        # Aggiornamento Barra Progresso
        print_progress(i + 1, total_rows, status=f"-> {status} ({score_fmt})")

        if match_data and (status == "MATCH" or status == "WARNING"):
            p_art = match_data['p_art'] or 0
            p_man = match_data['p_man'] or 0
            imp_tot = p_art * qta_req
            
            writer.writerow([
                "PADRE", raw_codice, raw_desc, match_data['desc'], match_data['source_file'],
                "CAD", "", qta_req, "", "", p_art, p_man, imp_tot,                      
                status, score_fmt, note       
            ])
            
            for c in get_components(match_data['id']):
                coeff = c['qty_coefficient'] or 0
                fab = qta_req * coeff
                writer.writerow([
                    "FIGLIO", "", "", f"   ↳ {c['description']}", "", "",
                    str(coeff).replace('.',','), "", fab, c['unit_price'], "", "", "", "", "", ""
                ])
        else:
            writer.writerow([
                "NOMATCH", "", desc_req, "", "", "CAD", "", qta_req, "", "", "", "", 
                0.0, "NO MATCH", "0.00", note
            ])
            
        f_csv.flush()

    f_csv.close()
    METRICS["end_time"] = time.time()
    finalize_excel()

def finalize_excel():
    print(f"\n\n🎨 Generazione Excel Finale (Style: Minimal)...")
    try:
        df = pd.read_csv(FILE_STREAM_CSV, sep=';', encoding='utf-8')
        df = df.fillna("") 
    except Exception as e: 
        print(f"❌ Errore lettura CSV per finalizzazione: {e}")
        return

    writer = pd.ExcelWriter(FILE_FINAL_XLSX, engine='xlsxwriter')
    
    # 1. Foglio Preventivo
    df.to_excel(writer, sheet_name='Preventivo', index=False)
    
    # 2. Foglio Riepilogo Metriche (Gestione robusta se METRICS è vuoto in caso di recovery)
    # Se stiamo recuperando da CSV senza aver girato il processo, le metriche in memoria sono a 0.
    # Possiamo calcolare alcune metriche base dal DF stesso.
    match_count = len(df[df['STATO'] == 'MATCH'])
    warning_count = len(df[df['STATO'] == 'WARNING'])
    nomatch_count = len(df[df['STATO'] == 'NO MATCH'])
    
    # Se METRICS ha dati reali (dal processo corrente) usiamo quelli, altrimenti stima dal CSV
    metrics_match = METRICS["match"] if METRICS["match"] > 0 else match_count
    metrics_warning = METRICS["warning"] if METRICS["warning"] > 0 else warning_count
    metrics_nomatch = METRICS["no_match"] if METRICS["no_match"] > 0 else nomatch_count
    
    summary_data = {
        "Metrica": [
            "Totale Voci Processate (Righe CSV)",
            "MATCH (Verde)", 
            "WARNING (Giallo)", 
            "NO MATCH (Rosso)", 
            "Chiamate API Embeddings", 
            "Chiamate API GPT-4o"
        ],
        "Valore": [
            len(df),
            metrics_match,
            metrics_warning,
            metrics_nomatch,
            METRICS["calls_embeddings"],
            METRICS["calls_gpt"]
        ]
    }
    pd.DataFrame(summary_data).to_excel(writer, sheet_name='Riepilogo', index=False)

    # --- FORMATTAZIONE ---
    wb = writer.book
    ws = writer.sheets['Preventivo']
    
    # DEFINIZIONE FORMATI
    fmt_green  = wb.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'align': 'center', 'bold': True, 'border': 1}) 
    fmt_yellow = wb.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C5700', 'align': 'center', 'bold': True, 'border': 1}) 
    fmt_red    = wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'align': 'center', 'bold': True, 'border': 1}) 
    fmt_child  = wb.add_format({'font_color': '#666666', 'italic': True})
    fmt_money  = wb.add_format({'num_format': '€ #,##0.00'})
    fmt_number = wb.add_format({'num_format': '#.##0,00'})
    fmt_error_text = wb.add_format({'font_color': '#9C0006'}) # Formato per evidenziare testo RDO in caso di errore

    idx_stato = 14 # Colonna O (0-based: O è la 14esima lettera? No, O è la 15esima (index 14). A=0, B=1... O=14)
    
    for i, row in df.iterrows():
        xls_row = i + 1
        stato = str(row['STATO'])
        tipo = str(row['TIPO_RIGA'])
        
        # Formattazione condizionale righe
        if tipo == 'PADRE':
            if stato == 'MATCH': ws.write(xls_row, idx_stato, stato, fmt_green)
            elif stato == 'WARNING': ws.write(xls_row, idx_stato, stato, fmt_yellow)
            
        elif tipo == 'NOMATCH':
            ws.write(xls_row, idx_stato, stato, fmt_red)
            # Evidenziamo descrizione RDO
            ws.write(xls_row, 2, row.get('DESCRIZIONE RDO', ''), fmt_error_text)
            
        elif tipo == 'FIGLIO':
            ws.set_row(xls_row, None, fmt_child)

    # Larghezza Colonne
    ws.set_column('C:F', 60) # Descrizioni larghe
    ws.set_column('G:I', 12, fmt_number) # Quantità
    ws.set_column('J:L', 12, fmt_money) # Prezzi
    ws.set_column('M:M', 15, fmt_money) # Totale
    ws.set_column('N:N', 15) # Stato
    ws.set_column('O:P', 25) # Note AI e Confidenza
    
    # Stili Riepilogo
    ws_rep = writer.sheets['Riepilogo']
    ws_rep.set_column('A:A', 35)
    ws_rep.set_column('B:B', 20)

    writer.close()
    print(f"🏆 File salvato: {FILE_FINAL_XLSX}")

if __name__ == "__main__":
    process_production()