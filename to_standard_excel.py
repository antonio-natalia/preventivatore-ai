import pandas as pd
import os
import warnings

# Ignoriamo warning specifici di openpyxl per stili/bordi non supportati (irrilevanti per i dati)
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

def process_hierarchical_excel(file_path):
    print(f"Lettura del file Excel: {file_path}")
    
    # 1. Lettura diretta del file Excel
    # header=4 indica che la 5a riga (indice 4) contiene le intestazioni (N., CODICE, ecc.)
    try:
        df = pd.read_excel(file_path, header=4, engine='openpyxl')
    except Exception as e:
        return print(f"Errore nell'apertura del file Excel: {e}")

    # Pulizia nomi colonne (rimuove spazi extra o ritorni a capo nei titoli)
    df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]
    
    # Identificazione colonne chiave
    # Cerchiamo le colonne anche se i nomi variano leggermente
    col_map = {
        'code': next((c for c in df.columns if 'ARTICOLO' in c), 'CODICE'),
        'desc': next((c for c in df.columns if 'Voce di lavoro' in c or 'DESCRIZIONE' in c), 'INDICAZIONE DEI LAVORI E DELLE PROVVISTE'),
        'um': next((c for c in df.columns if 'U.M.' in c), 'U.M.'),
        'qty': next((c for c in df.columns if 'q.tà' in c), "QUANTITA'"),
        'price': next((c for c in df.columns if 'IMPORTO UNITARIO' in c), 'PREZZO'),
        'importo': next((c for c in df.columns if 'IMPORTO TOTALE' in c), 'IMPORTO'),
        'n': next((c for c in df.columns if 'N.' in c), 'N.')
    }
    
    processed_rows = []

    # Stati per la gerarchia
    last_parent_code = None
    last_parent_desc = ""
    
    current_code = None
    current_desc_full = ""
    current_n = None

    for index, row in df.iterrows():
        # Gestione valori NaN (celle vuote in Excel)
        raw_code = str(row[col_map['code']]) if pd.notna(row[col_map['code']]) else ""
        raw_desc = str(row[col_map['desc']]) if pd.notna(row[col_map['desc']]) else ""
        
        # --- LOGICA DI RICONOSCIMENTO CODICI ---
        if raw_code and raw_code.lower() != "nan":
            # Pulizia codice (spazi)
            clean_code = raw_code.strip()
            
            # Verifica se è un FIGLIO
            # Criterio: Il codice inizia con il codice del padre precedente ED è più lungo
            is_child = (last_parent_code is not None) and \
                       clean_code.startswith(last_parent_code) and \
                       (len(clean_code) > len(last_parent_code))
            
            if is_child:
                # È un figlio: Eredita la descrizione del padre + la sua specifica
                current_code = clean_code
                current_desc_full = f"{last_parent_desc} {raw_desc}".strip()
                current_n = row[col_map['n']]
            else:
                # È un nuovo PADRE (o articolo standard)
                last_parent_code = clean_code
                last_parent_desc = raw_desc
                
                # Impostiamo anche questo come corrente, nel caso sia un articolo standard che ha totali propri
                current_code = clean_code
                current_desc_full = raw_desc
                current_n = row[col_map['n']]

        # --- LOGICA DI ESTRAZIONE DATI (Riga "Totale") ---
        # Cerchiamo la riga che contiene "Totale" nella descrizione e abbiamo un codice attivo
        elif "totale" in raw_desc.lower() and current_code is not None:
            
            # Estrazione valori numerici con gestione errori
            try:
                q_val = row[col_map['qty']]
                qty = float(q_val) if pd.notna(q_val) else 0.0
            except: qty = 0.0
            
            try:
                p_val = row[col_map['price']]
                price = float(p_val) if pd.notna(p_val) else 0.0
            except: price = 0.0
            
            try:
                i_val = row[col_map['importo']]
                imp = float(i_val) if pd.notna(i_val) else 0.0
            except: imp = 0.0

            # Calcolo inverso del prezzo se mancante (spesso capita nei totali calcolati)
            if price == 0 and qty != 0:
                price = imp / qty

            new_row = {
                'N.': current_n,
                'CODICE': current_code,
                'DESCRIZIONE': current_desc_full,
                'U.M.': row[col_map['um']],
                'QUANTITA': qty,
                'PREZZO': price,
                'IMPORTO': imp
            }
            processed_rows.append(new_row)
            
            # Resettiamo il codice corrente per non duplicare o assegnare totali errati
            # Manteniamo però last_parent_code perché potrebbero esserci altri figli
            current_code = None

    return pd.DataFrame(processed_rows)

# --- ESECUZIONE ---
# Inserisci qui il nome esatto del tuo file Excel
input_file = './richieste_ordine/LTE Impianti Srl Sacco Computo CF03.xls'

if os.path.exists(input_file):
    df_result = process_hierarchical_excel(input_file)
    
    if not df_result.empty:
        # Colonne richieste dal formato finale
        target_cols = ['N.', 'CODICE', 'DESCRIZIONE', 'U.M.', 'QUANTITA', 'PREZZO', 'IMPORTO']
        
        # Riordina e riempi colonne mancanti
        for col in target_cols:
            if col not in df_result.columns:
                df_result[col] = None
        
        df_final = df_result[target_cols]
        
        output_name = 'sacco_computo_cf03.xlsx'
        df_final.to_excel(output_name, index=False)
        print(f"Successo! File generato: {output_name}")
        print(df_final.head())
    else:
        print("Nessun dato trovato. Controlla che il file non sia vuoto o i nomi delle colonne.")
else:
    print(f"File non trovato: {input_file}")