import pandas as pd
import xlsxwriter
from src.core.entities import QuoteResult

def write_quote_dto_to_excel(result: QuoteResult, output_path: str):
    """Genera Excel rispettando formato legacy ma usando dati deterministici."""
    print(f"📊 Generazione Excel: {output_path}")
    
    rows_for_df = []
    
    for item in result.items:
        # Calcoli Totali
        p_tot_db = item.p_unit_tot_db * item.quantity_input
        p_tot_rdo = item.p_unit_tot_rdo * item.quantity_input
        p_unit_delta = item.p_unit_tot_db - item.p_unit_tot_rdo
        p_tot_delta = p_tot_db - p_tot_rdo
        
        # Riga PADRE
        rows_for_df.append({
            "TIPO": "PADRE",
            "CODICE": item.codice_input,
            "DESCRIZIONE": item.description_input,
            "QTA": item.quantity_input,
            "UM": item.um_input,
            "FAB": "",
            "SORGENTE": item.source_file,
            "CODICE_DB": item.match_sku, # NUOVO: Mostra SKU matched
            "DESC_DB": item.match_description,
            "P_UNIT_MAT_DB": item.p_unit_mat_db,
            "P_UNIT_MAN_DB": item.p_unit_man_db,
            "P_MAT_RDO": item.p_mat_rdo,
            "P_MAN_RDO": item.p_man_rdo,
            "P_UNIT_TOT_DB": item.p_unit_tot_db,
            "P_UNIT_TOT_RDO": item.p_unit_tot_rdo,
            "P_UNIT_DELTA": p_unit_delta,
            "P_TOT_DB": p_tot_db,
            "P_TOT_RDO": p_tot_rdo,
            "P_TOT_DELTA": p_tot_delta,
            "STATO": item.status,
            "INTEGRITA": item.integrity_status, # NUOVO: Info su calcolo
            "REASONING": item.reasoning
        })
        
        # Righe FIGLI (BOM)
        for child in item.children:
            child_fab = child.unit_quantity * item.quantity_input
            child_tot = child.unit_price * child_fab
            
            rows_for_df.append({
                "TIPO": "FIGLIO",
                "CODICE": child.sku, # SKU Figlio
                "DESCRIZIONE": f"↳ {child.description}",
                "QTA": child.unit_quantity,
                "UM": "",
                "FAB": child_fab,
                "SORGENTE": "",
                "CODICE_DB": "",
                "DESC_DB": "",
                "P_UNIT_MAT_DB": child.unit_price if child.type == "MAT" else 0,
                "P_UNIT_MAN_DB": child.unit_price if child.type == "MAN" else 0,
                "P_MAT_RDO": 0, "P_MAN_RDO": 0,
                "P_UNIT_TOT_DB": child.unit_price * child.unit_quantity,
                "P_UNIT_TOT_RDO": 0, "P_UNIT_DELTA": 0,
                "P_TOT_DB": child_tot,
                "P_TOT_RDO": 0, "P_TOT_DELTA": 0,
                "STATO": "", "INTEGRITA": "", "REASONING": ""
            })

    # Creazione DataFrame
    df = pd.DataFrame(rows_for_df)
    
    # 2. SCRITTURA E FORMATTAZIONE
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Preventivo', index=False)
        
        wb = writer.book
        ws = writer.sheets['Preventivo']
        
        # --- DEFINIZIONE FORMATI (Legacy) ---
        fmt_header = wb.add_format({'bold': True, 'bg_color': '#D9D9D9', 'border': 1})
        fmt_currency = wb.add_format({'num_format': '#,##0.00 €'})
        fmt_percentage = wb.add_format({'num_format': '0.0%'})
        
        # Formati Stato (Sfondo colorato per la cella stato)
        fmt_green = wb.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        fmt_yellow = wb.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C5700'})
        fmt_red = wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        
        # Formati Delta (Solo testo colorato + valuta)
        fmt_delta_green = wb.add_format({'font_color': '#006100', 'num_format': '#,##0.00 €'}) # Risparmio
        fmt_delta_red = wb.add_format({'font_color': '#9C0006', 'num_format': '#,##0.00 €'})   # Costo Maggiore
        
        # Formato Figlio
        fmt_child = wb.add_format({'italic': True, 'font_color': '#666666'})

        # --- APPLICAZIONE FORMATI COLONNE ---
        for col_num, value in enumerate(df.columns.values):
            ws.write(0, col_num, value, fmt_header)
            
        # Larghezze
        ws.set_column('C:C', 50) # Descrizione
        ws.set_column('H:H', 50) # Descrizione DB
        ws.set_column('T:T', 40) # Reasoning
        
        # Colonne Valuta generiche (P_UNIT_MAT_DB -> P_TOT_DELTA)
        # Nota: verranno sovrascritte dai formati specifici Delta dove necessario
        for i in range(8, 18):
            ws.set_column(i, i, 12, fmt_currency)

        # --- LOGICA DI FORMATTAZIONE RIGHE (COPIA 1-1) ---
        
        # Recupero indici colonne dinamici per sicurezza
        try:
            col_stato = df.columns.get_loc("STATO")
            col_delta_unit = df.columns.get_loc("P_UNIT_DELTA")
            col_delta_tot = df.columns.get_loc("P_TOT_DELTA")
        except KeyError:
            # Fallback se le colonne non esistono (non dovrebbe accadere)
            col_stato, col_delta_unit, col_delta_tot = 18, 14, 17

        for i, row in df.iterrows():
            xls_row = i + 1  # +1 per Excel (riga 0 è intestazione)
            row_type = str(row['TIPO'])
            status = str(row['STATO'])
            
            # Formattazione Delta Unitario (Valore)
            delta_unit = row['P_UNIT_DELTA']
            
            # Colorazione STATO e DELTA per righe PADRE
            if row_type == "PADRE":
                # 1. Colora Cella STATO
                if status == "MATCH" or status == "AUTO_MATCH": # Gestisco anche AUTO_MATCH come MATCH
                    ws.write(xls_row, col_stato, status, fmt_green)
                elif status == "WARNING" or status == "CHECK":
                    ws.write(xls_row, col_stato, status, fmt_yellow)
                elif status == "NOMATCH":
                    ws.write(xls_row, col_stato, status, fmt_red)
                
                # 2. Colora Cella DELTA UNITARIO
                if pd.notna(delta_unit):
                    if delta_unit < 0:
                        ws.write(xls_row, col_delta_unit, delta_unit, fmt_delta_green)
                    else:
                        ws.write(xls_row, col_delta_unit, delta_unit, fmt_delta_red)
                
                # 3. Colora Cella DELTA TOTALE
                delta_tot = row['P_TOT_DELTA']
                if pd.notna(delta_tot):
                    if delta_tot < 0:
                        ws.write(xls_row, col_delta_tot, delta_tot, fmt_delta_green)
                    else:
                        ws.write(xls_row, col_delta_tot, delta_tot, fmt_delta_red)
                        
            elif row_type == "FIGLIO":
                # Formattazione intera riga figlio (Sovrascrive colonne precedenti)
                ws.set_row(xls_row, None, fmt_child)
                
                # Opzionale: Se vuoi che i numeri mantengano la valuta anche se grigi,
                # devi fare write esplicita, ma set_row + fmt_child è fedele al tuo snippet.
                # Per rigore 1-1, ws.set_row è quello che faceva il tuo codice.

        # --- FOGLIO METRICHE ---
        ws_stats = wb.add_worksheet("Metriche")
        stats = result.stats
        
        ws_stats.write(0, 0, "Metriche Processo", fmt_header)
        ws_stats.write(1, 0, "Voci Totali")
        ws_stats.write(1, 1, stats.get("processed", 0))
        
        ws_stats.write(2, 0, "Match (Verde)")
        ws_stats.write(2, 1, stats.get("match", 0))
        if stats.get("processed", 0) > 0:
            ws_stats.write(2, 2, stats.get("match", 0) / stats.get("processed", 1), fmt_percentage)
        
        ws_stats.write(3, 0, "Warning (Giallo)")
        ws_stats.write(3, 1, stats.get("warning", 0))
        
        ws_stats.write(4, 0, "No Match (Rosso)")
        ws_stats.write(4, 1, stats.get("nomatch", 0))

    print("✅ Excel Legacy Formatted Written.")