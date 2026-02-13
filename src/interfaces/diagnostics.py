import sys
import os
import sqlite3
from tabulate import tabulate # Assicurati di avere tabulate o usa print semplice

# Setup Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.infrastructure.database import get_db_connection

def run_diagnostics():
    conn = get_db_connection()
    cursor = conn.cursor()

    print("\n🩺 DIAGNOSTICA SISTEMA DETERMINISTICO")
    print("=" * 60)

    # 1. STATISTICHE GENERALI
    print("\n📊 1. PANORAMICA STATO CATALOGO")
    cursor.execute("""
        SELECT cost_integrity_status, pricing_strategy, COUNT(*) 
        FROM catalog_items 
        GROUP BY cost_integrity_status, pricing_strategy
    """)
    rows = cursor.fetchall()
    print(tabulate(rows, headers=["Stato", "Strategia", "Conteggio"], tablefmt="simple"))

    # 2. NODI 'BROKEN' (Orfani o Errori)
    print("\n🚨 2. NODI 'BROKEN' (Impossibile calcolare il prezzo)")
    cursor.execute("""
        SELECT sku, description_short, source_file_origin 
        FROM catalog_items 
        WHERE cost_integrity_status = 'BROKEN'
        LIMIT 10
    """)
    broken = cursor.fetchall()
    if broken:
        print(tabulate(broken, headers=["SKU", "Descrizione", "Source"], tablefmt="grid"))
        print("... (vedi tabella bom_integrity_errors per dettagli)")
    else:
        print("✅ Nessun nodo rotto trovato.")

    # 3. ANOMALIE STRUTTURALI (MAKE senza figli)
    print("\n⚠️  3. ANOMALIE LOGICHE: 'SUM_CHILDREN' senza figli")
    cursor.execute("""
        SELECT i.sku, i.description_short 
        FROM catalog_items i
        LEFT JOIN bill_of_materials bom ON i.sku = bom.parent_sku
        WHERE i.pricing_strategy = 'SUM_CHILDREN' 
          AND bom.child_sku IS NULL
    """)
    anomalies = cursor.fetchall()
    if anomalies:
        print(f"Trovati {len(anomalies)} articoli marcati come assemblati ma vuoti (Prezzo sarà 0):")
        print(tabulate(anomalies[:5], headers=["SKU", "Descrizione"], tablefmt="simple"))
    else:
        print("✅ Tutti i nodi 'MAKE' hanno almeno un figlio.")

    # 4. TOP 5 ARTICOLI PIÙ COSTOSI (Sanity Check Valori)
    print("\n💰 4. TOP 5 ARTICOLI PIÙ COSTOSI (Check Ordine Grandezza)")
    cursor.execute("""
        SELECT sku, description_short, (current_material_cost + current_labor_cost) as tot 
        FROM catalog_items 
        ORDER BY tot DESC LIMIT 5
    """)
    print(tabulate(cursor.fetchall(), headers=["SKU", "Desc", "Prezzo Totale"], tablefmt="simple"))

    # 5. INTEGRITÀ GRAFO (Cicli o Dirty residui)
    cursor.execute("SELECT COUNT(*) FROM catalog_items WHERE cost_integrity_status = 'DIRTY'")
    dirty_count = cursor.fetchone()[0]
    if dirty_count > 0:
        print(f"\n☢️  ATTENZIONE: {dirty_count} nodi sono ancora 'DIRTY'.")
        print("    Possibili cause: Cicli infiniti (A->B->A) o dipendenze mancanti.")
    else:
        print("\n✅ Integrità Grafo: Nessun nodo pendente (Tutto VALID o BROKEN).")

    conn.close()

if __name__ == "__main__":
    run_diagnostics()