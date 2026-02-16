import sqlite3

def init_domain_schema(connection: sqlite3.Connection):
    """
    Inizializza lo schema del database seguendo il pattern Domain-Driven Design
    per un motore di calcolo costi deterministico (Bottom-Up).
    """
    cursor = connection.cursor()
    print("⚙️  [SCHEMA] Inizializzazione Schema Domain-Driven (Bottom-Up Pricing)...")

    # ---------------------------------------------------------
    # 1. CATALOG_ITEMS (Master Data)
    # Rappresenta l'entità principale (Nodo o Foglia).
    # ---------------------------------------------------------
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS catalog_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- IDENTITÀ
            sku TEXT UNIQUE NOT NULL,               -- Codice Univoco (Business Key)
            external_ref_id TEXT,                   -- ID numerico XML (per compatibilità/linkaggi)
            
            -- DESCRIZIONE E CATEGORIZZAZIONE
            description_short TEXT,
            description_long TEXT,                  -- Usato per Embeddings e Ricerca Semantica
            unit_of_measure TEXT,
            category_tag TEXT,
            
            -- MOTORE DI CALCOLO (Il "Semaforo" della logica)
            pricing_strategy TEXT NOT NULL,         -- ENUM: 'USE_DECLARED_PRICE', 'SUM_CHILDREN'
            cost_integrity_status TEXT NOT NULL,    -- ENUM: 'VALID', 'DIRTY', 'BROKEN'
            
            -- PREZZI ATTIVI (Current State)
            -- Se SUM_CHILDREN: questi valori sono il risultato del calcolo matematico
            -- Se USE_DECLARED_PRICE: questi valori sono la copia del listino fornitore
            current_material_cost REAL DEFAULT 0.0,
            current_labor_cost REAL DEFAULT 0.0,
            
            -- METADATA DI SISTEMA
            last_update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            source_file_origin TEXT
        )
    ''')

    # ---------------------------------------------------------
    # 2. BILL_OF_MATERIALS (La Struttura/Grafo)
    # Tabella di giunzione che definisce la topologia (Archi).
    # ---------------------------------------------------------
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bill_of_materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            parent_sku TEXT NOT NULL,               -- Chi viene assemblato (FK logica)
            child_sku TEXT NOT NULL,                -- Chi viene usato (FK logica)
            
            usage_quantity REAL NOT NULL,           -- Coefficiente tecnico di impiego
            
            -- Vincoli di integrità referenziale
            FOREIGN KEY(parent_sku) REFERENCES catalog_items(sku) ON DELETE CASCADE,
            FOREIGN KEY(child_sku) REFERENCES catalog_items(sku) ON DELETE RESTRICT,
            
            -- VINCOLO DI UNICITÀ (RIPRISTINATO)
            -- Una specifica coppia Padre-Figlio deve essere univoca.
            -- Se il listino contiene duplicati, devono essere risolti (deduplicati)
            -- prima dell'inserimento nel DB (logica Last Write Wins).
            UNIQUE(parent_sku, child_sku)
        )
    ''')

    # ---------------------------------------------------------
    # 3. COST_HISTORY_LOG (Audit Trail Finanziario)
    # Traccia ogni variazione di prezzo, sia da import che da ricalcolo.
    # ---------------------------------------------------------
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cost_history_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT NOT NULL,
            
            recorded_material_cost REAL,
            recorded_labor_cost REAL,
            
            event_type TEXT,                        -- ENUM: 'IMPORT_UPDATE', 'CALCULATION_CHANGE'
            change_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            source_context TEXT,                    -- Nome file o Processo scatenante
            
            FOREIGN KEY(sku) REFERENCES catalog_items(sku)
        )
    ''')

    # ---------------------------------------------------------
    # 4. BOM_INTEGRITY_ERRORS (Registro Anomalie)
    # Gestione degli "Orfani" e collegamenti rotti.
    # ---------------------------------------------------------
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bom_integrity_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            parent_sku TEXT,
            missing_child_sku TEXT,
            
            error_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            source_file_origin TEXT,
            
            is_resolved BOOLEAN DEFAULT 0
        )
    ''')

    # ---------------------------------------------------------
    # 5. VEC_CATALOG_ITEMS (Ricerca Semantica)
    # Tabella virtuale per estensione sqlite-vec.
    # ---------------------------------------------------------
    try:
        cursor.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS vec_catalog_items USING vec0(
                embedding float[1536]
            )
        ''')
    except sqlite3.OperationalError:
        # Gestisce il caso in cui l'estensione non sia caricata o la tabella esista già in modo incompatibile
        print("⚠️  Attenzione: Estensione vettoriale non disponibile o tabella già esistente.")
        pass
        
    # ---------------------------------------------------------
    # 6. BOM_HISTORY_LOG (Storico Versionamento Distinte)
    # Salva le vecchie versioni delle BOM prima che vengano sovrascritte.
    # ---------------------------------------------------------
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bom_history_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_sku TEXT NOT NULL,
            child_sku TEXT NOT NULL,
            usage_quantity REAL,
            
            archived_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            replaced_by_source_file TEXT,
            
            FOREIGN KEY(parent_sku) REFERENCES catalog_items(sku)
        )
    ''')

    # ---------------------------------------------------------
    # 7. INDICI PER PERFORMANCE
    # Fondamentali per velocizzare i JOIN ricorsivi durante il calcolo costi.
    # ---------------------------------------------------------
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_catalog_sku ON catalog_items(sku);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bom_parent ON bill_of_materials(parent_sku);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bom_child ON bill_of_materials(child_sku);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_catalog_status ON catalog_items(cost_integrity_status);")
    
    connection.commit()
    print("✅ [SCHEMA] Database pronto per Logica Deterministica.")