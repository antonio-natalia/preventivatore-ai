import pypdf
import re
import pandas as pd

def extract_from_pdf(file_path):
    print(f"Processing {file_path}...")
    
    # Regex for the data line
    # Matches: U.M. (word) + Space + Qty (number) + Space + Price (currency) + Space + Amount (currency)
    # Note: 'a corpo' needs handling.
    # Price and Amount can have '€' or not.
    
    # Pattern explanation:
    # (?P<um>...) : Group for U.M. (cad, m, m2, etc.)
    # \s+ : spaces
    # (?P<qty>...) : Quantity (1.234,56)
    # \s+
    # (?:€\s*)?(?P<price>...) : Price
    # \s+
    # (?:€\s*)?(?P<amount>...) : Amount
    
    # Adjust regex to be flexible with spaces and symbols
    # 'cad' or 'cad.' or 'm' or 'm2' or 'a corpo'
    # We use a list of known UMs to anchor.
    
    known_ums = [
        'm', 'm2', 'm3', 'mq', 'mc', 'kg', 'cad', 'cad.', 'corpo', 'a corpo', 
        'h', 'ore', 'litri', 'l', 'a.c.', 'ac'
    ]
    um_pattern = "|".join([re.escape(u) for u in known_ums])
    
    # Regex: End of line anchor $ is important? 
    # Sometimes there is trailing space.
    data_regex = re.compile(
        r'(?P<um>\b(?:' + um_pattern + r'))\s+'
        r'(?P<qty>[\d\.]+(?:,\d+)?)\s+'
        r'(?:€\s*)?(?P<price>[\d\.]+(?:,\d+)?)\s+'
        r'(?:€\s*)?(?P<amount>[\d\.]+(?:,\d+)?)\s*$', 
        re.IGNORECASE
    )
    
    # Regex for Code: Starts with A, B, C, numbers, dots. 
    # E.g. "A3.1.1", "84.3.26", "OS 30" (category, not item code?)
    # Valid codes in these files seem to be like "A3.1.1", "A4.1.2", "83.1.2", "C3.1..."
    # Or "OS 30" which is a category. 
    # We want the item code.
    # Code pattern: Alphanumeric + dots, length > 2.
    code_regex = re.compile(r'^(?P<code>[A-Z0-9]{1,3}[\.][A-Z0-9\.]+)\s')

    items = []
    buffer = []
    
    reader = pypdf.PdfReader(file_path)
    
    for page in reader.pages:
        text = page.extract_text()
        if not text:
            continue
            
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Filter noise headers
            if "SOGIN" in line or "Computo Metrico" in line or "Pag." in line or "Rev." in line:
                continue
            if "RIPILOGO" in line or "IMPORTO TOTALE" in line:
                continue

            match = data_regex.search(line)
            if match:
                # Found data line
                data = match.groupdict()
                
                # Check if buffer has content
                full_desc = " ".join(buffer).strip()
                
                # Extract code from the START of the description
                # It might be in the buffer (previous lines) or on the data line?
                # pypdf usually separates columns by \n if they are far apart, 
                # but if they are close, maybe not.
                # However, usually Code is on the left, Desc in middle, Data on right.
                # If Code and Desc are on same line in PDF, pypdf matches them on same line?
                # "A3.1.1 Fornitura... m 10 ..." -> This line matches data_regex.
                # The text BEFORE the match is the description/code.
                
                start_of_line_text = line[:match.start()].strip()
                if start_of_line_text:
                    full_desc = (full_desc + " " + start_of_line_text).strip()
                
                # Now parse code from full_desc
                item_code = ""
                # Try to find code at the very beginning
                code_match = code_regex.match(full_desc)
                if code_match:
                    item_code = code_match.group('code')
                    # Remove code from desc
                    description = full_desc[len(item_code):].strip()
                else:
                    # Maybe the code was just "OS 30"? No, that's category.
                    # Maybe "83.2.5.4"? Yes.
                    # Sometimes code is alone on a previous line.
                    # Let's try to find any code-like token at start.
                    tokens = full_desc.split()
                    if tokens and ('.' in tokens[0] or tokens[0].isalnum()):
                         if len(tokens[0]) > 2 and not tokens[0].lower() in ['il', 'la', 'per', 'tubo', 'cavo', 'fornitura']:
                             # Heuristic: It's a code
                             item_code = tokens[0]
                             description = " ".join(tokens[1:])
                         else:
                             description = full_desc
                    else:
                        description = full_desc

                # Clean amounts
                try:
                    qty = float(data['qty'].replace('.', '').replace(',', '.'))
                    price = float(data['price'].replace('.', '').replace(',', '.'))
                    amount = float(data['amount'].replace('.', '').replace(',', '.'))
                except:
                    qty = 0
                    price = 0
                    amount = 0
                
                items.append({
                    'N.': '', # Line number not clear
                    'CODICE': item_code,
                    'DESCRIZIONE': description,
                    'U.M.': data['um'],
                    'QUANTITA': qty,
                    'PREZZO': price,
                    'IMPORTO': amount
                })
                
                buffer = [] # Reset
            else:
                # Add to buffer
                buffer.append(line)
                
    return pd.DataFrame(items)

# List of files
files = [
    "./richieste_ordine/0005-26/CS GR 00317 Rev 03 Computo Metrico Estimativo – Impianto Elettrico e Sistemi Speciali.pdf",
    "./richieste_ordine/0005-26/CS GR 00321 Rev 03 Computo Metrico Estimativo – Strumentazione e Automazione e Controllo.pdf"
]

all_data = []
for f in files:
    try:
        df = extract_from_pdf(f)
        all_data.append(df)
    except Exception as e:
        print(f"Error {f}: {e}")

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    # Filter empty U.M. or Qty if they slipped through?
    # My regex requires U.M. and Qty, so they are not empty.
    # User requirement: "escluse le righe che rappresentano dei totali".
    # My regex requires a unit price and total amount. Totals usually don't have unit price.
    # So this should be fine.
    
    # Save
    output_file = "Computo_Metrico_Consolidato.xlsx"
    final_df.to_excel(output_file, index=False)
    print(f"File created: {output_file}")
    print(final_df.head())
else:
    print("No data extracted.")