import json
import requests
import pandas as pd
import time
import os
import argparse

def process_manifests(csv_path, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    for _, row in df.iterrows():
        title = row.get('title', 'Untitled')
        url = row.get('source_url')
        ranges = str(row.get('ranges', ''))
        filename = row.get('output_name', 'manifest.json')

        print(f"--- Processing: {title} ---")
        
        try:
            res = requests.get(url, timeout=30)
            res.raise_for_status()
            manifest = res.json()

            is_v3 = "items" in manifest
            
            # 1. Store original label for the 'magazine' metadata entry
            # Handles v3 (dict) and v2 (string/list) labels
            raw_label = manifest.get('label', 'Unknown')
            if isinstance(raw_label, dict): # v3
                orig_label_str = next(iter(raw_label.values()))[0]
            elif isinstance(raw_label, list): # v2 list
                orig_label_str = raw_label[0]
            else:
                orig_label_str = raw_label

            # 2. Extract Indices
            indices = [int(i.strip()) - 1 for i in ranges.split(',') if i.strip().isdigit()]

            # 3. Handle Canvas Splitting & Version Specifics
            if is_v3:
                all_items = manifest.get('items', [])
                manifest['items'] = [all_items[i] for i in indices if 0 <= i < len(all_items)]
                manifest['label'] = {"en": [title]}
                
                # Metadata Update (v3 style)
                new_entry = {"label": {"en": ["magazine"]}, "value": {"en": [str(orig_label_str)]}}
                current_metadata = manifest.get('metadata', [])
                manifest['metadata'] = [new_entry] + current_metadata
            else:
                all_canvases = manifest['sequences'][0]['canvases']
                manifest['sequences'][0]['canvases'] = [all_canvases[i] for i in indices if 0 <= i < len(all_canvases)]
                manifest['label'] = title
                
                # Metadata Update (v2 style)
                new_entry = {"label": "magazine", "value": str(orig_label_str)}
                current_metadata = manifest.get('metadata', [])
                manifest['metadata'] = [new_entry] + current_metadata

            # 4. Update ID to match the output filename (prevents viewer caching issues)
            # You should ideally replace 'localhost' with your actual hosting domain
            base_id = f"https://localhost/iiif/{filename}"
            manifest['id' if is_v3 else '@id'] = base_id

            # 5. Save file
            save_path = os.path.join(output_folder, filename)
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2, ensure_ascii=False)
            
            print(f"Successfully saved to {save_path}")
            time.sleep(0.2) 

        except Exception as e:
            print(f"Failed to process {url}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split IIIF Manifests based on a CSV configuration.")
    parser.add_argument("csv", help="Path to the input CSV file")
    parser.add_argument("-o", "--output", default="output_manifests", help="Folder to save new manifests")
    
    args = parser.parse_args()
    process_manifests(args.csv, args.output)