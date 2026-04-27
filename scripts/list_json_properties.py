import json
import sys

def list_properties(json_path):
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"Propiedades encontradas en {json_path}:")
        print("-" * 50)
        
        for key, value in data.items():
            type_name = type(value).__name__
            if isinstance(value, dict):
                sub_keys = list(value.keys())
                print(f"{key} ({type_name}): {sub_keys}")
            elif isinstance(value, list):
                sample = f" (ejemplo: {value[0]})" if value else " (vacío)"
                print(f"{key} ({type_name}): {len(value)} elementos{sample}")
            else:
                print(f"{key} ({type_name}): {value}")
                
    except Exception as e:
        print(f"Error al leer el archivo: {e}")

if __name__ == "__main__":
    path = r"C:\Users\jlja\Downloads\Untitled-1.json"
    if len(sys.argv) > 1:
        path = sys.argv[1]
    list_properties(path)
