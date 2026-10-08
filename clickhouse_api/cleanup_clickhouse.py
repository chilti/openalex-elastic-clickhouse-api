import os
import clickhouse_connect
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8123))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

def cleanup():
    print(f"🗑️  Conectando a ClickHouse para limpieza ({CH_HOST}:{CH_PORT})...")
    
    try:
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DATABASE
        )
        
        # 1. Identificar tablas con el prefijo antiguo
        tables = [t[0] for t in client.query("SHOW TABLES").result_rows]
        old_tables = [t for t in tables if t.startswith('openalex_')]
        
        if not old_tables:
            print("✨ No se encontraron tablas con el prefijo 'openalex_'. Nada que borrar.")
        else:
            print(f"\nSe encontraron las siguientes tablas antiguas:")
            for t in old_tables:
                count = client.command(f"SELECT count() FROM `{t}`")
                print(f"  - {t} ({count:,} registros)")
            
            confirm = input("\n⚠️  ¿Deseas BORRAR estas tablas permanentemente? (s/n): ")
            if confirm.lower() == 's':
                for t in old_tables:
                    print(f"  Dropping {t} (omitiendo límite de tamaño)...")
                    # Se agrega SETTINGS para permitir borrar tablas de más de 50GB
                    client.command(f"DROP TABLE IF EXISTS `{t}` SETTINGS max_table_size_to_drop = 0")
                print("✅ Tablas eliminadas.")
            else:
                print("Operación cancelada para las tablas.")

        # 2. Opción de resetear el progreso de archivos
        if '_processed_files' in tables:
            count = client.command("SELECT count() FROM _processed_files")
            print(f"\nLa tabla de control '_processed_files' tiene {count:,} registros.")
            confirm_reset = input("❓ ¿Deseas RESETEAR el progreso para volver a cargar todo desde cero? (s/n): ")
            
            if confirm_reset.lower() == 's':
                print("  Truncating _processed_files...")
                client.command("TRUNCATE TABLE _processed_files")
                print("✅ Progreso reseteado. La próxima carga procesará todos los archivos.")
            else:
                print("Se mantuvo el progreso de archivos.")

    except Exception as e:
        print(f"❌ Error durante la limpieza: {e}")

if __name__ == "__main__":
    cleanup()
