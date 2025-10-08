import zipfile
from pathlib import Path

def inspect_layer_zip():
    """Inspeciona a estrutura da Lambda Layer"""
    
    zip_path = "lambda_layer.zip"
    
    if not Path(zip_path).exists():
        print(f"❌ Arquivo {zip_path} não encontrado!")
        return
    
    print(f"🔍 Inspecionando {zip_path}...\n")
    
    with zipfile.ZipFile(zip_path, 'r') as zipf:
        all_files = zipf.namelist()
        
        print(f"📊 Total de arquivos: {len(all_files)}\n")
        
        # Verificar estrutura
        print("📁 ESTRUTURA DO ZIP:")
        print("=" * 50)
        
        # Primeiros níveis de diretórios
        root_items = set()
        for f in all_files:
            if '/' in f:
                root_items.add(f.split('/')[0])
            else:
                root_items.add(f)
        
        print("Diretórios/arquivos raiz:")
        for item in sorted(root_items):
            print(f"  📂 {item}/")
        
        # Verificar se existe python/
        has_python_dir = any(f.startswith('python/') for f in all_files)
        print(f"\n✓ Pasta 'python/': {'✅ SIM' if has_python_dir else '❌ NÃO'}")
        
        if has_python_dir:
            # Listar o que tem dentro de python/
            python_items = set()
            for f in all_files:
                if f.startswith('python/') and '/' in f[7:]:
                    python_items.add(f.split('/')[1])
            
            print(f"\n📦 Pacotes em 'python/':")
            for item in sorted(python_items):
                print(f"  - {item}")
            
            # Verificar pacotes críticos
            critical_packages = ['pandas', 's3fs', 'numpy', 'pyarrow']
            print(f"\n🔎 Verificação de pacotes críticos:")
            for pkg in critical_packages:
                exists = any(f.startswith(f'python/{pkg}/') for f in all_files)
                status = '✅' if exists else '❌'
                print(f"  {status} {pkg}")
        
        else:
            print("\n⚠️  PROBLEMA: A pasta 'python/' não existe no ZIP!")
            print("   A estrutura está INCORRETA.")
            print("\n📋 Primeiros 20 arquivos do ZIP:")
            for f in all_files[:20]:
                print(f"  - {f}")
        
        # Tamanho
        size_mb = Path(zip_path).stat().st_size / (1024 * 1024)
        print(f"\n📊 Tamanho do arquivo: {size_mb:.2f} MB")
        
        # Diagnóstico final
        print("\n" + "=" * 50)
        if has_python_dir and any(f.startswith('python/pandas/') for f in all_files):
            print("✅ ESTRUTURA CORRETA!")
            print("   O ZIP está pronto para uso.")
        else:
            print("❌ ESTRUTURA INCORRETA!")
            print("   Você precisa recriar o ZIP.")
            print("\n💡 ESTRUTURA ESPERADA:")
            print("   lambda_layer.zip")
            print("   └── python/")
            print("       ├── pandas/")
            print("       ├── s3fs/")
            print("       ├── numpy/")
            print("       └── ... (outros pacotes)")

if __name__ == "__main__":
    inspect_layer_zip()