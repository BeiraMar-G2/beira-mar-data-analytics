#!/usr/bin/env python3
"""
Script para criar Lambda Layer compatível com AWS Lambda Python 3.9
Cria a layer com pandas, s3fs, boto3 e dependências
"""

import subprocess
import shutil
import zipfile
from pathlib import Path
import sys

def criar_lambda_layer():
    """Cria uma Lambda Layer compatível com AWS Lambda"""
    
    print("🚀 Iniciando criação da Lambda Layer...")
    print("=" * 60)
    
    # Configurações
    layer_dir = Path("layer_build")
    python_dir = layer_dir / "python"
    zip_name = "lambda_layer.zip"
    
    # Limpar diretórios anteriores
    if layer_dir.exists():
        print(f"🧹 Limpando diretório anterior: {layer_dir}")
        shutil.rmtree(layer_dir)
    
    if Path(zip_name).exists():
        print(f"🧹 Removendo ZIP anterior: {zip_name}")
        Path(zip_name).unlink()
    
    # Criar estrutura de diretórios
    print(f"\n📁 Criando estrutura de diretórios...")
    python_dir.mkdir(parents=True, exist_ok=True)
    
    # Pacotes necessários
    packages = [
        "pandas==2.0.3",      # Versão compatível com Python 3.9
        "s3fs==2023.6.0",     # Compatível com boto3
        "numpy==1.24.3",      # Requerido pelo pandas
        "pyarrow==12.0.1",    # Para melhor performance com pandas
        "fsspec==2023.6.0",   # Requerido pelo s3fs
        "aiobotocore==2.5.2", # Requerido pelo s3fs
        "aiohttp==3.8.5",     # Requerido pelo aiobotocore
    ]
    
    print(f"\n📦 Instalando pacotes...")
    print(f"   Destino: {python_dir.absolute()}")
    print(f"   Python: {sys.version}")
    
    # Instalar cada pacote
    for i, package in enumerate(packages, 1):
        print(f"\n[{i}/{len(packages)}] Instalando {package}...")
        
        cmd = [
            sys.executable, "-m", "pip", "install",
            package,
            "--target", str(python_dir),
            "--platform", "manylinux2014_x86_64",
            "--python-version", "3.9",
            "--only-binary=:all:",
            "--no-deps"  # Instalar dependências separadamente
        ]
        
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )
            print(f"   ✅ {package} instalado")
        except subprocess.CalledProcessError as e:
            print(f"   ⚠️  Erro ao instalar {package}, tentando sem restrições...")
            # Tentar sem --only-binary
            cmd_fallback = [
                sys.executable, "-m", "pip", "install",
                package,
                "--target", str(python_dir),
                "--upgrade"
            ]
            subprocess.run(cmd_fallback, check=True)
    
    # Instalar dependências faltantes
    print(f"\n📦 Instalando dependências adicionais...")
    deps_cmd = [
        sys.executable, "-m", "pip", "install",
        "--target", str(python_dir),
        "python-dateutil",
        "pytz",
        "tzdata",
    ]
    subprocess.run(deps_cmd, check=True)
    
    # Limpar arquivos desnecessários para reduzir tamanho
    print(f"\n🧹 Removendo arquivos desnecessários...")
    
    patterns_to_remove = [
        "**/__pycache__",
        "**/*.pyc",
        "**/*.pyo",
        "**/*.dist-info",
        "**/*.egg-info",
        "**/tests",
        "**/test",
        "**/*.so.bak",
    ]
    
    removed_count = 0
    for pattern in patterns_to_remove:
        for item in python_dir.glob(pattern):
            if item.is_file():
                item.unlink()
                removed_count += 1
            elif item.is_dir():
                shutil.rmtree(item)
                removed_count += 1
    
    print(f"   Removidos {removed_count} arquivos/pastas")
    
    # Verificar estrutura
    print(f"\n🔍 Verificando estrutura...")
    critical_packages = ['pandas', 's3fs', 'numpy', 'fsspec']
    all_good = True
    
    for pkg in critical_packages:
        pkg_path = python_dir / pkg
        if pkg_path.exists():
            print(f"   ✅ {pkg}/")
        else:
            print(f"   ❌ {pkg}/ NÃO ENCONTRADO!")
            all_good = False
    
    if not all_good:
        print("\n❌ ERRO: Estrutura incompleta!")
        return False
    
    # Criar ZIP
    print(f"\n📦 Criando arquivo ZIP: {zip_name}")
    
    with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file_path in python_dir.rglob('*'):
            if file_path.is_file():
                arcname = file_path.relative_to(layer_dir)
                zipf.write(file_path, arcname)
    
    # Estatísticas finais
    zip_size = Path(zip_name).stat().st_size / (1024 * 1024)
    print(f"\n✅ Layer criada com sucesso!")
    print(f"   Arquivo: {zip_name}")
    print(f"   Tamanho: {zip_size:.2f} MB")
    
    if zip_size > 250:
        print(f"\n⚠️  ATENÇÃO: Tamanho da layer ({zip_size:.2f} MB) pode exceder")
        print(f"   o limite da AWS Lambda (250 MB descompactado)")
    
    # Limpar diretório temporário
    print(f"\n🧹 Limpando diretório temporário...")
    shutil.rmtree(layer_dir)
    
    print("\n" + "=" * 60)
    print("✅ PROCESSO CONCLUÍDO!")
    print("\n📋 Próximos passos:")
    print("   1. Execute: python inspect_layer.py")
    print("   2. Se OK, execute: terraform apply")
    
    return True

if __name__ == "__main__":
    try:
        sucesso = criar_lambda_layer()
        sys.exit(0 if sucesso else 1)
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)