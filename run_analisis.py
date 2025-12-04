# run_analysis.py
import subprocess
import sys
import os

def main():
    print("🚀 Iniciando sistema de análisis predictivo")
    print("=" * 60)
    
    # 1. Crear datos de prueba si no existen
    if not os.path.exists('data/raw/ventas_historico.csv'):
        print("📝 Creando datos de prueba...")
        subprocess.run([sys.executable, 'crear_datos_prueba.py'])
    else:
        print("✅ Datos ya existentes detectados")
    
    # 2. Ejecutar análisis exploratorio
    print("\n🔍 Ejecutando análisis exploratorio...")
    try:
        # Importar y ejecutar el análisis
        sys.path.append('.')
        from notebooks.01_EDA_ventas import ejecutar_analisis_completo
        df = ejecutar_analisis_completo('data/raw/ventas_historico.csv')
        
        print("\n" + "=" * 60)
        print("🎉 Análisis completado exitosamente!")
        print("=" * 60)
        
        # Mostrar resumen
        print("\n📋 RESUMEN EJECUTIVO:")
        print(f"   • Período analizado: {df['fecha'].min()} a {df['fecha'].max()}")
        print(f"   • Títulos analizados: {df['titulo'].nunique()}")
        print(f"   • Ventas totales: {df['cantidad'].sum():,} unidades")
        print(f"   • Ingresos totales: ${df['total'].sum():,.2f}")
        
    except Exception as e:
        print(f"❌ Error en el análisis: {e}")
        return False
    
    return True

if __name__ == "__main__":
    main()
