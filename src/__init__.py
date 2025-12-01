"""
Paquete para procesamiento y análisis de datos del Taller de Imprenta UBA

Este paquete contiene módulos para:
- ETL (Extracción, Transformación y Carga)
- Análisis de datos de producción
- Visualización de métricas
- Generación de reportes

Módulos disponibles:
- etl: Pipeline completo de procesamiento de datos
- database: Funciones para manejo de bases de datos
- analysis: Análisis estadístico y KPIs
- visualization: Generación de gráficos y dashboards

Autor: Martin Yanik
Fecha: 2024
"""

__version__ = "1.0.0"
__author__ = "Martin Yanik"
__email__ = "martin.yanik@email.com"

# Importar módulos principales para facilitar el acceso
from .etl import DataProcessor

__all__ = [
    'DataProcessor'
]

print(f"📦 Paquete de análisis de datos UBA v{__version__} cargado")
