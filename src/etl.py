"""
Módulo ETL (Extract, Transform, Load) para el sistema de análisis del Taller de Imprenta UBA

Autor: Martin Yanik
Fecha: 2024
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Clase principal para el procesamiento de datos del taller de imprenta
    """
    
    def __init__(self, data_path='data/'):
        """
        Inicializar el procesador de datos
        
        Args:
            data_path (str): Ruta a la carpeta de datos
        """
        self.data_path = data_path
        self.raw_path = os.path.join(data_path, 'raw')
        self.processed_path = os.path.join(data_path, 'processed')
        
        # Crear directorios si no existen
        self._create_directories()
    
    def _create_directories(self):
        """Crear estructura de directorios necesaria"""
        directories = [self.raw_path, self.processed_path]
        
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)
                logger.info(f"Directorio creado: {directory}")
    
    def extract_data(self, file_path=None):
        """
        Extraer datos desde archivos fuente
        
        Args:
            file_path (str, optional): Ruta al archivo de datos
            
        Returns:
            DataFrame: Datos extraídos
        """
        try:
            if file_path and os.path.exists(file_path):
                # Cargar desde archivo específico
                df = pd.read_csv(file_path)
                logger.info(f"Datos cargados desde: {file_path}")
            else:
                # Cargar datos de ejemplo (simulación)
                df = self._load_sample_data()
                logger.info("Datos de ejemplo cargados")
            
            logger.info(f"📊 Datos extraídos: {df.shape[0]} filas, {df.shape[1]} columnas")
            return df
            
        except Exception as e:
            logger.error(f"Error extrayendo datos: {e}")
            raise
    
    def _load_sample_data(self):
        """Cargar datos de ejemplo para desarrollo"""
        # Simulación de datos de producción
        np.random.seed(42)
        n_samples = 1000
        
        data = {
            'id_trabajo': [f'TRB-{i:05d}' for i in range(1, n_samples + 1)],
            'fecha_solicitud': pd.date_range(start='2023-01-01', periods=n_samples, freq='H'),
            'departamento': np.random.choice(
                ['Filosofía', 'Letras', 'Historia', 'Antropología', 'Biblioteca', 'Administración'],
                n_samples
            ),
            'tipo_trabajo': np.random.choice(
                ['Libro', 'Cuadernillo', 'Folleto', 'Tesis', 'Certificado', 'Diploma'],
                n_samples
            ),
            'cantidad_paginas': np.random.randint(1, 500, n_samples),
            'cantidad_copias': np.random.randint(1, 200, n_samples),
            'prioridad': np.random.choice(['Alta', 'Media', 'Baja'], n_samples, p=[0.2, 0.5, 0.3]),
            'estado': np.random.choice(['Pendiente', 'En Proceso', 'Completado', 'Entregado'], n_samples),
            'tiempo_produccion_horas': np.random.exponential(10, n_samples),
            'costo_estimado': np.random.uniform(50, 5000, n_samples),
            'material_utilizado': np.random.choice(
                ['Papel A4', 'Papel A3', 'Cartulina', 'Pergamino', 'Fotográfico'],
                n_samples
            )
        }
        
        return pd.DataFrame(data)
    
    def transform_data(self, df):
        """
        Transformar y limpiar los datos
        
        Args:
            df (DataFrame): Datos a transformar
            
        Returns:
            DataFrame: Datos transformados
        """
        logger.info("Iniciando transformación de datos...")
        
        # Crear copia para no modificar el original
        df_transformed = df.copy()
        
        # 1. Manejo de valores nulos
        numeric_cols = df_transformed.select_dtypes(include=[np.number]).columns
        categorical_cols = df_transformed.select_dtypes(include=['object']).columns
        
        for col in numeric_cols:
            if df_transformed[col].isnull().any():
                df_transformed[col] = df_transformed[col].fillna(df_transformed[col].median())
        
        for col in categorical_cols:
            if df_transformed[col].isnull().any():
                df_transformed[col] = df_transformed[col].fillna('Desconocido')
        
        # 2. Crear nuevas características (feature engineering)
        df_transformed['fecha_solicitud'] = pd.to_datetime(df_transformed['fecha_solicitud'])
        df_transformed['mes'] = df_transformed['fecha_solicitud'].dt.month
        df_transformed['dia_semana'] = df_transformed['fecha_solicitud'].dt.day_name()
        df_transformed['hora'] = df_transformed['fecha_solicitud'].dt.hour
        
        # 3. Calcular costo por página
        df_transformed['costo_por_pagina'] = df_transformed['costo_estimado'] / df_transformed['cantidad_paginas']
        df_transformed['costo_por_pagina'] = df_transformed['costo_por_pagina'].replace([np.inf, -np.inf], 0)
        
        # 4. Categorizar trabajos por tamaño
        df_transformed['categoria_tamano'] = pd.cut(
            df_transformed['cantidad_paginas'],
            bins=[0, 50, 200, 500],
            labels=['Pequeño', 'Mediano', 'Grande']
        )
        
        logger.info(f"✅ Datos transformados: {df_transformed.shape[0]} filas, {df_transformed.shape[1]} columnas")
        
        return df_transformed
    
    def load_data(self, df, filename='datos_procesados.csv'):
        """
        Guardar datos procesados
        
        Args:
            df (DataFrame): Datos a guardar
            filename (str): Nombre del archivo de salida
        """
        try:
            output_path = os.path.join(self.processed_path, filename)
            df.to_csv(output_path, index=False)
            logger.info(f"💾 Datos guardados en: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Error guardando datos: {e}")
            raise
    
    def run_pipeline(self, input_file=None, output_file='datos_procesados.csv'):
        """
        Ejecutar el pipeline ETL completo
        
        Args:
            input_file (str, optional): Archivo de entrada
            output_file (str): Archivo de salida
            
        Returns:
            DataFrame: Datos procesados
        """
        logger.info("🚀 Iniciando pipeline ETL...")
        
        # Extraer
        df_raw = self.extract_data(input_file)
        
        # Transformar
        df_processed = self.transform_data(df_raw)
        
        # Cargar
        self.load_data(df_processed, output_file)
        
        logger.info("✅ Pipeline ETL completado exitosamente")
        return df_processed

# Función principal para ejecución directa
def main():
    """Función principal para ejecutar el ETL"""
    print("=" * 60)
    print("SISTEMA ETL - TALLER DE IMPRENTA UBA")
    print("=" * 60)
    
    # Inicializar procesador
    processor = DataProcessor()
    
    # Ejecutar pipeline
    try:
        df_result = processor.run_pipeline()
        
        # Mostrar resumen
        print("\n📊 RESUMEN DEL PROCESAMIENTO:")
        print(f"• Total de trabajos procesados: {len(df_result):,}")
        print(f"• Período cubierto: {df_result['fecha_solicitud'].min().date()} a {df_result['fecha_solicitud'].max().date()}")
        print(f"• Departamentos atendidos: {df_result['departamento'].nunique()}")
        print(f"• Tipos de trabajo: {df_result['tipo_trabajo'].unique()}")
        print(f"• Archivo generado: data/processed/datos_procesados.csv")
        
        print("\n✅ Proceso completado exitosamente")
        
    except Exception as e:
        print(f"❌ Error durante el procesamiento: {e}")

if __name__ == "__main__":
    main()
