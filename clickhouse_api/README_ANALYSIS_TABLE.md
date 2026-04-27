# Infraestructura de Analítica de Alto Rendimiento (OpenAlex)

Este documento describe la arquitectura implementada para la gestión y consulta eficiente de datos bibliométricos masivos provenientes de OpenAlex en ClickHouse.

## 1. Arquitectura de Datos

Para optimizar el rendimiento sin perder la flexibilidad de los datos originales, se utiliza un modelo de dos capas:

### Capa de Aterrizaje (Landing Zone)
- **Tabla:** `works` (Motor: `ReplacingMergeTree`)
- **Propósito:** Almacena el snapshot crudo en formato JSON (`raw_data`). Es la "fuente de verdad" que permite actualizaciones incrementales y preserva la integridad de los datos para futuros usos.

### Capa de Analítica (Flattened Layer)
- **Tabla:** `works_flat` (Motor: `ReplacingMergeTree`)
- **Propósito:** Almacena los campos extraídos y tipados de forma nativa (columnar). Está optimizada para consultas multi-dimensionales, filtrado por facetas y agregaciones rápidas.

---

## 2. Automatización mediante Materialized View

La sincronización entre ambas capas es automática. Se ha implementado una **Materialized View (`works_flat_mv`)** que actúa como un disparador (trigger) a nivel de base de datos:

1. El proceso de ingesta inserta el JSON en la tabla `works`.
2. La vista materializada intercepta la inserción, ejecuta la lógica de extracción (`JSONExtract`) y proyecta los datos en `works_flat`.
3. El proceso es transparente para el cargador original en Python.

---

## 3. Estructura de la Tabla de Analítica (`works_flat`)

La tabla está diseñada bajo los siguientes principios:

- **Tipado Nativo:** Uso de `UInt16`, `Float32` y `Date` para minimizar el uso de memoria y maximizar la velocidad de cálculo.
- **LowCardinality:** Optimización de columnas con pocos valores repetitivos (lenguajes, tipos de documentos, países) para reducir el espacio en disco.
- **Manejo de Listas (Arrays):** Los autores, instituciones y tipos de instituciones se almacenan en columnas de tipo `Array(String)`. Esto permite filtrar por pertenencia (ej. `has(country_codes, 'MX')`) de forma extremadamente rápida.
- **Índices de Salto (Skip Indexes):** Se utilizan **Bloom Filters** en las columnas de mayor filtrado (países, tópicos e instituciones) para permitir que el motor descarte bloques de datos irrelevantes sin leerlos del disco.

---

## 4. Estrategia de Actualización y Deduplicación

Ambas tablas utilizan el motor `ReplacingMergeTree`. Esto garantiza que, en un flujo de datos incremental:
- Si se inserta una nueva versión de un trabajo con el mismo `id`, ClickHouse mantendrá la versión más reciente durante el proceso de fusión (merge).
- La consistencia se mantiene tanto en la tabla de JSON crudo como en la tabla plana de forma sincronizada.

---

## 5. Beneficios Implementados
- **Rendimiento:** Reducción del uso de CPU en un ~90% al eliminar el parsing de JSON en tiempo de consulta.
- **Espacio:** Alta tasa de compresión gracias al almacenamiento columnar nativo.
- **Escalabilidad:** Permite realizar filtros cruzados profundos (ej. Institución + Tópico + Año + Estado OA) en sub-segundos sobre millones de registros.
