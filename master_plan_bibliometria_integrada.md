# Master Plan: Ecosistema Bibliométrico Integrado (Sinapsis + Topics + Latam)

Este plan coordina la transición hacia una arquitectura unificada en ClickHouse que servirá como motor para el análisis nacional (México/Sinapsis), regional (Latam) y mundial (Topics/Fronts).

## Fase 1: Cimiento Universal (ClickHouse Core)
**Objetivo:** Consolidar ClickHouse como la única "Fuente de Verdad" bibliométrica.

1.  **Esquema `openalex` (Global):**
    *   Mantener y optimizar la tabla `works_flat`.
    *   Crear la tabla `embeddings_cache` con diseño "Wide-Format" (columnas para SPECTER2, FastRP, etc.).
2.  **Migración `revistaslatam`:**
    *   Mapear esquemas de PostgreSQL a ClickHouse.
    *   Implementar `SummingMergeTree` para pre-calcular métricas de revistas en tiempo real.
3.  **Despliegue de Índices Bloom Filter:**
    *   Aplicar `tokenbf_v1` en nombres de autores e instituciones para permitir búsquedas rápidas en el universo global.

## Fase 2: Resolución de Entidades (El "Linker")
**Objetivo:** Identificar con precisión a los actores mexicanos y latinoamericanos.

1.  **Descubrimiento SNII (En proceso):**
    *   Finalizar el pipeline de identidad SNII contra OpenAlex.
    *   Persistir los resultados en una tabla de mapeo `mexico.snii_to_oa`.
2.  **Mapeo Institucional ROR (Prioridad Sinapsis):**
    *   Crear tabla `mexico.institutions_ror` con mapeo de ROR IDs a OpenAlex IDs.
    *   Identificar variantes de nombres de instituciones mexicanas para agrupar producción dispersa.
3.  **Normalización Latam:**
    *   Cruzamiento de ISSNs y Scopus IDs para el proyecto de revistas.

## Fase 3: Proyecciones Especializadas (Sinapsis & Latam)
**Objetivo:** Crear tablas "Rich" filtradas para análisis de alta velocidad.

1.  **Esquema `mexico`:**
    *   `mexico.works`: Tabla física (no solo vista) con todos los campos desglosados de la producción mexicana.
    *   `mexico.authors`: Perfiles consolidados de investigadores en México.
2.  **Esquema `latam`:**
    *   Tabla especializada para indicadores de revistas latinoamericanas.
3.  **Enriquecimiento Vectorial (RTX 4090):**
    *   Poblar masivamente la columna `embedding_specter2` para todo el corpus de México y Latam.

## Fase 4: Productos Analíticos (InCites-like & RAG)
**Objetivo:** Desplegar las capacidades de análisis avanzado.

1.  **Research Fronts (Metodología Topics):**
    *   Ejecutar el Triple Pipeline (Estructural, Semántico, Topológico) sobre el corpus mexicano.
    *   Identificar frentes de investigación emergentes en México.
2.  **Cálculo de Indicadores de Impacto:**
    *   Implementar FWCI (Field Weighted Citation Impact) y percentiles usando la infraestructura de Topics.
3.  **Asistente Inteligente (RAG):**
    *   Sincronizar las tablas de ClickHouse con Qdrant y Neo4j para el asistente de Sinapsis.

---

## Próximos Pasos Inmediatos:
1.  **Fase 1.3**: Aplicar Bloom Filters en ClickHouse global para evitar lentitud.
2.  **Fase 2.2**: Script de descubrimiento ROR -> OpenAlex para instituciones mexicanas.
3.  **Fase 3.1**: Creación del DDL para `mexico.works` con "todos los campos".
