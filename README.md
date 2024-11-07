# TFM - Análisis, propuesta e implementación de soluciones de datos en un entorno empresarial real

**Autor:** D. Miguel Ángel del Rincón Manrique  
**Director del proyecto:** D. Óscar Cabanillas Núñez  
**Máster en Análisis de Datos Masivos (Big Data)** - Universidad Europea  
**Curso:** 2023-2024

## Descripción del proyecto

Este repositorio contiene el código y la documentación correspondiente al Trabajo de Fin de Máster (TFM) titulado "Análisis, propuesta e implementación de soluciones de datos en un entorno empresarial real", enfocado en **beWanted**, una plataforma digital en el sector de reclutamiento laboral. El objetivo central es mejorar la **arquitectura de datos**, el **algoritmo JobRank** de recomendaciones y el **sistema de inteligencia empresarial** (BI) de la empresa, permitiendo una explotación avanzada de datos y facilitando una toma de decisiones informada y eficiente.

## Resumen

El proyecto comenzó con el objetivo de implementar aprendizaje automático sobre el algoritmo JobRank. Sin embargo, el análisis del origen de los datos reveló la necesidad de una revisión más exhaustiva de la arquitectura de datos, lo que introdujo esta revisión como una nueva línea de trabajo.

Además, se detectó que la empresa carecía de un conocimiento adecuado sobre cómo aprovechar los datos para mejorar JobRank. Esta situación subrayó la necesidad crítica de desarrollar sistemas de inteligencia empresarial que guíen las decisiones de la empresa.

Por esta razón, el proyecto aborda tanto la arquitectura de datos como el algoritmo JobRank, además del desarrollo de un sistema de inteligencia empresarial, estableciendo dentro de él las líneas de trabajo futuras para cada área. Estas iniciativas proporcionan los pilares fundamentales para una estrategia orientada a datos, cubriendo desde el almacenamiento y procesamiento del dato hasta su explotación final, y buscando un futuro basado en la innovación.

## Abstract

This project was initially conceived with the objective of implementing machine learning techniques on the JobRank algorithm. However, an in-depth analysis of data sources exposed the need for a more comprehensive review of the data architecture, thus introducing this examination as an essential new line of work.

Furthermore, it became evident that the company lacked a clear understanding of how to harness its data effectively to enhance JobRank, a candidate recommendation algorithm. This gap underscored the critical importance of developing robust business intelligence systems that could inform and guide strategic decision-making within the organization.

Consequently, this project delves into the data architecture and the JobRank algorithm, alongside the development of an integrated business intelligence framework. These initiatives outline future pathways for the company across each of these domains, aiming to lay the foundational pillars for a successful data-driven strategy and paving the way toward an innovation-centered future.

## Objetivos

### Objetivo general
Trazar las líneas generales de las soluciones técnicas de datos que ofrece el mercado actual para permitir mejorar el almacenamiento, uso y aprovechamiento de los datos de beWanted, teniendo como fin último incrementar la eficiencia y efectividad de la toma de decisiones, así como la mejora en la calidad y experiencia de los usuarios de la plataforma.

### Objetivos específicos
1. Analizar la arquitectura actual de datos de beWanted para identificar sus limitaciones y áreas de mejora.
2. Explorar distintas alternativas y plantear diferentes posibles arquitecturas de datos que permitan el manejo eficiente y escalable de grandes volúmenes de información.
3. Revisar el algoritmo JobRank existente, desde su arquitectura, implementación y la lógica de funcionamiento.
4. Proponer soluciones que mejoren la relevancia y calidad de las recomendaciones del algoritmo JobRank.
5. Diseñar y trazar requisitos generales para un sistema de inteligencia empresarial.
6. Comparar, probar e implementar distintas soluciones de inteligencia empresarial para encontrar la que mejor se adapte a la plataforma.
7. Modelar los datos en sistemas de inteligencia empresarial para facilitar análisis avanzados y generación de informes.
8. Implementar pipelines ETL que permitan el procesamiento de datos internos y la integración de datos internos y externos.
9. Crear un sistema de seguridad basado en roles (RLS) dentro del sistema de inteligencia empresarial.
10. Crear cuadros de mando e informes iniciales sobre los que los equipos de negocio podrán trabajar.
11. Definir las métricas clave para los cuadros de mando.
12. Identificar puntos clave sobre el funcionamiento actual de la plataforma.

## Estructura del repositorio

```plaintext
├── Anexos
│   ├── 1-Setup
│   │   ├── Anexo-I.pdf
│   │   ├── Fix-Dates-DB
│   │   │   └── Fix-Dates-DB.py
│   │   └── IP-Whitelisting-Microsoft-Fabric
│   │       ├── Cloud-Function
│   │       │   ├── main.py
│   │       │   └── requirements.txt
│   │       └── Jupyter
│   │           └── Enable-Microsoft-IPs-whitelist.ipynb
│   ├── 2-BBDD
│   │   ├── Anexo-II.pdf
│   │   ├── BBDD-Analysis-Script.ipynb
│   │   └── BBDD-Analysis-Result.csv
│   ├── 3-Algoritmo-JobRank
│   │   └── Anexo-III.pdf
│   ├── 4-Soluciones-BI-ETL
│   │   └── Anexo-IV.pdf
│   └── 5-Cuadros-Mando
│       └── Anexo-V.pdf
├── LICENSE
├── Memoria-TFM-Miguel-Angel-del-Rincon-Manrique.pdf
└── README.md
```

### Descripción de los anexos

- **1-Setup**: Incluye configuraciones y scripts de preparación, como ajustes de fechas en la base de datos (`Fix-Dates-DB.py`) y un script de whitelisting de IPs para Microsoft Fabric.
- **2-BBDD**: Análisis exhaustivo de la estructura actual de la base de datos de beWanted, con scripts y resultados en formato CSV.
- **3-Algoritmo-JobRank**: Documentación detallada del análisis y las propuestas de mejora para el algoritmo JobRank.
- **4-Soluciones-BI-ETL**: Comparativa de distintas herramientas de BI/ETL y una evaluación para seleccionar la solución más adecuada, resultando en la implementación de Power BI.
- **5-Cuadros-Mando**: Deiseño de cuadros de mando en Power BI para monitorear los KPIs/métricas definidas en la memoria académica para la empresa.

## Licencia

Este proyecto se distribuye bajo la licencia especificada en el archivo LICENSE.