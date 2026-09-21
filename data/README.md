# Datos

La fuente utilizada en la prueba contiene información de reclamaciones y una variable objetivo de fraude. El archivo original se conserva únicamente de forma local en `data/raw/` y está excluido de Git.

## Motivo de la exclusión

La tabla incluye identificadores y atributos operacionales que no deben publicarse en un repositorio. Para reproducir el ejercicio en otro entorno se debe cargar una muestra anonimizada con el mismo contrato de datos.

## Contrato mínimo

El entrenamiento espera identificadores, fechas de vigencia y siniestro, atributos del asegurado, canal, sucursal, regional, causa, diagnóstico, cobertura, reserva inicial y la etiqueta histórica de fraude.

El scoring productivo no debe recibir información posterior a la decisión ni la etiqueta objetivo.
