# Plataforma de Datos YelpConnect — Grupo 06 (Ecodata)
### Programa de Arquitectura de Datos · UTEC · Caso Trust & Safety: detección de reseñas falsas

Implementación en Databricks Free Edition del **Documento Formal de Arquitectura de Datos del Grupo 06 (versión 0.9)**.
Muestra: Philadelphia (14.577 negocios, 400.000 reseñas, 150.000 usuarios).

**Integrantes y rol en la plataforma**

| Integrante | Rol en el equipo | Rol que actúa en Databricks |
|---|---|---|
| Leila Cordova | Arquitecta de datos | Data Owner |
| David Valentin | Steward de gobierno y calidad | Data Steward (grupo `data_stewards`) y Data Custodian |
| Fiorela Castillon | Ingeniera de datos | Data Custodian |
| Juan Carlos Revolledo | Analista de BI y valor | Consumidor de datos |
| Edgar Suarez | Modelador de datos | Consumidor de datos |

> **Dónde vive cada entregable.** El Capstone tiene dos soportes complementarios:
>
> | Soporte | Qué contiene | Naturaleza |
> |---|---|---|
> | **Documento Formal de Arquitectura de Datos** | Visión y SoAW, arquitectura de referencia (ABB/SBB), modelos conceptual y lógico, estrategia de gobierno y matriz RACI, reglas en lenguaje de negocio, requerimientos de información, modelo operativo. Anexos: Glosario (A), Diccionario (B), diagramas (C), actas (D) y Libro de Gobierno (E) | Diseño, definiciones y decisiones |
> | **Este repositorio (Databricks)** | Modelo físico, glosario y diccionario operativos, golden record, motor de calidad, controles técnicos, pipeline, job, consultas del tablero y bonus de IA | Implementación y evidencia ejecutable |
>
> Regla de oro: **el documento define QUÉ y POR QUÉ; la plataforma demuestra QUE SE CUMPLE.**
> Cada notebook declara en su cabecera qué sección del documento implementa (bloque **Trazabilidad**), y la
> sección 9 del documento vincula cada sección con su notebook y sus objetos en el catálogo.

---

## 1. Notebooks del Grupo 06

Los notebooks del grupo terminan en **`_grupo_06`**. Los que no tienen sufijo son el esqueleto original del curso y se
conservan solo como referencia.

| Carpeta | Notebook | Sección del documento | Qué deja en el catálogo |
|---|---|---|---|
| 00_setup | `00_configurar_lakehouse_grupo_06` | 2.6, 3.2 | Schemas, volumes `raw` y `artefactos`, tablas de control |
| 00_setup | `01_verificar_datos_raw_grupo_06` | 2.4, 4.2, 6.2 | Verificación de los 5 archivos y tamaño de la muestra en crudo |
| E3 | `01_modelo_fisico_ddl_grupo_06` | 4.2–4.4, ADR-04/05/06 | DDL de silver (5 tablas) y gold (7 tablas + 1 vista), con COMMENT, PK/FK, CHECK en gold y retención de 30 días |
| E4 | `01_glosario_negocio_grupo_06` | Anexo A | `yelp_gov.glosario_negocio` (29 términos) |
| E4 | `02_diccionario_datos_grupo_06` | Anexo B, 5.3 | `yelp_gov.diccionario_datos` (79 elementos) y tags `clasificacion` en silver y gold |
| E4 | `03_golden_record_negocio_grupo_06` | 4.5, Acta 08 | `yelp_gov.mdm_negocio_correspondencia` y `yelp_gov.mdm_negocio_golden` (14.559 negocios, 18 grupos) |
| E5 | `01_reglas_calidad_grupo_06` | 6, 5.5 | `dq_reglas` (19 reglas), `dq_resultados`, `dq_excepciones`, `dq_parametros_restringidos`, `vw_dq_reglas_publica` |
| E6 | `01_controles_unity_catalog_grupo_06` | 5 | Permisos por rol y al grupo `data_stewards`, máscaras de datos personales, tags en `yelp_gov`, `matriz_raci` |
| E7 | `01_bronze_ingesta_grupo_06` | 3.2, 5.4, ADR-04 | Tablas `brz_*`, Auto Loader para reseñas, lista de supresión `yelp_gov.solicitudes_supresion` |
| E7 | `02_silver_limpieza_grupo_06` | 3.2, 6.2 | Tablas `slv_*`, cuarentena `yelp_gov.cuarentena_*` y gate de calidad (ejecuta E5) |
| E7 | `03_gold_productos_datos_grupo_06` | 4.3, 6.4, 7 | Tablas `gld_*`, señales de fraude y VACUUM semanal |
| E7 | `04_job_orquestacion_grupo_06` | 8 | Lakeflow Job diario, `yelp_gov.pipeline_bitacora`, falla controlada y estimación de costo |
| E8 | `01_consultas_base_dashboard_grupo_06` | 7 | Consultas de los dos tableros y del Genie Space |
| EX | `01_analisis_ia_resenas_grupo_06` | 6.5 | `yelp_gov.ex_resenas_enriquecidas` (bonus) |

Las capturas de evidencia están en `img/`. Los diagramas editables (Anexo C) están en `doc/`.

## 2. Setup inicial del grupo (semana 0)

1. **Un integrante crea la cuenta del grupo** en [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
   con el correo del grupo. Free Edition permite un solo workspace, así que la cuenta es del equipo.
2. Invitar a los 5 integrantes (`Settings → Identity and access → Users`) y crear el grupo **`data_stewards`**
   (`Settings → Identity and access → Groups`) con la cuenta de David.
3. Conectar este repositorio con `Workspace → Create → Git folder`.
4. Ejecutar `00_setup/00_configurar_lakehouse_grupo_06`: crea schemas, volumes y tablas de control (es idempotente).
5. Preparar la muestra con `datos/preparar_muestra_yelp.py` (**en una laptop**, no en Databricks) y subir los archivos
   a `/Volumes/workspace/yelp_bronze/raw/`.
6. Ejecutar `00_setup/01_verificar_datos_raw_grupo_06`.

## 3. Convenciones del Lakehouse (obligatorias)

| Elemento | Convención |
|---|---|
| Catálogo | `workspace` (el default de Free Edition) |
| Schemas | `yelp_bronze`, `yelp_silver`, `yelp_gold`, `yelp_gov` |
| Volume de datos crudos | `/Volumes/workspace/yelp_bronze/raw/` |
| Volume de artefactos de gobierno | `/Volumes/workspace/yelp_gov/artefactos/` |
| Tablas | `brz_<entidad>`, `slv_<entidad>`, `gld_<producto_de_datos>` |
| Vistas | `vw_<nombre>` |
| Reglas de calidad | `DQ-nnn`, correlativas y únicas |
| Idioma de comentarios y metadata | Español |
| Tags de clasificación | `publico`, `interno`, `confidencial`, `restringido` (minúsculas y sin tilde) |
| Ramas Git | `main` protegida; trabajar en `feature/E<k>-<tema>` y fusionar por Pull Request revisado por otro integrante |

## 4. Orden de ejecución

### Primera vez (en este orden)

```
00_setup/00_configurar_lakehouse  →  00_setup/01_verificar_datos_raw
E7/01_bronze_ingesta              (carga los 5 archivos; reseñas con Auto Loader; aplica la lista de supresión)
E3/01_modelo_fisico_ddl           (crea silver y gold vacías, con CHECK y retención de 30 días)
[paso manual] parámetro de DQ-016 (ver abajo; antes de E7/02, porque su gate ejecuta E5)
E7/02_silver_limpieza             (llena silver y cuarentena; su gate ejecuta E5)
E7/03_gold_productos_datos        (llena gold y calcula las señales de fraude)
E4/01_glosario  →  E4/02_diccionario  (requiere subir los Excel _Grupo06 a /Volumes/workspace/yelp_gov/artefactos/)
E4/03_golden_record_negocio       (lee bronze; lo ejecuta un custodian)
E6/01_controles_unity_catalog     (requiere el grupo data_stewards y los correos en los widgets)
E4/02_diccionario                 (otra vez: valida las reglas citadas y etiqueta gold)
E7/04_job_orquestacion            (widget crear_job = si; después volver a no)
```

**Paso manual: parámetro Restringido de DQ-016.** El valor nunca se escribe en el repositorio (sección 5.3, ADR-07).
Lo registra **una sola vez** un custodian o el steward desde el **SQL editor**, con el valor aprobado en el Acta 04:

```sql
CREATE TABLE IF NOT EXISTS workspace.yelp_gov.dq_parametros_restringidos (
  regla_id STRING COMMENT 'Regla DQ-nnn a la que pertenece el parámetro',
  parametro STRING COMMENT 'Nombre del parámetro',
  valor INT COMMENT 'Valor vigente (Restringido)',
  aprobado_por STRING COMMENT 'Data Owner que aprobó el valor',
  fecha DATE COMMENT 'Fecha de aprobación')
COMMENT 'Parámetros Restringidos de las señales antifraude (sección 5.3, Actas 02 y 04). Solo steward y custodians';
INSERT INTO workspace.yelp_gov.dq_parametros_restringidos
VALUES ('DQ-016', 'max_resenas_dia', <valor aprobado>,
        'Andrew Collins (Data Owner) a propuesta de Rafael Nakamura', current_date());
```

E5 se detiene si el parámetro falta o está duplicado. Fusionar ramas no afecta este dato: vive en la tabla, no en el código.

### Operación diaria

El Lakeflow Job corre cada día a las **22:00 (hora de Lima)** con tres tareas dependientes, un reintento y un tiempo
máximo por tarea, y avisa por correo si algo falla:

```
bronze (01, aplica la supresión) → silver (02, cuarentena + gate que ejecuta E5) → gold (03, VACUUM los domingos)
```

El job ejecuta los notebooks de la rama activa del Git folder: después de fusionar un Pull Request, volver el Git folder a `main`.

E8 se arma en la interfaz (Dashboards y Genie) con las consultas del notebook. EX es opcional y no forma parte del job.

### Demostraciones para la sustentación

| Demostración | Cómo |
|---|---|
| Idempotencia de Auto Loader (M7) | Ver el experimento documentado en E7/01 (`img/M7_Idempotencia_autoloader.jpg`) |
| Falla controlada y reintento (M10) | En E7/04, widget 7 = `si` para crear el archivo de control; luego *Run now with different parameters* con `simular_falla = si`. El primer intento de bronze falla y el reintento la supera |
| Supresión de datos personales (5.4) | Insertar un `user_id` en `yelp_gov.solicitudes_supresion` y correr el job: la cuenta, sus reseñas y sus tips desaparecen de silver, cuarentena y gold |
| Permisos y máscaras (CE-07) | Consultar con la cuenta de cada rol: Leila ve datos enmascarados y no accede a la gold restringida; David los ve en claro |

## 5. Artefactos institucionales de gobierno (E4)

El glosario y el diccionario se completan en las plantillas oficiales del bloque de Gobierno
(`E4_diccionario_glosario/plantillas/`) y se cargan tal cual: las tablas Delta tienen exactamente las mismas columnas
que las plantillas (12 y 25).

| Archivo (subir a `/Volumes/workspace/yelp_gov/artefactos/` sin cambiar el nombre) | Se carga en | Anexo |
|---|---|---|
| `Glosario_de_terminos_de_negocio_YelpConnect_Grupo06.xlsx` | `yelp_gov.glosario_negocio` | A |
| `Diccionario_de_Datos_YelpConnect_Grupo06.xlsx` | `yelp_gov.diccionario_datos` | B |

Los validadores de E4/02 exigen que el diccionario y el catálogo coincidan **en ambos sentidos** (100 % de las columnas
de silver y gold, sin entradas sobrantes), que toda regla citada como `DQ-nnn` exista en `dq_reglas` y que la columna
*Obligatorio* coincida con el NOT NULL del DDL. El Libro de Gobierno de Datos (Anexo E) se anexa al documento y no se
carga en la plataforma; no transcribe el valor de DQ-016.

## 6. Reglas de juego en Free Edition

- Solo hay **cómputo serverless** con **cuota diaria**: se trabaja con la muestra de una ciudad y se agrega antes de
  visualizar. El tablero y Genie leen solo gold.
- Todo es **reproducible**: si se clona el repositorio y se ejecutan los notebooks en el orden de la sección 4, se obtiene
  el mismo resultado (salvo el parámetro de DQ-016, que se registra a mano por diseño).

## 7. Definition of Done global

Un entregable de plataforma está terminado cuando:

1. El notebook corre de inicio a fin sin errores y sus validadores pasan.
2. Las tablas y objetos existen en el schema correcto, con comentarios y tags.
3. La cabecera declara qué sección del Documento Formal implementa y no la contradice; si la implementación obligó a
   cambiar el diseño, el documento se actualiza y la decisión queda registrada (ADR o acta).
4. El markdown del notebook documenta decisiones y supuestos.
5. Está en `main` mediante un Pull Request revisado por al menos otro integrante.

## 8. Decisiones de implementación

| Decisión | Motivo | Dónde consta en el documento |
|---|---|---|
| La cuarentena vive en `yelp_gov.cuarentena_<entidad>`, no en silver | No es un producto de datos sino un registro de incidencias para el steward. Guarda datos personales sin máscara: lleva tag de clasificación, retención de 30 días y acceso solo del steward y los custodians | 5.3, sección 9 |
| La supresión se aplica en bronze en cada carga | Silver, cuarentena y gold se reconstruyen desde bronze: el borrado llega solo a todas las capas. Se borran la cuenta, sus reseñas y sus tips | 5.4, Acta 07 |
| Retención de 30 días en todas las capas, sin Change Data Feed en silver y con VACUUM semanal | Una supresión desaparece también de las versiones anteriores | ADR-04 |
| El parámetro de DQ-016 está en `dq_parametros_restringidos` | El umbral es Restringido: no aparece en el código, en `dq_reglas` ni en los anexos | 5.3, 6.2, ADR-07 |
| `dq_reglas` solo se reescribe si cambia una regla | `version_reglas` identifica de verdad una versión del catálogo (CE-06) | Sección 9 |
| El motor de calidad compara sin redondear | Un caso aislado no desaparece en una regla al 100 % | 6.3 |
| `try_cast` en las conversiones | Con ANSI activo, un valor mal formado detendría el pipeline; con `try_cast` el registro va a cuarentena | Sección 9 |
| Silver y gold se llenan con `insertInto` sobre el DDL de E3 | Respeta tipos, COMMENT, constraints y máscaras; las tablas no se recrean en cada corrida | Sección 9 |
| CHECK en cuatro tablas gold; sobre `stars` se mantiene DQ-001 + cuarentena | Calidad preventiva en gold y detectiva en silver | 6.3, Anexo C |
| Golden record en `yelp_gov` con la regla M5 | El uso del `golden_id` en gold está aprobado para la siguiente versión; M5 excluye las direcciones compartidas del aeropuerto | 4.5, Acta 08 |
| Permisos de gold tabla por tabla y al grupo `data_stewards` | `gld_salud_resenas` y `gld_dim_usuario_riesgo` son Restringidas (solo Trust & Safety) | 5.1, 5.3, CE-07 |
| Dos tableros: «Integridad y calidad de datos» (páginas Negocio y Gobierno, con credenciales embebidas) y «Alertas de Trust & Safety» (solo `data_stewards`, sin credenciales embebidas) | AI/BI no permite ocultar una página a algunos lectores. La página Gobierno lee `yelp_gov`, a la que los consumidores no tienen acceso directo; el tablero de alertas usa los permisos de cada lector | 7.2 |
| Falla controlada con el parámetro `simular_falla` | Demuestra el reintento sin tocar las reglas | 8.1 |
| El bonus de IA guarda su resultado en `yelp_gov` | Es un experimento no aprobado: tag Restringido, COMMENT por columna y supresión aplicada, pero fuera del Diccionario y de la gold de consumo | 6.5 |

El detalle completo está en la sección 9 del Documento Formal.
