# Monitor Subastas Valencia

Monitor automatizado de subastas oficiales en España, enfocado en la provincia de Valencia/València.
Agrega, normaliza y analiza datos de múltiples fuentes públicas para detectar oportunidades de inversión
sin recopilar datos personales.

------------------------------------------------------------
OBJETIVO
------------------------------------------------------------

Construir un sistema que:
- Agregue subastas oficiales (BOE, TGSS, etc.)
- Filtre por Valencia/València
- Excluya vehículos
- Evite datos personales
- Detecte oportunidades mediante scoring
- Genere reportes semanales

------------------------------------------------------------
FUENTES DE DATOS
------------------------------------------------------------

- Portal de Subastas del BOE (principal)
- Seguridad Social (TGSS)
- API / Sumario del BOE
- Fuentes públicas adicionales (Generalitat Valenciana, patrimonio público)

------------------------------------------------------------
FUNCIONALIDADES
------------------------------------------------------------

- Ingesta automática por fuente
- Normalización de datos
- Eliminación de duplicados
- Filtro de privacidad (sin datos personales)
- Sistema de scoring de oportunidades
- Exportación a CSV / JSON
- Reportes semanales

------------------------------------------------------------
CRITERIOS DE FILTRADO
------------------------------------------------------------

Incluye:
- Inmuebles
- Otros bienes muebles (no vehículos)

Excluye:
- Vehículos
- Datos personales (nombres, DNI, etc.)

------------------------------------------------------------
ARQUITECTURA
------------------------------------------------------------

monitor-subastas-valencia/
  src/monitor/
    sources/
    normalize.py
    dedupe.py
    scoring.py
    storage.py
    exports.py
    main.py

------------------------------------------------------------
ESQUEMA DE DATOS (SIMPLIFICADO)
------------------------------------------------------------

Campos principales:
- source
- external_id
- title
- province
- municipality
- asset_class
- asset_subclass
- is_vehicle
- official_status
- publication_date
- opening_date
- closing_date
- appraisal_value
- starting_bid
- current_bid
- deposit
- occupancy_status
- encumbrances_summary
- description
- official_url

------------------------------------------------------------
SCORING DE OPORTUNIDADES
------------------------------------------------------------

El sistema prioriza subastas en función de:
- Descuento sobre tasación
- Ausencia de ocupación conocida
- Cargas reducidas
- Baja competencia (pocas pujas)
- Ubicación en Valencia

------------------------------------------------------------
EJECUCIÓN
------------------------------------------------------------

Requisitos:
- Python 3.10+
- pip o poetry

Instalación:
git clone https://github.com/gspott/monitor-subastas-valencia.git
cd monitor-subastas-valencia
pip install -r requirements.txt

Ejecución manual:
python -m monitor.main

Programación semanal (cron):
0 8 * * 1 /usr/bin/python3 /ruta/monitor/main.py

------------------------------------------------------------
SALIDAS
------------------------------------------------------------

- data/new_auctions.csv
- data/changed_auctions.csv
- data/all_active_valencia.csv

------------------------------------------------------------
CONSIDERACIONES LEGALES
------------------------------------------------------------

- Solo se usan datos públicos accesibles
- No se almacenan datos personales
- Se aplica el principio de minimización
- Uso orientado a análisis interno

------------------------------------------------------------
TESTING
------------------------------------------------------------

pytest

------------------------------------------------------------
ROADMAP
------------------------------------------------------------

- [ ] Integración completa BOE
- [ ] Integración TGSS
- [ ] Sistema de alertas
- [ ] Dashboard web
- [ ] Mejora del scoring

------------------------------------------------------------
CONTRIBUCIÓN
------------------------------------------------------------

Pull requests bienvenidas.
El objetivo es construir una herramienta robusta, legal y útil para análisis de subastas.

------------------------------------------------------------
LICENCIA
------------------------------------------------------------

MIT
