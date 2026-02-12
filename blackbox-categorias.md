## Cómo usar esta clasificación

Cada entrada del BlackBox recibe **tres columnas**, una por dimensión:

- **Dimensión 1 — Tipo de entrada:** ¿Qué es esto? ¿Qué necesita el reportante?
- **Dimensión 2 — Módulo funcional:** ¿En qué parte del producto ocurre?
- **Dimensión 3 — Causa raíz:** ¿Por qué ocurrió?

Las dimensiones son independientes entre sí. No se condicionan mutuamente. Esto permite construir métricas cruzadas a lo largo del tiempo.

---

## Dimensión 1 — Tipo de entrada

Esta dimensión clasifica **qué es** la entrada desde la perspectiva de quién la reporta. La pregunta que responde: "¿Qué necesita esta persona?"

Existen cinco tipos posibles. Cada entrada debe caer en exactamente uno.

---

### Incidencia

**Definición:** Algo en MQPro no funciona como se espera. El sistema produce un resultado incorrecto, no responde, muestra un error, o se comporta de forma distinta a lo que el usuario razonablemente esperaría.

No importa si la causa es un error de código, un dato malo, un caso borde, o un problema de infraestructura. Si el usuario reporta que algo "no funciona", "está malo", "da error", "no carga", "muestra un monto incorrecto", o "hizo algo que no debería", es una Incidencia.

No requiere que haya existido un momento previo en que funcionaba bien. Una funcionalidad nueva que desde su lanzamiento produce resultados incorrectos también es una Incidencia.

**Ejemplos:**

- "MQPro no carga, sale error 504" → Incidencia
- "El cobro muestra $0 cuando debería mostrar $450.000" → Incidencia
- "Creé un contrato y no se generaron los cobros" → Incidencia
- "La renovación automática creó cobros duplicados" → Incidencia
- "El filtro de propiedades no funciona en el celular" → Incidencia
- "El botón de descargar reporte no hace nada" → Incidencia
- "El IPC no se aplicó al contrato" → Incidencia
- "El pago del arrendatario no cambió el estado del cobro a pagado" → Incidencia
- "Cuando creo un anexo, los montos salen en pesos en vez de UF" → Incidencia
- "La liquidación muestra un saldo inicial incorrecto" → Incidencia

**No es Incidencia si:** el sistema funciona correctamente pero el usuario no sabía cómo usarlo (→ Duda), o el usuario quiere que funcione distinto (→ Idea), o requiere intervención manual porque la funcionalidad no existe (→ Soporte operativo).

---

### Duda

**Definición:** El usuario tiene una pregunta sobre cómo funciona algo que ya existe en MQPro. No hay nada roto — el usuario no entiende cómo usar una funcionalidad, no sabe dónde encontrar algo, o no tiene claridad sobre qué permisos tiene.

La respuesta a una Duda es información, no un cambio de código. Si después de explicar, el usuario dice "ah, ok, entiendo", era una Duda. Si después de explicar, el usuario dice "pero eso está mal, debería funcionar distinto", puede derivar en una Idea o revelar una Incidencia.

**Ejemplos:**

- "¿Cómo muevo una reserva de una unidad a otra?" → Duda
- "¿Quién puede anular un contrato? ¿Yo o solo el administrador?" → Duda
- "¿Dónde veo el saldo pendiente de un arrendatario?" → Duda
- "¿El IPC se aplica automáticamente o tengo que hacer algo?" → Duda
- "¿Por qué el co-deudor no puede ver el link de pago?" → Duda (si es comportamiento esperado del sistema)
- "¿Qué significa el estado 'contrato pendiente'?" → Duda
- "No entiendo cómo se calcula la comisión" → Duda

**Señal clave:** Si el equipo de producto responde la pregunta sin tocar código y el tema queda resuelto, era una Duda. Si la Duda se repite con frecuencia desde distintos usuarios, puede motivar una Idea (mejorar la UX, agregar tooltip, crear FAQ).

---

### Idea

**Definición:** El usuario sugiere una funcionalidad nueva, una mejora a algo existente, o un cambio en cómo debería comportarse el sistema. No hay nada roto — el usuario quiere que el producto haga algo que hoy no hace, o que lo haga mejor.

Las Ideas son valiosas como señal de demanda, pero no generan acción inmediata. Entran al backlog para evaluación y priorización.

**Ejemplos:**

- "Estaría bueno poder subir archivos adjuntos al crear un cobro" → Idea
- "Deberían poner un comentario obligatorio al crear un anexo" → Idea
- "Quiero ver una bitácora de todos los cambios del contrato" → Idea
- "¿Se podría descargar el devengo en Excel?" → Idea
- "Sería útil una sección de preguntas frecuentes en MQPro" → Idea
- "Que la multa por defecto sea 1% diario en contratos nuevos" → Idea
- "Reunión quincenal entre Producto y Corretaje" → Idea (de proceso, no de producto, pero se registra igual)

**No es Idea si:** el usuario pide algo que el sistema ya hace pero no sabe cómo (→ Duda), o pide algo que el sistema debería hacer según su diseño pero no lo está haciendo (→ Incidencia).

---

### Soporte operativo

**Definición:** El usuario necesita una intervención manual en datos o en el sistema que no puede realizar por sí mismo desde la interfaz. No es una incidencia del sistema — es una limitación operativa donde se requiere que alguien con acceso al backend, a la base de datos, o a un permiso especial ejecute una acción puntual.

Incluye correcciones de datos históricos, cambios de estado que la interfaz no permite, y operaciones que requieren acceso directo a la base de datos.

**Ejemplos:**

- "Necesito que cambien el estado de esta liquidación de 'aprobado' a 'pendiente'" → Soporte operativo
- "Hay que borrar un cobro duplicado que se creó por error" → Soporte operativo
- "El RUT de este propietario está mal, pero no puedo editarlo" → Soporte operativo
- "Hay dos unidades duplicadas del mismo departamento" → Soporte operativo
- "Necesito que reactiven esta cuenta MQ que se desactivó" → Soporte operativo
- "Hay que rellenar el valor UF de ayer porque el worker no lo trajo" → Soporte operativo

**Señal clave:** Si la resolución requiere que un desarrollador ejecute un script, un query, o una acción en el admin de Django, es Soporte operativo. Si el mismo tipo de soporte se repite frecuentemente, debería motivar una Idea para que la funcionalidad exista en la interfaz.

---

### Aviso

**Definición:** Mensaje informativo que no requiere acción del equipo de producto. Incluye anuncios del equipo de producto hacia operaciones ("mañana hay deploy", "esta funcionalidad ya está disponible"), agradecimientos, y mensajes que no son solicitudes.

**Ejemplos:**

- "Mañana a las 8am hay actualización de MQPro" → Aviso
- "Ya está disponible la nueva vista de contratos" → Aviso
- "Gracias por arreglar el filtro, funciona perfecto" → Aviso
- "Les comento que Fintoc está caído a nivel nacional" → Aviso

**Nota:** Los Avisos se registran en el BlackBox para completitud del log, pero no generan tareas ni análisis de causa raíz. La Dimensión 3 (Causa raíz) no aplica para Avisos.

---

## Dimensión 2 — Módulo funcional

Esta dimensión clasifica **dónde** en el producto ocurre el problema. Cada módulo corresponde a un dominio desacoplado del backend, con sus propias entidades, servicios y tareas programadas.

Existen quince módulos organizados en cinco niveles. La pregunta que responde: "¿En qué parte de MQPro está el problema?"

---

### Nivel 1 — Ciclo de arriendo (núcleo)

Estos cuatro módulos representan el flujo principal del negocio y generan la mayor parte del volumen del BlackBox.

**Contratos.** Todo lo relacionado con el ciclo de vida del contrato de arriendo: creación, activación, renovación automática, anulación, expiración. Incluye condiciones (anexos), parámetros de pago (arriendo, GGCC, garantía, escalonado), garantías, reservas y documentos adjuntos. Entidades backend: `Contract`, `ContractStatus`, `Conditions`, `Parameters`, `Guarantee`, `Document`, `AuditContract`. Tareas programadas: `update_conditions_status`, `prepare_contracts_ending_next_month`, `verificar_ejecucion_preparacion_contratos`, `transcribe_pdf_async`.

**Cobros.** Generación de cargos mensuales, máquina de estados del cobro (NOT_ENABLED → AVAILABLE_FOR_PAYMENT → PARTIALLY_PAID → PAID → ANNULLED), habilitación mensual, multas, descuentos y cuotas. Entidades backend: `Collections`, `CollectionsStatus`, `Installment`, `CurrentDebt`, `Adjustment`, `AdjustmentCategory`. Tareas programadas: `update_collections_status`, `apply_daily_penalties`, `correct_collection_payment_states`.

**Pagos.** Transacciones de pago internas: creación, programación, aprobación, envío a pago, exportación. Incluye pagos programados y links de pago Fintoc entrantes (PaymentIntent). Entidades backend: `Payment`, `PaymentSchedule`, `PaymentScheduleStatus`, `PaymentIntent`, `PaymentActions`, `PaymentBackup`. Tareas programadas: `create_payment_schedule_async`, `update_payment_schedule_async`, tareas de exportación.

**Liquidaciones.** Liquidación mensual al propietario: cálculo de ingresos, egresos, comisiones y saldo neto. Incluye la configuración financiera de la MqAccount (porcentaje de comisión, tipo de liquidación, día de liquidación, cuenta destino). Entidades backend: `Liquidation`, `MqAccount` (aspectos financieros). Sin tareas programadas dedicadas.

---

### Nivel 2 — Infraestructura financiera

Estos dos módulos son la capa de integración bancaria y ajuste económico. Se distinguen del Nivel 1 porque tienen sus propios pipelines de tareas asíncronas.

**Conciliación bancaria.** Todo el flujo de conciliación: ingesta de movimientos bancarios (desde Fintoc API o Microsoft Graph), creación de reconciliaciones, matching entre movimientos bancarios y pagos internos, y clearing. Entidades backend: `BankAccount`, `BankMovement`, `FintocMovement`, `Reconciliation`, `ReconciliationStatus`, `Movements`, `ClearingGroup`, `ClearingLine`, `BankAccountMqAccount`. Tareas programadas: `create_bank_movements_from_fintoc`, `create_bank_movements_from_email_graph`, `check_reconciliation_consistency`.

**IPC / UF / Reajuste.** Obtención diaria del valor UF desde la API del Banco Central, verificación de disponibilidad, y recálculo de cobros indexados a IPC. Este módulo es transversal (afecta Contratos y Cobros), pero tiene su propio pipeline de tres tareas con lock distribuido. Entidades backend: `IPCChile`, `ClfClpRate`, campos IPC en `Parameters.details_kind="ipc_adjustment"`. Tareas programadas: `get_uf_posterior_from_today`, `verificar_uf_actual_y_futura`, `recalculate_ipc_collections`.

---

### Nivel 3 — Datos maestros

Entidades compartidas que son referenciadas por múltiples módulos. Los problemas aquí suelen ser de calidad de datos, no de lógica de negocio.

**Propiedades / Unidades.** Gestión de propiedades y unidades de arriendo: datos de la propiedad, tipo de unidad, estado, fotos, videos, amenidades, propiedad (ownership) y su registro. Entidades backend: `Property`, `Unit`, `UnitOwnership`, `RegisterUnitOwnership`, `UnitPhoto`, `UnitVideo`, `PropertyAmenities`, `UnitType`, `UnitTypology`, `UnitStatus`, `StatusProperty`. Sin tareas programadas dedicadas.

**Stakeholders / Figuras.** La capa de identidad: personas naturales, empresas, la entidad polimórfica Stakeholder, y la entidad legacy Owner. Incluye problemas con la estructura Stakeholder → Person/Company y con la relación MqAccount → Customer (el aspecto de identidad, no el financiero). Entidades backend: `Stakeholder`, `Person`, `Company`, `Owner`, `CustomerStatus`. Sin tareas programadas dedicadas.

---

### Nivel 4 — Portales y productos externos

Productos con su propia app de Django, su propio modelo de usuario, o su propia integración externa.

**Portal Arrendatarios.** El portal de acceso para arrendatarios y propietarios: autenticación, visualización de cobros, links de pago, acceso de co-deudores, y notificaciones de deuda. Entidades backend: `tenants_owners.Users`, `DebtNotification`, PaymentIntent (links de pago). Tareas programadas: `send_notification`, `send_contract_email_task`.

**CRM / Bot WhatsApp.** Gestión de leads vía WhatsApp: contactos, conversaciones, mensajes, funnel de ventas, agentes, y el bot de IA para calificación de leads. Entidades backend: `Contact`, `Conversation`, `Message`, `Agent`, `Funnel`, `FunnelStage`, `ConversationEvent`, `BotProfile`, `BotSession`, `BotKnowledge`. Tareas programadas: `process_inbound_message`, `process_bot_ai_response`.

**Corretaje.** Corredores, llaves inteligentes (KeyBox), prospectos, agendamiento de visitas, e integración con MercadoLibre. Entidades backend: `Broker`, `KeyBox`, `KeyBoxRecord`, `Prospect`, `Scheduling`, `ScheduledTime`, modelos de estado MercadoLibre. Sin tareas programadas dedicadas.

---

### Nivel 5 — Herramientas internas y transversales

**Planner.** Tableros Kanban internos y sincronización con Microsoft Planner: notebooks, tarjetas, columnas, tags, documentos adjuntos, y el registro de tareas sincronizadas. Entidades backend: `Notebook`, `NotebookRow`, `Card`, `Tag`, `NotebookMember`, `TaskDocument`, `PlannerTaskRecord`. Tareas programadas: `sync_planner_tasks`, `rematch_null_units_task`.

**Servicios básicos.** Consulta automática de cuentas de agua (Agua Andina) y electricidad (Enel) mediante scraping con rotación de proxies. Entidades backend: `AccountAguaAndina`, `AccountEnel`. Tareas programadas: todas las tareas `consultar_*` y `procesar_*` de agua_andina y enel.

**Reportes / Exports.** Generación de archivos Excel, ZIP y PDF para descarga: nóminas, comisiones, servicios básicos, reportes de deuda, RentRoll. No tiene entidades propias (genera archivos temporales). Tareas programadas: `export_payment_comision`, `export_payment_manual`, `export_payment_nomina`, `export_payment_zip`, `export_basic_services`, `generate_and_send_delinquency_excel`.

**Permisos / Auth.** Autenticación de usuarios staff MQ, roles, permisos, y gestión de contraseñas. Entidades backend: `authenticate.User`, `Roles`, `UserRoles`. Sin tareas programadas dedicadas.

**Infraestructura.** Problemas de la plataforma técnica que no son atribuibles a un módulo funcional específico: límites de Vercel, timeouts entre frontend y backend, fallos de workers/Celery, problemas de S3/SQS, errores de deploy, y configuración de entornos. No tiene entidades propias — este tag captura cuando la causa está en la capa de plataforma, no en la lógica de negocio.

---

## Dimensión 3 — Causa raíz

Esta dimensión clasifica **por qué** ocurrió el problema. Se determina durante la sesión de revisión quincenal, después de diagnosticar la entrada. La pregunta que responde: "¿Cuál es el origen del problema?"

Existen nueve categorías posibles, incluyendo "Desconocido" para cuando no se puede determinar la causa.

**Importante:** Esta dimensión solo aplica para entradas de tipo Incidencia y Soporte operativo. Las entradas de tipo Duda, Idea y Aviso no requieren causa raíz (se puede dejar vacío o marcar "No aplica").

---

### Dato legacy / Dato histórico con problemas

**Definición:** El problema fue causado por datos que existían antes de una migración, antes de un cambio de reglas de negocio, o que fueron ingresados en una versión anterior del sistema con validaciones distintas. El código actual funciona correctamente, pero los datos antiguos no cumplen las reglas actuales.

Este tag se usa cuando el dato "malo" ya estaba en la base de datos antes de que ocurriera el problema. No es que el sistema creó un dato incorrecto ahora — es que un dato creado en el pasado generó un conflicto con la lógica actual.

**Ejemplos:**

- Cobros antiguos con `total_amount` en decimales (CLP) que ahora causan que el estado del cobro cambie de "pagado" a "pago parcial" porque `paid_amount` es entero → Dato legacy
- Unidades duplicadas de una migración anterior que no pueden eliminarse → Dato legacy
- Un contrato creado sin campo de moneda (`currency`) porque en esa versión no era obligatorio, y ahora la renovación automática falla → Dato legacy
- Un usuario creado dos veces porque antes no existía validación case-insensitive de email → Dato legacy

**No es Dato legacy si:** el dato malo fue creado por la versión actual del sistema (→ probablemente Lógica de negocio o Caso borde).

---

### Lógica de negocio

**Definición:** La regla de negocio implementada en el código es incorrecta o incompleta. El sistema hace exactamente lo que el código dice, pero lo que el código dice no corresponde a lo que el negocio necesita. Incluye cálculos mal implementados, reglas de estado mal definidas, y flujos que producen resultados que no tienen sentido desde el punto de vista operativo.

Este es el tag más común para Incidencias. Si el sistema procesó datos correctos y produjo un resultado incorrecto, probablemente es Lógica de negocio.

**Ejemplos:**

- La renovación automática en modo escalonado crea cobros duplicados (uno de la condición vieja y uno proporcional de la nueva) → Lógica de negocio
- La comisión se calcula por pago individual y luego se suma, en vez de sumar pagos y luego calcular la comisión (produce diferencias de redondeo) → Lógica de negocio
- El sistema reajusta un cobro por IPC, pero como tiene un descuento, el neto queda negativo → Lógica de negocio
- La función de email solo filtra cobros del mes actual o anterior, omitiendo cobros futuros → Lógica de negocio
- Al editar condiciones, el sistema modifica la primera condición en vez de la vigente → Lógica de negocio
- El status de cobro "contrato pendiente" no fue deprecado y sigue apareciendo en cobros nuevos → Lógica de negocio

**No es Lógica de negocio si:** la lógica es correcta pero el caso específico no fue contemplado (→ Caso borde), o la lógica es correcta pero los datos de entrada son malos (→ Dato legacy).

---

### Caso borde no cubierto

**Definición:** El flujo principal (happy path) funciona correctamente, pero un escenario específico no fue contemplado en el diseño o la implementación. El sistema no tiene código para manejar esa situación particular, así que falla, produce un resultado inesperado, o simplemente no hace nada.

La diferencia con Lógica de negocio: en Lógica de negocio, el código existe pero está mal. En Caso borde, el código para ese escenario no existe o es insuficiente.

**Ejemplos:**

- Una reserva de $0 con descuento de $0 no dispara el cambio de estado a "reservado" → Caso borde (el flujo normal con montos > 0 funciona bien)
- Un contrato creado sin gastos comunes (GGCC) porque el formulario no valida que ese parámetro sea obligatorio → Caso borde
- Fintoc no puede procesar pagos superiores a CLP 10.000.000 (límite no documentado) → Caso borde
- Un stakeholder que es persona natural (no empresa) genera una estructura Customer + MqAccount + Owner + BankAccount que se siente innecesariamente compleja → Caso borde (el modelo fue diseñado para empresas)
- La conciliación manual no considera cobros ya vinculados, causando doble matching → Caso borde

**No es Caso borde si:** el escenario era conocido y la lógica intentó manejarlo pero lo hizo mal (→ Lógica de negocio).

---

### Gap de QA

**Definición:** El problema pudo haberse detectado antes de llegar a producción con pruebas más exhaustivas. Esto incluye falta de tests automatizados, falta de pruebas manuales con datos reales, falta de pruebas de combinaciones de filtros, o falta de pruebas con volúmenes reales de datos.

Este tag reconoce una oportunidad de mejora en el proceso de desarrollo, no culpa individual. Se usa cuando el equipo identifica durante la revisión que "esto se pudo haber evitado testeando X".

**Ejemplos:**

- El formulario de creación de contrato falla porque una validación de frontend para "promociones" no fue testeada con el flujo real → Gap de QA
- Las tablas con lazy loading no paginan correctamente con volúmenes reales de datos → Gap de QA ("se pudo haber testeado con datos reales")
- Cambios en liquidaciones no fueron probados con todos los tipos de liquidación → Gap de QA
- Un filtro múltiple en la tabla de contratos falla cuando se combina con otro filtro → Gap de QA ("se pudo haber evitado con pruebas de filtros múltiples")

**Nota:** Gap de QA frecuentemente coexiste con otra causa raíz (ej. Lógica de negocio + Gap de QA). En ese caso, elegir la causa raíz primaria — la que explica *por qué el error existe*, no por qué no se detectó. Gap de QA se usa cuando la causa principal es genuinamente que no se probó lo suficiente, no como tag secundario.

---

### Integración externa

**Definición:** El problema fue causado por un servicio de terceros fuera del control del equipo: Fintoc (pagos), Vercel (hosting), Microsoft Graph (email/Planner), MercadoLibre, Tuya (KeyBox), Agua Andina, Enel, o la API del Banco Central (UF).

Incluye tanto caídas del servicio externo como comportamiento inesperado del servicio que afecta a MQPro.

**Ejemplos:**

- Fintoc está caído a nivel nacional y los pagos no se procesan → Integración externa
- Fintoc limita los montos de pago de arrendatarios recurrentes como si fueran cuentas nuevas → Integración externa
- La API del Banco Central no publica el valor UF del día siguiente → Integración externa
- Microsoft Planner no responde y la sincronización falla → Integración externa
- El scraping de Enel falla porque cambiaron su página web → Integración externa

**No es Integración externa si:** MQPro no maneja correctamente un error del servicio externo (ej. no tiene retry, no alerta, falla silenciosamente). En ese caso la causa raíz es Lógica de negocio o Caso borde — el servicio externo es solo el trigger.

---

### UX / Capacitación

**Definición:** El sistema funciona correctamente, pero el usuario no logra usarlo como se espera porque la interfaz es confusa, la funcionalidad no es descubrible, o la operación requiere conocimiento que no ha sido transferido formalmente.

La diferencia con Duda (Dimensión 1): una Duda es el tipo de entrada; UX/Capacitación es la causa raíz de esa Duda. Pero UX/Capacitación también puede ser causa raíz de una Incidencia aparente: el usuario reporta "esto no funciona" cuando en realidad funciona, pero de una manera que no es intuitiva.

**Ejemplos:**

- Múltiples usuarios no saben qué acciones requieren permisos de administrador vs. qué pueden hacer ellos mismos → UX / Capacitación
- Un usuario crea cobros manualmente en vez de usar anexos, generando inconsistencias → UX / Capacitación
- Confusión entre "cliente", "propietario" y "figura" en la interfaz → UX / Capacitación
- El usuario no sabe que existe la funcionalidad "mover reserva" → UX / Capacitación
- Un arrendatario guardó un link de pago Fintoc y lo reutiliza mes a mes con el mismo monto → UX / Capacitación (el arrendatario no entendió que el link es de uso único)
- El equipo de SAC no sabe cómo funciona el saldo pendiente → UX / Capacitación

**Señal clave:** Si la resolución es explicar cómo funciona (sin cambiar código), la causa raíz es UX/Capacitación. Si el mismo tema genera múltiples entradas desde distintos usuarios, es señal fuerte de que la UX necesita mejora o se necesita capacitación formal.

---

### Performance

**Definición:** El sistema funciona correctamente a nivel lógico, pero es demasiado lento, consume demasiados recursos, o colapsa bajo carga. Incluye timeouts, queries lentos, saturación de conexiones, y regresiones de rendimiento por funcionalidades nuevas.

**Ejemplos:**

- El auto-resize de columnas de tabla generó degradación de rendimiento en todo el sistema (queries excesivos de column widths por usuario por vista) → Performance
- Los clearing groups y clearing lines saturan las conexiones de la base de datos al cargar en el admin → Performance
- El frontend tiene timeout de 15 segundos mientras el backend tiene timeout de 200 segundos, causando desconexión prematura → Performance
- Un dropdown para seleccionar ítems se cuelga al cargar datasets grandes → Performance
- Error 504 porque el backend tarda demasiado en responder → Performance

**No es Performance si:** el sistema es lento porque la lógica está calculando algo incorrectamente o haciendo trabajo innecesario por un error de diseño (→ Lógica de negocio). Performance se usa cuando la lógica es correcta pero la ejecución es ineficiente.

---

### Configuración / Deploy

**Definición:** El problema fue causado por un error en la configuración del entorno, un error en el proceso de deploy, o una discrepancia entre el entorno de desarrollo y el de producción. El código es correcto en sí mismo, pero se ejecutó con parámetros incorrectos.

**Ejemplos:**

- La multa diaria se desplegó al 100% en producción en vez de 1% porque los valores de configuración no estaban separados por entorno → Configuración / Deploy
- El límite de tamaño de archivo de Vercel (4.5 MB) causó que las subidas fallaran silenciosamente → Configuración / Deploy
- Un worker programado se activó pero no filtró los casos correctamente porque no tenía monitoreo post-ejecución configurado → Configuración / Deploy
- El endpoint de reset de contraseña estaba roto en producción pero funcionaba en local → Configuración / Deploy

**No es Configuración / Deploy si:** el error está en el código mismo, no en cómo se configuró o desplegó (→ Lógica de negocio).

---

### Desconocido

**Definición:** No se pudo determinar la causa raíz durante la sesión de revisión. Esto puede ocurrir porque el problema no es reproducible, porque no hay suficiente información en el reporte original, porque el diagnóstico requiere más investigación de la que es posible en la sesión, o porque el contexto del reporte es ambiguo.

Usar "Desconocido" es preferible a forzar una clasificación cuando no hay evidencia suficiente. Un porcentaje alto de "Desconocido" en un mes es una señal de que los reportes necesitan más contexto o que las sesiones de revisión necesitan más tiempo de diagnóstico.

**Ejemplos:**

- "MQPro se cayó ayer a las 3pm" pero no hay logs ni error reproducible → Desconocido
- El usuario reporta un problema pero cuando el equipo lo revisa, funciona correctamente y no hay forma de reproducirlo → Desconocido
- El reporte es demasiado vago para diagnosticar ("algo raro pasó con un contrato") → Desconocido
- El problema se resolvió solo y no se encontró evidencia de qué lo causó → Desconocido

**Nota:** Las entradas marcadas como "Desconocido" pueden reclasificarse si se obtiene más información posteriormente. No es un estado terminal.

---

## Reglas de clasificación

**Una entrada, tres tags.** Cada entrada del BlackBox recibe exactamente una Dimensión 1, una Dimensión 2, y una o más Dimensión 3 (excepto para Avisos, donde Dimensión 3 no aplica).

**La Dimensión 1 se asigna al recibir la entrada.** No requiere diagnóstico técnico — se determina por la naturaleza del mensaje del reportante.

**La Dimensión 2 se asigna al recibir la entrada o durante la sesión.** Generalmente el reportante ya menciona el módulo ("el contrato no...", "el cobro tiene..."). En casos ambiguos, se asigna durante la sesión de revisión.

**La Dimensión 3 se asigna durante la sesión de revisión.** Requiere diagnóstico técnico del equipo. Si no se puede determinar en la sesión, se marca "Desconocido".

**En caso de duda entre dos tags de la misma dimensión**, elegir el más específico. Si el problema es un cobro con monto decimal heredado de un contrato viejo, es "Dato legacy" (específico) en vez de "Lógica de negocio" (genérico). Si un caso borde revela que la lógica completa está mal, es "Lógica de negocio" (la causa raíz más profunda).
