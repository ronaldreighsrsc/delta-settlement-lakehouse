# 🤖 AGENTS.md — Reglas y Guardrails de delta-settlement-lakehouse

## 🛠️ Stack y Verificación
- **Stack:** Python 3.12+, Clean Architecture, Delta Lake (`deltalake`), DuckDB, PyArrow, Pydantic v2, Pytest.
- **Tests (mandatorio en verde):** `py -m pytest tests -q`

## 🧠 Protocolo de Memoria Estándar (Anti-Bloat / Presupuesto <100 Líneas)
- **Git y Pytest son la Memoria Histórica:** Git (`git log`) y la suite de tests (`pytest`) son la única fuente histórica de bugs y resoluciones. ESTRICTAMENTE PROHIBIDO redactar relatos de bugs, autopsias o historias de sesiones en `MEMORY.md`.
- **Presupuesto Estricto (<100 Líneas / <10 KB):** `MEMORY.md` es exclusivamente memoria de trabajo caliente de alta densidad. Si entra una nueva decisión, se condensa o poda lo anterior para nunca superar las 100 líneas.
- **Contenido Autorizado en `MEMORY.md`:** 1) Estado activo y tareas en vuelo. 2) Decisiones arquitectónicas activas (1 viñeta concisa por hito). 3) Trampas y reglas no obvias.
- **Aislamiento en Concurrencia (Anti-Race Condition):** Si trabajan dos agentes en paralelo (`agy` en terminal + IDE), la memoria activa en vuelo vive en `specs/NNN-*/tasks.md`. PROHIBIDO que múltiples agentes editen concurrentemente `MEMORY.md` raíz. La consolidación global se realiza al cierre formal de la tarea.
- **Git Worktrees para Paralelismo Real:** Para ejecutar dos agentes simultáneamente en distintas specs, usar ramas desacopladas con `git worktree add ../<carpeta> -b <rama>` para prevenir torn writes y colisiones de disco.

## ⚖️ Protocolo Autónomo de Enrutamiento: TDD Directo vs. Vertical Slice SDD + `/pair`
- **Fast-Path en Terminal y Git (<5s / Cero Fricción):** Comandos explícitos de terminal (`git add`, `git commit -m "..."`, ejecución de scripts). Ejecución inmediata sin auditorías de seguridad no solicitadas, sin arqueología forense ni modales `ask_question`.
- **TDD Directo (Cero Desperdicio / *Muda*):** Bugfixes, regex, selectores, afinación de constantes o refactors internos. Flujo: Test en rojo $\rightarrow$ Fix mínimo $\rightarrow$ Pytest en verde (<1s). Sin sobrecarga documental en `specs/`.
- **Vertical Slice SDD + Modo Semáforo `/pair` (Arquitectura y Nuevas Capacidades):** Nuevos módulos, subsistemas, cambios en contratos o esquemas de datos. Flujo: `specs/NNN-nombre/` (`spec.md` EARS $\rightarrow$ `plan.md` $\rightarrow$ `tasks.md`) con aprobación previa.
  - **Vertical Slices (Prohibido cortar por capas horizontales):** Cada spec y tarea debe atravesar las capas necesarias de punta a punta entregando un comportamiento verificable desde el minuto 1. Cero abstracciones "para el futuro".
  - **Modo Semáforo `/pair` (Driver/Navigator):** Implementar 1 tarea/comportamiento a la vez (máx. 3 archivos, ~50-80 líneas): 🔴 Test en Rojo $\rightarrow$ 🟢 Código mínimo para Verde $\rightarrow$ 🔵 Refactor sobre verde. Prohibido modificar tests existentes para hacer trampa.
- **Autonomía:** El asistente clasifica y ejecuta automáticamente el protocolo adecuado sin que el usuario tenga que solicitarlo.

## 🛑 Límites y Guardrails Operativos
- 🚫 **NUNCA:**
  - **Over-Engineering Burocrático:** Jamás convertir una orden operativa directa en una ceremonia de auditoría, lectura de 100 líneas irrelevantes o encuesta interactiva.
  - Crear scripts sueltos en la raíz; todo desarrollo vive en `delta_settlement_lakehouse/`.
  - Implementar lógica no aprobada en la spec activa.
  - Alucinar librerías externas o modificar esquemas sin autorización.
- ⚠️ **PREGUNTA ANTES:**
  - Cambios mayores en la arquitectura o interfaces públicas.
- ✅ **SIEMPRE:**
  - Flujo SDD estricto cuando aplique: Spec ➔ Plan ➔ Tasks ➔ TDD ➔ Validación.
  - Actualizar `MEMORY.md` de forma concisa (<100 líneas) solo al culminar specs completas, sin narrar bugs resueltos.
  - Mantener todos los tests en verde antes de dar por cerrada cualquier tarea.
