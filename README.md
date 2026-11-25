# Dashboard AquaNivel — Avance 3

Este panel muestra las lecturas del ESP32 de Wokwi con un tanque animado. **No usa base de datos**: la gráfica y los eventos recientes se guardan únicamente mientras la página está abierta.

## Qué muestra

- Agua dentro de un tanque semitransparente que sube y baja con el nivel real.
- Nivel en porcentaje, clasificación (CRÍTICO, BAJO, MEDIO o LLENO) y distancia del HC-SR04.
- Sensor de agua exterior, caída de nivel, diagnóstico y bloqueo por posible fuga.
- LED de bomba (no es una bomba real), modo AUTO/OFF y valor del switch.
- Gráfica temporal de las lecturas y eventos recientes.
- Control por MQTT del modo de bomba, rearme de fuga, pausa/reanudación de telemetría y ajuste del umbral crítico.
- Movimiento progresivo del agua tanto al subir como al bajar, incluso cuando cambia mucho el nivel.

## Forma 1: Presentación desde la computadora (recomendada)

1. Extrae esta carpeta sin separar `puente_dashboard.py` de la carpeta `dist`.
2. Abre esta carpeta en Visual Studio Code.
3. En la terminal de PowerShell ejecuta:

   ```powershell
   uv run --with paho-mqtt python puente_dashboard.py
   ```

4. Abre **http://127.0.0.1:8765** en Chrome o Edge.
5. Inicia tu proyecto ESP32 con MicroPython en Wokwi (pulsa ▶).
6. En el dashboard, presiona **Conectar MQTT** y luego **Conectar ahora**.
7. Mueve el sensor ultrasónico y el potenciómetro en Wokwi. El porcentaje y el agua animada deben reaccionar a las lecturas (cada cinco segundos, según `main.py`).
8. Prueba los botones **Automático** y **Apagar**; el LED de bomba del Wokwi responde según la lógica del ESP32. Si el tanque está lleno, la bomba seguirá apagada aunque selecciones AUTO.
9. Abajo, en **Telemetría y parámetros del ESP32**, presiona **Pausar**. Wokwi deja de publicar lecturas, pero sigue conectado y puede recibir nuevas órdenes.
10. Presiona **Reanudar** para recuperar las lecturas y los movimientos del tanque. El panel espera una respuesta de confirmación del ESP32.
11. Prueba el control **Umbral de nivel crítico**: elige, por ejemplo, 28 % y presiona **Aplicar umbral**. En Wokwi se actualizará el criterio de nivel crítico.

No hay que abrir `requirements.txt` ni ejecutar `pip install` en cada prueba. El comando con `--with paho-mqtt` se encarga de esa dependencia. Para detener el puente, presiona **Ctrl+C** en la terminal.

**Importante:** al conectarse el panel local, el programa de Python usa MQTT por TCP (1883) y la web toma los datos por HTTP local. Así funciona incluso si la red bloquea el WebSocket MQTT del navegador.

## Forma 2: Abrir el sitio web directamente

El dashboard también permite conectarse sin Python a `wss://broker.hivemq.com:8884/mqtt`. Si el navegador y la red aceptan ese puerto, basta con iniciar Wokwi y presionar **Conectar MQTT**.

Si no conecta con WSS, utiliza la Forma 1. **No cambies el broker de Wokwi por probar otro puerto:** 1883 es para MicroPython y 8884 es para WebSocket seguro.

## Modo demostración (sin conectar Wokwi)

Al iniciar, el tablero enseña el **Modo demostración**. En la barra inferior puedes:

- Mover el deslizador para poner el agua entre 0 y 100 %.
- Presionar **Probar fuga** para ver el diagnóstico en rojo.
- Presionar **Automático** para reactivar la animación.

El modo demostración **no envía comandos al ESP32**. Se identifica claramente para no confundirlo con una prueba MQTT real.

## Temas y JSON utilizados

```text
Broker (ESP32 y puente Python): broker.hivemq.com:1883
Broker (dashboard web directo): wss://broker.hivemq.com:8884/mqtt
Base: utp/ds8/1gs133/aquanivel/aq01
Suscripciones: /telemetria, /estado, /respuesta
Publicación de comandos: /orden
```

Ejemplo de telemetría:

```json
{
  "dispositivo":"aq01", "seq":42, "distancia_cm":65.0,
  "nivel_pct":67.5, "clasificacion":"MEDIO",
  "sensor_externo_pct":5.0, "agua_externa":false,
  "caida_pct":0.0, "diagnostico":"NORMAL",
  "bomba":false, "modo":"AUTO", "umbral_critico":20.0,
  "bloqueo_fuga":false, "switch":1
}
```

Las órdenes son JSON y se publican en `/orden`:

```json
{"tipo":"modo_bomba","valor":"OFF"}
{"tipo":"modo_bomba","valor":"AUTO"}
{"tipo":"telemetria","valor":"PAUSAR"}
{"tipo":"telemetria","valor":"REANUDAR"}
{"tipo":"umbral_critico","valor":28}
{"tipo":"rearme","valor":"CONFIRMAR"}
```

El dispositivo confirma si aceptó o rechazó la orden mediante `/respuesta`. El umbral admite valores de 5 a 35 %. **El intervalo de envío sigue fijo en 5 segundos** porque esa opción no está implementada como orden remota en `main.py`.

## Notas para la exposición

- El switch GPIO32 solo reporta su posición y genera una interrupción; **el código actual no lo usa para encender o apagar la bomba**.
- El sensor exterior está representado en Wokwi mediante un potenciómetro; agua exterior por sí sola no confirma una fuga. La lógica de prueba combina agua exterior con una caída del nivel de al menos 8 puntos porcentuales entre lecturas.
- Si no hay telemetría durante más de 16 segundos, el dashboard lo señala, excepto cuando nosotros mismos hemos pausado las lecturas.
- En esta etapa **no se instala SQLite**, no se guardan datos de manera permanente y no se requiere modificar `main.py` para usar el panel.
- El broker HiveMQ público es solamente para aprendizaje y pruebas: los temas no tienen autenticación y no se debe enviar información privada.

## Archivos

```text
AquaNivel_Dashboard/
├── puente_dashboard.py   # Único Python que ejecutamos en la PC; recibe datos y manda órdenes
├── README.md             # Esta guía
└── dist/
    ├── index.html       # Estructura del panel
    ├── styles.css       # Diseño principal del tanque y demás elementos
    ├── refinamientos.css  # Gráfica adaptable, ondas suaves y controles nuevos
    └── app.js           # Animación progresiva, órdenes MQTT y simulador
```
