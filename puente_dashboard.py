# uv run --with paho-mqtt python puente_dashboard.py
import csv #guarda cada medición en un archivo de filas y columnas.
import json #convierte los mensajes entre texto JSON y datos de Python.
import threading #permite trabajar con MQTT y el servidor web al mismo tiempo.
import uuid #genera una identificación diferente para cada conexión del dashboard.
from datetime import datetime # datetime: sirve para guardar la hora de la última lectura recibida.
# BaseHTTPRequestHandler: recibe las peticiones que hace el navegador.
# ThreadingHTTPServer: atiende varias peticiones sin bloquear toda la página.
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path #permite encontrar la carpeta del proyecto sin escribir la ruta completa.
from urllib.parse import urlsplit #separa la ruta solicitada de otros datos de la dirección web.
import paho.mqtt.client as paho #conecta Python con el broker y permite enviar y recibir mensajes.

BROKER = "broker.hivemq.com" # Este es el broker público al que también se conecta Wokwi.
PORT = 1883 # Puerto usado por MQTT para la conexión de Python con el broker.
GROUP = "1gs133" # Estos identificadores deben coincidir con los del main.py del ESP32.
DEVICE = "aq01"
# Tema principal del proyecto. De aquí salen /telemetria, /estado,
# /respuesta y /orden para que cada mensaje llegue al lugar correcto.
TOPIC = f"utp/ds8/{GROUP}/aquanivel/{DEVICE}"
# La página se abre solo en esta computadora mediante el puerto 8765.
HOST, HTTP_PORT = "127.0.0.1", 8765
# Busco la carpeta dist, donde están el HTML, CSS y JavaScript del dashboard.
WEB_DIR = Path(__file__).resolve().parent / "dist"

# El historial se guarda al lado del backend
CSV_DIR = Path(__file__).resolve().parent / "datos"
CSV_PATH = CSV_DIR / "telemetria_aquanivel.csv"
CSV_COLUMNS = (
    "fecha_hora", "dispositivo", "seq", "nivel_pct", "distancia_cm",
    "clasificacion", "sensor_externo_pct", "agua_externa", "caida_pct",
    "diagnostico", "bomba", "modo", "umbral_critico", "bloqueo_fuga", "switch"
)

# El candado evita que dos tareas cambien o consulten estos datos a la vez.
# Por ejemplo, MQTT puede recibir una lectura mientras la web la está leyendo.
lock = threading.Lock()
csv_lock = threading.Lock() # Evita que dos mensajes escriban a la vez en el CSV.
# Aquí conservo el estado más reciente. No se guarda en una base de datos.
state = {
    "connected": False,             # Indica si Python está conectado al broker.
    "device_status": "desconocido", # Último estado informado por el ESP32.
    "revision": 0,                 # Número que aumenta con cada lectura nueva.
    "payload": None,               # Última medición enviada por el dispositivo.
    "reply_revision": 0,           # Número que aumenta con cada respuesta de orden.
    "reply": None,                 # Última respuesta del ESP32.
    "last_telemetry": None          # Fecha y hora de la última medición.
}


# Guarda una fila en el archivo CSV cada vez que llega una medición válida.
# Si todavía no existe, crea la carpeta, el archivo y sus encabezados.
# Si ya existe, agrega la lectura sin borrar los datos anteriores.
def guardar_telemetria_csv(datos, fecha_hora):
    fila = {columna: datos.get(columna) for columna in CSV_COLUMNS if columna != "fecha_hora"}
    fila["fecha_hora"] = fecha_hora

    with csv_lock:
        try:
            CSV_DIR.mkdir(parents=True, exist_ok=True)
            nuevo = not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0
            codificacion = "utf-8-sig" if nuevo else "utf-8"

            with CSV_PATH.open("a", newline="", encoding=codificacion) as archivo:
                escritor = csv.DictWriter(archivo, fieldnames=CSV_COLUMNS)
                if nuevo:
                    escritor.writeheader()
                escritor.writerow(fila)

        except OSError as error:
            # Si hay un problema con el CSV, el dashboard continúa funcionando.
            print("No se pudo guardar la lectura en CSV:", error)

# Se ejecuta automáticamente cuando Python recibe la respuesta de conexión
# del broker. Revisa si se conectó bien y se suscribe a los temas de AquaNivel.
def al_conectar(client, userdata, flags, reason_code, properties):
    # En Paho MQTT 2, reason_code se compara directamente con cero.
    # Si es cero, significa que el broker aceptó la conexión.
    ok = reason_code == 0

    # Actualizo el estado compartido usando el candado.
    with lock:
        state["connected"] = ok

    if ok:
        # Me suscribo a tres temas:
        # telemetria: mediciones de sensores; estado: conexión del ESP32;
        # respuesta: confirmaciones de las órdenes enviadas desde el panel.
        for suffix in ("telemetria", "estado", "respuesta"):
            client.subscribe(f"{TOPIC}/{suffix}", qos=0)

        # Muestro en la terminal que ya estoy escuchando los mensajes.
        print("MQTT conectado. Escuchando:", TOPIC)
    else:
        print("El broker rechazó la conexión:", reason_code)


# Función al_desconectar:
# Se llama cuando la conexión con el broker se pierde o termina.
# Su trabajo es avisar al dashboard que ya no hay conexión MQTT.
def al_desconectar(client, userdata, disconnect_flags, reason_code, properties):
    with lock:
        state["connected"] = False

    # El cliente MQTT puede intentar reconectar mientras sigue ejecutándose.
    print("MQTT desconectado. Se intentará reconectar.")

# Función al_recibir_mensaje:
# Se ejecuta cada vez que llega un mensaje MQTT de alguno de los temas
# suscritos. Identifica de qué tema viene y guarda la información necesaria.
def al_recibir_mensaje(client, userdata, msg):
    # MQTT entrega los mensajes como bytes; los convierto a texto UTF-8.
    # 'replace' evita que el programa falle por un carácter desconocido.
    value = msg.payload.decode("utf-8", errors="replace")

    # Las mediciones se guardan en el CSV; las órdenes y respuestas no.
    datos_para_csv = None
    fecha_para_csv = None

    # Protejo los datos compartidos antes de modificarlos.
    with lock:
        if msg.topic.endswith("/telemetria"):
            # Aquí llegan el porcentaje del tanque, distancia, estado de bomba,
            # posible fuga y demás datos que envía Wokwi.
            try:
                datos = json.loads(value)
                if not isinstance(datos, dict):
                    raise ValueError("Se esperaba un objeto JSON")
                fecha_para_csv = datetime.now().isoformat(timespec="seconds")
                state["payload"] = datos  # Paso de JSON a datos Python.
                state["revision"] += 1               # Marco que llegó una lectura nueva.
                state["last_telemetry"] = fecha_para_csv
                datos_para_csv = datos
            except (ValueError, TypeError):
                # Si el JSON es incorrecto, lo ignoro sin detener el servidor.
                print("Lectura JSON inválida, se ignoró.")

        elif msg.topic.endswith("/estado"):
            # Aquí normalmente llega si el ESP32 está online u offline.
            state["device_status"] = value

        elif msg.topic.endswith("/respuesta"):
            # Aquí Wokwi confirma si aceptó órdenes como PAUSAR o REANUDAR.
            try:
                state["reply"] = json.loads(value)
            except (ValueError, TypeError):
                # Si la respuesta no llegó como JSON, la registro como rechazo.
                state["reply"] = {"aceptada": False, "detalle": value}

            # Aviso al navegador que hay una respuesta más reciente.
            state["reply_revision"] += 1

    # Escribo fuera del candado principal para no detener la consulta del dashboard.
    if datos_para_csv is not None:
        guardar_telemetria_csv(datos_para_csv, fecha_para_csv)

# Clase ManejadorWeb:
# Es la parte que comunica el navegador con Python usando HTTP.
# Entrega los archivos visuales, permite consultar las últimas lecturas
# y recibe las órdenes de los botones del dashboard.
class ManejadorWeb(BaseHTTPRequestHandler):

    # Método log_message:
    # Controla cuáles peticiones aparecen escritas en la terminal.
    # Oculta las peticiones exitosas (200 y 204) para evitar demasiado texto.
    def log_message(self, fmt, *args):
        if args and str(args[1]) not in ("200", "204"):
            super().log_message(fmt, *args)

    # Método responder:
    # Prepara y envía una respuesta HTTP al navegador. Se reutiliza para
    # devolver JSON, archivos del dashboard y mensajes de error.
    def responder(self, code, data, mime="application/json; charset=utf-8"):
        self.send_response(code)                           # Código HTTP: 200, 400, 404, etc.
        self.send_header("Content-Type", mime)            # Tipo de contenido que envío.
        self.send_header("Content-Length", str(len(data))) # Tamaño del contenido en bytes.
        self.send_header("Cache-Control", "no-store")      # Evita mostrar datos viejos en caché.
        self.end_headers()                                 # Termino de escribir los encabezados.
        self.wfile.write(data)                             # Envío el contenido de la respuesta.

    # Método do_GET:
    # Atiende las solicitudes GET, que se usan para abrir la página y consultar
    # información. También entrega los archivos HTML, CSS y JavaScript.
    def do_GET(self):
        # Obtengo solo la ruta, sin parámetros adicionales de la dirección.
        path = urlsplit(self.path).path

        if path == "/favicon.ico":
            # El navegador solicita un ícono. Como no lo utilizamos, respondo
            # que no hay contenido y así evito un error 404 innecesario.
            self.send_response(204)
            self.end_headers()
            return

        if path == "/api/snapshot":
            # El JavaScript consulta esta ruta para leer el estado más reciente.
            with lock:
                # Convierto el diccionario a JSON y luego a bytes UTF-8.
                data = json.dumps(dict(state), ensure_ascii=False).encode("utf-8")

            self.responder(200, data)
            return

        # Solo permito solicitar estos archivos. Esto evita que alguien
        # intente leer otras carpetas de la computadora desde el navegador.
        allowed = {
            "/": ("index.html", "text/html; charset=utf-8"),
            "/index.html": ("index.html", "text/html; charset=utf-8"),
            "/styles.css": ("styles.css", "text/css; charset=utf-8"),
            "/refinamientos.css": ("refinamientos.css", "text/css; charset=utf-8"),
            "/app.js": ("app.js", "application/javascript; charset=utf-8")
        }

        if path not in allowed:
            # 404 significa que la dirección solicitada no existe.
            self.responder(404, b"No encontrado", "text/plain; charset=utf-8")
            return

        # Busco el nombre del archivo y el tipo de contenido que corresponde.
        filename, mime = allowed[path]

        try:
            # Leo el archivo desde dist y se lo envío al navegador.
            self.responder(200, (WEB_DIR / filename).read_bytes(), mime)
        except FileNotFoundError:
            # Aviso si falta alguno de los archivos del dashboard.
            self.responder(404, b"Archivo faltante", "text/plain; charset=utf-8")

    # Método do_POST:
    # Atiende las solicitudes POST. En este proyecto se usa cuando presiono
    # un botón de control para enviar una orden al ESP32 por MQTT.
    def do_POST(self):
        # Solo acepto órdenes enviadas a esta dirección de la API local.
        if urlsplit(self.path).path != "/api/orden":
            self.responder(404, b'{}')
            return

        try:
            # Averiguo cuántos bytes contiene el mensaje del navegador.
            size = int(self.headers.get("Content-Length", "0"))

            # No acepto mensajes vacíos ni demasiado grandes.
            if size < 1 or size > 1024:
                raise ValueError("Tamaño inválido")

            # Leo el cuerpo de la petición y convierto su JSON a diccionario.
            order = json.loads(self.rfile.read(size).decode("utf-8"))

            # Por ejemplo: {"tipo":"telemetria", "valor":"PAUSAR"}.
            kind, value = order.get("tipo"), order.get("valor")

            valid = ((kind == "modo_bomba" and value in ("AUTO", "OFF")) or
                     (kind == "rearme" and value == "CONFIRMAR") or
                     (kind == "telemetria" and value in ("PAUSAR", "REANUDAR")) or
                     (kind == "umbral_critico" and type(value) in (int, float)
                      and 5 <= value <= 35))

            if not valid:
                # Rechazo las instrucciones desconocidas o con valores inválidos.
                raise ValueError("Orden no permitida")

            with lock:
                connected = state["connected"]

            if not connected:
                # El código 503 indica que el servicio no está disponible.
                self.responder(503, b'{"error":"MQTT sin conexion"}')
                return

            # Publico la orden al tema /orden para que el ESP32 la reciba.
            # qos=0 es un envío sin confirmación de entrega del broker.
            info = client.publish(TOPIC + "/orden", json.dumps(order), qos=0)

            if info.rc != paho.MQTT_ERR_SUCCESS:
                # Si Python no pudo publicar la orden, aviso al dashboard.
                self.responder(503, b'{"error":"No se pudo publicar"}')
                return

            # La orden se envió al broker. La confirmación del ESP32 llega
            # después por el tema /respuesta; no significa que ya se ejecutó.
            self.responder(200, b'{"enviada":true}')

        except (ValueError, TypeError, AttributeError) as error:
            # Si la petición está mal formada, devuelvo el error HTTP 400.
            self.responder(400, json.dumps({"error": str(error)}).encode("utf-8"))

# Este bloque solo se ejecuta si inicio directamente este archivo Python.
# Primero prepara MQTT y después abre el servidor HTTP del dashboard.
if __name__ == "__main__":
    print("=" * 55)
    print("AQUANIVEL | DASHBOARD")
    print("=" * 55)
    print("Grupo:", GROUP, "| Dispositivo:", DEVICE)
    print("Historial CSV:", CSV_PATH)

    # Creo el cliente MQTT con una identificación única para evitar choques
    # con otras conexiones al mismo broker.
    client = paho.Client(
        callback_api_version=paho.CallbackAPIVersion.VERSION2,
        client_id="aq-panel-" + uuid.uuid4().hex[:10],
        protocol=paho.MQTTv311,
    )

    # Le indico a la librería qué función debe llamar en cada situación.
    client.on_connect = al_conectar        # Cuando se conecta.
    client.on_disconnect = al_desconectar  # Cuando se desconecta.
    client.on_message = al_recibir_mensaje # Cuando recibe un mensaje.

    # Inicio la conexión al broker sin detener el resto del programa.
    client.connect_async(BROKER, PORT, keepalive=30)

    # Mantengo MQTT funcionando en segundo plano para recibir mensajes.
    client.loop_start()

    try:
        # Creo el servidor web local, que puede atender varias peticiones.
        server = ThreadingHTTPServer((HOST, HTTP_PORT), ManejadorWeb)

        print("Abre el navegador en: http://127.0.0.1:8765")
        print("Enciende Wokwi y presiona Conectar MQTT en el panel.")
        print("Para terminar presiona Ctrl+C.\n")

        # Mantengo el servidor web activo hasta que decida detenerlo.
        server.serve_forever()

    except KeyboardInterrupt:
        # Ctrl+C detiene el programa de forma manual.
        print("Panel detenido.")

    finally:
        # Este bloque se ejecuta incluso al salir por Ctrl+C.
        # Cierro la recepción MQTT y después el servidor web.
        client.loop_stop()
        if "server" in locals():
            server.server_close()
