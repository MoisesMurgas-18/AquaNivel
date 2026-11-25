/* Dashboard AquaNivel - Avance 3.
   Muestra las lecturas de Wokwi por MQTT. Por ahora todo se conserva
   en memoria: no hay archivos ni base de datos. */

const $ = (id) => document.getElementById(id);
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const clamp = (n, min = 0, max = 100) => Math.min(max, Math.max(min, Number(n)));
const round1 = (n) => Math.round(Number(n) * 10) / 10;
const hhmm = (date = new Date()) => date.toLocaleTimeString('es-PA', {hour:'2-digit',minute:'2-digit',hour12:false});
const stored = (() => { try {return JSON.parse(localStorage.getItem('aquanivel-mqtt') || '{}');} catch {return {};} })();
const config = {
  url: stored.url || 'wss://broker.hivemq.com:8884/mqtt',
  group: stored.group || '1gs133',
  device: stored.device || 'aq01'
};
const baseTopic = () => `utp/ds8/${config.group}/aquanivel/${config.device}`;
// Si abrimos el panel con puente_dashboard.py, el navegador usa HTTP local.
// Así evitamos problemas con redes que bloquean WebSockets externos.
const useLocalBridge = ['127.0.0.1','localhost'].includes(location.hostname) && location.port === '8765';

// MQTT 3.1.1 básico usando WebSocket, sin librerías ni servidor adicional.
// Es el mismo MQTT del ESP32; cambia el transporte porque el navegador usa WSS.
function mqttLength(n) {
  const out = [];
  do { let b = n % 128; n = Math.floor(n / 128); if (n) b |= 128; out.push(b); } while (n);
  return out;
}
function mqttText(s) {
  const bytes = encoder.encode(String(s));
  return [bytes.length >> 8, bytes.length & 255, ...bytes];
}
function mqttFrame(type, body = []) { return new Uint8Array([type, ...mqttLength(body.length), ...body]); }

class BrowserMQTT {
  constructor(url, onMessage, onStatus) {
    this.url = url;
    this.onMessage = onMessage;
    this.onStatus = onStatus;
    this.ws = null;
    this.connected = false;
    this.packetId = 0;
    this.buffer = new Uint8Array(0);
    this.pingTimer = null;
    this.lastReceived = Date.now();
    this.explicitClose = false;
    this.clientId = 'aq-dashboard-' + Math.random().toString(36).slice(2, 11);
  }

  connect() {
    this.explicitClose = false;
    this.onStatus('connecting');
    try { this.ws = new WebSocket(this.url, ['mqtt']); }
    catch (error) { this.onStatus('error', String(error)); return; }
    this.ws.binaryType = 'arraybuffer';
    this.ws.onopen = () => {
      // MQTT CONNECT con sesión limpia y keepalive de 30 segundos.
      const head = [...mqttText('MQTT'), 4, 2, 0, 30];
      this.send(mqttFrame(0x10, [...head, ...mqttText(this.clientId)]));
    };
    this.ws.onmessage = async (event) => {
      const bytes = new Uint8Array(event.data instanceof Blob ? await event.data.arrayBuffer() : event.data);
      this.lastReceived = Date.now();
      this.buffer = new Uint8Array([...this.buffer, ...bytes]);
      this.readFrames();
    };
    this.ws.onerror = () => { if (!this.explicitClose) this.onStatus('error', 'No se pudo abrir el WebSocket MQTT.'); };
    this.ws.onclose = () => {
      this.connected = false;
      clearInterval(this.pingTimer);
      if (!this.explicitClose) this.onStatus('closed');
    };
  }

  send(bytes) { if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(bytes); }

  readFrames() {
    // WebSocket puede entregar uno o varios paquetes MQTT juntos.
    // Esperamos si todavía no llegó el paquete completo.
    while (this.buffer.length >= 2) {
      let length = 0, factor = 1, pos = 1, complete = false;
      for (let i = 0; i < 4; i++) {
        if (pos >= this.buffer.length) return;
        const part = this.buffer[pos++];
        length += (part & 127) * factor;
        if (!(part & 128)) { complete = true; break; }
        factor *= 128;
      }
      if (!complete) { this.onStatus('error','Paquete MQTT inválido'); this.close(); return; }
      if (this.buffer.length < pos + length) return;
      const type = this.buffer[0];
      const body = this.buffer.slice(pos, pos + length);
      this.buffer = this.buffer.slice(pos + length);
      this.processFrame(type, body);
    }
  }

  processFrame(header, body) {
    const type = header >> 4;
    if (type === 2) { // CONNACK: el broker aceptó o rechazó la conexión.
      if (body.length < 2 || body[1] !== 0) { this.onStatus('error',`El broker rechazó la conexión (${body[1] ?? '?'})`); this.close(); return; }
      this.connected = true;
      this.onStatus('connected');
      this.subscribe(`${baseTopic()}/telemetria`);
      this.subscribe(`${baseTopic()}/estado`);
      this.subscribe(`${baseTopic()}/respuesta`);
      this.pingTimer = setInterval(() => {
        if (this.connected && Date.now() - this.lastReceived > 45000) {
          this.onStatus('error','El broker no responde a los pings'); this.close(); return;
        }
        this.send(new Uint8Array([0xC0,0]));
      }, 12000);
    } else if (type === 3 && body.length > 2) { // PUBLISH: mensaje de un tema.
      const n = body[0] * 256 + body[1];
      if (n + 2 > body.length) return;
      let offset = n + 2;
      if (((header >> 1) & 3) !== 0) offset += 2;
      const topic = decoder.decode(body.slice(2, n + 2));
      const value = decoder.decode(body.slice(offset));
      this.onMessage(topic, value);
    }
    // SUBACK (9) y PINGRESP (13) no requieren acciones adicionales.
  }

  subscribe(topic) {
    const id = ++this.packetId;
    this.send(mqttFrame(0x82, [id >> 8, id & 255, ...mqttText(topic), 0]));
  }

  publish(topic, payload) {
    if (!this.connected) return false;
    this.send(mqttFrame(0x30, [...mqttText(topic), ...encoder.encode(payload)]));
    return true;
  }

  close() {
    this.explicitClose = true;
    clearInterval(this.pingTimer);
    if (this.ws) {
      if (this.ws.readyState === WebSocket.OPEN) this.send(new Uint8Array([0xE0,0]));
      this.ws.close();
    }
    this.connected = false;
  }
}

// Opción para la presentación: el Python de la PC escucha MQTT por el
// puerto 1883 y nos pasa la última lectura por una dirección local.
class LocalBridge {
  constructor(onMessage,onStatus) {
    this.onMessage = onMessage;
    this.onStatus = onStatus;
    this.connected = false;
    this.closed = false;
    this.revision = -1;
    this.replyRevision = -1;
    this.deviceStatus = null;
    this.timer = null;
    this.polling = false;
  }

  connect() {
    this.closed = false;
    this.onStatus('connecting');
    this.poll();
    this.timer = setInterval(() => this.poll(),1500);
  }

  async poll() {
    if (this.closed || this.polling) return;
    this.polling = true;
    try {
      const response = await fetch('/api/snapshot',{cache:'no-store'});
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const snapshot = await response.json();
      if (this.closed) return;
      if (snapshot.connected && !this.connected) {this.connected = true;this.onStatus('connected');}
      if (!snapshot.connected && this.connected) {this.connected = false;this.onStatus('closed');}
      if (snapshot.device_status !== this.deviceStatus) {
        this.deviceStatus = snapshot.device_status;
        if (this.deviceStatus === 'online' || this.deviceStatus === 'offline')
          this.onMessage(`${baseTopic()}/estado`,this.deviceStatus);
      }
      if (snapshot.revision > this.revision) {
        this.revision = snapshot.revision;
        if (snapshot.payload && snapshot.connected) this.onMessage(`${baseTopic()}/telemetria`,JSON.stringify(snapshot.payload));
      }
      if (snapshot.reply_revision > this.replyRevision) {
        this.replyRevision = snapshot.reply_revision;
        if (snapshot.reply) this.onMessage(`${baseTopic()}/respuesta`,JSON.stringify(snapshot.reply));
      }
    } catch (error) {
      if (!this.closed && this.connected) {this.connected = false;this.onStatus('error','El puente local no responde.');}
    } finally {this.polling = false;}
  }

  publish(topic,payload) {
    if (!this.connected) return false;
    fetch('/api/orden',{method:'POST',headers:{'Content-Type':'application/json'},body:payload})
      .then(async (response) => {if (!response.ok) throw new Error(`HTTP ${response.status}`);})
      .catch((error) => toast('No se pudo enviar la orden: ' + error.message,true));
    return true;
  }

  close() {this.closed = true;clearInterval(this.timer);this.connected = false;}
}

let source = 'demo';
let mqtt = null;
let mqttConnected = false;
let lastTelemetry = 0;
let deviceStatus = 'desconocido';
let history = [];
let events = [];
let previous = null;
let demoLevel = 64;
let demoFuga = false;
let demoMoving = true;
let demoDirection = -1;
let demoPumpMode = 'AUTO';
let toastTimer = null;
let demoCritical = 20;
let telemetryPaused = false;
let thresholdTouched = false;
let pendingControl = null;
let currentCritical = 20;

// Empiezo con 64%, que es el nivel de agua dibujado al abrir el panel.
// Cuando cambie Wokwi, avanzo poco a poco hacia el nuevo porcentaje.
let waterTarget = 64;
let waterDisplayed = 64;
let waterFrame = null;
let waterFrameTime = null;

function moveWater(now) {
  const elapsed = waterFrameTime === null ? 16 : Math.min(60, now - waterFrameTime);
  waterFrameTime = now;
  const difference = waterTarget - waterDisplayed;
  if (Math.abs(difference) <= 0.05) {
    waterDisplayed = waterTarget;
    $('water-fill').style.height = `${waterDisplayed}%`;
    waterFrame = null;
    waterFrameTime = null;
    return;
  }
  // Limito la velocidad: aunque el sensor pase de lleno a vacío,
  // la animación no puede saltar más de 14 puntos por segundo.
  const limit = (elapsed * 14) / 1000;
  const smoothPart = Math.abs(difference) * (1 - Math.exp(-elapsed / 1200));
  const step = Math.min(Math.abs(difference), limit, Math.max(0.02, smoothPart));
  waterDisplayed += Math.sign(difference) * step;
  $('water-fill').style.height = `${waterDisplayed.toFixed(2)}%`;
  waterFrame = requestAnimationFrame(moveWater);
}

// El porcentaje de texto sí es exacto. Solo evito que las pequeñas
// variaciones del sensor hagan temblar el dibujo del agua.
function updateWaterAnimation(level) {
  const next = clamp(level);
  if (Math.abs(next - waterTarget) < 0.8) return;
  waterTarget = next;
  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    if (waterFrame !== null) cancelAnimationFrame(waterFrame);
    waterFrame = null; waterFrameTime = null;
    waterDisplayed = next;
    $('water-fill').style.height = `${next}%`;
  } else if (waterFrame === null) waterFrame = requestAnimationFrame(moveWater);
}

function classify(level, critical = 20) {
  if (level < critical) return 'CRITICO';
  if (level < 40) return 'BAJO';
  if (level < 80) return 'MEDIO';
  return 'LLENO';
}

function demoData() {
  const nivel = round1(demoLevel);
  const exterior = demoFuga ? 68.0 : 8.0;
  return {
    dispositivo: config.device,
    nivel_pct: nivel,
    distancia_cm: round1((200 - nivel * 2)),
    clasificacion: classify(nivel,demoCritical),
    sensor_externo_pct: exterior,
    agua_externa: demoFuga,
    caida_pct: demoFuga ? 12.0 : 0.0,
    diagnostico: demoFuga ? 'POSIBLE_FUGA' : nivel < demoCritical ? 'NIVEL_CRITICO' : 'NORMAL',
    bomba: !demoFuga && demoPumpMode === 'AUTO' && nivel < demoCritical,
    modo: demoPumpMode,
    umbral_critico: demoCritical,
    bloqueo_fuga: demoFuga,
    switch: 1
  };
}

function eventLog(title, detail, kind = 'info') {
  if (events.length && events[0].title === title && events[0].detail === detail) return;
  events.unshift({title,detail,kind,time:hhmm()});
  events = events.slice(0, 4);
  $('events-list').replaceChildren(...events.map((e) => {
    const li = document.createElement('li'); li.className = 'event-item';
    const icon = document.createElement('span'); icon.className = `event-symbol ${e.kind}`;
    icon.textContent = e.kind === 'danger' ? '!' : e.kind === 'warn' ? '△' : '✓';
    const copy = document.createElement('div'); copy.className = 'event-copy';
    const strong = document.createElement('strong'); strong.textContent = e.title;
    const sub = document.createElement('p'); sub.textContent = e.detail;
    copy.append(strong,sub);
    const time = document.createElement('span');time.className = 'event-time';time.textContent = e.time;
    li.append(icon,copy,time);return li;
  }));
}

function toast(text, bad = false) {
  const el = $('toast'); el.textContent = text; el.className = `toast show${bad ? ' bad' : ''}`;
  clearTimeout(toastTimer); toastTimer = setTimeout(() => el.classList.remove('show'), 4500);
}

function connectionStatus(kind, text) {
  $('connection-pill').className = `connection-pill ${kind}`;
  $('connection-text').textContent = text;
  $('sidebar-mode').textContent = kind === 'demo' ? 'MODO DEMOSTRACIÓN' : kind === 'online' ? 'MQTT CONECTADO' : 'ESPERANDO MQTT';
  $('reading-chip').innerHTML = kind === 'online' ? '<span></span>DATOS EN VIVO' : kind === 'demo' ? '<span></span>ANIMACIÓN ACTIVA' : '<span></span>SIN LECTURAS';
}

function showNoData() {
  ['stat-level','stat-distance','stat-external','stat-pump','tank-percent','data-level','data-free','data-distance'].forEach((id) => $(id).textContent = '--');
  $('tank-class').textContent = 'Esperando lecturas';
  $('stat-class').textContent = 'SIN DATOS';
  $('stat-external-status').textContent = '--';
  updateWaterAnimation(0);
  $('data-progress').style.width = '0%';
  $('measurement-note').textContent = 'Enciende Wokwi para recibir las lecturas del ESP32.';
  $('diagnosis-title').textContent = 'Esperando telemetría';
  $('diagnosis-subtitle').textContent = 'La conexión no garantiza que el ESP32 esté enviando datos.';
  $('diagnosis-box').className = 'diagnosis-status warn';
  $('diagnosis-symbol').textContent = '…';
  ['detail-water','detail-drop','detail-lock','detail-switch'].forEach((id) => $(id).textContent = '--');
  $('pump-state-title').textContent = 'Sin datos de la bomba';
  $('pump-state-sub').textContent = 'Esperando estado del ESP32';
  $('pump-light').classList.remove('on');
  $('mode-auto').classList.remove('active');$('mode-off').classList.remove('active');
}

function renderReading(raw) {
  const level = Number(raw.nivel_pct), distance = Number(raw.distancia_cm), outside = Number(raw.sensor_externo_pct);
  if (![level,distance,outside].every(Number.isFinite) || level < 0 || level > 100 || distance < 0 || outside < 0 || outside > 100) return;
  const classification = String(raw.clasificacion || classify(level,raw.umbral_critico));
  const status = String(raw.diagnostico || 'NORMAL');
  const blocked = raw.bloqueo_fuga === true;
  const external = raw.agua_externa === true;
  const pump = raw.bomba === true;
  const mode = raw.modo === 'OFF' ? 'OFF' : 'AUTO';
  const drop = Number.isFinite(Number(raw.caida_pct)) ? Number(raw.caida_pct) : 0;

  if (previous) {
    if (previous.clasificacion !== classification) eventLog('Nivel ' + classification.toLowerCase(), `El tanque cambió a ${round1(level)}%`, level < 20 ? 'danger' : level < 40 ? 'warn' : 'info');
    if (previous.bloqueo_fuga !== blocked && blocked) eventLog('Posible fuga detectada', 'La bomba se bloqueó para proteger el sistema.', 'danger');
    if (previous.bomba !== pump) eventLog(pump ? 'LED de bomba encendido' : 'LED de bomba apagado', 'Nuevo estado recibido del dispositivo.');
  }
  previous = {clasificacion:classification,bloqueo_fuga:blocked,bomba:pump};

  $('stat-level').innerHTML = `${round1(level)}<span>%</span>`;
  $('stat-class').textContent = classification;
  $('stat-distance').innerHTML = `${round1(distance)}<span>cm</span>`;
  $('stat-external').innerHTML = `${round1(outside)}<span>%</span>`;
  $('stat-external-status').textContent = external ? 'DETECTADA' : 'SECO';
  $('stat-external-status').classList.toggle('quiet',!external);
  $('stat-pump').textContent = pump ? 'ON' : 'OFF';
  $('stat-pump-mode').textContent = mode;
  $('tank-percent').innerHTML = `${Math.round(level)}<span>%</span>`;
  $('tank-class').textContent = `Nivel ${classification.toLowerCase()}`;
  $('float-dot').style.background = classification === 'CRITICO' || blocked ? '#ff898a' : classification === 'BAJO' ? '#ffcd83' : '#5de1c9';
  updateWaterAnimation(level);
  // Leo el umbral que informa Wokwi. Si estoy moviendo la barra,
  // no le cambio el valor antes de presionar Aplicar.
  const reportedCritical = Number(raw.umbral_critico);
  if (Number.isFinite(reportedCritical) && reportedCritical >= 5 && reportedCritical <= 35) {
    currentCritical = reportedCritical;
    $('critical-current').textContent = `Actual: ${round1(currentCritical)}%`;
    if (!thresholdTouched) {
      $('critical-range').value = String(Math.round(currentCritical));
      $('critical-selected').textContent = `${Math.round(currentCritical)}%`;
    }
  }
  $('data-level').textContent = `${round1(level).toFixed(1)}%`;
  $('data-free').textContent = `${round1(100 - level).toFixed(1)}%`;
  $('data-distance').textContent = `${round1(distance).toFixed(1)} cm`;
  $('data-progress').style.width = `${clamp(level)}%`;
  $('measurement-note').textContent = source === 'demo' ? 'El nivel se actualiza automáticamente en la demostración.' : `Última lectura recibida a las ${hhmm()} desde Wokwi.`;
  $('detail-water').textContent = external ? 'Detectada' : 'No detectada';
  $('detail-drop').textContent = `${round1(drop).toFixed(1)}%`;
  $('detail-lock').textContent = blocked ? 'Activado' : 'Inactivo';
  $('detail-switch').textContent = Number(raw.switch) === 0 ? '0 · Cerrado' : '1 · Abierto';

  const alarm = blocked || status === 'POSIBLE_FUGA';
  const warn = status !== 'NORMAL' || level < 40;
  $('diagnosis-box').className = 'diagnosis-status ' + (alarm ? 'danger' : warn ? 'warn' : 'ok');
  $('diagnosis-symbol').textContent = alarm ? '!' : warn ? '△' : '✓';
  const titles = {POSIBLE_FUGA:'Posible fuga detectada',AGUA_EXTERNA:'Agua detectada afuera',DESCENSO_ANORMAL:'Descenso anormal',NIVEL_CRITICO:'Nivel crítico',NORMAL:'Todo en orden'};
  const subtitles = {POSIBLE_FUGA:'Agua afuera y descenso rápido de nivel.',AGUA_EXTERNA:'Hay agua fuera del tanque; no confirma una fuga.',DESCENSO_ANORMAL:'El nivel bajó de forma considerable.',NIVEL_CRITICO:'El tanque necesita atención.',NORMAL:'No se observan señales de fuga.'};
  $('diagnosis-title').textContent = blocked && status !== 'POSIBLE_FUGA' ? 'Bloqueo por fuga activo' : (titles[status] || status.replaceAll('_',' '));
  $('diagnosis-subtitle').textContent = blocked && status !== 'POSIBLE_FUGA' ? 'Debes rearmar el bloqueo con el sensor seco.' : (subtitles[status] || 'Estado reportado por el dispositivo.');
  $('pump-light').classList.toggle('on',pump);
  $('pump-state-title').textContent = pump ? 'LED de bomba encendido' : 'LED de bomba apagado';
  $('pump-state-sub').textContent = `${mode === 'AUTO' ? 'Modo automático' : 'Apagado manual'}${blocked ? ' · bloqueo por fuga' : ''}`;
  $('mode-auto').classList.toggle('active',mode === 'AUTO');
  $('mode-off').classList.toggle('active',mode === 'OFF');
  history.push({level, time:Date.now()});
  if (history.length > 28) history.shift();
  drawChart();
}

function drawChart() {
  $('chart-meta').textContent = `Últimas ${history.length} lecturas · ${source === 'demo' ? 'demostración' : 'MQTT'}`;
  if (!history.length) { $('chart-line').setAttribute('d',''); $('chart-area').setAttribute('d',''); $('chart-last').setAttribute('opacity','0'); return; }
  const points = history.map((p,i) => {
    const x = history.length === 1 ? 690 : 7 + i * (683 / (history.length - 1));
    return [x, 174 - clamp(p.level) * 1.7];
  });
  const line = points.map((p,i) => `${i ? 'L' : 'M'}${p[0].toFixed(2)},${p[1].toFixed(2)}`).join(' ');
  $('chart-line').setAttribute('d',line);
  $('chart-area').setAttribute('d',`${line} L${points.at(-1)[0]},174 L${points[0][0]},174 Z`);
  $('chart-last').setAttribute('cx',points.at(-1)[0]);
  $('chart-last').setAttribute('cy',points.at(-1)[1]);
  $('chart-last').setAttribute('opacity','1');
  $('chart-start').textContent = history.length > 1 ? hhmm(new Date(history[0].time)) : 'Primera lectura';
  $('chart-end').textContent = hhmm(new Date(history.at(-1).time));
}

function updateTelemetryControls() {
  // Estos estados ayudan a distinguir una pausa voluntaria de una falla de red.
  const badge = $('telemetry-status');
  if (source === 'live' && !mqttConnected) {
    badge.className = 'telemetry-status waiting';
    badge.textContent = '● MQTT sin conexión';
  } else if (pendingControl?.type === 'telemetria') {
    badge.className = 'telemetry-status waiting';
    badge.textContent = '● Esperando confirmación';
  } else if (telemetryPaused === true) {
    badge.className = 'telemetry-status paused';
    badge.textContent = 'Ⅱ Telemetría pausada';
  } else {
    badge.className = 'telemetry-status running';
    badge.textContent = telemetryPaused === false ? '● Enviando lecturas' : '● Estado por confirmar';
  }
  $('telemetry-resume').classList.toggle('active', telemetryPaused === false);
  $('telemetry-pause').classList.toggle('active', telemetryPaused === true);
  const ready = source === 'demo' || mqttConnected;
  $('telemetry-resume').disabled = !ready || pendingControl?.type === 'telemetria';
  $('telemetry-pause').disabled = !ready || pendingControl?.type === 'telemetria';
  $('critical-apply').disabled = !ready || pendingControl?.type === 'umbral_critico';
  $('telemetry-help').textContent = source === 'demo'
    ? 'En demostración puedes probar los botones sin enviar órdenes reales.'
    : ready ? 'La orden se aplica cuando el ESP32 confirma la respuesta por MQTT.'
      : 'Conecta MQTT para enviar estas órdenes al ESP32.';
}

function updateControls() {
  const enabled = source === 'demo' || mqttConnected;
  $('mode-auto').disabled = !enabled;
  $('mode-off').disabled = !enabled;
  $('reset-leak').disabled = !enabled;
  $('control-hint').textContent = source === 'demo' ? 'Puedes probar los botones: no se envían órdenes reales.' : enabled ? 'Cada botón envía una orden MQTT al ESP32.' : 'Los controles se habilitan al conectar MQTT.';
  updateTelemetryControls();
}

function setDemo() {
  mqtt?.close(); mqtt = null; mqttConnected = false;
  source = 'demo'; previous = null; history = []; events = [];
  demoMoving = true; demoFuga = false; demoLevel = 64; demoPumpMode = 'AUTO';
  demoCritical = 20;telemetryPaused = false;currentCritical = 20;thresholdTouched = false;
  pendingControl = null;
  $('critical-range').value = '20';$('critical-selected').textContent = '20%';
  $('demo-controls').classList.remove('hidden');
  $('toggle-mode').innerHTML = '<svg><use href="#i-wifi"/></svg>Conectar MQTT';
  $('demo-slider').value = '64';$('demo-value').textContent = '64%';
  for (let i = 17; i >= 1; i--) history.push({level: Math.max(0,64 + (i - 9) * 0.95 + Math.sin(i)*2),time:Date.now() - i*2500});
  connectionStatus('demo','Modo demostración');
  eventLog('Demostración activa','Tanque animado con datos simulados.');
  renderReading(demoData());updateControls();
}

function startLive() {
  if (mqtt) mqtt.close();
  source = 'live';mqttConnected = false;lastTelemetry = 0;deviceStatus='desconocido';
  previous = null;history=[];events=[];
  telemetryPaused = null;pendingControl = null;thresholdTouched = false;
  $('demo-controls').classList.add('hidden');
  $('toggle-mode').innerHTML = '<svg><use href="#i-refresh"/></svg>Volver a demo';
  connectionStatus('waiting','Conectando MQTT…');
  showNoData();drawChart();updateControls();
  eventLog('Conectando al broker','Esperando respuesta de ' + config.url);
  mqtt = useLocalBridge
    ? new LocalBridge(onMqttMessage,onMqttStatus)
    : new BrowserMQTT(config.url,onMqttMessage,onMqttStatus);
  mqtt.connect();
}

function onMqttStatus(kind, detail='') {
  if (source !== 'live') return;
  if (kind === 'connected') {
    mqttConnected = true;
    connectionStatus('waiting','Esperando datos del ESP32');
    eventLog('Broker MQTT conectado','Suscripción a los temas de AquaNivel.');
    toast('Conectado al broker. Ahora inicia Wokwi.');
  } else if (kind === 'error' || kind === 'closed') {
    mqttConnected = false;
    connectionStatus('offline','Sin conexión MQTT');
    eventLog('No se pudo conectar',detail || 'El WebSocket MQTT está desconectado.','warn');
    if (detail) toast(detail,true);
  }
  updateControls();
}

function onMqttMessage(topic,message) {
  if (source !== 'live') return;
  if (topic === `${baseTopic()}/estado`) {
    deviceStatus = message.trim().toLowerCase();
    if (deviceStatus === 'offline') {connectionStatus('offline','ESP32 desconectado'); eventLog('ESP32 desconectado','El broker reportó estado offline.','warn');}
    else if (deviceStatus === 'online' && !lastTelemetry) connectionStatus('waiting','ESP32 en línea · sin lecturas');
  } else if (topic === `${baseTopic()}/telemetria`) {
    try {
      const data = JSON.parse(message);
      if (!Number.isFinite(data.nivel_pct) || !Number.isFinite(data.distancia_cm)) throw new Error('Lectura incompleta');
      lastTelemetry = Date.now();deviceStatus='online';
      // Si siguen entrando mediciones, el envío no está pausado.
      telemetryPaused = false;
      connectionStatus('online','Recibiendo datos en vivo');
      renderReading(data);updateTelemetryControls();
    } catch (error) {eventLog('Mensaje no válido', String(error), 'warn');}
  } else if (topic === `${baseTopic()}/respuesta`) {
    try {
      const result = JSON.parse(message);
      const detail = String(result.detalle || '');
      // El ESP32 responde por el tema /respuesta después de procesar la orden.
      // Con esa respuesta actualizo la pantalla, no solamente con el clic.
      if (result.aceptada && detail.startsWith('Telemetria: ')) {
        telemetryPaused = detail.includes('PAUSAR');
        if (telemetryPaused) connectionStatus('waiting','Telemetría pausada por orden');
        else connectionStatus('waiting','Reanudando telemetría…');
      }
      if (result.aceptada && detail.startsWith('Nuevo umbral: ')) {
        const matched = detail.match(/Nuevo umbral:\s*(\d+(?:\.\d+)?)%/);
        if (matched) {
          currentCritical = Number(matched[1]);
          thresholdTouched = false;
          $('critical-current').textContent = `Actual: ${round1(currentCritical)}%`;
          $('critical-range').value = String(Math.round(currentCritical));
          $('critical-selected').textContent = `${Math.round(currentCritical)}%`;
        }
      }
      if (pendingControl) pendingControl = null;
      updateTelemetryControls();
      eventLog(result.aceptada ? 'Orden confirmada' : 'Orden rechazada',result.detalle || 'Sin detalle',result.aceptada ? 'info':'warn');
      toast(result.detalle || 'Respuesta recibida',!result.aceptada);
    } catch {eventLog('Respuesta MQTT recibida',message.slice(0,85));}
  }
}

function sendOrder(type, value) {
  if (source === 'demo') {
    if (type === 'modo_bomba') {demoPumpMode = value;renderReading(demoData());eventLog('Modo cambiado',`Simulación: ${value}`);}
    if (type === 'rearme') {if (demoFuga) {toast('Primero detén la fuga simulada.',true);return;} eventLog('Bloqueo rearmado','Demostración sin fuga.');}
    if (type === 'telemetria') {
      telemetryPaused = value === 'PAUSAR';
      demoMoving = !telemetryPaused;
      eventLog(telemetryPaused ? 'Telemetría pausada' : 'Telemetría reanudada','Orden de prueba en el dashboard.');
      if (!telemetryPaused) renderReading(demoData());
    }
    if (type === 'umbral_critico') {
      demoCritical = value;currentCritical=value;thresholdTouched=false;
      $('critical-current').textContent = `Actual: ${value}%`;
      eventLog('Umbral actualizado',`Demostración: ${value}%`);
      if (!telemetryPaused) renderReading(demoData());
    }
    updateControls();
    return;
  }
  // El mismo tema /orden sirve para bomba, telemetría, umbral y rearme.
  if (!mqtt?.publish(`${baseTopic()}/orden`,JSON.stringify({tipo:type,valor:value}))) {toast('No hay conexión MQTT. Revisa Wokwi.',true);return;}
  if (type === 'telemetria' || type === 'umbral_critico') {
    const sent = {type,value};
    pendingControl = sent;
    updateTelemetryControls();
    setTimeout(() => {
      if (pendingControl !== sent) return;
      pendingControl = null;
      updateTelemetryControls();
      toast('El ESP32 todavía no confirmó la orden. Puedes intentar otra vez.',true);
    },8000);
  }
  toast('Orden enviada. Esperando respuesta del ESP32.');
}

function syncInputs() {
  $('broker-url').value = config.url;
  $('group-input').value = config.group;
  $('device-input').value = config.device;
  previewTopic();
}

function previewTopic() {
  $('topic-preview').textContent = `utp/ds8/${$('group-input').value.trim()}/aquanivel/${$('device-input').value.trim()}/telemetria`;
}

function openSettings() {
  $('settings-modal').classList.remove('hidden');
  syncInputs();
  $('connection-info').textContent = useLocalBridge
    ? 'Estás usando el puente Python de tu computadora. Al conectar, el panel leerá el MQTT recibido por ese programa. Si cambiaste el grupo o dispositivo en Wokwi, debes cambiarlo también en puente_dashboard.py.'
    : 'Usamos los mismos temas MQTT que el ESP32 del Avance 3. Solo debes iniciar Wokwi y conectar este panel.';
  for (const id of ['broker-url','group-input','device-input']) $(id).disabled = useLocalBridge;
  (useLocalBridge ? $('connect-settings') : $('broker-url')).focus();
}
function closeSettings() {$('settings-modal').classList.add('hidden');}

function useSettings() {
  const url = $('broker-url').value.trim();
  const group = $('group-input').value.trim();
  const device = $('device-input').value.trim();
  if (!/^wss:\/\/[^\s]+$/i.test(url)) {toast('Usa una dirección segura que empiece con wss://',true);return;}
  if (!/^[A-Za-z0-9_-]{1,40}$/.test(group) || !/^[A-Za-z0-9_-]{1,40}$/.test(device)) {toast('Grupo y dispositivo: solo letras, números, guion y guion bajo.',true);return;}
  Object.assign(config,{url,group,device});
  try {localStorage.setItem('aquanivel-mqtt',JSON.stringify(config));} catch {}
  closeSettings();startLive();
}

// Las simulaciones solo cambian lo que vemos en la web; no alteran Wokwi.
setDemo();
setInterval(() => {
  $('local-clock').textContent = new Date().toLocaleTimeString('es-PA',{hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});
  // Si yo mismo pausé la telemetría, no muestro una falsa alerta de desconexión.
  if (source === 'live' && mqttConnected && telemetryPaused !== true && (!lastTelemetry || Date.now()-lastTelemetry > 16000)) {
    if ($('connection-pill').classList.contains('online')) {connectionStatus('waiting','Sin lecturas recientes');eventLog('Telemetría interrumpida','El ESP32 no envía datos desde hace más de 16 segundos.','warn');}
  }
},1000);

setInterval(() => {
  if (source !== 'demo' || !demoMoving || demoFuga) return;
  demoLevel = clamp(demoLevel + demoDirection * (3 + Math.random()*2.5),4,93);
  if (demoLevel < 8) demoDirection = 1;
  if (demoLevel > 92) demoDirection = -1;
  $('demo-slider').value = String(Math.round(demoLevel));
  $('demo-value').textContent = `${Math.round(demoLevel)}%`;
  renderReading(demoData());
},2800);

$('toggle-mode').addEventListener('click',() => source === 'demo' ? openSettings() : setDemo());
for (const id of ['open-settings','quick-settings']) $(id).addEventListener('click',openSettings);
for (const id of ['close-settings','cancel-settings']) $(id).addEventListener('click',closeSettings);
$('settings-modal').addEventListener('click',(e) => {if(e.target === $('settings-modal')) closeSettings();});
document.addEventListener('keydown',(e) => {if(e.key === 'Escape')closeSettings();});
$('group-input').addEventListener('input',previewTopic);
$('device-input').addEventListener('input',previewTopic);
$('connect-settings').addEventListener('click',useSettings);
$('mode-auto').addEventListener('click',() => sendOrder('modo_bomba','AUTO'));
$('mode-off').addEventListener('click',() => sendOrder('modo_bomba','OFF'));
$('reset-leak').addEventListener('click',() => sendOrder('rearme','CONFIRMAR'));
$('telemetry-pause').addEventListener('click',() => sendOrder('telemetria','PAUSAR'));
$('telemetry-resume').addEventListener('click',() => sendOrder('telemetria','REANUDAR'));
$('critical-range').addEventListener('input', (event) => {
  thresholdTouched = true;
  $('critical-selected').textContent = `${Math.round(Number(event.target.value))}%`;
});
$('critical-apply').addEventListener('click',() => {
  const value = Number($('critical-range').value);
  if (!Number.isInteger(value) || value < 5 || value > 35) {
    toast('El umbral debe estar entre 5% y 35%.',true);
    return;
  }
  sendOrder('umbral_critico',value);
});
$('demo-slider').addEventListener('input', (e) => {demoMoving=false;demoLevel=Number(e.target.value);$('demo-value').textContent=`${Math.round(demoLevel)}%`;if (!telemetryPaused) renderReading(demoData());});
$('demo-fuga').addEventListener('click',() => {
  demoMoving=false;
  demoFuga=!demoFuga;
  if (demoFuga) {demoLevel=clamp(demoLevel-12);eventLog('Prueba de posible fuga','Agua exterior y caída de nivel detectadas.','danger');}
  else eventLog('Agua externa retirada','La demostración vuelve al estado normal.');
  $('demo-fuga').textContent = demoFuga ? 'Quitar fuga' : 'Probar fuga';
  $('demo-slider').value=String(Math.round(demoLevel));$('demo-value').textContent=`${Math.round(demoLevel)}%`;
  if (!telemetryPaused) renderReading(demoData());
});
$('demo-auto').addEventListener('click',() => {demoMoving=true;demoFuga=false;demoDirection=1;demoPumpMode='AUTO';telemetryPaused=false;$('demo-fuga').textContent='Probar fuga';renderReading(demoData());updateControls();toast('Simulación automática reanudada.');});
document.querySelectorAll('a.nav-item').forEach((a) => a.addEventListener('click',() => {document.querySelectorAll('a.nav-item').forEach((b) => b.classList.remove('active'));a.classList.add('active');}));
