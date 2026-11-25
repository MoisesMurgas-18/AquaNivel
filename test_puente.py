"""Pruebas locales del puente, sin conectar a ningún broker externo."""

import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.request import Request, urlopen

import puente_dashboard as bridge
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode


class FakeClient:
    def __init__(self):
        self.published = []
        self.subscribed = []

    def subscribe(self, topic, qos=0):
        self.subscribed.append((topic, qos))

    def publish(self, topic, payload, qos=0):
        self.published.append((topic, json.loads(payload), qos))
        return type("Result", (), {"rc": 0})()


class TestPuente(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        bridge.client = FakeClient()
        with bridge.lock:
            bridge.state.update(connected=True, payload={"nivel_pct": 48.5}, revision=1)
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), bridge.WebHandler)
        cls.server.timeout = 2
        cls.address = f"http://127.0.0.1:{cls.server.server_port}"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_sirve_panel(self):
        with urlopen(self.address) as response:
            self.assertEqual(response.status, 200)
            self.assertIn(b"AquaNivel", response.read())

    def test_sirve_estilos_del_tanque(self):
        # Verifica que el navegador reciba los ajustes de animación.
        with urlopen(self.address + "/refinamientos.css") as response:
            self.assertEqual(response.status, 200)
            self.assertIn(b"water-fill", response.read())

    def test_codigo_de_conexion_paho2(self):
        # Reproduce el mismo objeto ReasonCode que genero el error en Windows.
        fake = FakeClient()
        reason = ReasonCode(PacketTypes.CONNACK, "Success")
        bridge.on_connect(fake, None, None, reason, None)
        self.assertTrue(bridge.state["connected"])
        self.assertEqual(len(fake.subscribed), 3)

    def test_favicon_sin_error(self):
        with urlopen(self.address + "/favicon.ico") as response:
            self.assertEqual(response.status, 204)

    def test_entrega_lectura(self):
        with urlopen(self.address + "/api/snapshot") as response:
            data = json.load(response)
            self.assertTrue(data["connected"])
            self.assertEqual(data["payload"]["nivel_pct"], 48.5)

    def test_envia_orden(self):
        data = json.dumps({"tipo": "modo_bomba", "valor": "OFF"}).encode()
        with urlopen(Request(self.address + "/api/orden", data=data, method="POST")) as response:
            self.assertEqual(response.status, 200)
        self.assertEqual(bridge.client.published[-1][1]["valor"], "OFF")

    def test_controla_pausa_de_telemetria(self):
        for accion in ("PAUSAR", "REANUDAR"):
            data = json.dumps({"tipo": "telemetria", "valor": accion}).encode()
            with urlopen(Request(self.address + "/api/orden", data=data, method="POST")) as response:
                self.assertEqual(response.status, 200)
            self.assertEqual(bridge.client.published[-1][1], {"tipo": "telemetria", "valor": accion})

    def test_cambia_umbral_critico(self):
        data = json.dumps({"tipo": "umbral_critico", "valor": 28}).encode()
        with urlopen(Request(self.address + "/api/orden", data=data, method="POST")) as response:
            self.assertEqual(response.status, 200)
        self.assertEqual(bridge.client.published[-1][1]["valor"], 28)

    def test_rechaza_umbral_fuera_de_rango(self):
        for valor in (4, 36, True, "20"):
            data = json.dumps({"tipo": "umbral_critico", "valor": valor}).encode()
            with self.assertRaises(Exception):
                urlopen(Request(self.address + "/api/orden", data=data, method="POST"))

    def test_no_publica_orden_peligrosa(self):
        data = json.dumps({"tipo": "cualquier_cosa", "valor": "ON"}).encode()
        with self.assertRaises(Exception):
            urlopen(Request(self.address + "/api/orden", data=data, method="POST"))


if __name__ == "__main__":
    unittest.main()
