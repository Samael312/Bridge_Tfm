"""
bridge.py — Railway
-------------------
Suscriptor MQTT que persiste en PostgreSQL.
Escucha TODOS los sensores bajo el topic base (uja/#).
"""

import json
import os
import time
from datetime import datetime, timezone

import psycopg2
import paho.mqtt.client as mqtt

# ==========================================
# CONFIGURACIÓN DESDE VARIABLES DE ENTORNO
# ==========================================
DB_URL      = os.getenv("DATABASE_URL")
# Acepta tanto MQTT_BROKER como MQTT_HOST para compatibilidad
MQTT_BROKER = os.getenv("MQTT_HOST", "autorack.proxy.rlwy.net")
MQTT_PORT   = int(os.getenv("MQTT_PORT", 35512))
# El topic base sin slash inicial (ej: "uja")
MQTT_TOPIC_BASE = os.getenv("MQTT_TOPIC_BASE", "uja").lstrip("/")
# Suscripción a todos los sensores: uja/#
TOPIC_SUB   = f"{MQTT_TOPIC_BASE}/#"

print("=" * 60)
print("🚀 Iniciando Bridge MQTT → Postgres")
print(f"   MQTT_BROKER    : {MQTT_BROKER}")
print(f"   MQTT_PORT      : {MQTT_PORT}")
print(f"   MQTT_TOPIC_BASE: {MQTT_TOPIC_BASE}")
print(f"   TOPIC_SUB      : {TOPIC_SUB}")
print(f"   DATABASE_URL   : {'SET' if DB_URL else '❌ NO DEFINIDA'}")
print("=" * 60)

# ==========================================
# VARIABLES SFA CONOCIDAS
# ==========================================
KNOWN_VARIABLES = {
    "radiacion",
    "temp_amb",
    "i_generada",
    "v_bateria",
    "i_carga",
    "temp_pan",
    "temp_bat",   # ← era "tamp_bat" (typo corregido)
}

print(f"📋 Variables conocidas: {KNOWN_VARIABLES}")

# ==========================================
# HELPER: PARSEAR TOPIC
# Formato esperado: uja/{sensor_id}/{variable}
# ==========================================
def _parse_topic(topic: str):
    """
    Extrae (sensor_id, variable) de cualquier topic con formato:
      {MQTT_TOPIC_BASE}/{sensor_id}/{variable}
    Devuelve (None, None) si el formato no coincide.
    """
    prefix = MQTT_TOPIC_BASE + "/"
    if not topic.startswith(prefix):
        print(f"   ⚠️  Topic '{topic}' no empieza por '{prefix}' — ignorando")
        return None, None

    rest = topic[len(prefix):]      # "s1/radiacion" o "s2/v_bateria"
    parts = rest.split("/")
    if len(parts) != 2:
        print(f"   ⚠️  Topic '{topic}' tiene estructura inesperada (partes={parts}) — ignorando")
        return None, None

    sensor_id, variable = parts[0], parts[1]
    return sensor_id, variable


# ==========================================
# CONEXIÓN A POSTGRESQL CON REINTENTOS
# ==========================================
conn   = None
cursor = None

while True:
    try:
        print("🔗 Conectando a PostgreSQL...")
        conn   = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        print("✅ Conexión a PostgreSQL establecida")

        # ── Tablas ──────────────────────────────────────────────
        print("🏗️  Creando/verificando tablas...")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                name VARCHAR(255),
                surname VARCHAR(255),
                password_hash VARCHAR(255) NOT NULL,
                reset_token VARCHAR(255),
                reset_token_expires TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS telemetria (
                id      SERIAL PRIMARY KEY,
                topic   TEXT,
                payload JSONB,
                fecha   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sfa_readings (
                id            BIGSERIAL        PRIMARY KEY,
                timestamp     TIMESTAMPTZ      NOT NULL,
                sensor_id     VARCHAR(64)      NOT NULL,
                variable      VARCHAR(64)      NOT NULL,
                value         DOUBLE PRECISION NOT NULL,
                source        VARCHAR(20)      DEFAULT 'mqtt',
                telemetria_id BIGINT
            );
            CREATE INDEX IF NOT EXISTS idx_sfa_readings_sensor_var_ts
                ON sfa_readings (sensor_id, variable, timestamp DESC);
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sfa_alerts (
                id         BIGSERIAL        PRIMARY KEY,
                reading_id BIGINT           REFERENCES sfa_readings(id) ON DELETE CASCADE,
                timestamp  TIMESTAMPTZ      NOT NULL,
                sensor_id  VARCHAR(64)      NOT NULL,
                level      VARCHAR(10)      NOT NULL,
                variable   VARCHAR(64)      NOT NULL,
                value      DOUBLE PRECISION NOT NULL,
                message    TEXT             NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sfa_alerts_sensor_ts
                ON sfa_alerts (sensor_id, timestamp DESC);
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_rules (
                id         BIGSERIAL    PRIMARY KEY,
                sensor_id  VARCHAR(64)  NOT NULL,
                variable   VARCHAR(64)  NOT NULL,
                operator   VARCHAR(2)   NOT NULL,
                threshold  DOUBLE PRECISION NOT NULL,
                level      VARCHAR(10)  NOT NULL DEFAULT 'warning',
                message    TEXT         NOT NULL,
                created_at TIMESTAMPTZ  DEFAULT NOW(),
                UNIQUE (sensor_id, variable, operator)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_snooze (
                id         BIGSERIAL    PRIMARY KEY,
                sensor_id  VARCHAR(64)  NOT NULL,
                variable   VARCHAR(64),
                until_ts   TIMESTAMPTZ  NOT NULL,
                created_at TIMESTAMPTZ  DEFAULT NOW(),
                UNIQUE (sensor_id, variable)
            );
            CREATE INDEX IF NOT EXISTS idx_alert_snooze_sensor
                ON alert_snooze (sensor_id, until_ts DESC);
        """)

        # ── Trigger NOTIFY ──────────────────────────────────────
        print("⚡ Configurando trigger NOTIFY...")

        cursor.execute("""
            CREATE OR REPLACE FUNCTION notify_sfa_update()
            RETURNS trigger AS $$
            BEGIN
              PERFORM pg_notify(
                'sfa_update',
                json_build_object(
                  'sensor_id', NEW.sensor_id,
                  'variable',  NEW.variable,
                  'value',     NEW.value,
                  'timestamp', NEW.timestamp,
                  'source',    NEW.source
                )::text
              );
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)

        cursor.execute("""
            DROP TRIGGER IF EXISTS trg_sfa_update ON sfa_readings;
            CREATE TRIGGER trg_sfa_update
            AFTER INSERT ON sfa_readings
            FOR EACH ROW
            EXECUTE FUNCTION notify_sfa_update();
        """)

        conn.commit()
        print("✅ Esquema de BD listo (tablas, índices, triggers)")

        # ── Estadísticas iniciales ──────────────────────────────
        cursor.execute("SELECT sensor_id, COUNT(*) FROM sfa_readings GROUP BY sensor_id ORDER BY sensor_id")
        rows = cursor.fetchall()
        if rows:
            print("📊 Lecturas actuales en BD:")
            for r in rows:
                print(f"   sensor={r[0]}  filas={r[1]}")
        else:
            print("📊 BD vacía — sin lecturas previas")

        break

    except Exception as e:
        print(f"⏳ Error configurando BD: {e} — reintentando en 5s...")
        if conn:
            conn.rollback()
        time.sleep(5)


# ==========================================
# CONTADORES DE DIAGNÓSTICO
# ==========================================
_stats = {
    "mensajes_recibidos": 0,
    "lecturas_insertadas": 0,
    "errores": 0,
    "topics_ignorados": 0,
    "variables_ignoradas": 0,
}


# ==========================================
# HELPERS DB
# ==========================================
def _insert_telemetria(topic: str, payload_dict: dict) -> int:
    cursor.execute(
        "INSERT INTO telemetria (topic, payload) VALUES (%s, %s) RETURNING id",
        (topic, json.dumps(payload_dict))
    )
    return cursor.fetchone()[0]


def _insert_reading(sensor_id: str, variable: str, value: float,
                    timestamp: datetime, source: str, telemetria_id: int) -> int:
    cursor.execute("""
        INSERT INTO sfa_readings
            (timestamp, sensor_id, variable, value, source, telemetria_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (timestamp, sensor_id, variable, value, source, telemetria_id))
    return cursor.fetchone()[0]


# ==========================================
# CALLBACKS MQTT
# ==========================================
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ Conectado al broker MQTT: {MQTT_BROKER}:{MQTT_PORT}")
        result, mid = client.subscribe(TOPIC_SUB)
        print(f"📡 Suscrito a '{TOPIC_SUB}' (result={result}, mid={mid})")
    else:
        codes = {1: "protocolo", 2: "client_id", 3: "broker no disponible",
                 4: "credenciales", 5: "no autorizado"}
        print(f"❌ Fallo MQTT connect — rc={rc} ({codes.get(rc, 'desconocido')})")


def on_disconnect(client, userdata, rc, properties=None):
    if rc == 0:
        print("🔌 Desconectado limpiamente del broker MQTT")
    else:
        print(f"⚠️  Desconexión inesperada del broker MQTT (rc={rc}) — paho reconectará")


def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print(f"✅ Suscripción confirmada: mid={mid}, QoS={granted_qos}")


def on_message(client, userdata, msg):
    _stats["mensajes_recibidos"] += 1
    topic = msg.topic

    # Log cada 10 mensajes para no saturar
    verbose = (_stats["mensajes_recibidos"] % 10 == 1)
    if verbose:
        print(f"\n📨 Mensaje #{_stats['mensajes_recibidos']} en '{topic}'")

    try:
        raw = msg.payload.decode()

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            print(f"   ⚠️  Payload no es JSON: {raw[:80]}")
            payload = {"raw_text": raw}

        # 1. Insertar en telemetria (siempre)
        tel_id = _insert_telemetria(topic, payload)

        # 2. Parsear topic
        sensor_id, variable = _parse_topic(topic)

        if sensor_id is None:
            _stats["topics_ignorados"] += 1
            conn.commit()
            return

        if verbose:
            print(f"   sensor_id={sensor_id}  variable={variable}")

        # 3. Verificar variable conocida
        if variable not in KNOWN_VARIABLES:
            _stats["variables_ignoradas"] += 1
            if verbose:
                print(f"   ⚠️  Variable '{variable}' no está en KNOWN_VARIABLES — solo guardado en telemetria")
            conn.commit()
            return

        # 4. Extraer valor numérico
        raw_value = payload.get("value") or payload.get(variable)
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            print(f"   ❌ No se pudo convertir a float: raw_value={raw_value!r}")
            _stats["errores"] += 1
            conn.commit()
            return

        # 5. Timestamp
        ts_raw = payload.get("timestamp")
        if ts_raw:
            try:
                ts = datetime.fromisoformat(ts_raw)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except ValueError:
                print(f"   ⚠️  Timestamp inválido '{ts_raw}' — usando NOW()")
                ts = datetime.now(timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        source = payload.get("source", "mqtt")

        # 6. Insertar reading
        reading_id = _insert_reading(sensor_id, variable, value, ts, source, tel_id)
        _stats["lecturas_insertadas"] += 1

        conn.commit()

        print(f"   ✅ [{sensor_id}] {variable}={value} src={source} reading_id={reading_id}"
              f"  [total insertadas: {_stats['lecturas_insertadas']}]")

    except Exception as e:
        _stats["errores"] += 1
        print(f"   ❌ Error procesando mensaje: {e}")
        if conn:
            conn.rollback()


def on_log(client, userdata, level, buf):
    # Solo mostrar errores internos de paho
    if level <= mqtt.MQTT_LOG_WARNING:
        print(f"   [paho] {buf}")


# ==========================================
# CLIENTE MQTT
# ==========================================
print("\n🔌 Creando cliente MQTT...")
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect    = on_connect
client.on_disconnect = on_disconnect
client.on_subscribe  = on_subscribe
client.on_message    = on_message
client.on_log        = on_log

print(f"🔄 Conectando a {MQTT_BROKER}:{MQTT_PORT}...")
while True:
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        print("✅ Conexión MQTT establecida")
        break
    except Exception as e:
        print(f"⏳ Broker no disponible ({e}) — reintentando en 5s...")
        time.sleep(5)

print("\n🎧 Escuchando mensajes MQTT... (Ctrl+C para detener)\n")

try:
    client.loop_forever()
except KeyboardInterrupt:
    print("\n🛑 Bridge detenido manualmente")
    print(f"📊 Estadísticas finales: {_stats}")
finally:
    if cursor: cursor.close()
    if conn:   conn.close()
    print("🔒 Conexiones cerradas")