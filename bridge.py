"""
bridge.py — Railway
-------------------
Suscriptor MQTT que persiste en PostgreSQL.

Topics soportados (ambos, con y sin slash inicial):
  uja/{sensor_id}/{variable}
 /uja/{sensor_id}/{variable}

El mock por defecto publica a /uja/... por lo que es necesario
suscribirse a ambas variantes.
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
MQTT_BROKER = os.getenv("MQTT_BROKER", os.getenv("MQTT_HOST", "mqtt-server"))
MQTT_PORT   = int(os.getenv("MQTT_PORT", 1883))

# MQTT_TOPIC_BASE puede llegar con o sin slash inicial (ej: "uja" o "/uja")
# Normalizamos a SIN slash para comparaciones internas
_RAW_BASE       = os.getenv("MQTT_TOPIC_BASE", "/uja")
MQTT_TOPIC_BASE = _RAW_BASE.lstrip("/")          # → "uja"

# Suscribirse a AMBAS variantes porque el mock usa "/uja" por defecto
TOPIC_SUB_NO_SLASH = f"{MQTT_TOPIC_BASE}/#"      # uja/#
TOPIC_SUB_SLASH    = f"/{MQTT_TOPIC_BASE}/#"     # /uja/#

print("=" * 60)
print("🚀 Iniciando Bridge MQTT → Postgres")
print(f"   MQTT_BROKER      : {MQTT_BROKER}")
print(f"   MQTT_PORT        : {MQTT_PORT}")
print(f"   MQTT_TOPIC_BASE  : '{_RAW_BASE}' → normalizado: '{MQTT_TOPIC_BASE}'")
print(f"   SUSCRIPCIÓN #1   : {TOPIC_SUB_NO_SLASH}")
print(f"   SUSCRIPCIÓN #2   : {TOPIC_SUB_SLASH}")
print(f"   DATABASE_URL     : {'✅ SET' if DB_URL else '❌ NO DEFINIDA — ABORTANDO'}")
print("=" * 60)

if not DB_URL:
    raise RuntimeError("DATABASE_URL no está definida")

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
    "temp_bat",    # ← corregido (era "tamp_bat")
}

print(f"📋 Variables conocidas: {sorted(KNOWN_VARIABLES)}")

# ==========================================
# HELPER: PARSEAR TOPIC
# Acepta tanto "uja/s1/radiacion" como "/uja/s1/radiacion"
# ==========================================
def _parse_topic(topic: str):
    """
    Extrae (sensor_id, variable) normalizando el slash inicial.
    Formato: [/]uja/{sensor_id}/{variable}
    """
    clean = topic.lstrip("/")               # quitar slash inicial si existe
    prefix = MQTT_TOPIC_BASE + "/"          # "uja/"

    if not clean.startswith(prefix):
        return None, None

    rest = clean[len(prefix):]              # "s2/radiacion"
    parts = rest.split("/")
    if len(parts) != 2:
        return None, None

    return parts[0], parts[1]              # ("s2", "radiacion")


# ==========================================
# CONEXIÓN A POSTGRESQL CON REINTENTOS
# ==========================================
conn   = None
cursor = None

while True:
    try:
        print("\n🔗 Conectando a PostgreSQL...")
        conn   = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        print("✅ PostgreSQL conectado")

        print("🏗️  Creando/verificando esquema...")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id                  SERIAL PRIMARY KEY,
                username            VARCHAR(255) UNIQUE NOT NULL,
                email               VARCHAR(255) UNIQUE NOT NULL,
                name                VARCHAR(255),
                surname             VARCHAR(255),
                password_hash       VARCHAR(255) NOT NULL,
                reset_token         VARCHAR(255),
                reset_token_expires TIMESTAMPTZ,
                created_at          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
            CREATE TABLE IF NOT EXISTS soc_state (
            sensor_id       VARCHAR(64)      PRIMARY KEY,
            soc_pct         DOUBLE PRECISION NOT NULL DEFAULT 50.0,
            last_calibrated TIMESTAMPTZ,
            calibration_soc DOUBLE PRECISION,
            updated_at      TIMESTAMPTZ      DEFAULT NOW()
            );
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
                id        BIGSERIAL        PRIMARY KEY,
                sensor_id VARCHAR(64)      NOT NULL,
                variable  VARCHAR(64)      NOT NULL,
                operator  VARCHAR(2)       NOT NULL,
                threshold DOUBLE PRECISION NOT NULL,
                level     VARCHAR(10)      NOT NULL DEFAULT 'warning',
                message   TEXT             NOT NULL,
                created_at TIMESTAMPTZ     DEFAULT NOW(),
                UNIQUE (sensor_id, variable, operator)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_snooze (
                id         BIGSERIAL   PRIMARY KEY,
                sensor_id  VARCHAR(64) NOT NULL,
                variable   VARCHAR(64),
                until_ts   TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (sensor_id, variable)
            );
            CREATE INDEX IF NOT EXISTS idx_alert_snooze_sensor
                ON alert_snooze (sensor_id, until_ts DESC);
        """)

        # Trigger NOTIFY para WebSocket en tiempo real
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
            FOR EACH ROW EXECUTE FUNCTION notify_sfa_update();
        """)

        conn.commit()
        print("✅ Esquema listo (tablas + índices + triggers)")

        # Estado actual de la BD
        cursor.execute("""
            SELECT sensor_id, COUNT(*) as total,
                   MAX(timestamp) as ultimo,
                   EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/60 as minutos_ago
            FROM sfa_readings
            GROUP BY sensor_id ORDER BY sensor_id
        """)
        rows = cursor.fetchall()
        if rows:
            print("\n📊 Estado actual en BD:")
            for r in rows:
                mins = int(r[3]) if r[3] else None
                print(f"   sensor={r[0]}  filas={r[1]}  último dato hace {mins}m")
        else:
            print("📊 BD vacía — sin lecturas previas")

        break

    except Exception as e:
        print(f"⏳ Error configurando BD: {e} — reintentando en 5s...")
        if conn:
            conn.rollback()
        time.sleep(5)


# ==========================================
# CONTADORES
# ==========================================
_stats = {
    "recibidos":  0,
    "insertados": 0,
    "ignorados":  0,
    "errores":    0,
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


def _insert_reading(sensor_id, variable, value, timestamp, source, telemetria_id):
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
        print(f"\n✅ Conectado al broker MQTT: {MQTT_BROKER}:{MQTT_PORT}")
        # Suscribir a AMBAS variantes (con y sin slash inicial)
        r1, m1 = client.subscribe(TOPIC_SUB_NO_SLASH)
        r2, m2 = client.subscribe(TOPIC_SUB_SLASH)
        print(f"📡 Suscrito a '{TOPIC_SUB_NO_SLASH}' (rc={r1}, mid={m1})")
        print(f"📡 Suscrito a '{TOPIC_SUB_SLASH}'  (rc={r2}, mid={m2})")
    else:
        codes = {
            1: "protocolo incompatible",
            2: "client_id inválido",
            3: "broker no disponible",
            4: "credenciales incorrectas",
            5: "no autorizado",
        }
        print(f"❌ Fallo MQTT connect rc={rc}: {codes.get(rc, 'desconocido')}")


def on_disconnect(client, userdata, rc, properties=None):
    if rc == 0:
        print("🔌 Desconectado limpiamente del broker")
    else:
        print(f"⚠️  Desconexión inesperada rc={rc} — paho reconectará automáticamente")


def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print(f"✅ Suscripción confirmada mid={mid} QoS={granted_qos}")


def on_message(client, userdata, msg):
    _stats["recibidos"] += 1
    topic = msg.topic

    # Log detallado primer mensaje y luego cada 20
    verbose = (_stats["recibidos"] == 1 or _stats["recibidos"] % 20 == 0)
    if verbose:
        print(f"\n📨 Mensaje #{_stats['recibidos']} | topic='{topic}' | "
              f"insertados={_stats['insertados']} errores={_stats['errores']}")

    try:
        raw = msg.payload.decode()

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            print(f"   ⚠️  Payload no es JSON: {raw[:100]}")
            payload = {"raw_text": raw}

        # 1. Log en telemetria (siempre)
        tel_id = _insert_telemetria(topic, payload)

        # 2. Parsear topic (normaliza slash inicial)
        sensor_id, variable = _parse_topic(topic)

        if sensor_id is None:
            _stats["ignorados"] += 1
            if verbose:
                print(f"   ⚠️  Topic '{topic}' no coincide con base '{MQTT_TOPIC_BASE}' — ignorado")
            conn.commit()
            return

        # 3. Variable conocida
        if variable not in KNOWN_VARIABLES:
            _stats["ignorados"] += 1
            if verbose:
                print(f"   ⚠️  Variable '{variable}' desconocida — solo en telemetria")
            conn.commit()
            return

        # 4. Extraer valor numérico
        if "value" in payload:
            raw_value = payload["value"]
        else:
            raw_value = payload.get(variable)

        try:
            if raw_value is None:
                raise ValueError("No se encontró el campo de valor en el JSON")
            value = float(raw_value)
        except (TypeError, ValueError):
            print(f"   ❌ Valor no convertible: {raw_value!r} en topic={topic}")
            _stats["errores"] += 1
            conn.commit()
            return

        # 5. Timestamp
        ts_raw = payload.get("timestamp")
        if ts_raw:
            # Corregir el formato de fecha: cambiar "YYYY/MM/DD" a "YYYY-MM-DD"
            ts_raw_fixed = ts_raw.replace("/", "-")
            try:
                ts = datetime.fromisoformat(ts_raw_fixed)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except ValueError:
                print(f"   ⚠️ Timestamp inválido '{ts_raw}' — usando NOW()")
                ts = datetime.now(timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        source = payload.get("source", "mqtt")

        # 6. Insertar reading → dispara NOTIFY automáticamente via trigger
        reading_id = _insert_reading(sensor_id, variable, value, ts, source, tel_id)
        conn.commit()

        _stats["insertados"] += 1
        print(f"   ✅ [{sensor_id}] {variable}={value} src={source} "
              f"reading_id={reading_id} (total={_stats['insertados']})")

    except Exception as e:
        _stats["errores"] += 1
        print(f"   ❌ Error procesando '{topic}': {e}")
        if conn:
            conn.rollback()


def on_log(client, userdata, level, buf):
    # Solo errores internos de paho
    if level <= mqtt.MQTT_LOG_WARNING:
        print(f"   [paho-warn] {buf}")


# ==========================================
# CLIENTE MQTT
# ==========================================
print("\n🔌 Configurando cliente MQTT...")
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
        break
    except Exception as e:
        print(f"⏳ Broker no disponible ({e}) — reintentando en 5s...")
        time.sleep(5)

print("\n🎧 Esperando mensajes MQTT...\n")

try:
    client.loop_forever()
except KeyboardInterrupt:
    print("\n🛑 Bridge detenido")
    print(f"📊 Estadísticas: {_stats}")
finally:
    if cursor: cursor.close()
    if conn:   conn.close()
    print("🔒 Conexiones cerradas")