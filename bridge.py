"""
bridge.py — Railway (Actualizado con Triggers y Snooze)
-------------------
Suscriptor MQTT que persiste en PostgreSQL.
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
MQTT_BROKER = os.getenv("MQTT_HOST", "mqtt-server")
MQTT_PORT   = int(os.getenv("MQTT_PORT", 1883))
TOPIC_SUB   = "uja/s1/#"
TOPIC_BASE  = "uja/s1"

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
    "tamp_bat",
}

# ==========================================
# CONEXIÓN A POSTGRESQL CON REINTENTOS
# ==========================================
conn   = None
cursor = None

print("🚀 Iniciando Bridge MQTT → Postgres...")

while True:
    try:
        print("🔗 Conectando a la DB y actualizando esquemas...")
        conn   = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        # 1. TABLAS BÁSICAS Y USUARIOS
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

        # 2. TABLA EAV Y ALERTAS
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
                operator   VARCHAR(2)   NOT NULL,  -- '<=' o '>='
                threshold  DOUBLE PRECISION NOT NULL,
                level      VARCHAR(10)  NOT NULL DEFAULT 'warning',
                message    TEXT         NOT NULL,
                created_at TIMESTAMPTZ  DEFAULT NOW(),
                UNIQUE (sensor_id, variable, operator)
            );
        """)

        # 3. NUEVA TABLA: ALERT SNOOZE
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_snooze (
                id         BIGSERIAL    PRIMARY KEY,
                sensor_id  VARCHAR(64)  NOT NULL,
                variable   VARCHAR(64),           -- NULL = snooze todo el sensor
                until_ts   TIMESTAMPTZ  NOT NULL,
                created_at TIMESTAMPTZ  DEFAULT NOW(),
                UNIQUE (sensor_id, variable)
            );
            CREATE INDEX IF NOT EXISTS idx_alert_snooze_sensor
                ON alert_snooze (sensor_id, until_ts DESC);
        """)

        # 4. TRIGGER DE NOTIFICACIÓN EN TIEMPO REAL (NOTIFY)
        # Definimos la función
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

        # Creamos el trigger (DROP y CREATE para asegurar actualización)
        cursor.execute("""
            DROP TRIGGER IF EXISTS trg_sfa_update ON sfa_readings;
            CREATE TRIGGER trg_sfa_update
            AFTER INSERT ON sfa_readings
            FOR EACH ROW
            EXECUTE FUNCTION notify_sfa_update();
        """)

        conn.commit()
        print("✅ Base de datos actualizada: Tablas, Índices y Triggers listos.")
        break

    except Exception as e:
        print(f"⏳ Error configurando la DB, reintentando en 5s... ({e})")
        if conn: conn.rollback()
        time.sleep(5)


# ==========================================
# HELPERS (Sin cambios)
# ==========================================
def _parse_topic(topic: str) -> tuple[str, str] | tuple[None, None]:
    prefix = TOPIC_BASE + "/"
    if not topic.startswith(prefix):
        return None, None
    parts = topic[len(prefix):].split("/")
    if len(parts) != 2:
        return None, None
    return parts[0], parts[1]

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
# CALLBACKS MQTT (Sin cambios)
# ==========================================
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ Conectado al Broker MQTT interno ({MQTT_BROKER})")
        client.subscribe(TOPIC_SUB)
    else:
        print(f"❌ Error de conexión MQTT. Código: {rc}")

def on_message(client, userdata, msg):
    try:
        raw = msg.payload.decode()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"raw_text": raw}

        tel_id = _insert_telemetria(msg.topic, payload)
        sensor_id, variable = _parse_topic(msg.topic)

        if sensor_id and variable and variable in KNOWN_VARIABLES:
            try:
                value = float(payload.get("value") or payload.get(variable))
            except (TypeError, ValueError):
                conn.commit()
                return

            ts_raw = payload.get("timestamp")
            if ts_raw:
                ts = datetime.fromisoformat(ts_raw)
                if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)
            else:
                ts = datetime.now(timezone.utc)

            _insert_reading(sensor_id, variable, value, ts, payload.get("source", "mqtt"), tel_id)
            print(f"📥 [{sensor_id}] {variable} = {value} (Real-time NOTIFY enviado)")
        
        conn.commit()
    except Exception as e:
        print(f"❌ Error al procesar mensaje: {e}")
        if conn: conn.rollback()

# ==========================================
# CONEXIÓN MQTT
# ==========================================
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        break
    except Exception:
        time.sleep(5)

try:
    client.loop_forever()
except KeyboardInterrupt:
    print("🛑 Bridge detenido.")
finally:
    if cursor: cursor.close()
    if conn:   conn.close()