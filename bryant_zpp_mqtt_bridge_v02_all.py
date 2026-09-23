#!/usr/bin/env python3
"""
Bryant Zone Perfect Plus (CZII) -> MQTT / Home Assistant bridge

Tested protocol behavior on a 3-zone Bryant Zone Perfect Plus system:
  - CZII / RS-485: 9600 8N1
  - bridge address: 99
  - T1/R2  : master summary
  - T1/R6  : Zone 1 temperature/humidity
  - T1/R12 : HVAC mode, TEMP/HOLD/OUT masks
  - T1/R16 : 8 cool + 8 heat setpoints
  - T1/R17 : fan Auto/On bit
  - T9/R1  : remote sensor temperatures (empirically confirmed)
  - T9/R3  : outside + leaving-air temperature
  - T9/R4  : damper positions
  - T9/R5  : equipment output state

The bridge uses read-modify-write for every writable CZII row and reads
the row back afterward. Unknown/controller-managed bytes are preserved.

Dependencies:
    python3 -m pip install pyserial paho-mqtt

Configure the SETTINGS section below or use the matching environment variables.
"""

import json
import os
import queue
import signal
import sys
import time
from collections import deque

import serial
import paho.mqtt.client as mqtt

# ============================================================================
# SETTINGS
# ============================================================================

SERIAL_PORT = os.getenv("BRYANT_SERIAL_PORT", "/dev/your-serial-port")

MQTT_HOST = os.getenv("MQTT_HOST", "your_ip_here")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME") or None
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD") or None

# MQTT_USERNAME = "username"
# MQTT_PASSWORD = "password"  # Set UN and PW as environment variables.

# Give the second HVAC system a different ID/name when you run another instance.
SYSTEM_ID = os.getenv("BRYANT_SYSTEM_ID", "bryant_hvac_1")
SYSTEM_NAME = os.getenv("BRYANT_SYSTEM_NAME", "Bryant HVAC 1")

TOPIC_BASE = os.getenv("BRYANT_TOPIC_BASE", f"bryant/{SYSTEM_ID}")
DISCOVERY_PREFIX = os.getenv("HA_DISCOVERY_PREFIX", "homeassistant")

OUR_ADDRESS = 99
EXPECTED_ZONES = 3

BAUD = 9600
BUS_IDLE_TIME = 1.0
RESPONSE_TIMEOUT = 3.0
POLL_INTERVAL = 30.0

MIN_SETPOINT = 50
MAX_SETPOINT = 90

DEBUG_FRAMES = os.getenv("BRYANT_DEBUG", "0").lower() in ("1", "true", "yes", "on")


# ============================================================================
# CONSTANTS
# ============================================================================

MODE_CODE_TO_HA = {
    0: "heat",
    1: "cool",
    2: "auto",
    4: "off",
}

MODE_HA_TO_CODE = {
    "heat": 0,
    "cool": 1,
    "auto": 2,
    "off": 4,
}

SCHEDULE_NAMES = {
    0: "Wake",
    1: "Day",
    2: "Evening",
    3: "Night",
}

AVAILABILITY_TOPIC = f"{TOPIC_BASE}/availability"
MODE_STATE_TOPIC = f"{TOPIC_BASE}/system/mode/state"
MODE_COMMAND_TOPIC = f"{TOPIC_BASE}/system/mode/set"
FAN_STATE_TOPIC = f"{TOPIC_BASE}/system/fan/state"
FAN_COMMAND_TOPIC = f"{TOPIC_BASE}/system/fan/set"
ALL_STATE_TOPIC = f"{TOPIC_BASE}/system/all/state"
ALL_COMMAND_TOPIC = f"{TOPIC_BASE}/system/all/set"

ALL_OPTIONS = [
    "Individual Zones",
    "All Follow Zone 1",
    "All Follow Zone 2",
    "All Follow Zone 3",
]

DEVICE_INFO = {
    "identifiers": [SYSTEM_ID],
    "name": SYSTEM_NAME,
    "manufacturer": "Bryant",
    "model": "Zone Perfect Plus",
    "sw_version": "CZII MQTT bridge 0.2",
}


# ============================================================================
# CZII SERIAL / PROTOCOL
# ============================================================================


class CZII:
    def __init__(self, port):
        self.ser = serial.Serial(
            port,
            baudrate=BAUD,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            timeout=0.05,
        )

        self.rx_buffer = bytearray()
        self.last_rx = time.monotonic()
        self.last_tx = 0.0
        self.seen_valid_frame = False

        # callback(frame) called for every valid frame, including passive traffic
        self.frame_callback = None

    @staticmethod
    def crc16_arc(data):
        crc = 0x0000

        for byte in data:
            crc ^= byte

            for _ in range(8):
                if crc & 1:
                    crc = (crc >> 1) ^ 0xA001
                else:
                    crc >>= 1

        return crc

    @classmethod
    def finish_frame(cls, frame):
        crc = cls.crc16_arc(frame)
        frame += bytes([crc & 0xFF, (crc >> 8) & 0xFF])
        return frame

    def build_read(self, destination, table, row):
        frame = bytearray(
            [
                destination & 0xFF,
                (destination >> 8) & 0xFF,
                OUR_ADDRESS & 0xFF,
                (OUR_ADDRESS >> 8) & 0xFF,
                0x03,
                0x00,
                0x00,
                0x0B,  # READ
                0x00,
                table,
                row,
            ]
        )
        return self.finish_frame(frame)

    def build_write(self, destination, table, row, payload):
        frame = bytearray(
            [
                destination & 0xFF,
                (destination >> 8) & 0xFF,
                OUR_ADDRESS & 0xFF,
                (OUR_ADDRESS >> 8) & 0xFF,
                3 + len(payload),
                0x00,
                0x00,
                0x0C,  # WRITE
                0x00,
                table,
                row,
            ]
        )
        frame.extend(payload)
        return self.finish_frame(frame)

    @staticmethod
    def frame_fields(frame):
        dst = int.from_bytes(frame[0:2], "little")
        src = int.from_bytes(frame[2:4], "little")
        length = frame[4]
        function = frame[7]
        data = frame[8 : 8 + length]
        return src, dst, function, data

    def pump(self):
        """Read available bytes, return all complete valid frames."""
        frames = []

        chunk = self.ser.read(256)

        if chunk:
            self.last_rx = time.monotonic()
            self.rx_buffer.extend(chunk)

        while len(self.rx_buffer) >= 10:
            # Header sanity checks based on observed CZII traffic.
            if (
                self.rx_buffer[1] != 0x00
                or self.rx_buffer[3] != 0x00
                or self.rx_buffer[5] != 0x00
                or self.rx_buffer[6] != 0x00
                or self.rx_buffer[7] not in (0x06, 0x0B, 0x0C, 0x15)
            ):
                del self.rx_buffer[0]
                continue

            length = self.rx_buffer[4]
            frame_length = 10 + length

            if len(self.rx_buffer) < frame_length:
                break

            frame = bytes(self.rx_buffer[:frame_length])

            received_crc = frame[-2] | (frame[-1] << 8)
            calculated_crc = self.crc16_arc(frame[:-2])

            if received_crc != calculated_crc:
                del self.rx_buffer[0]
                continue

            del self.rx_buffer[:frame_length]
            self.seen_valid_frame = True
            frames.append(frame)

            if self.frame_callback:
                self.frame_callback(frame)

        return frames

    def wait_for_bus(self):
        print("Waiting for valid CZII traffic...")
        while not self.seen_valid_frame:
            self.pump()
        print("CZII bus detected.")

    def wait_for_idle(self, idle_time=BUS_IDLE_TIME):
        while True:
            self.pump()
            if time.monotonic() - max(self.last_rx, self.last_tx) >= idle_time:
                return

    def transmit(self, frame):
        if DEBUG_FRAMES:
            print("TX:", frame.hex(" "))

        self.ser.write(frame)
        self.ser.flush()
        self.last_tx = time.monotonic()

    def wait_for_ack(self, timeout=RESPONSE_TIMEOUT):
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            for frame in self.pump():
                src, dst, function, data = self.frame_fields(frame)

                if (
                    src == 1
                    and dst == OUR_ADDRESS
                    and function == 0x06
                    and len(data) == 1
                    and data == b"\x00"
                ):
                    return True

            time.sleep(0.01)

        return False

    def wait_for_row(self, table, row, timeout=RESPONSE_TIMEOUT):
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            for frame in self.pump():
                src, dst, function, data = self.frame_fields(frame)

                if (
                    src == 1
                    and dst == OUR_ADDRESS
                    and function == 0x06
                    and len(data) >= 3
                    and data[0] == 0x00
                    and data[1] == table
                    and data[2] == row
                ):
                    return list(data[3:])

            time.sleep(0.01)

        return None

    def read_row(self, table, row):
        self.wait_for_idle()
        self.transmit(self.build_read(1, table, row))

        payload = self.wait_for_row(table, row)

        if payload is None:
            raise RuntimeError(f"No response reading T{table}/R{row}")

        return payload

    def write_row(self, table, row, payload):
        self.wait_for_idle()
        self.transmit(self.build_write(1, table, row, payload))

        if not self.wait_for_ack():
            raise RuntimeError(f"No ACK writing T{table}/R{row}")

    def write_row_verified(self, table, row, payload):
        self.write_row(table, row, payload)
        readback = self.read_row(table, row)

        # Some controller-managed fields can normalize after a write, especially
        # T1/R12. Callers should verify the field(s) they intentionally changed.
        return readback

    def close(self):
        self.ser.close()


# ============================================================================
# BRIDGE STATE / DECODING
# ============================================================================


class BryantBridge:
    def __init__(self, czii):
        self.czii = czii
        self.czii.frame_callback = self.on_frame

        self.configured_zones = EXPECTED_ZONES
        self.displayed_zone = None
        self.schedule = None

        self.mode_code = None
        self.effective_mode_code = None

        self.temp_mask = 0
        self.hold_mask = 0
        self.out_mask = 0
        self.all_zone = 0

        self.cool_setpoints = [None] * 8
        self.heat_setpoints = [None] * 8

        self.zone_temp = {z: None for z in range(1, EXPECTED_ZONES + 1)}
        self.zone_humidity = {z: None for z in range(1, EXPECTED_ZONES + 1)}
        self.damper = {z: None for z in range(1, EXPECTED_ZONES + 1)}

        self.outside_temp = None
        self.lat_temp = None

        self.equipment_raw = None
        self.fan_mode = None

        self.row12 = None
        self.row16 = None
        self.row17 = None

        self.dirty = True

    @staticmethod
    def signed_word(high_byte, low_byte):
        value = (high_byte << 8) | low_byte
        if value & 0x8000:
            value -= 0x10000
        return value

    @classmethod
    def temp16(cls, high_byte, low_byte):
        return cls.signed_word(high_byte, low_byte) / 16.0

    def mark_dirty(self):
        self.dirty = True

    def decode_row(self, table, row, payload, function=0x06):
        changed = False

        # T1/R2 - master summary
        if table == 1 and row == 2 and function == 0x06 and len(payload) >= 10:
            configured = payload[1]
            displayed = payload[7]
            schedule = SCHEDULE_NAMES.get(payload[9], f"Unknown({payload[9]})")

            if (
                configured != self.configured_zones
                or displayed != self.displayed_zone
                or schedule != self.schedule
            ):
                changed = True

            self.configured_zones = configured
            self.displayed_zone = displayed
            self.schedule = schedule

        # T1/R6 - Zone 1 temperature/humidity
        elif table == 1 and row == 6 and function == 0x06 and len(payload) >= 5:
            temp = self.temp16(payload[2], payload[3])
            humidity = payload[4]

            if self.zone_temp[1] != temp or self.zone_humidity[1] != humidity:
                changed = True

            self.zone_temp[1] = temp
            self.zone_humidity[1] = humidity

        # T1/R12 - mode / TEMP / HOLD / OUT
        elif table == 1 and row == 12 and function == 0x06 and len(payload) >= 14:
            new_values = (
                payload[1],
                payload[3],
                payload[6],
                payload[7],
                payload[9],
                payload[12],
            )

            old_values = (
                self.mode_code,
                self.effective_mode_code,
                self.temp_mask,
                self.hold_mask,
                self.out_mask,
                self.all_zone,
            )

            if new_values != old_values or payload != self.row12:
                changed = True

            self.row12 = list(payload)
            self.mode_code = payload[1]
            self.effective_mode_code = payload[3]
            self.temp_mask = payload[6]
            self.hold_mask = payload[7]
            self.out_mask = payload[9]
            self.all_zone = payload[12]

        # T1/R16 - setpoints
        elif table == 1 and row == 16 and function == 0x06 and len(payload) >= 16:
            cool = list(payload[0:8])
            heat = list(payload[8:16])

            if cool != self.cool_setpoints or heat != self.heat_setpoints:
                changed = True

            self.row16 = list(payload)
            self.cool_setpoints = cool
            self.heat_setpoints = heat

        # T1/R17 - fan configuration
        elif table == 1 and row == 17 and function == 0x06 and len(payload) >= 1:
            fan = "on" if (payload[0] & 0x04) else "auto"

            if fan != self.fan_mode or payload != self.row17:
                changed = True

            self.row17 = list(payload)
            self.fan_mode = fan

        # T9/R1 - empirical remote room sensor temperatures
        elif table == 9 and row == 1 and function == 0x06 and len(payload) >= 8:
            for slot in range(4):
                hi = payload[slot * 2]
                lo = payload[slot * 2 + 1]

                if hi == 0xFF and lo == 0xFF:
                    continue

                zone = slot + 1

                if 2 <= zone <= EXPECTED_ZONES:
                    temp = self.temp16(hi, lo)

                    if self.zone_temp[zone] != temp:
                        self.zone_temp[zone] = temp
                        changed = True

        # T9/R3 - outside / leaving-air temperature
        elif table == 9 and row == 3 and function == 0x06 and len(payload) >= 7:
            outside = self.temp16(payload[1], payload[2])
            lat = None if payload[3] == 0xFF else float(payload[3])

            if outside != self.outside_temp or lat != self.lat_temp:
                changed = True

            self.outside_temp = outside
            self.lat_temp = lat

        # T9/R4 - damper commands are writes from master to equipment controller
        elif (
            table == 9
            and row == 4
            and function == 0x0C
            and len(payload) >= EXPECTED_ZONES
        ):
            for zone in range(1, EXPECTED_ZONES + 1):
                value = payload[zone - 1]
                if self.damper[zone] != value:
                    self.damper[zone] = value
                    changed = True

        # T9/R5 - equipment state
        elif table == 9 and row == 5 and function == 0x0C and payload:
            raw = payload[0]
            if raw != self.equipment_raw:
                self.equipment_raw = raw
                changed = True

        if changed:
            self.mark_dirty()

    def on_frame(self, frame):
        src, dst, function, data = self.czii.frame_fields(frame)

        if DEBUG_FRAMES:
            print(
                f"RX src={src} dst={dst} fn=0x{function:02x} " f"data={data.hex(' ')}"
            )

        if len(data) < 3:
            return

        table = data[1]
        row = data[2]
        payload = list(data[3:])

        self.decode_row(table, row, payload, function)

    def refresh_master(self):
        for table, row in ((1, 2), (1, 6), (1, 12), (1, 16), (1, 17)):
            payload = self.czii.read_row(table, row)
            self.decode_row(table, row, payload, 0x06)

    # ------------------------------------------------------------------------
    # State helpers
    # ------------------------------------------------------------------------

    def zone_bit(self, zone):
        return 1 << (zone - 1)

    def effective_control_zone(self, requested_zone):
        """Return the zone whose settings currently control the system.

        In Bryant ALL mode, T1/R12 byte 12 selects one source zone whose
        settings apply to every zone. Room temperatures and dampers remain
        physically per-zone, but setpoints/TEMP/HOLD/OUT control follows the
        selected source zone.
        """
        if 1 <= self.all_zone <= EXPECTED_ZONES:
            return self.all_zone
        return requested_zone

    def all_control_label(self):
        if 1 <= self.all_zone <= EXPECTED_ZONES:
            return f"All Follow Zone {self.all_zone}"
        return "Individual Zones"

    def zone_hold(self, zone):
        return bool(self.hold_mask & self.zone_bit(zone))

    def zone_temp_override(self, zone):
        return bool(self.temp_mask & self.zone_bit(zone))

    def zone_out(self, zone):
        return bool(self.out_mask & self.zone_bit(zone))

    def ha_mode(self):
        return MODE_CODE_TO_HA.get(self.mode_code, "off")

    def effective_mode(self):
        if self.mode_code == 2:
            return "cool" if self.effective_mode_code == 1 else "heat"
        return self.ha_mode()

    def equipment_action(self, zone):
        if self.mode_code == 4:
            return "off"

        raw = self.equipment_raw
        damper = self.damper.get(zone)

        if raw is None:
            return "idle"

        # If a zone's damper is fully closed, don't claim that zone is actively
        # being conditioned even though the common equipment is running.
        accepting_air = damper is None or damper > 0

        if accepting_air and (raw & 0x03):  # Y1/Y2
            return "cooling"

        if accepting_air and (raw & 0x0C):  # W1/W2
            return "heating"

        if accepting_air and (raw & 0x20):  # FAN
            return "fan"

        return "idle"

    def equipment_text(self):
        if self.equipment_raw is None:
            return "Unknown"

        raw = self.equipment_raw
        signals = []

        if raw & 0x20:
            signals.append("FAN")
        if raw & 0x01:
            signals.append("Y1")
        if raw & 0x02:
            signals.append("Y2")
        if raw & 0x04:
            signals.append("W1")
        if raw & 0x08:
            signals.append("W2")
        if raw & 0x10:
            signals.append("REV")

        if not signals:
            return f"Idle (0x{raw:02X})"

        return " + ".join(signals) + f" (0x{raw:02X})"

    # ------------------------------------------------------------------------
    # Safe read-modify-write controls
    # ------------------------------------------------------------------------

    def set_mode(self, mode):
        mode = mode.lower()

        if mode not in MODE_HA_TO_CODE:
            raise ValueError(f"Unsupported mode: {mode}")

        row = self.czii.read_row(1, 12)
        row[1] = MODE_HA_TO_CODE[mode]

        readback = self.czii.write_row_verified(1, 12, row)
        self.decode_row(1, 12, readback)

        if readback[1] != MODE_HA_TO_CODE[mode]:
            raise RuntimeError("Mode write did not verify")

    def set_fan(self, fan_mode):
        fan_mode = fan_mode.lower()

        if fan_mode not in ("auto", "on"):
            raise ValueError("Fan mode must be auto or on")

        row = self.czii.read_row(1, 17)

        if fan_mode == "on":
            row[0] |= 0x04
        else:
            row[0] &= ~0x04 & 0xFF

        readback = self.czii.write_row_verified(1, 17, row)
        self.decode_row(1, 17, readback)

        actual = "on" if (readback[0] & 0x04) else "auto"

        if actual != fan_mode:
            raise RuntimeError("Fan write did not verify")

    def set_setpoint(self, zone, kind, value):
        zone = self.effective_control_zone(zone)
        if zone < 1 or zone > EXPECTED_ZONES:
            raise ValueError(f"Invalid zone: {zone}")

        # The Bryant stores whole degrees F.
        value_float = float(value)
        value_int = int(round(value_float))

        if abs(value_float - value_int) > 0.001:
            raise ValueError("Bryant setpoints must be whole degrees F")

        if not MIN_SETPOINT <= value_int <= MAX_SETPOINT:
            raise ValueError(f"Setpoint must be {MIN_SETPOINT}-{MAX_SETPOINT} F")

        row16 = self.czii.read_row(1, 16)

        if kind == "cool":
            index = zone - 1
        elif kind == "heat":
            index = 8 + zone - 1
        else:
            raise ValueError("Setpoint kind must be heat or cool")

        row16[index] = value_int

        readback = self.czii.write_row_verified(1, 16, row16)
        self.decode_row(1, 16, readback)

        if readback[index] != value_int:
            raise RuntimeError("Setpoint write did not verify")

        # Mimic the thermostat: if the zone is in normal scheduled operation,
        # a manual setpoint adjustment becomes a temporary override. HOLD and
        # OUT are left alone.
        row12 = self.czii.read_row(1, 12)
        bit = self.zone_bit(zone)

        if not (row12[7] & bit) and not (row12[9] & bit):
            if not (row12[6] & bit):
                row12[6] |= bit
                state_readback = self.czii.write_row_verified(1, 12, row12)
                self.decode_row(1, 12, state_readback)

    def set_active_temperature(self, zone, value):
        zone = self.effective_control_zone(zone)
        mode = self.ha_mode()

        if mode == "cool":
            self.set_setpoint(zone, "cool", value)
        elif mode == "heat":
            self.set_setpoint(zone, "heat", value)
        elif mode == "auto":
            # A generic target command in Auto is ambiguous. HA should normally
            # use the low/high command topics. If it doesn't, adjust the side
            # that the Bryant currently considers effective.
            self.set_setpoint(zone, self.effective_mode(), value)
        else:
            raise ValueError("Cannot set active temperature while HVAC is off")

    def set_all_control(self, selection):
        """Set Bryant ALL mode using the system-level HA select value."""
        text = str(selection).strip()
        normalized = text.lower().replace("_", " ").replace("-", " ")

        aliases = {
            "individual zones": 0,
            "individual": 0,
            "normal": 0,
            "off": 0,
            "0": 0,
            "all follow zone 1": 1,
            "zone 1": 1,
            "zone1": 1,
            "1": 1,
            "all follow zone 2": 2,
            "zone 2": 2,
            "zone2": 2,
            "2": 2,
            "all follow zone 3": 3,
            "zone 3": 3,
            "zone3": 3,
            "3": 3,
        }

        if normalized not in aliases:
            raise ValueError(
                "Zone Control must be Individual Zones or All Follow Zone 1/2/3"
            )

        source_zone = aliases[normalized]
        row = self.czii.read_row(1, 12)
        row[12] = source_zone

        readback = self.czii.write_row_verified(1, 12, row)
        self.decode_row(1, 12, readback)

        if readback[12] != source_zone:
            raise RuntimeError("ALL mode write did not verify")

    def set_hold(self, zone, enabled):
        zone = self.effective_control_zone(zone)
        row = self.czii.read_row(1, 12)
        bit = self.zone_bit(zone)

        if enabled:
            row[7] |= bit
        else:
            # Deliberately clear HOLD only. A TEMP override can legally remain,
            # matching the independent bits we observed on the Bryant.
            row[7] &= ~bit & 0xFF

        readback = self.czii.write_row_verified(1, 12, row)
        self.decode_row(1, 12, readback)

        if bool(readback[7] & bit) != bool(enabled):
            raise RuntimeError("HOLD write did not verify")

    def set_out(self, zone, enabled):
        zone = self.effective_control_zone(zone)
        row = self.czii.read_row(1, 12)
        bit = self.zone_bit(zone)

        if enabled:
            # This is the exact combination we physically tested:
            # OUT on, TEMP off, HOLD off for this zone.
            row[6] &= ~bit & 0xFF
            row[7] &= ~bit & 0xFF
            row[9] |= bit
        else:
            # OUT off returns the zone to scheduled operation.
            row[6] &= ~bit & 0xFF
            row[7] &= ~bit & 0xFF
            row[9] &= ~bit & 0xFF

        readback = self.czii.write_row_verified(1, 12, row)
        self.decode_row(1, 12, readback)

        if bool(readback[9] & bit) != bool(enabled):
            raise RuntimeError("OUT write did not verify")

    def resume_schedule(self, zone):
        zone = self.effective_control_zone(zone)
        row = self.czii.read_row(1, 12)
        bit = self.zone_bit(zone)

        row[6] &= ~bit & 0xFF  # TEMP off
        row[7] &= ~bit & 0xFF  # HOLD off
        row[9] &= ~bit & 0xFF  # OUT off

        readback = self.czii.write_row_verified(1, 12, row)
        self.decode_row(1, 12, readback)

        if (readback[6] | readback[7] | readback[9]) & bit:
            raise RuntimeError("Resume-schedule write did not verify")


# ============================================================================
# MQTT / HOME ASSISTANT
# ============================================================================


class MQTTInterface:
    def __init__(self, bridge):
        self.bridge = bridge
        self.commands = queue.Queue()
        self.connected = False

        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"{SYSTEM_ID}_bridge",
            protocol=mqtt.MQTTv311,
        )

        if MQTT_USERNAME:
            self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

        self.client.will_set(
            AVAILABILITY_TOPIC,
            payload="offline",
            qos=1,
            retain=True,
        )

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def start(self):
        print(f"Connecting to MQTT broker {MQTT_HOST}:{MQTT_PORT}...")
        self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.client.loop_start()

    def stop(self):
        try:
            if self.connected:
                self.publish(AVAILABILITY_TOPIC, "offline", retain=True)
                time.sleep(0.1)
            self.client.disconnect()
        finally:
            self.client.loop_stop()

    def publish(self, topic, payload, retain=True):
        if payload is None:
            return

        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload, separators=(",", ":"))

        self.client.publish(topic, str(payload), qos=0, retain=retain)

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            print(f"MQTT connection failed: {reason_code}")
            return

        self.connected = True
        print("MQTT connected.")

        client.subscribe(f"{TOPIC_BASE}/+/+/set")
        client.subscribe(f"{TOPIC_BASE}/+/+/+/set")
        client.subscribe(f"{TOPIC_BASE}/system/+/set")
        client.subscribe("homeassistant/status")

        self.publish_discovery()

        if self.bridge.czii.seen_valid_frame:
            self.publish(AVAILABILITY_TOPIC, "online", retain=True)

        self.publish_state(force=True)

    def on_disconnect(
        self, client, userdata, disconnect_flags, reason_code, properties
    ):
        self.connected = False
        print(f"MQTT disconnected: {reason_code}")

    def on_message(self, client, userdata, message):
        topic = message.topic
        payload = message.payload.decode("utf-8", errors="replace").strip()

        if topic == "homeassistant/status":
            if payload.lower() == "online":
                self.publish_discovery()
                self.publish_state(force=True)
            return

        # Never touch the serial bus from the MQTT network thread.
        # Queue the command for the main thread.
        self.commands.put((topic, payload))

    # ------------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------------

    def discovery(self, component, object_id, config):
        topic = f"{DISCOVERY_PREFIX}/{component}/{SYSTEM_ID}/{object_id}/config"

        payload = dict(config)
        payload["device"] = DEVICE_INFO
        payload["availability_topic"] = AVAILABILITY_TOPIC

        self.publish(topic, payload, retain=True)

    def publish_discovery(self):
        # Three climate entities
        for zone in range(1, EXPECTED_ZONES + 1):
            prefix = f"{TOPIC_BASE}/zone/{zone}"
            unique = f"{SYSTEM_ID}_zone_{zone}"

            climate = {
                "name": f"Zone {zone}",
                "unique_id": f"{unique}_climate",
                "modes": ["off", "heat", "cool", "auto"],
                "mode_state_topic": MODE_STATE_TOPIC,
                "mode_command_topic": MODE_COMMAND_TOPIC,
                "fan_modes": ["auto", "on"],
                "fan_mode_state_topic": FAN_STATE_TOPIC,
                "fan_mode_command_topic": FAN_COMMAND_TOPIC,
                "current_temperature_topic": f"{prefix}/temperature/state",
                "temperature_state_topic": f"{prefix}/target/state",
                "temperature_command_topic": f"{prefix}/target/set",
                "temperature_low_state_topic": f"{prefix}/heat_setpoint/state",
                "temperature_low_command_topic": f"{prefix}/heat_setpoint/set",
                "temperature_high_state_topic": f"{prefix}/cool_setpoint/state",
                "temperature_high_command_topic": f"{prefix}/cool_setpoint/set",
                "action_topic": f"{prefix}/action/state",
                "temperature_unit": "F",
                "precision": 1.0,
                "temp_step": 1.0,
                "min_temp": MIN_SETPOINT,
                "max_temp": MAX_SETPOINT,
            }

            if zone == 1:
                climate["current_humidity_topic"] = f"{prefix}/humidity/state"

            self.discovery("climate", f"zone_{zone}", climate)

            # Always-visible Bryant heat/cool setpoints.
            #
            # Home Assistant's standard climate card only presents the setpoint
            # relevant to the current HVAC mode (and presents the pair in Auto).
            # The physical Bryant thermostat allows both heat and cool setpoints
            # to be edited in any mode, so expose both as independent Number
            # entities using the same MQTT topics as the climate entity.
            self.discovery(
                "number",
                f"zone_{zone}_heat_setpoint",
                {
                    "name": f"Zone {zone} Heat Setpoint",
                    "unique_id": f"{unique}_heat_setpoint_number",
                    "command_topic": f"{prefix}/heat_setpoint/set",
                    "state_topic": f"{prefix}/heat_setpoint/state",
                    "device_class": "temperature",
                    "unit_of_measurement": "°F",
                    "min": MIN_SETPOINT,
                    "max": MAX_SETPOINT,
                    "step": 1,
                    "mode": "box",
                },
            )

            self.discovery(
                "number",
                f"zone_{zone}_cool_setpoint",
                {
                    "name": f"Zone {zone} Cool Setpoint",
                    "unique_id": f"{unique}_cool_setpoint_number",
                    "command_topic": f"{prefix}/cool_setpoint/set",
                    "state_topic": f"{prefix}/cool_setpoint/state",
                    "device_class": "temperature",
                    "unit_of_measurement": "°F",
                    "min": MIN_SETPOINT,
                    "max": MAX_SETPOINT,
                    "step": 1,
                    "mode": "box",
                },
            )

            self.discovery(
                "switch",
                f"zone_{zone}_hold",
                {
                    "name": f"Zone {zone} Hold",
                    "unique_id": f"{unique}_hold",
                    "command_topic": f"{prefix}/hold/set",
                    "state_topic": f"{prefix}/hold/state",
                    "payload_on": "ON",
                    "payload_off": "OFF",
                },
            )

            self.discovery(
                "switch",
                f"zone_{zone}_out",
                {
                    "name": f"Zone {zone} Out",
                    "unique_id": f"{unique}_out",
                    "command_topic": f"{prefix}/out/set",
                    "state_topic": f"{prefix}/out/state",
                    "payload_on": "ON",
                    "payload_off": "OFF",
                },
            )

            self.discovery(
                "button",
                f"zone_{zone}_resume_schedule",
                {
                    "name": f"Zone {zone} Resume Schedule",
                    "unique_id": f"{unique}_resume_schedule",
                    "command_topic": f"{prefix}/resume/set",
                    "payload_press": "PRESS",
                    "entity_category": "config",
                },
            )

            self.discovery(
                "binary_sensor",
                f"zone_{zone}_temporary",
                {
                    "name": f"Zone {zone} Temporary Override",
                    "unique_id": f"{unique}_temporary",
                    "state_topic": f"{prefix}/temporary/state",
                    "payload_on": "ON",
                    "payload_off": "OFF",
                    "enabled_by_default": False,
                    "entity_category": "diagnostic",
                },
            )

            self.discovery(
                "sensor",
                f"zone_{zone}_damper",
                {
                    "name": f"Zone {zone} Damper",
                    "unique_id": f"{unique}_damper",
                    "state_topic": f"{prefix}/damper/state",
                    "unit_of_measurement": "%",
                    "state_class": "measurement",
                    "entity_category": "diagnostic",
                },
            )

        # Bryant ALL mode / source-zone control.
        #
        # This remains a single system-level select rather than dynamically
        # merging climate entities. When ALL is active, all zone setpoint and
        # override commands are routed to the selected source zone.
        self.discovery(
            "select",
            "zone_control",
            {
                "name": "Zone Control",
                "unique_id": f"{SYSTEM_ID}_zone_control",
                "command_topic": ALL_COMMAND_TOPIC,
                "state_topic": ALL_STATE_TOPIC,
                "options": ALL_OPTIONS,
                "icon": "mdi:home-group",
            },
        )

        # System diagnostics
        self.discovery(
            "sensor",
            "outside_temperature",
            {
                "name": "Outside Temperature",
                "unique_id": f"{SYSTEM_ID}_outside_temperature",
                "state_topic": f"{TOPIC_BASE}/system/outside_temperature/state",
                "device_class": "temperature",
                "unit_of_measurement": "°F",
                "state_class": "measurement",
            },
        )

        self.discovery(
            "sensor",
            "leaving_air_temperature",
            {
                "name": "Leaving Air Temperature",
                "unique_id": f"{SYSTEM_ID}_leaving_air_temperature",
                "state_topic": f"{TOPIC_BASE}/system/leaving_air_temperature/state",
                "device_class": "temperature",
                "unit_of_measurement": "°F",
                "state_class": "measurement",
                "entity_category": "diagnostic",
            },
        )

        self.discovery(
            "sensor",
            "equipment_state",
            {
                "name": "Equipment State",
                "unique_id": f"{SYSTEM_ID}_equipment_state",
                "state_topic": f"{TOPIC_BASE}/system/equipment/state",
                "entity_category": "diagnostic",
            },
        )

        self.discovery(
            "sensor",
            "schedule_period",
            {
                "name": "Schedule Period",
                "unique_id": f"{SYSTEM_ID}_schedule_period",
                "state_topic": f"{TOPIC_BASE}/system/schedule/state",
                "entity_category": "diagnostic",
                "enabled_by_default": False,
            },
        )

        print("Home Assistant MQTT discovery published.")

    # ------------------------------------------------------------------------
    # State publishing
    # ------------------------------------------------------------------------

    def publish_state(self, force=False):
        if not self.connected:
            return

        bridge = self.bridge

        if not force and not bridge.dirty:
            return

        if bridge.czii.seen_valid_frame:
            self.publish(AVAILABILITY_TOPIC, "online", retain=True)

        mode = bridge.ha_mode()
        self.publish(MODE_STATE_TOPIC, mode, retain=True)

        if bridge.fan_mode is not None:
            self.publish(FAN_STATE_TOPIC, bridge.fan_mode, retain=True)

        self.publish(ALL_STATE_TOPIC, bridge.all_control_label(), retain=True)

        for zone in range(1, EXPECTED_ZONES + 1):
            prefix = f"{TOPIC_BASE}/zone/{zone}"

            temp = bridge.zone_temp.get(zone)
            if temp is not None:
                self.publish(f"{prefix}/temperature/state", f"{temp:.1f}", retain=True)

            if zone == 1 and bridge.zone_humidity.get(1) is not None:
                self.publish(
                    f"{prefix}/humidity/state",
                    bridge.zone_humidity[1],
                    retain=True,
                )

            control_zone = bridge.effective_control_zone(zone)
            cool = bridge.cool_setpoints[control_zone - 1]
            heat = bridge.heat_setpoints[control_zone - 1]

            if cool is not None:
                self.publish(f"{prefix}/cool_setpoint/state", cool, retain=True)

            if heat is not None:
                self.publish(f"{prefix}/heat_setpoint/state", heat, retain=True)

            # Single-target state for Heat/Cool modes. In Auto, HA can use the
            # lower/upper target topics above.
            target = None

            if mode == "cool":
                target = cool
            elif mode == "heat":
                target = heat
            elif mode == "auto":
                target = cool if bridge.effective_mode() == "cool" else heat

            if target is not None:
                self.publish(f"{prefix}/target/state", target, retain=True)

            self.publish(
                f"{prefix}/hold/state",
                "ON" if bridge.zone_hold(control_zone) else "OFF",
                retain=True,
            )

            self.publish(
                f"{prefix}/out/state",
                "ON" if bridge.zone_out(control_zone) else "OFF",
                retain=True,
            )

            self.publish(
                f"{prefix}/temporary/state",
                "ON" if bridge.zone_temp_override(control_zone) else "OFF",
                retain=True,
            )

            damper = bridge.damper.get(zone)
            if damper is not None:
                percent = round(damper / 15.0 * 100)
                self.publish(f"{prefix}/damper/state", percent, retain=True)

            self.publish(
                f"{prefix}/action/state",
                bridge.equipment_action(zone),
                retain=True,
            )

        if bridge.outside_temp is not None:
            self.publish(
                f"{TOPIC_BASE}/system/outside_temperature/state",
                f"{bridge.outside_temp:.1f}",
                retain=True,
            )

        if bridge.lat_temp is not None:
            self.publish(
                f"{TOPIC_BASE}/system/leaving_air_temperature/state",
                f"{bridge.lat_temp:.1f}",
                retain=True,
            )

        if bridge.equipment_raw is not None:
            self.publish(
                f"{TOPIC_BASE}/system/equipment/state",
                bridge.equipment_text(),
                retain=True,
            )

        if bridge.schedule is not None:
            self.publish(
                f"{TOPIC_BASE}/system/schedule/state",
                bridge.schedule,
                retain=True,
            )

        bridge.dirty = False

    # ------------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------------

    @staticmethod
    def payload_bool(payload):
        value = payload.strip().lower()

        if value in ("on", "1", "true", "yes"):
            return True
        if value in ("off", "0", "false", "no"):
            return False

        raise ValueError(f"Expected ON/OFF, got {payload!r}")

    def process_one_command(self):
        try:
            topic, payload = self.commands.get_nowait()
        except queue.Empty:
            return False

        try:
            self._execute_command(topic, payload)
            self.publish_state(force=True)
        except Exception as exc:
            print(f"Command failed: topic={topic} payload={payload!r}: {exc}")
            # Refresh after any failed command so HA sees actual controller state.
            try:
                self.bridge.refresh_master()
                self.publish_state(force=True)
            except Exception as refresh_exc:
                print(f"Refresh after command failure also failed: {refresh_exc}")

        return True

    def _execute_command(self, topic, payload):
        print(f"MQTT command: {topic} = {payload}")

        if topic == MODE_COMMAND_TOPIC:
            self.bridge.set_mode(payload)
            return

        if topic == FAN_COMMAND_TOPIC:
            self.bridge.set_fan(payload)
            return

        if topic == ALL_COMMAND_TOPIC:
            self.bridge.set_all_control(payload)
            return

        prefix = f"{TOPIC_BASE}/zone/"

        if not topic.startswith(prefix):
            return

        remainder = topic[len(prefix) :]
        parts = remainder.split("/")

        if len(parts) != 3 or parts[2] != "set":
            return

        zone = int(parts[0])
        command = parts[1]

        if zone < 1 or zone > EXPECTED_ZONES:
            raise ValueError(f"Invalid zone {zone}")

        if command == "target":
            self.bridge.set_active_temperature(zone, payload)

        elif command == "cool_setpoint":
            self.bridge.set_setpoint(zone, "cool", payload)

        elif command == "heat_setpoint":
            self.bridge.set_setpoint(zone, "heat", payload)

        elif command == "hold":
            self.bridge.set_hold(zone, self.payload_bool(payload))

        elif command == "out":
            self.bridge.set_out(zone, self.payload_bool(payload))

        elif command == "resume":
            self.bridge.resume_schedule(zone)

        else:
            raise ValueError(f"Unknown command: {command}")


# ============================================================================
# MAIN
# ============================================================================


def main():
    print()
    print("Bryant Zone Perfect Plus -> MQTT / Home Assistant")
    print("=================================================")
    print(f"Serial:      {SERIAL_PORT}")
    print(f"MQTT broker: {MQTT_HOST}:{MQTT_PORT}")
    print(f"System ID:   {SYSTEM_ID}")
    print(f"Topic base:  {TOPIC_BASE}")
    print()

    czii = CZII(SERIAL_PORT)
    bridge = BryantBridge(czii)
    mqtt_if = MQTTInterface(bridge)

    running = True

    def stop_handler(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    try:
        # Verify HVAC bus before advertising availability.
        czii.wait_for_bus()

        mqtt_if.start()

        # Populate all master state immediately.
        bridge.refresh_master()
        bridge.mark_dirty()

        next_poll = time.monotonic() + POLL_INTERVAL

        while running:
            # Passive bus decoding.
            czii.pump()

            # MQTT commands run only in this main thread.
            # Do at most one command per pass so passive traffic stays serviced.
            mqtt_if.process_one_command()

            now = time.monotonic()

            if now >= next_poll:
                try:
                    bridge.refresh_master()
                except Exception as exc:
                    print(f"Periodic master refresh failed: {exc}")

                next_poll = time.monotonic() + POLL_INTERVAL

            mqtt_if.publish_state()
            time.sleep(0.01)

    finally:
        print("\nStopping bridge...")

        try:
            mqtt_if.stop()
        except Exception as exc:
            print(f"MQTT shutdown warning: {exc}")

        czii.close()
        print("Serial port closed.")


if __name__ == "__main__":
    main()
