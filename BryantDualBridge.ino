/*
  Bryant Zone Perfect Plus - Dual HVAC MQTT/Home Assistant Bridge
  ===============================================================

  Target hardware:
    - Olimex ESP32-POE-ISO-EA-IND (ESP32-WROOM version)
    - 2x DFRobot DFR0845 isolated RS-485/UART modules

  Bryant/CZII protocol:
    - 9600 baud, 8N1
    - Bridge address 99
    - Three configured zones per Bryant system
    - Read-modify-write + readback verification for controls

  UART wiring:
    Bryant system 1:
      ESP32 GPIO33 TX -> DFR0845 #1 T
      ESP32 GPIO32 RX <- DFR0845 #1 R

    Bryant system 2:
      ESP32 GPIO13 TX -> DFR0845 #2 T
      ESP32 GPIO16 RX <- DFR0845 #2 R

    DFR0845 isolated side, each system separately:
      A   -> Bryant RS+
      B   -> Bryant RS-
      GND -> Bryant VG

    Leave Bryant V+ disconnected.
    Leave the DFR0845 120-ohm termination disabled.

  Networking:
    - Wi-Fi is used as fallback.
    - Ethernet is preferred. When Ethernet receives an IP address, Wi-Fi is
      disconnected so the default route moves to Ethernet.
    - Olimex WROOM Ethernet:
        PHY LAN8720-compatible
        PHY address 0
        power GPIO12
        MDC GPIO23
        MDIO GPIO18
        RMII clock GPIO17 output

  Home Assistant:
    - MQTT discovery
    - Three climate entities per system
    - Always-visible Heat Setpoint / Cool Setpoint Number entities
    - Hold / Out switches
    - Resume Schedule buttons
    - Zone Control select:
        Individual Zones
        All Follow Zone 1
        All Follow Zone 2
        All Follow Zone 3
    - Damper, outside temp, leaving-air temp, equipment state diagnostics

  Requires:
    - Arduino-ESP32 core 3.x (tested target API: 3.3.x)
    - No third-party MQTT library; uses ESP-IDF ESP-MQTT bundled with core.

  Put secrets.h in this sketch folder before compiling.
*/

#include <Arduino.h>
#include <WiFi.h>
#include <ETH.h>
#include <Network.h>
#include "mqtt_client.h"
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"

#include "secrets.h"

// ============================================================================
// USER-ADJUSTABLE DEVICE SETTINGS
// ============================================================================

static const char *DISCOVERY_PREFIX = "homeassistant";
static const char *MQTT_ROOT = "bryant";

static const char *SYSTEM1_ID = "bryant_hvac_1";
static const char *SYSTEM1_NAME = "Bryant HVAC 1";

static const char *SYSTEM2_ID = "bryant_hvac_2";
static const char *SYSTEM2_NAME = "Bryant HVAC 2";

// UART pins chosen to avoid the Olimex Ethernet pins.
static const int HVAC1_RX_PIN = 32;
static const int HVAC1_TX_PIN = 33;

static const int HVAC2_RX_PIN = 16;
static const int HVAC2_TX_PIN = 13;

// ============================================================================
// FIXED PROTOCOL / TIMING SETTINGS
// ============================================================================

static const uint32_t CZII_BAUD = 9600;
static const uint16_t OUR_ADDRESS = 99;
static const int EXPECTED_ZONES = 3;

static const uint32_t BUS_IDLE_MS = 400;
static const uint32_t RESPONSE_TIMEOUT_MS = 3000;
static const uint32_t MASTER_POLL_MS = 30000;

static const int MIN_SETPOINT = 50;
static const int MAX_SETPOINT = 90;

static const size_t ROW_MAX = 32;
static const size_t FRAME_MAX = 64;
static const size_t RX_BUFFER_MAX = 512;

static const char *BRIDGE_AVAILABILITY_TOPIC = "bryant/bridge/availability";

static const char *ALL_OPTIONS[] = {
  "Individual Zones",
  "All Follow Zone 1",
  "All Follow Zone 2",
  "All Follow Zone 3"
};

// ============================================================================
// OLIMEX ESP32-POE-ISO ETHERNET SETTINGS
// ============================================================================

static const eth_phy_type_t OLIMEX_ETH_PHY_TYPE = ETH_PHY_LAN8720;
static const int OLIMEX_ETH_PHY_ADDR = 0;
static const int OLIMEX_ETH_PHY_POWER = 12;
static const int OLIMEX_ETH_MDC_PIN = 23;
static const int OLIMEX_ETH_MDIO_PIN = 18;
static const eth_clock_mode_t OLIMEX_ETH_CLK_MODE = ETH_CLOCK_GPIO17_OUT;

// ============================================================================
// GLOBAL NETWORK / MQTT STATE
// ============================================================================

volatile bool gEthHasIP = false;
volatile bool gWifiHasIP = false;
volatile bool gMqttConnected = false;
volatile bool gMqttNeedsDiscovery = false;

bool gMqttStarted = false;
uint32_t gLastWifiAttempt = 0;

esp_mqtt_client_handle_t gMqttClient = nullptr;
char gMqttUri[192];

// MQTT events arrive on an ESP-IDF task. Queue commands to the Arduino loop.
struct MqttCommand {
  char topic[192];
  char payload[96];
};

QueueHandle_t gMqttCommandQueue = nullptr;

// ============================================================================
// UTILITY
// ============================================================================

static bool elapsedMs(uint32_t now, uint32_t then, uint32_t interval) {
  return (uint32_t)(now - then) >= interval;
}

static String jsonEscape(const String &input) {
  String out;
  out.reserve(input.length() + 8);

  for (size_t i = 0; i < input.length(); ++i) {
    char c = input[i];
    switch (c) {
      case '\\': out += "\\\\"; break;
      case '"':  out += "\\\""; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:   out += c; break;
    }
  }
  return out;
}

static String q(const String &s) {
  return String("\"") + jsonEscape(s) + "\"";
}

static String boolPayload(bool value) {
  return value ? "ON" : "OFF";
}

// ============================================================================
// ROW CONTAINER
// ============================================================================

struct RowData {
  uint8_t data[ROW_MAX];
  size_t len = 0;

  void clear() {
    len = 0;
    memset(data, 0, sizeof(data));
  }

  bool copyFrom(const uint8_t *src, size_t n) {
    if (n > ROW_MAX) return false;
    memcpy(data, src, n);
    len = n;
    return true;
  }
};

// ============================================================================
// MQTT publish forward declaration
// ============================================================================

bool mqttPublish(const String &topic, const String &payload, bool retain);

// ============================================================================
// BRYANT SYSTEM
// ============================================================================

class BryantSystem {
public:
  BryantSystem(
    HardwareSerial &serialPort,
    int rxPin,
    int txPin,
    const char *systemId,
    const char *systemName
  )
    : serial(serialPort),
      rxPin(rxPin),
      txPin(txPin),
      systemId(systemId),
      systemName(systemName) {
    topicBase = String(MQTT_ROOT) + "/" + systemId;
  }

  void begin() {
    serial.setRxBufferSize(1024);
    serial.begin(CZII_BAUD, SERIAL_8N1, rxPin, txPin);

    lastRxMs = millis();
    lastTxMs = 0;
    dirty = true;

    Serial.printf("[%s] UART started: RX=%d TX=%d @ %lu 8N1\n",
                  systemName.c_str(), rxPin, txPin, (unsigned long)CZII_BAUD);
  }

  // --------------------------------------------------------------------------
  // Passive service / parser
  // --------------------------------------------------------------------------

  void service() {
    while (serial.available()) {
      int c = serial.read();
      if (c < 0) break;

      lastRxMs = millis();

      if (rxLen < RX_BUFFER_MAX) {
        rxBuf[rxLen++] = (uint8_t)c;
      } else {
        // Drop oldest byte if something goes badly out of sync.
        memmove(rxBuf, rxBuf + 1, RX_BUFFER_MAX - 1);
        rxBuf[RX_BUFFER_MAX - 1] = (uint8_t)c;
      }
    }

    parseFrames();
  }

  bool hasSeenBus() const {
    return seenValidFrame;
  }

  uint32_t lastBusActivity() const {
    return lastRxMs;
  }

  // --------------------------------------------------------------------------
  // Active master refresh
  // --------------------------------------------------------------------------

  bool refreshMaster() {
    RowData row;
    bool ok = true;

    ok &= readRow(1, 2, row);
    ok &= readRow(1, 6, row);
    ok &= readRow(1, 12, row);
    ok &= readRow(1, 16, row);
    ok &= readRow(1, 17, row);

    return ok;
  }

  // --------------------------------------------------------------------------
  // State helpers
  // --------------------------------------------------------------------------

  int effectiveControlZone(int requestedZone) const {
    if (allZone >= 1 && allZone <= EXPECTED_ZONES) {
      return allZone;
    }
    return requestedZone;
  }

  String allControlLabel() const {
    if (allZone >= 1 && allZone <= EXPECTED_ZONES) {
      return String("All Follow Zone ") + allZone;
    }
    return "Individual Zones";
  }

  bool zoneHoldRaw(int zone) const {
    return holdMask & zoneBit(zone);
  }

  bool zoneTempRaw(int zone) const {
    return tempMask & zoneBit(zone);
  }

  bool zoneOutRaw(int zone) const {
    return outMask & zoneBit(zone);
  }

  String haMode() const {
    switch (modeCode) {
      case 0: return "heat";
      case 1: return "cool";
      case 2: return "auto";
      case 4: return "off";
      default: return "off";
    }
  }

  String effectiveMode() const {
    if (modeCode == 2) {
      return effectiveModeCode == 1 ? "cool" : "heat";
    }
    return haMode();
  }

  String equipmentText() const {
    if (!haveEquipmentRaw) return "Unknown";

    String s;

    if (equipmentRaw & 0x20) appendSignal(s, "FAN");
    if (equipmentRaw & 0x01) appendSignal(s, "Y1");
    if (equipmentRaw & 0x02) appendSignal(s, "Y2");
    if (equipmentRaw & 0x04) appendSignal(s, "W1");
    if (equipmentRaw & 0x08) appendSignal(s, "W2");
    if (equipmentRaw & 0x10) appendSignal(s, "REV");

    if (s.length() == 0) s = "Idle";

    char raw[12];
    snprintf(raw, sizeof(raw), " (0x%02X)", equipmentRaw);
    s += raw;

    return s;
  }

  String equipmentAction(int physicalZone) const {
    if (modeCode == 4) return "off";
    if (!haveEquipmentRaw) return "idle";

    uint8_t d = damper[physicalZone - 1];
    bool acceptingAir = !haveDamper[physicalZone - 1] || d > 0;

    if (acceptingAir && (equipmentRaw & 0x03)) return "cooling";
    if (acceptingAir && (equipmentRaw & 0x0C)) return "heating";
    if (acceptingAir && (equipmentRaw & 0x20)) return "fan";

    return "idle";
  }

  // --------------------------------------------------------------------------
  // Controls - all use read/modify/write + readback verification
  // --------------------------------------------------------------------------

  bool setMode(const String &mode) {
    int code = -1;
    if (mode == "heat") code = 0;
    else if (mode == "cool") code = 1;
    else if (mode == "auto") code = 2;
    else if (mode == "off") code = 4;
    else return false;

    RowData row;
    if (!readRow(1, 12, row) || row.len < 14) return false;

    row.data[1] = (uint8_t)code;

    if (!writeRow(1, 12, row)) return false;

    RowData verify;
    if (!readRow(1, 12, verify) || verify.len < 14) return false;

    return verify.data[1] == code;
  }

  bool setFan(const String &fanMode) {
    if (fanMode != "auto" && fanMode != "on") return false;

    RowData row;
    if (!readRow(1, 17, row) || row.len < 1) return false;

    if (fanMode == "on") row.data[0] |= 0x04;
    else row.data[0] &= (uint8_t)~0x04;

    if (!writeRow(1, 17, row)) return false;

    RowData verify;
    if (!readRow(1, 17, verify) || verify.len < 1) return false;

    bool actualOn = verify.data[0] & 0x04;
    return actualOn == (fanMode == "on");
  }

  bool setAllControl(const String &selection) {
    int source = -1;

    if (selection == "Individual Zones") source = 0;
    else if (selection == "All Follow Zone 1") source = 1;
    else if (selection == "All Follow Zone 2") source = 2;
    else if (selection == "All Follow Zone 3") source = 3;
    else return false;

    RowData row;
    if (!readRow(1, 12, row) || row.len < 14) return false;

    row.data[12] = source;

    if (!writeRow(1, 12, row)) return false;

    RowData verify;
    if (!readRow(1, 12, verify) || verify.len < 14) return false;

    return verify.data[12] == source;
  }

  bool setSetpoint(int requestedZone, bool cooling, int value) {
    int zone = effectiveControlZone(requestedZone);

    if (zone < 1 || zone > EXPECTED_ZONES) return false;
    if (value < MIN_SETPOINT || value > MAX_SETPOINT) return false;

    RowData row16;
    if (!readRow(1, 16, row16) || row16.len < 16) return false;

    int index = cooling ? (zone - 1) : (8 + zone - 1);
    row16.data[index] = (uint8_t)value;

    if (!writeRow(1, 16, row16)) return false;

    RowData verify16;
    if (!readRow(1, 16, verify16) || verify16.len < 16) return false;
    if (verify16.data[index] != value) return false;

    // Match Bryant manual setpoint behavior: if the effective zone is in normal
    // schedule operation (not HOLD and not OUT), make it a TEMP override.
    RowData row12;
    if (!readRow(1, 12, row12) || row12.len < 14) return false;

    uint8_t bit = zoneBit(zone);

    if (!(row12.data[7] & bit) && !(row12.data[9] & bit)) {
      if (!(row12.data[6] & bit)) {
        row12.data[6] |= bit;

        if (!writeRow(1, 12, row12)) return false;

        RowData verify12;
        if (!readRow(1, 12, verify12) || verify12.len < 14) return false;

        if (!(verify12.data[6] & bit)) return false;
      }
    }

    return true;
  }

  bool setActiveTemperature(int requestedZone, int value) {
    String mode = haMode();

    if (mode == "cool") return setSetpoint(requestedZone, true, value);
    if (mode == "heat") return setSetpoint(requestedZone, false, value);

    if (mode == "auto") {
      return setSetpoint(requestedZone, effectiveMode() == "cool", value);
    }

    return false;
  }

  bool setHold(int requestedZone, bool enabled) {
    int zone = effectiveControlZone(requestedZone);

    RowData row;
    if (!readRow(1, 12, row) || row.len < 14) return false;

    uint8_t bit = zoneBit(zone);

    if (enabled) row.data[7] |= bit;
    else row.data[7] &= (uint8_t)~bit;

    if (!writeRow(1, 12, row)) return false;

    RowData verify;
    if (!readRow(1, 12, verify) || verify.len < 14) return false;

    return bool(verify.data[7] & bit) == enabled;
  }

  bool setOut(int requestedZone, bool enabled) {
    int zone = effectiveControlZone(requestedZone);

    RowData row;
    if (!readRow(1, 12, row) || row.len < 14) return false;

    uint8_t bit = zoneBit(zone);

    if (enabled) {
      // Exact combination tested on the Bryant:
      // OUT on, TEMP off, HOLD off.
      row.data[6] &= (uint8_t)~bit;
      row.data[7] &= (uint8_t)~bit;
      row.data[9] |= bit;
    } else {
      // Turning OUT off returns the effective zone to scheduled operation.
      row.data[6] &= (uint8_t)~bit;
      row.data[7] &= (uint8_t)~bit;
      row.data[9] &= (uint8_t)~bit;
    }

    if (!writeRow(1, 12, row)) return false;

    RowData verify;
    if (!readRow(1, 12, verify) || verify.len < 14) return false;

    return bool(verify.data[9] & bit) == enabled;
  }

  bool resumeSchedule(int requestedZone) {
    int zone = effectiveControlZone(requestedZone);

    RowData row;
    if (!readRow(1, 12, row) || row.len < 14) return false;

    uint8_t bit = zoneBit(zone);

    row.data[6] &= (uint8_t)~bit;  // TEMP off
    row.data[7] &= (uint8_t)~bit;  // HOLD off
    row.data[9] &= (uint8_t)~bit;  // OUT off

    if (!writeRow(1, 12, row)) return false;

    RowData verify;
    if (!readRow(1, 12, verify) || verify.len < 14) return false;

    return !((verify.data[6] | verify.data[7] | verify.data[9]) & bit);
  }

  // --------------------------------------------------------------------------
  // MQTT discovery and state
  // --------------------------------------------------------------------------

  void publishDiscovery() {
    for (int zone = 1; zone <= EXPECTED_ZONES; ++zone) {
      publishClimateDiscovery(zone);
      publishSetpointNumberDiscovery(zone, false);
      publishSetpointNumberDiscovery(zone, true);
      publishSwitchDiscovery(zone, "hold", "Hold");
      publishSwitchDiscovery(zone, "out", "Out");
      publishResumeButtonDiscovery(zone);
      publishTemporaryDiscovery(zone);
      publishDamperDiscovery(zone);
    }

    publishAllSelectDiscovery();
    publishSystemSensorDiscovery(
      "outside_temperature",
      "Outside Temperature",
      "system/outside_temperature/state",
      "\"device_class\":\"temperature\",\"unit_of_measurement\":\"°F\",\"state_class\":\"measurement\""
    );

    publishSystemSensorDiscovery(
      "leaving_air_temperature",
      "Leaving Air Temperature",
      "system/leaving_air_temperature/state",
      "\"device_class\":\"temperature\",\"unit_of_measurement\":\"°F\",\"state_class\":\"measurement\",\"entity_category\":\"diagnostic\""
    );

    publishSystemSensorDiscovery(
      "equipment_state",
      "Equipment State",
      "system/equipment/state",
      "\"entity_category\":\"diagnostic\""
    );

    publishSystemSensorDiscovery(
      "schedule_period",
      "Schedule Period",
      "system/schedule/state",
      "\"entity_category\":\"diagnostic\",\"enabled_by_default\":false"
    );

    Serial.printf("[%s] HA discovery published\n", systemName.c_str());
  }

  void publishState(bool force = false) {
    if (!gMqttConnected) return;
    if (!force && !dirty) return;

    String mode = haMode();

    mqttPublish(topicBase + "/system/mode/state", mode, true);
    mqttPublish(topicBase + "/system/zone_control/state", allControlLabel(), true);

    if (haveFanMode) {
      mqttPublish(topicBase + "/system/fan/state", fanOn ? "on" : "auto", true);
    }

    for (int physicalZone = 1; physicalZone <= EXPECTED_ZONES; ++physicalZone) {
      String p = topicBase + "/zone/" + physicalZone;

      if (haveZoneTemp[physicalZone - 1]) {
        mqttPublish(p + "/temperature/state", String(zoneTemp[physicalZone - 1], 1), true);
      }

      if (physicalZone == 1 && haveZoneHumidity[0]) {
        mqttPublish(p + "/humidity/state", String(zoneHumidity[0]), true);
      }

      int controlZone = effectiveControlZone(physicalZone);
      int ci = controlZone - 1;

      if (haveSetpoints) {
        int cool = coolSetpoint[ci];
        int heat = heatSetpoint[ci];

        mqttPublish(p + "/cool_setpoint/state", String(cool), true);
        mqttPublish(p + "/heat_setpoint/state", String(heat), true);

        int target = 0;
        bool haveTarget = true;

        if (mode == "cool") target = cool;
        else if (mode == "heat") target = heat;
        else if (mode == "auto") target = effectiveMode() == "cool" ? cool : heat;
        else haveTarget = false;

        if (haveTarget) {
          mqttPublish(p + "/target/state", String(target), true);
        }
      }

      mqttPublish(
        p + "/hold/state",
        boolPayload(zoneHoldRaw(controlZone)),
        true
      );

      mqttPublish(
        p + "/out/state",
        boolPayload(zoneOutRaw(controlZone)),
        true
      );

      mqttPublish(
        p + "/temporary/state",
        boolPayload(zoneTempRaw(controlZone)),
        true
      );

      if (haveDamper[physicalZone - 1]) {
        int pct = lround((damper[physicalZone - 1] / 15.0f) * 100.0f);
        mqttPublish(p + "/damper/state", String(pct), true);
      }

      mqttPublish(
        p + "/action/state",
        equipmentAction(physicalZone),
        true
      );
    }

    if (haveOutsideTemp) {
      mqttPublish(topicBase + "/system/outside_temperature/state", String(outsideTemp, 1), true);
    }

    if (haveLatTemp) {
      mqttPublish(topicBase + "/system/leaving_air_temperature/state", String(latTemp, 1), true);
    }

    if (haveEquipmentRaw) {
      mqttPublish(topicBase + "/system/equipment/state", equipmentText(), true);
    }

    if (haveSchedule) {
      mqttPublish(topicBase + "/system/schedule/state", scheduleName(), true);
    }

    dirty = false;
  }

  bool handleMqttCommand(const String &topic, const String &payload) {
    String systemPrefix = topicBase + "/";

    if (!topic.startsWith(systemPrefix)) return false;

    String relative = topic.substring(systemPrefix.length());

    Serial.printf("[%s] MQTT command: %s = %s\n",
                  systemName.c_str(), relative.c_str(), payload.c_str());

    bool ok = false;

    if (relative == "system/mode/set") {
      ok = setMode(payload);
    }
    else if (relative == "system/fan/set") {
      ok = setFan(payload);
    }
    else if (relative == "system/zone_control/set") {
      ok = setAllControl(payload);
    }
    else if (relative.startsWith("zone/")) {
      int firstSlash = relative.indexOf('/', 5);
      if (firstSlash < 0) return true;

      int zone = relative.substring(5, firstSlash).toInt();
      if (zone < 1 || zone > EXPECTED_ZONES) return true;

      String command = relative.substring(firstSlash + 1);

      if (command == "target/set") {
        ok = setActiveTemperature(zone, lround(payload.toFloat()));
      }
      else if (command == "cool_setpoint/set") {
        ok = setSetpoint(zone, true, lround(payload.toFloat()));
      }
      else if (command == "heat_setpoint/set") {
        ok = setSetpoint(zone, false, lround(payload.toFloat()));
      }
      else if (command == "hold/set") {
        ok = setHold(zone, parseBool(payload));
      }
      else if (command == "out/set") {
        ok = setOut(zone, parseBool(payload));
      }
      else if (command == "resume/set") {
        ok = resumeSchedule(zone);
      }
      else {
        return true;
      }
    }
    else {
      return false;
    }

    if (ok) {
      Serial.printf("[%s] command verified\n", systemName.c_str());
      publishState(true);
    } else {
      Serial.printf("[%s] command FAILED verification; refreshing state\n", systemName.c_str());
      refreshMaster();
      publishState(true);
    }

    return true;
  }

  // Public data / scheduling flags
  String systemId;
  String systemName;
  String topicBase;

  bool dirty = true;
  uint32_t nextPollMs = 0;

private:
  HardwareSerial &serial;
  int rxPin;
  int txPin;

  uint8_t rxBuf[RX_BUFFER_MAX];
  size_t rxLen = 0;

  bool seenValidFrame = false;
  uint32_t lastRxMs = 0;
  uint32_t lastTxMs = 0;

  enum WaitType {
    WAIT_NONE,
    WAIT_ACK,
    WAIT_ROW
  };

  WaitType waitType = WAIT_NONE;
  bool waitDone = false;
  uint8_t waitTable = 0;
  uint8_t waitRow = 0;
  RowData waitPayload;

  // System state
  int configuredZones = EXPECTED_ZONES;
  int displayedZone = 1;
  int scheduleCode = 0;
  bool haveSchedule = false;

  int modeCode = 4;
  int effectiveModeCode = 0;

  uint8_t tempMask = 0;
  uint8_t holdMask = 0;
  uint8_t outMask = 0;
  uint8_t allZone = 0;

  int coolSetpoint[8] = {0};
  int heatSetpoint[8] = {0};
  bool haveSetpoints = false;

  float zoneTemp[EXPECTED_ZONES] = {0};
  bool haveZoneTemp[EXPECTED_ZONES] = {false};

  int zoneHumidity[EXPECTED_ZONES] = {0};
  bool haveZoneHumidity[EXPECTED_ZONES] = {false};

  uint8_t damper[EXPECTED_ZONES] = {0};
  bool haveDamper[EXPECTED_ZONES] = {false};

  float outsideTemp = 0;
  bool haveOutsideTemp = false;

  float latTemp = 0;
  bool haveLatTemp = false;

  uint8_t equipmentRaw = 0;
  bool haveEquipmentRaw = false;

  bool fanOn = false;
  bool haveFanMode = false;

  // --------------------------------------------------------------------------
  // Low-level CZII
  // --------------------------------------------------------------------------

  static uint16_t crc16Arc(const uint8_t *data, size_t len) {
    uint16_t crc = 0x0000;

    for (size_t i = 0; i < len; ++i) {
      crc ^= data[i];

      for (int bit = 0; bit < 8; ++bit) {
        if (crc & 1) crc = (crc >> 1) ^ 0xA001;
        else crc >>= 1;
      }
    }

    return crc;
  }

  static int16_t signedWord(uint8_t highByte, uint8_t lowByte) {
    uint16_t v = ((uint16_t)highByte << 8) | lowByte;
    return (int16_t)v;
  }

  static float temp16(uint8_t highByte, uint8_t lowByte) {
    return signedWord(highByte, lowByte) / 16.0f;
  }

  static uint8_t zoneBit(int zone) {
    return (uint8_t)(1U << (zone - 1));
  }

  static void appendSignal(String &s, const char *value) {
    if (s.length()) s += " + ";
    s += value;
  }

  static bool parseBool(const String &payload) {
    String s = payload;
    s.toLowerCase();
    s.trim();
    return s == "on" || s == "1" || s == "true" || s == "yes";
  }

  String scheduleName() const {
    switch (scheduleCode) {
      case 0: return "Morning";
      case 1: return "Midday";
      case 2: return "Evening";
      case 3: return "Night";
      default: return String("Unknown(") + scheduleCode + ")";
    }
  }

  void parseFrames() {
    while (rxLen >= 10) {
      if (
        rxBuf[1] != 0x00 ||
        rxBuf[3] != 0x00 ||
        rxBuf[5] != 0x00 ||
        rxBuf[6] != 0x00 ||
        !(rxBuf[7] == 0x06 || rxBuf[7] == 0x0B || rxBuf[7] == 0x0C || rxBuf[7] == 0x15)
      ) {
        dropOneByte();
        continue;
      }

      size_t dataLen = rxBuf[4];
      size_t frameLen = 10 + dataLen;

      if (frameLen > FRAME_MAX) {
        dropOneByte();
        continue;
      }

      if (rxLen < frameLen) break;

      uint16_t receivedCrc = rxBuf[frameLen - 2] | ((uint16_t)rxBuf[frameLen - 1] << 8);
      uint16_t calculatedCrc = crc16Arc(rxBuf, frameLen - 2);

      if (receivedCrc != calculatedCrc) {
        dropOneByte();
        continue;
      }

      uint8_t frame[FRAME_MAX];
      memcpy(frame, rxBuf, frameLen);

      memmove(rxBuf, rxBuf + frameLen, rxLen - frameLen);
      rxLen -= frameLen;

      seenValidFrame = true;
      handleFrame(frame, frameLen);
    }
  }

  void dropOneByte() {
    if (rxLen == 0) return;
    memmove(rxBuf, rxBuf + 1, rxLen - 1);
    --rxLen;
  }

  void handleFrame(const uint8_t *frame, size_t frameLen) {
    (void)frameLen;

    uint16_t dst = frame[0] | ((uint16_t)frame[1] << 8);
    uint16_t src = frame[2] | ((uint16_t)frame[3] << 8);
    size_t dataLen = frame[4];
    uint8_t function = frame[7];
    const uint8_t *data = frame + 8;

    // Synchronous ACK match
    if (
      waitType == WAIT_ACK &&
      src == 1 &&
      dst == OUR_ADDRESS &&
      function == 0x06 &&
      dataLen == 1 &&
      data[0] == 0x00
    ) {
      waitDone = true;
    }

    // Synchronous row response match
    if (
      waitType == WAIT_ROW &&
      src == 1 &&
      dst == OUR_ADDRESS &&
      function == 0x06 &&
      dataLen >= 3 &&
      data[0] == 0x00 &&
      data[1] == waitTable &&
      data[2] == waitRow
    ) {
      waitPayload.copyFrom(data + 3, dataLen - 3);
      waitDone = true;
    }

    if (dataLen < 3) return;

    uint8_t table = data[1];
    uint8_t row = data[2];
    const uint8_t *payload = data + 3;
    size_t payloadLen = dataLen - 3;

    decodeRow(table, row, payload, payloadLen, function);
  }

  bool waitForBusIdle() {
    uint32_t start = millis();

    while (true) {
      service();

      uint32_t now = millis();
      uint32_t lastActivity = lastRxMs > lastTxMs ? lastRxMs : lastTxMs;

      if (elapsedMs(now, lastActivity, BUS_IDLE_MS)) return true;

      // Don't wait forever for an abnormally chatty/broken bus.
      if (elapsedMs(now, start, 10000)) return false;

      delay(2);
      yield();
    }
  }

  void sendFrame(const uint8_t *frame, size_t len) {
    serial.write(frame, len);
    serial.flush();
    lastTxMs = millis();
  }

  bool readRow(uint8_t table, uint8_t row, RowData &result) {
    if (!waitForBusIdle()) {
      Serial.printf("[%s] bus never became idle for read T%u/R%u\n",
                    systemName.c_str(), table, row);
      return false;
    }

    uint8_t frame[16];
    size_t p = 0;

    frame[p++] = 0x01; frame[p++] = 0x00;  // destination master
    frame[p++] = OUR_ADDRESS & 0xFF;
    frame[p++] = (OUR_ADDRESS >> 8) & 0xFF;
    frame[p++] = 0x03;
    frame[p++] = 0x00; frame[p++] = 0x00;
    frame[p++] = 0x0B;                     // READ
    frame[p++] = 0x00;
    frame[p++] = table;
    frame[p++] = row;

    uint16_t crc = crc16Arc(frame, p);
    frame[p++] = crc & 0xFF;
    frame[p++] = (crc >> 8) & 0xFF;

    waitType = WAIT_ROW;
    waitTable = table;
    waitRow = row;
    waitDone = false;
    waitPayload.clear();

    sendFrame(frame, p);

    uint32_t start = millis();

    while (!waitDone && !elapsedMs(millis(), start, RESPONSE_TIMEOUT_MS)) {
      service();
      delay(2);
      yield();
    }

    bool ok = waitDone;

    if (ok) {
      result = waitPayload;
    } else {
      Serial.printf("[%s] timeout reading T%u/R%u\n",
                    systemName.c_str(), table, row);
    }

    waitType = WAIT_NONE;
    waitDone = false;

    return ok;
  }

  bool writeRow(uint8_t table, uint8_t row, const RowData &payload) {
    if (payload.len + 13 > FRAME_MAX) return false;

    if (!waitForBusIdle()) {
      Serial.printf("[%s] bus never became idle for write T%u/R%u\n",
                    systemName.c_str(), table, row);
      return false;
    }

    uint8_t frame[FRAME_MAX];
    size_t p = 0;

    frame[p++] = 0x01; frame[p++] = 0x00;
    frame[p++] = OUR_ADDRESS & 0xFF;
    frame[p++] = (OUR_ADDRESS >> 8) & 0xFF;
    frame[p++] = 3 + payload.len;
    frame[p++] = 0x00; frame[p++] = 0x00;
    frame[p++] = 0x0C;                     // WRITE
    frame[p++] = 0x00;
    frame[p++] = table;
    frame[p++] = row;

    memcpy(frame + p, payload.data, payload.len);
    p += payload.len;

    uint16_t crc = crc16Arc(frame, p);
    frame[p++] = crc & 0xFF;
    frame[p++] = (crc >> 8) & 0xFF;

    waitType = WAIT_ACK;
    waitDone = false;

    sendFrame(frame, p);

    uint32_t start = millis();

    while (!waitDone && !elapsedMs(millis(), start, RESPONSE_TIMEOUT_MS)) {
      service();
      delay(2);
      yield();
    }

    bool ok = waitDone;

    if (!ok) {
      Serial.printf("[%s] no ACK writing T%u/R%u\n",
                    systemName.c_str(), table, row);
    }

    waitType = WAIT_NONE;
    waitDone = false;

    return ok;
  }

  // --------------------------------------------------------------------------
  // Decoder
  // --------------------------------------------------------------------------

  void decodeRow(
    uint8_t table,
    uint8_t row,
    const uint8_t *payload,
    size_t len,
    uint8_t function
  ) {
    bool changed = false;

    // T1/R2 - master summary
    if (table == 1 && row == 2 && function == 0x06 && len >= 10) {
      int newZones = payload[1];
      int newDisplayed = payload[7];
      int newSchedule = payload[9];

      changed =
        newZones != configuredZones ||
        newDisplayed != displayedZone ||
        newSchedule != scheduleCode ||
        !haveSchedule;

      configuredZones = newZones;
      displayedZone = newDisplayed;
      scheduleCode = newSchedule;
      haveSchedule = true;
    }

    // T1/R6 - Zone 1 temperature/humidity
    else if (table == 1 && row == 6 && function == 0x06 && len >= 5) {
      float t = temp16(payload[2], payload[3]);
      int h = payload[4];

      changed =
        !haveZoneTemp[0] ||
        fabs(zoneTemp[0] - t) > 0.01f ||
        !haveZoneHumidity[0] ||
        zoneHumidity[0] != h;

      zoneTemp[0] = t;
      haveZoneTemp[0] = true;

      zoneHumidity[0] = h;
      haveZoneHumidity[0] = true;
    }

    // T1/R12 - mode / TEMP / HOLD / OUT / ALL
    else if (table == 1 && row == 12 && function == 0x06 && len >= 14) {
      changed =
        modeCode != payload[1] ||
        effectiveModeCode != payload[3] ||
        tempMask != payload[6] ||
        holdMask != payload[7] ||
        outMask != payload[9] ||
        allZone != payload[12];

      modeCode = payload[1];
      effectiveModeCode = payload[3];
      tempMask = payload[6];
      holdMask = payload[7];
      outMask = payload[9];
      allZone = payload[12];
    }

    // T1/R16 - all zone setpoints
    else if (table == 1 && row == 16 && function == 0x06 && len >= 16) {
      if (!haveSetpoints) changed = true;

      for (int i = 0; i < 8; ++i) {
        if (coolSetpoint[i] != payload[i]) changed = true;
        if (heatSetpoint[i] != payload[8 + i]) changed = true;

        coolSetpoint[i] = payload[i];
        heatSetpoint[i] = payload[8 + i];
      }

      haveSetpoints = true;
    }

    // T1/R17 - fan configuration bit
    else if (table == 1 && row == 17 && function == 0x06 && len >= 1) {
      bool newFanOn = payload[0] & 0x04;
      changed = !haveFanMode || newFanOn != fanOn;

      fanOn = newFanOn;
      haveFanMode = true;
    }

    // T9/R1 - empirically confirmed remote sensor temperatures
    else if (table == 9 && row == 1 && function == 0x06 && len >= 8) {
      for (int slot = 0; slot < 4; ++slot) {
        uint8_t hi = payload[slot * 2];
        uint8_t lo = payload[slot * 2 + 1];

        if (hi == 0xFF && lo == 0xFF) continue;

        int zone = slot + 1;

        if (zone >= 2 && zone <= EXPECTED_ZONES) {
          float t = temp16(hi, lo);
          int i = zone - 1;

          if (!haveZoneTemp[i] || fabs(zoneTemp[i] - t) > 0.01f) changed = true;

          zoneTemp[i] = t;
          haveZoneTemp[i] = true;
        }
      }
    }

    // T9/R3 - outside and leaving-air temperature
    else if (table == 9 && row == 3 && function == 0x06 && len >= 7) {
      float outside = temp16(payload[1], payload[2]);
      bool latValid = payload[3] != 0xFF;
      float lat = latValid ? payload[3] : 0;

      if (!haveOutsideTemp || fabs(outsideTemp - outside) > 0.01f) changed = true;
      if (latValid && (!haveLatTemp || fabs(latTemp - lat) > 0.01f)) changed = true;

      outsideTemp = outside;
      haveOutsideTemp = true;

      if (latValid) {
        latTemp = lat;
        haveLatTemp = true;
      }
    }

    // T9/R4 - damper positions written by master to equipment controller
    else if (table == 9 && row == 4 && function == 0x0C && len >= EXPECTED_ZONES) {
      for (int i = 0; i < EXPECTED_ZONES; ++i) {
        if (!haveDamper[i] || damper[i] != payload[i]) changed = true;
        damper[i] = payload[i];
        haveDamper[i] = true;
      }
    }

    // T9/R5 - equipment output state
    else if (table == 9 && row == 5 && function == 0x0C && len >= 1) {
      if (!haveEquipmentRaw || equipmentRaw != payload[0]) changed = true;
      equipmentRaw = payload[0];
      haveEquipmentRaw = true;
    }

    if (changed) dirty = true;
  }

  // --------------------------------------------------------------------------
  // HA discovery builders
  // --------------------------------------------------------------------------

  String deviceFragment() const {
    String d;
    d.reserve(260);

    d += "\"device\":{";
    d += "\"identifiers\":[" + q(systemId) + "],";
    d += "\"name\":" + q(systemName) + ",";
    d += "\"manufacturer\":\"Bryant\",";
    d += "\"model\":\"Zone Perfect Plus\",";
    d += "\"sw_version\":\"Dual ESP32 CZII bridge 1.0\"";
    d += "}";

    return d;
  }

  void publishDiscoveryPayload(
    const String &component,
    const String &objectId,
    String payload
  ) {
    if (!gMqttConnected) return;

    if (!payload.endsWith("}")) return;

    payload.remove(payload.length() - 1);

    if (payload.length() > 1) payload += ",";
    payload += deviceFragment();
    payload += ",\"availability_topic\":" + q(BRIDGE_AVAILABILITY_TOPIC);
    payload += "}";

    String topic =
      String(DISCOVERY_PREFIX) + "/" +
      component + "/" +
      systemId + "/" +
      objectId + "/config";

    mqttPublish(topic, payload, true);
    delay(5);
  }

  void publishClimateDiscovery(int zone) {
    String p = topicBase + "/zone/" + zone;
    String unique = systemId + "_zone_" + zone + "_climate";

    String s;
    s.reserve(1800);

    s += "{";
    s += "\"name\":" + q(String("Zone ") + zone) + ",";
    s += "\"unique_id\":" + q(unique) + ",";
    s += "\"modes\":[\"off\",\"heat\",\"cool\",\"auto\"],";
    s += "\"mode_state_topic\":" + q(topicBase + "/system/mode/state") + ",";
    s += "\"mode_command_topic\":" + q(topicBase + "/system/mode/set") + ",";
    s += "\"fan_modes\":[\"auto\",\"on\"],";
    s += "\"fan_mode_state_topic\":" + q(topicBase + "/system/fan/state") + ",";
    s += "\"fan_mode_command_topic\":" + q(topicBase + "/system/fan/set") + ",";
    s += "\"current_temperature_topic\":" + q(p + "/temperature/state") + ",";
    s += "\"temperature_state_topic\":" + q(p + "/target/state") + ",";
    s += "\"temperature_command_topic\":" + q(p + "/target/set") + ",";
    s += "\"temperature_low_state_topic\":" + q(p + "/heat_setpoint/state") + ",";
    s += "\"temperature_low_command_topic\":" + q(p + "/heat_setpoint/set") + ",";
    s += "\"temperature_high_state_topic\":" + q(p + "/cool_setpoint/state") + ",";
    s += "\"temperature_high_command_topic\":" + q(p + "/cool_setpoint/set") + ",";
    s += "\"action_topic\":" + q(p + "/action/state") + ",";
    s += "\"temperature_unit\":\"F\",";
    s += "\"precision\":1.0,";
    s += "\"temp_step\":1.0,";
    s += "\"min_temp\":" + String(MIN_SETPOINT) + ",";
    s += "\"max_temp\":" + String(MAX_SETPOINT);

    if (zone == 1) {
      s += ",\"current_humidity_topic\":" + q(p + "/humidity/state");
    }

    s += "}";

    publishDiscoveryPayload("climate", String("zone_") + zone, s);
  }

  void publishSetpointNumberDiscovery(int zone, bool cooling) {
    String p = topicBase + "/zone/" + zone;
    String kind = cooling ? "cool" : "heat";
    String title = cooling ? "Cool Setpoint" : "Heat Setpoint";

    String s;
    s.reserve(900);

    s += "{";
    s += "\"name\":" + q(String("Zone ") + zone + " " + title) + ",";
    s += "\"unique_id\":" + q(systemId + "_zone_" + zone + "_" + kind + "_setpoint_number") + ",";
    s += "\"command_topic\":" + q(p + "/" + kind + "_setpoint/set") + ",";
    s += "\"state_topic\":" + q(p + "/" + kind + "_setpoint/state") + ",";
    s += "\"device_class\":\"temperature\",";
    s += "\"unit_of_measurement\":\"°F\",";
    s += "\"min\":" + String(MIN_SETPOINT) + ",";
    s += "\"max\":" + String(MAX_SETPOINT) + ",";
    s += "\"step\":1,";
    s += "\"mode\":\"box\"";
    s += "}";

    publishDiscoveryPayload(
      "number",
      String("zone_") + zone + "_" + kind + "_setpoint",
      s
    );
  }

  void publishSwitchDiscovery(int zone, const char *commandName, const char *title) {
    String p = topicBase + "/zone/" + zone;

    String s;
    s.reserve(800);

    s += "{";
    s += "\"name\":" + q(String("Zone ") + zone + " " + title) + ",";
    s += "\"unique_id\":" + q(systemId + "_zone_" + zone + "_" + commandName) + ",";
    s += "\"command_topic\":" + q(p + "/" + commandName + "/set") + ",";
    s += "\"state_topic\":" + q(p + "/" + commandName + "/state") + ",";
    s += "\"payload_on\":\"ON\",";
    s += "\"payload_off\":\"OFF\"";
    s += "}";

    publishDiscoveryPayload(
      "switch",
      String("zone_") + zone + "_" + commandName,
      s
    );
  }

  void publishResumeButtonDiscovery(int zone) {
    String p = topicBase + "/zone/" + zone;

    String s;
    s.reserve(850);

    s += "{";
    s += "\"name\":" + q(String("Zone ") + zone + " Resume Schedule") + ",";
    s += "\"unique_id\":" + q(systemId + "_zone_" + zone + "_resume_schedule") + ",";
    s += "\"command_topic\":" + q(p + "/resume/set") + ",";
    s += "\"payload_press\":\"PRESS\",";
    s += "\"entity_category\":\"config\"";
    s += "}";

    publishDiscoveryPayload(
      "button",
      String("zone_") + zone + "_resume_schedule",
      s
    );
  }

  void publishTemporaryDiscovery(int zone) {
    String p = topicBase + "/zone/" + zone;

    String s;
    s.reserve(900);

    s += "{";
    s += "\"name\":" + q(String("Zone ") + zone + " Temporary Override") + ",";
    s += "\"unique_id\":" + q(systemId + "_zone_" + zone + "_temporary") + ",";
    s += "\"state_topic\":" + q(p + "/temporary/state") + ",";
    s += "\"payload_on\":\"ON\",";
    s += "\"payload_off\":\"OFF\",";
    s += "\"enabled_by_default\":false,";
    s += "\"entity_category\":\"diagnostic\"";
    s += "}";

    publishDiscoveryPayload(
      "binary_sensor",
      String("zone_") + zone + "_temporary",
      s
    );
  }

  void publishDamperDiscovery(int zone) {
    String p = topicBase + "/zone/" + zone;

    String s;
    s.reserve(850);

    s += "{";
    s += "\"name\":" + q(String("Zone ") + zone + " Damper") + ",";
    s += "\"unique_id\":" + q(systemId + "_zone_" + zone + "_damper") + ",";
    s += "\"state_topic\":" + q(p + "/damper/state") + ",";
    s += "\"unit_of_measurement\":\"%\",";
    s += "\"state_class\":\"measurement\",";
    s += "\"entity_category\":\"diagnostic\"";
    s += "}";

    publishDiscoveryPayload(
      "sensor",
      String("zone_") + zone + "_damper",
      s
    );
  }

  void publishAllSelectDiscovery() {
    String s;
    s.reserve(1000);

    s += "{";
    s += "\"name\":\"Zone Control\",";
    s += "\"unique_id\":" + q(systemId + "_zone_control") + ",";
    s += "\"command_topic\":" + q(topicBase + "/system/zone_control/set") + ",";
    s += "\"state_topic\":" + q(topicBase + "/system/zone_control/state") + ",";
    s += "\"options\":[";
    for (int i = 0; i < 4; ++i) {
      if (i) s += ",";
      s += q(ALL_OPTIONS[i]);
    }
    s += "],";
    s += "\"icon\":\"mdi:home-group\"";
    s += "}";

    publishDiscoveryPayload("select", "zone_control", s);
  }

  void publishSystemSensorDiscovery(
    const String &objectId,
    const String &name,
    const String &relativeStateTopic,
    const String &extraJson
  ) {
    String s;
    s.reserve(1000);

    s += "{";
    s += "\"name\":" + q(name) + ",";
    s += "\"unique_id\":" + q(systemId + "_" + objectId) + ",";
    s += "\"state_topic\":" + q(topicBase + "/" + relativeStateTopic);

    if (extraJson.length()) {
      s += ",";
      s += extraJson;
    }

    s += "}";

    publishDiscoveryPayload("sensor", objectId, s);
  }

};

// ============================================================================
// HARDWARE SERIALS + TWO BRYANT SYSTEMS
// ============================================================================

HardwareSerial HVACSerial1(1);
HardwareSerial HVACSerial2(2);

BryantSystem HVAC1(
  HVACSerial1,
  HVAC1_RX_PIN,
  HVAC1_TX_PIN,
  SYSTEM1_ID,
  SYSTEM1_NAME
);

BryantSystem HVAC2(
  HVACSerial2,
  HVAC2_RX_PIN,
  HVAC2_TX_PIN,
  SYSTEM2_ID,
  SYSTEM2_NAME
);

BryantSystem *SYSTEMS[] = { &HVAC1, &HVAC2 };
static const size_t SYSTEM_COUNT = sizeof(SYSTEMS) / sizeof(SYSTEMS[0]);

// ============================================================================
// MQTT
// ============================================================================

bool mqttPublish(const String &topic, const String &payload, bool retain) {
  if (!gMqttClient || !gMqttConnected) return false;

  int msgId = esp_mqtt_client_publish(
    gMqttClient,
    topic.c_str(),
    payload.c_str(),
    payload.length(),
    0,
    retain ? 1 : 0
  );

  return msgId >= 0;
}

static void mqttEventHandler(
  void *handlerArgs,
  esp_event_base_t eventBase,
  int32_t eventId,
  void *eventData
) {
  (void)handlerArgs;
  (void)eventBase;

  esp_mqtt_event_handle_t event = (esp_mqtt_event_handle_t)eventData;

  switch ((esp_mqtt_event_id_t)eventId) {
    case MQTT_EVENT_CONNECTED:
      gMqttConnected = true;
      gMqttNeedsDiscovery = true;

      esp_mqtt_client_subscribe(
        gMqttClient,
        "bryant/+/system/+/set",
        0
      );

      esp_mqtt_client_subscribe(
        gMqttClient,
        "bryant/+/zone/+/+/set",
        0
      );

      esp_mqtt_client_subscribe(
        gMqttClient,
        "homeassistant/status",
        0
      );

      Serial.println("[MQTT] connected");
      break;

    case MQTT_EVENT_DISCONNECTED:
      gMqttConnected = false;
      Serial.println("[MQTT] disconnected");
      break;

    case MQTT_EVENT_DATA: {
      // HA command payloads are tiny; reject fragmented messages rather than
      // guessing at reassembly.
      if (event->total_data_len != event->data_len) break;

      MqttCommand cmd = {};

      int topicLen = min((int)sizeof(cmd.topic) - 1, event->topic_len);
      int dataLen = min((int)sizeof(cmd.payload) - 1, event->data_len);

      memcpy(cmd.topic, event->topic, topicLen);
      cmd.topic[topicLen] = '\0';

      memcpy(cmd.payload, event->data, dataLen);
      cmd.payload[dataLen] = '\0';

      if (gMqttCommandQueue) {
        xQueueSend(gMqttCommandQueue, &cmd, 0);
      }

      break;
    }

    default:
      break;
  }
}

void initMqtt() {
  if (gMqttStarted) return;

  snprintf(
    gMqttUri,
    sizeof(gMqttUri),
    "mqtt://%s:%u",
    MQTT_HOST,
    (unsigned)MQTT_PORT
  );

  esp_mqtt_client_config_t cfg = {};

  cfg.broker.address.uri = gMqttUri;
  cfg.credentials.client_id = BRIDGE_HOSTNAME;

  if (strlen(MQTT_USERNAME) > 0) {
    cfg.credentials.username = MQTT_USERNAME;
  }

  if (strlen(MQTT_PASSWORD) > 0) {
    cfg.credentials.authentication.password = MQTT_PASSWORD;
  }

  cfg.session.keepalive = 30;

  cfg.session.last_will.topic = BRIDGE_AVAILABILITY_TOPIC;
  cfg.session.last_will.msg = "offline";
  cfg.session.last_will.msg_len = 0;
  cfg.session.last_will.qos = 1;
  cfg.session.last_will.retain = true;

  cfg.buffer.size = 4096;
  cfg.buffer.out_size = 4096;

  cfg.task.stack_size = 6144;

  gMqttClient = esp_mqtt_client_init(&cfg);

  if (!gMqttClient) {
    Serial.println("[MQTT] esp_mqtt_client_init failed");
    return;
  }

  esp_mqtt_client_register_event(
    gMqttClient,
    MQTT_EVENT_ANY,
    mqttEventHandler,
    nullptr
  );

  esp_err_t err = esp_mqtt_client_start(gMqttClient);

  if (err == ESP_OK) {
    gMqttStarted = true;
    Serial.printf("[MQTT] client started -> %s\n", gMqttUri);
  } else {
    Serial.printf("[MQTT] start failed: %d\n", (int)err);
  }
}

void processMqttCommands() {
  if (!gMqttCommandQueue) return;

  MqttCommand cmd;

  while (xQueueReceive(gMqttCommandQueue, &cmd, 0) == pdTRUE) {
    String topic(cmd.topic);
    String payload(cmd.payload);

    if (topic == "homeassistant/status") {
      payload.toLowerCase();
      payload.trim();

      if (payload == "online") {
        gMqttNeedsDiscovery = true;
      }
      continue;
    }

    bool handled = false;

    for (size_t i = 0; i < SYSTEM_COUNT; ++i) {
      if (SYSTEMS[i]->handleMqttCommand(topic, payload)) {
        handled = true;
        break;
      }
    }

    if (!handled) {
      Serial.printf("[MQTT] unhandled command topic: %s\n", topic.c_str());
    }
  }
}

void publishDiscoveryAndInitialState() {
  if (!gMqttConnected) return;

  mqttPublish(BRIDGE_AVAILABILITY_TOPIC, "online", true);

  for (size_t i = 0; i < SYSTEM_COUNT; ++i) {
    SYSTEMS[i]->publishDiscovery();
    SYSTEMS[i]->publishState(true);
  }

  gMqttNeedsDiscovery = false;
}

// ============================================================================
// NETWORK
// ============================================================================

static void networkEvent(arduino_event_id_t event) {
  switch (event) {
    case ARDUINO_EVENT_ETH_START:
      // Hostname must be set after ETH starts and before DHCP completes.
      ETH.setHostname(BRIDGE_HOSTNAME);
      Serial.println("[ETH] started");
      break;

    case ARDUINO_EVENT_ETH_CONNECTED:
      Serial.println("[ETH] link up");
      break;

    case ARDUINO_EVENT_ETH_GOT_IP:
      gEthHasIP = true;
      Serial.printf(
        "[ETH] IP %s\n",
        ETH.localIP().toString().c_str()
      );
      break;

    case ARDUINO_EVENT_ETH_LOST_IP:
    case ARDUINO_EVENT_ETH_DISCONNECTED:
      gEthHasIP = false;
      Serial.println("[ETH] unavailable");
      break;

    case ARDUINO_EVENT_WIFI_STA_GOT_IP:
      gWifiHasIP = true;
      Serial.printf(
        "[WiFi] IP %s\n",
        WiFi.localIP().toString().c_str()
      );
      break;

    case ARDUINO_EVENT_WIFI_STA_DISCONNECTED:
      gWifiHasIP = false;
      Serial.println("[WiFi] disconnected");
      break;

    default:
      break;
  }
}

void startWifiFallback() {
  if (strlen(WIFI_SSID) == 0) return;

  Serial.printf("[WiFi] connecting to %s\n", WIFI_SSID);

  WiFi.mode(WIFI_STA);
  WiFi.setHostname(BRIDGE_HOSTNAME);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  gLastWifiAttempt = millis();
}

void startEthernet() {
  // Olimex-specific requirement: keep PHY power low until ETH.begin() has
  // arranged the 50 MHz RMII clock on GPIO17.
  pinMode(OLIMEX_ETH_PHY_POWER, OUTPUT);
  digitalWrite(OLIMEX_ETH_PHY_POWER, LOW);
  delay(200);

  bool ok = ETH.begin(
    OLIMEX_ETH_PHY_TYPE,
    OLIMEX_ETH_PHY_ADDR,
    OLIMEX_ETH_MDC_PIN,
    OLIMEX_ETH_MDIO_PIN,
    OLIMEX_ETH_PHY_POWER,
    OLIMEX_ETH_CLK_MODE
  );

  Serial.printf("[ETH] begin: %s\n", ok ? "OK" : "FAILED");
}

void manageNetwork() {
  // Ethernet preferred. WiFi STA has a higher default lwIP route priority than
  // Ethernet, so explicitly disconnect WiFi once Ethernet has an IP.
  if (gEthHasIP) {
    if (gWifiHasIP || WiFi.status() == WL_CONNECTED) {
      Serial.println("[NET] Ethernet ready; disconnecting WiFi");
      WiFi.disconnect(false, false);
    }
  } else {
    if (!gWifiHasIP && strlen(WIFI_SSID) > 0) {
      if (elapsedMs(millis(), gLastWifiAttempt, 10000)) {
        startWifiFallback();
      }
    }
  }

  bool online = gEthHasIP || gWifiHasIP;

  if (online && !gMqttStarted) {
    initMqtt();
  }
}

// ============================================================================
// SETUP / LOOP
// ============================================================================

void setup() {
  Serial.begin(115200);
  delay(500);

  Serial.println();
  Serial.println("Bryant Zone Perfect Plus Dual ESP32 Bridge");
  Serial.println("==========================================");

  gMqttCommandQueue = xQueueCreate(12, sizeof(MqttCommand));

  if (!gMqttCommandQueue) {
    Serial.println("ERROR: unable to create MQTT command queue");
  }

  HVAC1.begin();
  HVAC2.begin();

  // Stagger active polls so both HVAC buses aren't queried simultaneously.
  HVAC1.nextPollMs = millis() + 4000;
  HVAC2.nextPollMs = millis() + 19000;

  WiFi.onEvent(networkEvent);

  // Start WiFi immediately for commissioning; Ethernet takes over automatically
  // when a link/DHCP lease is available.
  startWifiFallback();
  startEthernet();

  Serial.println("Setup complete.");
}

void loop() {
  // Keep both serial buses drained continuously.
  HVAC1.service();
  HVAC2.service();

  manageNetwork();

  if (gMqttNeedsDiscovery && gMqttConnected) {
    publishDiscoveryAndInitialState();
  }

  processMqttCommands();

  uint32_t now = millis();

  for (size_t i = 0; i < SYSTEM_COUNT; ++i) {
    BryantSystem *sys = SYSTEMS[i];

    if (sys->hasSeenBus() && (int32_t)(now - sys->nextPollMs) >= 0) {
      Serial.printf("[%s] periodic master refresh\n", sys->systemName.c_str());

      sys->refreshMaster();
      sys->publishState(true);

      sys->nextPollMs = millis() + MASTER_POLL_MS;
    } else {
      sys->publishState(false);
    }
  }

  delay(2);
}
