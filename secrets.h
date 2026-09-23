#pragma once

// Wi-Fi fallback.
// Leave WIFI_SSID empty if you want to disable Wi-Fi completely.
#define WIFI_SSID       "YOUR_WIFI_SSID"
#define WIFI_PASSWORD   "YOUR_WIFI_PASSWORD"

// MQTT broker.
// Define the IP address of your MQTT broker.
// If using Mosquitto, it is the IP of your Home Assistant instance.
#define MQTT_HOST       "your_ip"
#define MQTT_PORT       1883

// Leave these as empty strings if your broker allows anonymous access.
#define MQTT_USERNAME   "YOUR_MQTT_USERNAME"
#define MQTT_PASSWORD   "YOUR_MQTT_PASSWORD"

// Device hostname and MQTT client ID.
#define BRIDGE_HOSTNAME "bryant-zpp-bridge"
