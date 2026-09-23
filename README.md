Abstract

This project allows one or more Bryant Zone Perfect Plus zoned HVAC systems to be used with platforms such as Home Assistant via MQTT. The Python script runs on a computer connected to the second RS-485 port of the HVAC control board via a USB to RS-485 adapter. It serves as a proof-of-concept and way to test whether the serial protocol is correct. To actually deploy the program via an ESP32, the Arduino program in C++ is used. It is able to interface with multiple Zone Perfect Plus systems at once. All system functions are controlled via MQTT, and they show up as entities of a device in Home Assistant.

"Why not use a smart thermostat?"

Anyone with one of these systems will find that they simply cannot use standard thermostats. Instead of normal 24V wiring with wires for heat, cool, fan, etc., the special Bryant thermostat communicates with the system over an RS-485 serial interface. This allows it to request the system state, including temperature in each zone, position of each damper, heat/cool information, outside temperature, and so on. In order to use a standard thermostat, the zoning control board, all room temperature sensors, and perhaps even all dampers would have to be replaced with something like a Honeywell system. This is expensive and hard to justify when your HVAC system is fully working. Additionally, a smart thermostat may have to be installed in every room which currently has a Bryant temperature sensor, and each sensor only uses 2 wires. This means that your home wiring is likely to lack the number of conductors needed for standard thermostats in each room.

"What hardware is needed?"

I used an Olimex ESP32-POE-EA-IND along with two DFRobot DFR0845 isolated RS-485 to UART interfaces. The Olimex board is a good fit for an attic environment because it is rated for -40 to 85 C and has an Ethernet interface galvanically isolated from the ESP32 up to 3000 VDC. This means you can avoid dealing with flaky WiFi reception in your attic (it still has WiFi if you need it). See https://www.olimex.com/Products/IoT/ESP32/ESP32-POE-ISO/open-source-hardware for more information. The DFR0845 interface is used to protect the Bryant control board from harm, since it isolates the RS-485 interface up to 3000 VDC. It can also be powered by the 3.3 V output of the ESP32 to avoid the clutter of a separate power supply or connecting to any power output pins of the HVAC board. It is also rated for -40 to 105 C. One such serial interface is needed for each Zone Perfect Plus system. See https://wiki.dfrobot.com/dfr0845/#tech_specs for more information. While isolating the Bryant control board may not be needed to facilitate communication, it is an easy way to reduce the likelihood of board damage. These control boards are hard to come by and may be expensive to repair/replace. With that said, try this at your own risk and shut off power before making any connections. I mounted my electronics in a generic ABS case and cut holes for ports as needed. I then mounted the finished box below one of my Bryant control boards in the attic. A standard 4-conductor copper alarm wire rated for attic use worked perfectly, since the serial communication is at a low baud rate.

"What functions are supported?"

The program supports every function of the system, save for scheduling, which is so painful to use that it's best left at the thermostat. You can always create automations to change temperatures and enable/disable zones while leaving the thermostat on HOLD mode. Supported functions include:
Temperature at each zone
Leaving air temperature
Position of each damper
Indoor relative humidity at thermostat (zone 1)
Outside temperature
Mode (Heat, Cool, Auto, Off)
Fan (Auto, On)
OUT (used to disable certain zones)
HOLD (hold mode for each zone)
ALL (makes the system a single zone, based on a zone of your choice)
Equipment status (Fan, Cooling, Heating, Idle, etc.)
Resume schedule (disables HOLD and OUT for a given zone and lets the thermostat use its schedule)

The system status is broadcast over the serial interface every 10-11 seconds, so the information being sent to your MQTT broker is constantly updated. If your HVAC system is set up with dehumidification mode and a reversible heat pump, some of those functions might not work since I can't test them. All the functions of my HVAC system (conventional AC and gas furnace) work properly.

"How do I set it up?"

To use the Python test script, you will need a USB to RS-485 interface. Use properly rated 3-or-more conductor copper wire (e.g. 4 conductor alarm wire) to connect to one of the RS-485 communication ports on the Zone Perfect Plus control board (usually located in the attic). DO NOT connect to V+, as it is not needed and poses a risk of being shorted to ground. Connect to RS+, RS-, and VG. Follow your adapter's instructions regarding which pin is TX and which is RX, but do not connect VG to your adapter. For testing, I ran a length of wire from my attic down to the living area through the access hatch. Find the address of the serial port and modify the line in the beginning of the program accordingly. Additionally, create a new username and password to publish to your MQTT broker, such as Mosquitto in Home Assistant. Modify the Python script with these credentials. The script should work for systems up to 8 zones, but make sure they are not ignored somewhere as I only needed 3. Run the script and test all functions.

To deploy the actual code on the ESP32, use the Arduino IDE and make a project containing the .ino file and the secrets.h file. Put the MQTT credentials and your WiFi credentials in the secrets.h file. It is recommended that you place the ESP32 on a separate VLAN and block internet access for security. If you are using PoE, then WiFi is optional. However, it serves as a backup, with the program switching to WiFi if Ethernet goes down. If you do not have PoE in your attic, then the board can be powered via USB. Upload the script to the ESP32, following the installation instructions. I found that it only worked when the baud rate was reduced to 921600. Once the ESP board is flashed, connect the DFRobot serial board(s). Shut off power at the breaker, then connect one Zone Perfect Plus system and try it out. RS+ to A, RS- to B, and VG to GND on the serial board. Again, DO NOT connect V+ from the Bryant control board to anything! Ensure that the 120 Ohm termination resistor is switched off. If that works, connect the other Bryant system in the same manner. Once you have confirmed everything is working, build an enclosure for the hardware and install it near your control board(s). Please make sure power is turned off before making connections.
Please note that the Arduino file is designed for two Zone Perfect Plus systems with 3 zones each. You will need to modify it to suit your setup, but it should be able to read all 8 zones of each system properly.
Please see the attached pictures and read Bryant's installation manual for the Zone Perfect Plus system for connecting to its RS-485 port.

Sources and Attributions

I referenced the "CZII_to_MQTT" repository (https://github.com/jwarcd/CZII_to_MQTT) since the Carrier Comfort Zone II system seems to use the same protocol. However, my HVAC system is set up differently than the one shown in the repository (I do not have a reversible heat pump). Additionally, I used ChatGPT for assistance with programming as I was not familiar with serial communication and MQTT in Python. It also helped me rewrite the script in C++ for use with the ESP32, as well as research hardware for the final deployment. I am not trying to plagiarise anyone here in case I missed some attribution; this project is aimed at helping those with Zone Perfect Plus systems save money on costly system replacements just for app access.
