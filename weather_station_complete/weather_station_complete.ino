/*
  IoT Weather Station - Complete Version
  Board: Arduino Nano 33 IoT
  Sensor: BME280 (I2C)
  Storage: microSD card module (SPI)
  Dashboard: ThingSpeak (over MQTT)

  WHAT THIS CODE DOES:
  1. Reads temperature, humidity, and pressure from the BME280 on a timer.
  2. If Wi-Fi + MQTT are connected, publishes the reading live to ThingSpeak.
  3. If NOT connected, saves the reading (with a timestamp) to the SD card
     instead of losing it.
  4. Once the connection comes back, automatically reads everything saved
     on the SD card and sends it to ThingSpeak, then clears the backlog.

  Before uploading, fill in anything in the SECRETS section if it changes.
*/

#include <WiFiNINA.h>
#include <ArduinoMqttClient.h>
#include <Wire.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_BME280.h>
#include <SPI.h>
#include <SD.h>

// ---------------- SECRETS: fill these in ----------------
const char* WIFI_SSID     = "lifeline2";
const char* WIFI_PASSWORD = "coolspoons193";

// From ThingSpeak: Devices > MQTT > (your device)
const char* MQTT_CLIENT_ID = "IQUAGTwPMhMcBBINHSsABA4";
const char* MQTT_USERNAME  = "IQUAGTwPMhMcBBINHSsABA4";
const char* MQTT_PASSWORD  = "RRdJYIi8fNQGtf2b1oZKOp/4";

// From your ThingSpeak channel page
const long CHANNEL_ID = 3500432;
// ----------------------------------------------------------

const char* MQTT_BROKER = "mqtt3.thingspeak.com";
const int   MQTT_PORT   = 1883;

// How often to take and handle a new reading (in milliseconds)
const unsigned long READING_INTERVAL_MS = 20000; // 20 seconds
unsigned long lastReadingTime = 0;

// SD card chip-select pin -- change this if you wired CS to a different pin
const int SD_CS_PIN = 10;
const char* LOG_FILE_NAME = "weather.csv";

bool sdCardReady = false;

WiFiClient wifiClient;
MqttClient mqttClient(wifiClient);
Adafruit_BME280 bme;

void setup() {
  Serial.begin(9600);
  unsigned long serialStart = millis();
  while (!Serial && millis() - serialStart < 3000) {
    ; // wait up to 3 seconds for Serial Monitor, but don't block forever
  }

  Serial.println("Starting IoT Weather Station...");

  // --- BME280 setup ---
  if (!bme.begin(0x76)) {
    if (!bme.begin(0x77)) {
      Serial.println("Could not find a valid BME280 sensor. Check wiring!");
      while (1) delay(10);
    }
  }
  Serial.println("BME280 sensor found.");

  // --- SD card setup ---
  if (!SD.begin(SD_CS_PIN)) {
    Serial.println("SD card initialization failed! Offline backup will not work.");
    sdCardReady = false;
  } else {
    Serial.println("SD card ready.");
    sdCardReady = true;
  }

  connectToWiFi();
  connectToMqttBroker();
}

void loop() {
  // Try to keep Wi-Fi and MQTT alive
  if (WiFi.status() != WL_CONNECTED) {
    connectToWiFi();
  }

  if (WiFi.status() == WL_CONNECTED && !mqttClient.connected()) {
    connectToMqttBroker();
  }

  if (mqttClient.connected()) {
    mqttClient.poll();
  }

  // If we just regained a full connection, try to flush anything backed up on the SD card
  static bool wasConnected = false;
  bool isConnectedNow = (WiFi.status() == WL_CONNECTED && mqttClient.connected());
  if (isConnectedNow && !wasConnected) {
    Serial.println("Connection restored. Checking for backed-up readings...");
    resendBackedUpReadings();
  }
  wasConnected = isConnectedNow;

  // Take and handle a new reading every READING_INTERVAL_MS
  if (millis() - lastReadingTime >= READING_INTERVAL_MS) {
    lastReadingTime = millis();
    handleNewReading();
  }
}

void connectToWiFi() {
  Serial.print("Connecting to Wi-Fi: ");
  Serial.println(WIFI_SSID);

  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  unsigned long startAttempt = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - startAttempt < 10000) {
    Serial.print(".");
    delay(500);
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println();
    Serial.print("Wi-Fi connected. IP address: ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println();
    Serial.println("Wi-Fi connection failed. Will keep retrying.");
  }
}

void connectToMqttBroker() {
  if (WiFi.status() != WL_CONNECTED) return;

  Serial.print("Connecting to MQTT broker: ");
  Serial.println(MQTT_BROKER);

  mqttClient.setId(MQTT_CLIENT_ID);
  mqttClient.setUsernamePassword(MQTT_USERNAME, MQTT_PASSWORD);

  if (mqttClient.connect(MQTT_BROKER, MQTT_PORT)) {
    Serial.println("Connected to MQTT broker!");
  } else {
    Serial.print("MQTT connection failed. Error code: ");
    Serial.println(mqttClient.connectError());
  }
}

// Reads the sensor once, then either publishes live or saves to SD
void handleNewReading() {
  float temperature = bme.readTemperature();        // Celsius
  float humidity     = bme.readHumidity();           // %
  float pressure      = bme.readPressure() / 100.0F; // hPa
  unsigned long timestamp = millis(); // time since boot, in ms (swap for an RTC if you add one)

  bool isConnected = (WiFi.status() == WL_CONNECTED && mqttClient.connected());

  if (isConnected) {
    publishReading(temperature, humidity, pressure);
  } else {
    Serial.println("Not connected. Saving reading to SD card instead.");
    saveReadingToSD(timestamp, temperature, humidity, pressure);
  }
}

// Publishes one reading live to ThingSpeak over MQTT
void publishReading(float temperature, float humidity, float pressure) {
  String topic = "channels/" + String(CHANNEL_ID) + "/publish";
  String payload = "field1=" + String(temperature, 2) +
                    "&field2=" + String(humidity, 2) +
                    "&field3=" + String(pressure, 2);

  Serial.print("Publishing live: ");
  Serial.println(payload);

  mqttClient.beginMessage(topic);
  mqttClient.print(payload);
  mqttClient.endMessage();
}

// Appends one reading as a CSV line to the SD card log file
void saveReadingToSD(unsigned long timestamp, float temperature, float humidity, float pressure) {
  if (!sdCardReady) {
    Serial.println("SD card not available. Reading is being lost!");
    return;
  }

  File logFile = SD.open(LOG_FILE_NAME, FILE_WRITE);
  if (logFile) {
    logFile.print(timestamp);
    logFile.print(",");
    logFile.print(temperature, 2);
    logFile.print(",");
    logFile.print(humidity, 2);
    logFile.print(",");
    logFile.println(pressure, 2);
    logFile.close();
    Serial.println("Reading saved to SD card.");
  } else {
    Serial.println("Error opening SD log file for writing.");
  }
}

// Reads every line out of the SD log file, publishes each one, then clears the file
void resendBackedUpReadings() {
  if (!sdCardReady) return;

  if (!SD.exists(LOG_FILE_NAME)) {
    Serial.println("No backed-up readings found.");
    return;
  }

  File logFile = SD.open(LOG_FILE_NAME, FILE_READ);
  if (!logFile) {
    Serial.println("Error opening SD log file for reading.");
    return;
  }

  int linesSent = 0;

  while (logFile.available()) {
    String line = logFile.readStringUntil('\n');
    line.trim();
    if (line.length() == 0) continue;

    // Expected format: timestamp,temperature,humidity,pressure
    int firstComma  = line.indexOf(',');
    int secondComma = line.indexOf(',', firstComma + 1);
    int thirdComma  = line.indexOf(',', secondComma + 1);

    if (firstComma == -1 || secondComma == -1 || thirdComma == -1) {
      continue; // skip malformed lines instead of crashing
    }

    float temperature = line.substring(firstComma + 1, secondComma).toFloat();
    float humidity     = line.substring(secondComma + 1, thirdComma).toFloat();
    float pressure      = line.substring(thirdComma + 1).toFloat();

    publishReading(temperature, humidity, pressure);
    linesSent++;

    delay(1500); // ThingSpeak's free tier limits updates to about 1 every 15 seconds;
                 // adjust this delay if you're catching up on a lot of backed-up readings
  }

  logFile.close();

  // Clear the file now that everything has been resent
  SD.remove(LOG_FILE_NAME);

  Serial.print("Resent and cleared ");
  Serial.print(linesSent);
  Serial.println(" backed-up readings.");
}
