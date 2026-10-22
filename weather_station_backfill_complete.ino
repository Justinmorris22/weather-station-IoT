/*
  IoT Weather Station - SD Log + Timestamped Backfill
  Board: Arduino Nano 33 IoT
  Sensor: BME280 (I2C)
  Storage: microSD card module (SPI)
  Dashboard: ThingSpeak over MQTT

  How it works:
  - Reads temperature, humidity, and pressure every 20 seconds.
  - EVERY reading is saved to weather.csv (permanent log, never deleted).
  - If a reading can't be published (or older readings are still waiting),
    it is also added to pending.csv, a queue of readings not yet on ThingSpeak.
  - When online, the queue is sent oldest-first, one row every 16 seconds,
    using each reading's ORIGINAL timestamp (created_at).
  - pending.csv is deleted once everything in it has been sent.
    weather.csv always keeps the full history.
  - The queue position is saved on the SD card (pendoff.txt), so a reboot
    in the middle of a backfill won't resend rows.

  IMPORTANT:
  1. Enter your Wi-Fi and ThingSpeak credentials below.
  2. If credentials were shared publicly, change them before using this sketch.
  3. ThingSpeak free channels require at least 15 seconds between updates.
     This sketch uses a 16-second minimum publish gap.
  4. Readings taken before the clock has synced are marked UNSYNCED and can't
     be backfilled with a correct time. They stay in weather.csv only.
*/

#include <WiFiNINA.h>
#include <ArduinoMqttClient.h>
#include <Wire.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_BME280.h>
#include <SPI.h>
#include <SD.h>
#include <TimeLib.h>
#include <math.h>

// ------------------- ENTER YOUR CREDENTIALS -------------------
const char* WIFI_SSID     = "lifeline2";
const char* WIFI_PASSWORD = "coolspoons193";

// From ThingSpeak: Devices > MQTT > (your device)
const char* MQTT_CLIENT_ID = "IQUAGTwPMhMcBBINHSsABA4";
const char* MQTT_USERNAME  = "IQUAGTwPMhMcBBINHSsABA4";
const char* MQTT_PASSWORD  = "RRdJYIi8fNQGtf2b1oZKOp/4";

const long CHANNEL_ID = 3500432;
// --------------------------------------------------------------

const char* MQTT_BROKER = "mqtt3.thingspeak.com";
const int MQTT_PORT = 1883;

// ------------------------- SETTINGS ----------------------------
const unsigned long READING_INTERVAL_MS  = 60000UL; // 30 seconds
const unsigned long BACKFILL_INTERVAL_MS = 15000UL; // one backfill row per 16 s
const unsigned long MIN_PUBLISH_GAP_MS   = 15000UL; // ThingSpeak rate limit
const unsigned long RECONNECT_RETRY_MS   = 15000UL; // retry every 15 seconds
const unsigned long WIFI_CONNECT_TIMEOUT_MS = 10000UL;

const int SD_CS_PIN = 10;
const char* LOG_FILE     = "weather.csv";  // permanent log
const char* PENDING_FILE = "pending.csv";  // queue of unsent readings
const char* OFFSET_FILE  = "pendoff.txt";  // how far into the queue we've sent
// --------------------------------------------------------------

bool sdCardReady = false;
bool timeIsSynced = false;
bool pendingExists = false;

unsigned long pendingOffset = 0;
unsigned long lastReadingTime = 0;
unsigned long lastBackfillTime = 0;
unsigned long lastPublishTime = 0;
unsigned long lastWifiAttempt = 0;
unsigned long lastMqttAttempt = 0;
bool hasPublishedBefore = false;
int mqttFailCount = 0;

WiFiClient wifiClient;
MqttClient mqttClient(wifiClient);
Adafruit_BME280 bme;

// Forward declarations
bool appendLine(const char* filename, const String& line);
String getIsoTimestamp();
String buildRow(const String& timestamp, float temperature,
                float humidity, float pressure);
void connectToWiFi();
void connectToMqttBroker();
void maintainConnection();
void syncTimeFromWiFi();
void handleNewReading();
bool publishReading(float temperature, float humidity,
                    float pressure, const String& createdAt);
void resendOneBackedUpReading();
void advancePending(unsigned long newOffset, bool isLastLine);
void clearPending();
void savePendingOffset();
unsigned long loadPendingOffset();
bool isOnline();

void setup() {
  Serial.begin(9600);

  unsigned long serialStart = millis();
  while (!Serial && millis() - serialStart < 3000UL) {
    // Wait briefly for Serial Monitor, but don't block forever.
  }

  Serial.println();
  Serial.println(F("Starting Arduino Nano 33 IoT weather station..."));

  // Check Wi-Fi module and firmware.
  Serial.print(F("Wi-Fi firmware: "));
  Serial.println(WiFi.firmwareVersion());

  if (WiFi.firmwareVersion() == "0.0.0") {
    Serial.println(F("WARNING: Wi-Fi firmware version could not be read."));
    Serial.println(F("Check the WiFiNINA firmware using Arduino's firmware updater."));
  }

  // Initialize BME280 sensor.
  if (!bme.begin(0x76) && !bme.begin(0x77)) {
    Serial.println(F("ERROR: BME280 not found. Check I2C wiring and address."));
    while (true) {
      delay(100);
    }
  }
  Serial.println(F("BME280 sensor found."));

  // Initialize SD card.
  if (!SD.begin(SD_CS_PIN)) {
    Serial.println(F("WARNING: SD card initialization failed."));
    Serial.println(F("CSV logging and offline backfill will not work."));
    sdCardReady = false;
  } else {
    sdCardReady = true;
    Serial.println(F("SD card ready."));

    if (!SD.exists(LOG_FILE)) {
      appendLine(LOG_FILE,
        "timestamp_utc,temperature_c,humidity_pct,pressure_hpa,status");
    }

    pendingExists = SD.exists(PENDING_FILE);
    if (pendingExists) {
      pendingOffset = loadPendingOffset();
      Serial.println(F("Unsent readings found in pending.csv. Will send when online."));
    } else {
      SD.remove(OFFSET_FILE); // clear any stale position file
    }
  }

  connectToWiFi();

  if (WiFi.status() == WL_CONNECTED) {
    connectToMqttBroker();
  }

  // Take a reading immediately after setup.
  lastReadingTime = millis() - READING_INTERVAL_MS;
}

void loop() {
  maintainConnection();

  if (mqttClient.connected()) {
    mqttClient.poll();
  }

  unsigned long now = millis();

  if (now - lastReadingTime >= READING_INTERVAL_MS) {
    lastReadingTime = now;
    handleNewReading();
  }

  // Refresh time after reading/publishing because those operations may take
  // time. Never block the main loop while waiting for the publish gap.
  now = millis();
  bool publishGapPassed = !hasPublishedBefore ||
      (now - lastPublishTime >= MIN_PUBLISH_GAP_MS);

  if (isOnline() && pendingExists && publishGapPassed &&
      now - lastBackfillTime >= BACKFILL_INTERVAL_MS) {
    lastBackfillTime = now;
    resendOneBackedUpReading();
  }
}

bool isOnline() {
  return WiFi.status() == WL_CONNECTED && mqttClient.connected();
}

void maintainConnection() {
  // Recover Wi-Fi first. Do not try MQTT until Wi-Fi is back.
  if (WiFi.status() != WL_CONNECTED) {
    if (millis() - lastWifiAttempt >= RECONNECT_RETRY_MS) {
      lastWifiAttempt = millis();
      Serial.println(F("Wi-Fi is disconnected. Restarting Wi-Fi connection..."));

      // Clear stale MQTT/TCP state before reconnecting Wi-Fi.
      mqttClient.stop();
      wifiClient.stop();
      connectToWiFi();
    }
    return;
  }

  // Wi-Fi is connected, so check both the MQTT state and its TCP socket.
  if (!mqttClient.connected() || !wifiClient.connected()) {
    if (millis() - lastMqttAttempt >= RECONNECT_RETRY_MS) {
      lastMqttAttempt = millis();
      Serial.println(F("MQTT is disconnected. Resetting socket and retrying..."));

      mqttClient.stop();
      wifiClient.stop();
      delay(250);

      connectToMqttBroker();
    }
  }
  if (mqttClient.connected()) {
    mqttFailCount = 0;
    }  else if (++mqttFailCount >= 3) {
        Serial.println(F("MQTT keeps failing. Restarting Wi-Fi link..."));
        mqttFailCount = 0;
        mqttClient.stop();
        wifiClient.stop();
        connectToWiFi();
      }
}

void connectToWiFi() {
  Serial.print(F("Connecting to Wi-Fi: "));
  Serial.println(WIFI_SSID);

  WiFi.disconnect();
  delay(250);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  unsigned long startAttempt = millis();
  while (WiFi.status() != WL_CONNECTED &&
         millis() - startAttempt < WIFI_CONNECT_TIMEOUT_MS) {
    Serial.print('.');
    delay(500);
  }
  Serial.println();

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println(F("Wi-Fi connected."));
    Serial.print(F("IP address: "));
    Serial.println(WiFi.localIP());

    mqttClient.stop();
    wifiClient.stop();
    lastWifiAttempt = millis();
    syncTimeFromWiFi();
  } else {
    Serial.print(F("Wi-Fi connection failed. Status code: "));
    Serial.println(WiFi.status());
    Serial.println(F("Will retry automatically."));
  }
}

void connectToMqttBroker() {
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println(F("MQTT connection skipped because Wi-Fi is disconnected."));
    return;
  }

  Serial.print(F("Connecting to MQTT broker "));
  Serial.print(MQTT_BROKER);
  Serial.print(':');
  Serial.println(MQTT_PORT);
  Serial.print(F("Wi-Fi IP: "));
  Serial.println(WiFi.localIP());

  // Start each attempt with a clean MQTT/TCP client state.
  mqttClient.stop();
  wifiClient.stop();
  delay(250);

  mqttClient.setId(MQTT_CLIENT_ID);
  mqttClient.setUsernamePassword(MQTT_USERNAME, MQTT_PASSWORD);
  // ArduinoMqttClient keep-alive is in MILLISECONDS (60 seconds here).
  mqttClient.setKeepAliveInterval(60UL * 1000UL);

  if (mqttClient.connect(MQTT_BROKER, MQTT_PORT)) {
    lastMqttAttempt = millis();
    Serial.println(F("MQTT connected successfully."));
    Serial.print(F("TCP socket connected: "));
    Serial.println(wifiClient.connected() ? F("yes") : F("no"));
  } else {
    Serial.print(F("MQTT connection failed. Error code: "));
    Serial.println(mqttClient.connectError());

    Serial.print(F("WiFi status: "));
    Serial.println(WiFi.status());

    Serial.print(F("IP address: "));
    Serial.println(WiFi.localIP());

    Serial.print(F("WiFi signal strength (dBm): "));
    Serial.println(WiFi.RSSI());

    Serial.println(F("Check Wi-Fi, router access to port 1883, broker details,"));
    Serial.println(F("and the ThingSpeak MQTT device credentials."));
    mqttClient.stop();
    wifiClient.stop();
  }
}

void syncTimeFromWiFi() {
  if (WiFi.status() != WL_CONNECTED) {
    return;
  }

  for (int attempt = 0; attempt < 5; attempt++) {
    unsigned long epoch = WiFi.getTime();

    if (epoch > 1700000000UL) {
      setTime(epoch);
      timeIsSynced = true;
      Serial.print(F("UTC time synchronized: "));
      Serial.println(getIsoTimestamp());
      return;
    }

    delay(1000);
  }

  Serial.println(F("Could not synchronize time yet. Timestamps will be marked UNSYNCED."));
}

String getIsoTimestamp() {
  if (!timeIsSynced) {
    return "UNSYNCED-" + String(millis());
  }

  char buffer[25];
  snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02dZ",
           year(), month(), day(), hour(), minute(), second());
  return String(buffer);
}

void handleNewReading() {
  float temperature = bme.readTemperature();
  float humidity = bme.readHumidity();
  float pressure = bme.readPressure() / 100.0F;

  if (isnan(temperature) || isnan(humidity) || isnan(pressure)) {
    Serial.println(F("Sensor returned an invalid reading. Skipping it."));
    return;
  }

  String timestamp = getIsoTimestamp();

  Serial.print(timestamp);
  Serial.print(F(" | Temp: "));
  Serial.print(temperature, 2);
  Serial.print(F(" C | Humidity: "));
  Serial.print(humidity, 2);
  Serial.print(F(" % | Pressure: "));
  Serial.print(pressure, 2);
  Serial.println(F(" hPa"));

  String row = buildRow(timestamp, temperature, humidity, pressure);

  // Only publish live when nothing older is waiting, so the chart stays in
  // order. If a queue exists, this reading joins the back of it.
  bool sent = false;
  if (!pendingExists && isOnline()) {
    sent = publishReading(temperature, humidity, pressure, "");
  }

  bool queued = false;
  if (!sent) {
    queued = appendLine(PENDING_FILE, row);
    if (queued) {
      pendingExists = true;
    }
  }

  // The permanent log always gets the reading.
  bool saved = appendLine(LOG_FILE, row + (sent ? ",LIVE" : ",QUEUED"));

  if (sent) {
    Serial.print(F("Published to ThingSpeak"));
  } else if (queued) {
    Serial.print(F("Queued for sending with its original timestamp"));
  } else {
    Serial.print(F("Not published and could not be queued"));
  }
  Serial.println(saved ? F(" and saved to SD log.") : F(" but NOT saved to SD log."));
}

String buildRow(const String& timestamp, float temperature,
                float humidity, float pressure) {
  return timestamp + "," + String(temperature, 2) + "," +
         String(humidity, 2) + "," + String(pressure, 2);
}

bool publishReading(float temperature, float humidity,
                    float pressure, const String& createdAt) {
  if (!isOnline()) {
    Serial.println(F("Publish skipped because Wi-Fi or MQTT is disconnected."));
    return false;
  }

  // Do not block the loop waiting for ThingSpeak's rate limit.
  if (hasPublishedBefore &&
      millis() - lastPublishTime < MIN_PUBLISH_GAP_MS) {
    Serial.println(F("Publish deferred to respect ThingSpeak rate limit."));
    return false;
  }

  if (!wifiClient.connected()) {
    Serial.println(F("Connection dropped before publishing."));
    return false;
  }

  String topic = "channels/" + String(CHANNEL_ID) + "/publish";
  String payload = "field1=" + String(temperature, 2) +
                   "&field2=" + String(humidity, 2) +
                   "&field3=" + String(pressure, 2);

  if (createdAt.length() > 0) {
    payload += "&created_at=" + createdAt;
  }

  if (!mqttClient.beginMessage(topic)) {
    Serial.println(F("Could not begin MQTT message."));
    return false;
  }

  mqttClient.print(payload);
  int result = mqttClient.endMessage();

  if (result != 1) {
    Serial.print(F("MQTT publish failed. Result: "));
    Serial.println(result);
    Serial.print(F("MQTT connected after publish: "));
    Serial.println(mqttClient.connected() ? F("yes") : F("no"));
    Serial.print(F("TCP socket connected after publish: "));
    Serial.println(wifiClient.connected() ? F("yes") : F("no"));
    return false;
  }

  lastPublishTime = millis();
  hasPublishedBefore = true;
  return true;
}

// ----------------------- Backfill queue ------------------------

void savePendingOffset() {
  SD.remove(OFFSET_FILE);
  File f = SD.open(OFFSET_FILE, FILE_WRITE);
  if (f) {
    f.println(pendingOffset);
    f.close();
  }
}

unsigned long loadPendingOffset() {
  if (!SD.exists(OFFSET_FILE)) {
    return 0;
  }
  File f = SD.open(OFFSET_FILE, FILE_READ);
  if (!f) {
    return 0;
  }
  String s = f.readStringUntil('\n');
  f.close();
  s.trim();
  return (unsigned long)s.toInt();
}

// Deletes only the queue. weather.csv is never touched.
void clearPending() {
  SD.remove(PENDING_FILE);
  SD.remove(OFFSET_FILE);
  pendingExists = false;
  pendingOffset = 0;
}

void advancePending(unsigned long newOffset, bool isLastLine) {
  if (isLastLine) {
    clearPending();
    Serial.println(F("All queued readings handled. pending.csv cleared."));
  } else {
    pendingOffset = newOffset;
    savePendingOffset();
  }
}

void resendOneBackedUpReading() {
  if (!sdCardReady || !pendingExists) {
    return;
  }

  File pendingFile = SD.open(PENDING_FILE, FILE_READ);
  if (!pendingFile) {
    Serial.println(F("Could not open pending.csv."));
    return;
  }

  if (pendingOffset > pendingFile.size()) {
    pendingOffset = 0;
  }
  pendingFile.seek(pendingOffset);

  String line;
  while (pendingFile.available() && line.length() == 0) {
    line = pendingFile.readStringUntil('\n');
    line.trim();
  }

  unsigned long newOffset = pendingFile.position();
  bool isLastLine = newOffset >= pendingFile.size();
  pendingFile.close();

  if (line.length() == 0) {
    clearPending();
    Serial.println(F("No queued readings remain. pending.csv cleared."));
    return;
  }

  int c1 = line.indexOf(',');
  int c2 = line.indexOf(',', c1 + 1);
  int c3 = line.indexOf(',', c2 + 1);

  if (c1 < 0 || c2 < 0 || c3 < 0) {
    Serial.println(F("Malformed row in pending.csv. Skipping it."));
    advancePending(newOffset, isLastLine);
    return;
  }

  String timestamp = line.substring(0, c1);
  float temperature = line.substring(c1 + 1, c2).toFloat();
  float humidity = line.substring(c2 + 1, c3).toFloat();
  float pressure = line.substring(c3 + 1).toFloat();

  // Without a real time we can't place the point correctly on the chart.
  // It stays in weather.csv, but isn't sent.
  if (timestamp.startsWith("UNSYNCED")) {
    Serial.print(F("Skipping reading with no valid time: "));
    Serial.println(timestamp);
    advancePending(newOffset, isLastLine);
    return;
  }

  if (publishReading(temperature, humidity, pressure, timestamp)) {
    Serial.print(F("Backfilled reading from: "));
    Serial.println(timestamp);
    advancePending(newOffset, isLastLine);
  } else {
    Serial.println(F("Backfill failed. This row will be retried."));
  }
}

bool appendLine(const char* filename, const String& line) {
  if (!sdCardReady) {
    Serial.println(F("SD card unavailable; could not write file."));
    return false;
  }

  File file = SD.open(filename, FILE_WRITE);
  if (!file) {
    Serial.print(F("Could not open file: "));
    Serial.println(filename);
    return false;
  }

  file.println(line);
  file.close();
  return true;
}
