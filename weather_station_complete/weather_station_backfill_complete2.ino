/*
  IoT Weather Station - Offline Storage + Timestamped Backfill
  Board: Arduino Nano 33 IoT
  Sensor: BME280 (I2C)
  Storage: microSD card module (SPI)
  Dashboard: ThingSpeak over MQTT

  How it works:
  - Reads temperature, humidity, and pressure once a minute.
  - If the reading is sent to ThingSpeak right away, nothing is stored
    on the SD card.
  - If it can't be sent (Wi-Fi/MQTT down), it is saved to the SD card:
      pending.csv  queue of readings waiting to be sent (deleted when empty)
      weather.csv  permanent record of the readings taken during outages
  - When MQTT reconnects, the queue is sent oldest-first, one row every
    16 seconds, with each reading's ORIGINAL timestamp (created_at).
  - The queue position is saved (pendoff.txt), so a reboot in the middle of
    a backfill won't resend rows.
  - Clock: set from WiFi.getTime(), then NTP. Times earlier than the day the
    sketch was compiled are rejected. Readings taken before the clock is set
    are marked UNSYNCED; they stay in weather.csv only.
  - DNS: answers that point to a private address (such as your own router)
    are rejected and public DNS is used instead.
  - Watchdog: the board restarts itself after 2 minutes with Wi-Fi up but
    MQTT down, or after 10 minutes with Wi-Fi down.

  IMPORTANT:
  1. Enter your Wi-Fi and ThingSpeak credentials below.
  2. Never upload your real credentials to a public repo.
  3. ThingSpeak free channels allow one update per 15 seconds; this sketch
     keeps a 16-second gap.
*/

#include <WiFiNINA.h>
#include <WiFiUdp.h>
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
const unsigned long READING_INTERVAL_MS  = 60000UL; // 60 seconds (use 20000UL for 20 s)
const unsigned long BACKFILL_INTERVAL_MS = 16000UL; // one backfill row per 16 s
const unsigned long MIN_PUBLISH_GAP_MS   = 16000UL; // ThingSpeak limit is 15 s
const unsigned long RECONNECT_RETRY_MS   = 15000UL; // retry every 15 seconds
const unsigned long WIFI_CONNECT_TIMEOUT_MS = 20000UL;
const unsigned long WATCHDOG_MS = 120000UL;           // Wi-Fi up, no working link
const unsigned long WATCHDOG_WIFI_DOWN_MS = 600000UL; // Wi-Fi down (restart loses the clock)

const int SD_CS_PIN = 10;
const char* LOG_FILE     = "weather.csv";  // readings taken during outages
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
unsigned long lastSyncAttempt = 0;
int wifiFailCount = 0;
unsigned long lastLinkGoodTime = 0;   // last time MQTT was up or an upload worked
bool hasPublishedBefore = false;
int mqttFailCount = 0;

WiFiClient wifiClient;
WiFiUDP ntpUdp;
MqttClient mqttClient(wifiClient);
Adafruit_BME280 bme;

// Forward declarations
bool appendLine(const char* filename, const String& line);
String getIsoTimestamp();
String buildRow(const String& timestamp, float temperature,
                float humidity, float pressure);
void connectToWiFi(bool fullReset = false);
void connectToMqttBroker();
void maintainConnection();
void syncTimeFromWiFi();
void trySyncTimeOnce();
unsigned long getNtpTime(const char* host);
bool resolveHost(const char* host, IPAddress& out);
bool isPrivateIp(const IPAddress& ip);
unsigned long compileEpoch();
bool timeLooksValid(unsigned long epoch);
unsigned long parseIsoToEpoch(const String& iso);
void handleNewReading();
bool publishReading(float temperature, float humidity,
                    float pressure, const String& createdAt);
void resendOneBackedUpReading();
void advancePending(unsigned long newOffset, bool isLastLine);
void clearPending();
void savePendingOffset();
unsigned long loadPendingOffset();
bool isOnline();
void printLogSize();

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

    // Self-test: prove the card can be written to.
    File t = SD.open("sdtest.txt", FILE_WRITE);
    if (t) {
      t.println("ok");
      t.close();
      SD.remove("sdtest.txt");
      Serial.println(F("SD write test: OK"));
    } else {
      Serial.println(F("SD write test: FAILED. Check the card is FAT32 and the wiring."));
    }

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
  lastLinkGoodTime = millis();   // start the watchdog clock
}

void loop() {
  maintainConnection();

  // Watchdog: if nothing has uploaded and MQTT is down for too long,
  // restart the whole board. Readings are on the SD card and the queue
  // position is saved, so nothing is lost by rebooting.
  if (mqttClient.connected() && wifiClient.connected()) {
    lastLinkGoodTime = millis();
  }
  // A restart loses the clock, so be patient when Wi-Fi itself is down
  // (a real outage) and quicker when Wi-Fi is up but nothing gets through.
  unsigned long watchdogLimit =
      (WiFi.status() == WL_CONNECTED) ? WATCHDOG_MS : WATCHDOG_WIFI_DOWN_MS;
  if (millis() - lastLinkGoodTime >= watchdogLimit) {
    Serial.println(F("Watchdog: no working connection. Restarting board..."));
    Serial.flush();
    delay(200);
    NVIC_SystemReset();
  }

  if (mqttClient.connected()) {
    mqttClient.poll();
  }

  // Keep retrying the clock every 10 s until it syncs.
  if (!timeIsSynced && WiFi.status() == WL_CONNECTED &&
      millis() - lastSyncAttempt >= 10000UL) {
    lastSyncAttempt = millis();
    trySyncTimeOnce();
  }

  unsigned long now = millis();

  // If Wi-Fi is up but the clock isn't synced yet, wait (up to 30 s after
  // boot) so readings don't get an UNSYNCED timestamp.
  bool clockOk = timeIsSynced || WiFi.status() != WL_CONNECTED || millis() > 30000UL;

  if (clockOk && now - lastReadingTime >= READING_INTERVAL_MS) {
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

      // Normal retries first; after 3 failures in a row, reset the chip.
      bool doFullReset = (wifiFailCount >= 3);
      connectToWiFi(doFullReset);
      if (WiFi.status() == WL_CONNECTED || doFullReset) {
        wifiFailCount = 0;
      } else {
        wifiFailCount++;
      }
    }
    return;
  }

  // Wi-Fi is connected, so check both the MQTT state and its TCP socket.
  if (mqttClient.connected() && wifiClient.connected()) {
    mqttFailCount = 0;
    return;
  }

  // Only try again every RECONNECT_RETRY_MS (never on every pass of loop()).
  if (millis() - lastMqttAttempt < RECONNECT_RETRY_MS) {
    return;
  }
  lastMqttAttempt = millis();
  Serial.println(F("MQTT is disconnected. Resetting socket and retrying..."));

  mqttClient.stop();
  wifiClient.stop();
  delay(250);

  connectToMqttBroker();

  if (mqttClient.connected()) {
    mqttFailCount = 0;
  } else if (++mqttFailCount >= 3) {
    // Three real failed attempts in a row: restart the Wi-Fi link.
    Serial.println(F("MQTT keeps failing. Restarting Wi-Fi link..."));
    mqttFailCount = 0;
    mqttClient.stop();
    wifiClient.stop();
    connectToWiFi(true);   // MQTT stuck while Wi-Fi is "up": reset the chip
  }
}

void connectToWiFi(bool fullReset) {
  lastWifiAttempt = millis();   // next retry is 15 s from now, not instant
  Serial.print(F("Connecting to Wi-Fi: "));
  Serial.println(WIFI_SSID);

  if (fullReset) {
    // WiFi.end() powers the Wi-Fi chip fully off and on (clears stuck state).
    Serial.println(F("Full Wi-Fi chip reset..."));
    WiFi.end();
    delay(1000);
  } else {
    WiFi.disconnect();
    delay(250);
  }
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
    lastMqttAttempt = millis();   // wait 15 s before the first MQTT attempt
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

  lastMqttAttempt = millis();   // counts failed attempts too, so retries are 15 s apart
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

  // Look up the broker ourselves and refuse bad (router) answers.
  IPAddress brokerIp;
  if (!resolveHost(MQTT_BROKER, brokerIp)) {
    Serial.println(F("DNS lookup for broker FAILED (router DNS is giving bad answers)."));
    return;
  }
  Serial.print(F("DNS lookup for broker: "));
  Serial.println(brokerIp);

  if (mqttClient.connect(brokerIp, MQTT_PORT)) {
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
  trySyncTimeOnce();   // loop() keeps retrying every 10 s if this fails
}

// True for addresses that can never be a real internet server
// (your own router, 192.168.x.x, 10.x.x.x, etc.).
bool isPrivateIp(const IPAddress& ip) {
  if (ip[0] == 0 || ip[0] == 127 || ip[0] == 10) return true;
  if (ip[0] == 192 && ip[1] == 168) return true;
  if (ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31) return true;
  if (ip[0] == 169 && ip[1] == 254) return true;
  return false;
}

// Look up a hostname. Some home gateways answer DNS with their OWN address,
// so a private answer is rejected and public DNS is used instead.
bool resolveHost(const char* host, IPAddress& out) {
  if (WiFi.hostByName(host, out) == 1 && !isPrivateIp(out)) {
    return true;
  }
  Serial.print(F("Router DNS gave a bad answer for "));
  Serial.print(host);
  Serial.println(F(". Using public DNS..."));
  WiFi.setDNS(IPAddress(8, 8, 8, 8), IPAddress(1, 1, 1, 1));
  if (WiFi.hostByName(host, out) == 1 && !isPrivateIp(out)) {
    return true;
  }
  return false;
}

// The day this sketch was compiled, as Unix time. Any "synced" time earlier
// than this is wrong (for example, a stale clock answer from a router).
unsigned long compileEpoch() {
  const char* months = "JanFebMarAprMayJunJulAugSepOctNovDec";
  char mon[4] = {0};
  int day = 1, yr = 2026;
  sscanf(__DATE__, "%3s %d %d", mon, &day, &yr);
  const char* m = strstr(months, mon);
  if (!m) return 0;
  tmElements_t tm;
  tm.Day = day;
  tm.Month = ((m - months) / 3) + 1;
  tm.Year = yr - 1970;
  tm.Hour = 0;
  tm.Minute = 0;
  tm.Second = 0;
  return (unsigned long)makeTime(tm);
}

bool timeLooksValid(unsigned long epoch) {
  return epoch >= compileEpoch() - 86400UL && epoch < 4000000000UL;
}

// "2026-10-23T00:08:38Z" -> Unix time (0 if it can't be read).
unsigned long parseIsoToEpoch(const String& iso) {
  if (iso.length() < 20) return 0;
  tmElements_t tm;
  tm.Year   = iso.substring(0, 4).toInt() - 1970;
  tm.Month  = iso.substring(5, 7).toInt();
  tm.Day    = iso.substring(8, 10).toInt();
  tm.Hour   = iso.substring(11, 13).toInt();
  tm.Minute = iso.substring(14, 16).toInt();
  tm.Second = iso.substring(17, 19).toInt();
  return (unsigned long)makeTime(tm);
}

// Ask an NTP server directly over UDP. Returns Unix time, or 0 on failure.
unsigned long getNtpTime(const char* host) {
  IPAddress ip;
  if (!resolveHost(host, ip)) {
    return 0;
  }
  byte pkt[48];
  memset(pkt, 0, sizeof(pkt));
  pkt[0] = 0xE3;  // LI = 3, version 4, mode 3 (client)

  ntpUdp.begin(2390);
  ntpUdp.beginPacket(ip, 123);
  ntpUdp.write(pkt, sizeof(pkt));
  ntpUdp.endPacket();

  unsigned long start = millis();
  while (millis() - start < 2000UL) {
    if (ntpUdp.parsePacket() >= 48) {
      ntpUdp.read(pkt, sizeof(pkt));
      ntpUdp.stop();
      unsigned long secs = ((unsigned long)pkt[40] << 24) |
                           ((unsigned long)pkt[41] << 16) |
                           ((unsigned long)pkt[42] << 8) |
                           (unsigned long)pkt[43];
      return secs - 2208988800UL;  // NTP epoch (1900) -> Unix epoch (1970)
    }
    delay(20);
  }
  ntpUdp.stop();
  return 0;
}

void trySyncTimeOnce() {
  unsigned long epoch = WiFi.getTime();
  const char* method = "WiFi.getTime";

  if (!timeLooksValid(epoch)) {
    epoch = getNtpTime("pool.ntp.org");
    method = "NTP pool.ntp.org";
  }
  if (!timeLooksValid(epoch)) {
    epoch = getNtpTime("time.google.com");
    method = "NTP time.google.com";
  }

  if (timeLooksValid(epoch)) {
    setTime(epoch);
    timeIsSynced = true;
    Serial.print(F("UTC time synchronized ("));
    Serial.print(method);
    Serial.print(F("): "));
    Serial.println(getIsoTimestamp());
  } else {
    Serial.println(F("Clock sync failed (all methods). Will try again in 10 s."));
  }
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

  if (sent) {
    // Delivered live: nothing needs to be stored.
    Serial.println(F("Published to ThingSpeak."));
    return;
  }

  // Not delivered: keep a permanent record too (queue is deleted once sent).
  bool saved = appendLine(LOG_FILE, row + (isOnline() ? ",QUEUED" : ",OFFLINE"));

  if (queued) {
    Serial.print(F("Not sent. Queued with its original timestamp"));
  } else {
    Serial.print(F("Not sent and could not be queued"));
  }
  Serial.println(saved ? F(" and saved to SD log.") : F(" but NOT saved to SD log."));
  printLogSize();
}

void printLogSize() {
  File f = SD.open(LOG_FILE, FILE_READ);
  if (f) {
    Serial.print(F("weather.csv size: "));
    Serial.print(f.size());
    Serial.println(F(" bytes"));
    f.close();
  }
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

  if (!mqttClient.connected() || !wifiClient.connected()) {
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
  if (timestamp.startsWith("UNSYNCED") || !timeLooksValid(parseIsoToEpoch(timestamp))) {
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
