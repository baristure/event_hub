/**
 * Azure Event Hub Protobuf Publisher
 * Topic: azure/device_history/device_telemetry/reported
 *
 * Publishes 300 protobuf-encoded DeviceTelemetry messages per minute
 * to an Azure Event Hub.
 *
 * Install dependencies:
 *   npm install @azure/event-hubs protobufjs uuid dotenv
 *
 * Environment variables (set in .env):
 *   EVENTHUB_CONNECTION_STRING  - e.g. Endpoint=sb://xxx.servicebus.windows.net/;SharedAccessKeyName=xxx;SharedAccessKey=xxx
 *   EVENTHUB_NAME               - e.g. rcs-eventhub-hub01
 */

"use strict";

require("dotenv").config();

const { EventHubProducerClient } = require("@azure/event-hubs");
const protobuf = require("protobufjs");
const { v4: uuidv4 } = require("uuid");
const path = require("path");

// ─── Config ───────────────────────────────────────────────────────────────────

const CONNECTION_STRING = process.env.EVENTHUB_CONNECTION_STRING;
const EVENTHUB_NAME = process.env.EVENTHUB_NAME;
const TOPIC = process.env.EVENTHUB_TOPIC;

const MESSAGES_PER_MINUTE = 300;
const INTERVAL_MS = Math.floor(60_000 / MESSAGES_PER_MINUTE); // ~200ms

if (!CONNECTION_STRING || !EVENTHUB_NAME || !TOPIC) {
  console.error(
    "❌ EVENTHUB_CONNECTION_STRING, EVENTHUB_NAME ve EVENTHUB_TOPIC .env missing",
  );
  process.exit(1);
}

// ─── Azure Event Hub Client ───────────────────────────────────────────────────

const producer = new EventHubProducerClient(CONNECTION_STRING, EVENTHUB_NAME);

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Simulates a real HVAC device payload (replace with real data source). */
function buildSamplePayload() {
  const modes = ["COOL", "HEAT", "FAN_ONLY", "AUTO", "OFF"];
  const fanSpeeds = ["LOW", "MEDIUM", "HIGH", "AUTO"];

  return {
    device_name: `HVAC-Unit-${Math.floor(Math.random() * 100)
      .toString()
      .padStart(3, "0")}`,
    mac_address: "AA:BB:CC:DD:EE:FF",
    transaction_id: uuidv4(),
    serial_number: `SN-${Date.now()}`,
    temperature: parseFloat((20 + Math.random() * 10).toFixed(2)),
    set_point: parseFloat((22 + Math.random() * 4).toFixed(2)),
    hvac_mode: modes[Math.floor(Math.random() * modes.length)],
    fan_speed: fanSpeeds[Math.floor(Math.random() * fanSpeeds.length)],
    compressor_active: Math.random() > 0.5,
    filter_life_pct: Math.floor(Math.random() * 100),
  };
}

/** Rate-limit counters for basic observability. */
let sentCount = 0;
let errorCount = 0;
let periodStart = Date.now();

function logStats() {
  const elapsed = ((Date.now() - periodStart) / 1_000).toFixed(1);
  console.log(
    `[${new Date().toISOString()}] Stats — sent: ${sentCount} | errors: ${errorCount} | elapsed: ${elapsed}s`,
  );
  sentCount = 0;
  errorCount = 0;
  periodStart = Date.now();
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`Loading protobuf schema…`);

  const root = await protobuf.load(
    path.join(__dirname, "device_telemetry.proto"),
  );
  const DeviceTelemetry = root.lookupType("device_telemetry.DeviceTelemetry");

  console.log(
    `Publishing to Event Hub "${EVENTHUB_NAME}" at ${MESSAGES_PER_MINUTE} msg/min`,
  );
  console.log(`Topic: ${TOPIC}`);
  console.log(`Interval: ${INTERVAL_MS}ms per message\n`);

  // Print stats every 60 seconds
  setInterval(logStats, 60_000);

  // Publish loop
  const publish = async () => {
    const payload = buildSamplePayload();

    // Validate against proto schema
    const err = DeviceTelemetry.verify(payload);
    if (err) {
      console.error("Proto validation error:", err);
      errorCount++;
      return;
    }

    // Encode to binary protobuf
    const message = DeviceTelemetry.create(payload);
    const encoded = DeviceTelemetry.encode(message).finish(); // Uint8Array → Buffer

    try {
      const batch = await producer.createBatch();
      batch.tryAdd({
        body: Buffer.from(encoded),
        properties: {
          topic: TOPIC,
          device_name: payload.device_name,
          transaction_id: payload.transaction_id,
          content_type: "application/protobuf",
        },
      });

      await producer.sendBatch(batch);
      sentCount++;

      if (process.env.DEBUG === "true") {
        console.log(
          `✓ Sent | device: ${payload.device_name} | txn: ${payload.transaction_id}`,
        );
        console.log(`  Payload: ${JSON.stringify(payload)}`);
      }
    } catch (e) {
      errorCount++;
      console.error(`✗ Event Hub error: ${e.message}`);
    }
  };

  // Schedule at fixed interval
  setInterval(publish, INTERVAL_MS);

  // Fire first one immediately
  await publish();
}

// Graceful shutdown
process.on("SIGINT", async () => {
  await producer.close();
  process.exit(0);
});
process.on("SIGTERM", async () => {
  await producer.close();
  process.exit(0);
});

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
