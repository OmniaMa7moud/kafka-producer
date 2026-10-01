require("dotenv").config();

const { Kafka } = require("kafkajs");

const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry");

const kafka = new Kafka({
  clientId: "orders-producer",
  brokers: process.env.KAFKA_BROKERS.split(","),

  ssl: true,

  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

const producer = kafka.producer();

const topic = process.env.KAFKA_TOPIC;

const registry = new SchemaRegistry({
  host: process.env.SCHEMA_REGISTRY_URL,
  auth: {
    username: process.env.SCHEMA_REGISTRY_API_KEY,
    password: process.env.SCHEMA_REGISTRY_API_SECRET,
  },
});

function generateOrder() {
  return {
    orderId: `ORD-${Date.now()}`,
    customerId: `CUST-${Math.floor(Math.random() * 1000)}`,
    product: ["Laptop", "Phone", "Keyboard", "Mouse"][
      Math.floor(Math.random() * 4)
    ],
    quantity: Math.floor(Math.random() * 5) + 1,
    amount: Number((Math.random() * 1000 + 50).toFixed(2)),
    timestamp: new Date().toISOString(),
  };
}

async function produceMessage() {
  const order = generateOrder();

  // Get the latest schema ID registered under orders-value
  const schemaId = await registry.getLatestSchemaId("orders-value");

  // Serialize the order using the JSON Schema
  const encodedValue = await registry.encode(schemaId, order);

  await producer.send({
    topic,
    messages: [
      {
        key: order.orderId,
        value: encodedValue,
      },
    ],
  });

  console.log("Produced:", order);
}

async function start() {
  await producer.connect();

  console.log(`Producer connected.`);
  console.log(`Producing messages to topic: ${topic}`);

  // Produce one message immediately
  await produceMessage();

  // Then produce every 5 seconds
  setInterval(async () => {
    try {
      await produceMessage();
    } catch (error) {
      console.error("Failed to produce message:", error);
    }
  }, 2000);
}

start().catch(async (error) => {
  console.error("Producer error:", error);
  await producer.disconnect();
  process.exit(1);
});
