require("dotenv").config();

const { Kafka } = require("kafkajs");

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

  await producer.send({
    topic,
    messages: [
      {
        key: order.orderId,
        value: JSON.stringify(order),
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
