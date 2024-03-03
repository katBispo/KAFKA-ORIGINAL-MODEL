//CONSUMER-TEST.JS
const { Kafka } = require('kafkajs');



async function createKafkaConsumer() {
  const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092'],
  });
  const consumer = kafka.consumer({ groupId: 'test-group' });
  try {
    await consumer.connect();
    console.log('Kafka consumer connected successfully');
  } catch (error) {
    console.error('Error connecting to Kafka:', error);
  }
  return consumer;
}

module.exports = { createKafkaConsumer };