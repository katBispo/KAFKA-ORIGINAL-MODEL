const { Kafka, Partitioners } = require('kafkajs');
const createKafkaProducer = async () => {
  const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092'],
  });
  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  });
  await producer.connect();
  return producer;
};
module.exports = { createKafkaProducer };