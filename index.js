const { KafkaConsumer } = require('./KafkaConsumer');
const KafkaProducer = require('./KafkaProducer');

const kafkaConfig = {
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  //logLevel: logLevel.ERROR,
};

const topicConfig = {
  topic: 'test-topic',
  configEntries: [
    {
      name: 'retention.ms',
      value: '10000',
    },
  ]
};

async function run() {
  const producer = new KafkaProducer(kafkaConfig);
  const consumer = new KafkaConsumer(topicConfig.topic, kafkaConfig);

  try {
    await producer.connect();
    await producer.sendMessage(topicConfig.topic, 'Hello KafkaJS user!');
    await consumer.consume();
  } catch (error) {
    console.error('Erro na execução:', error);
  } finally {
    await producer.disconnect();
  }
}

run().catch(error => {
  console.error('Erro:', error);
});
