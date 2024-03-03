const { Kafka, logLevel } = require('kafkajs');

class KafkaProducer {
  constructor(kafkaConfig) {
    this.kafkaConfig = kafkaConfig;
    this.producer = null;
  }

  async connect() {
    const kafka = new Kafka(this.kafkaConfig);
    this.producer = kafka.producer({
      retry: {
        retries: Infinity,
        maxRetryTime: 30000,
      },
    });
    await this.producer.connect();
    console.log('Conectado com sucesso ao Kafka (produtor)!');
  }

  async sendMessage(topic, messageValue) {
    try {
      console.log(`Enviando mensagem para o tópico ${topic}: ${messageValue}`);
      const result = await this.producer.send({
        topic: topic,
        messages: [{ value: messageValue }],
      });
      console.log('Mensagem enviada com sucesso:', result);
    } catch (error) {
      console.error('Erro ao enviar mensagem:', error);
    }
  }

  async disconnect() {
    if (this.producer) {
      await this.producer.disconnect();
      console.log('Desconectado do Kafka (produtor)');
    }
  }
}

module.exports = KafkaProducer;
