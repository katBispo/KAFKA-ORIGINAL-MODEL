const { Kafka } = require('kafkajs');

const MAX_RETRIES = 3;

class KafkaConsumer {
  constructor(topic, consumerConfig) {
    this.topic = topic;
    this.consumer = new Kafka(consumerConfig).consumer({ groupId: 'test-group' });
    this.errorOccurred = false; // Variável de controle para rastrear se o erro já ocorreu
  }

  async consume() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic });
    console.log(`${this.topic} Started to consume messages`);

    return await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
        console.log(`- ${prefix} ${message.key}`);

        let retryCount = 0;
        const maxRetries = 3; // Número máximo de tentativas de processamento

        let messageProcessed = false;

        while (!messageProcessed && retryCount < maxRetries) {
          try {
            const eventData = await this.transformMessage(message.value);
            await this.handleData(eventData);
            await this.consumer.commitOffsets([{ topic, partition, offset: message.offset }]);
            messageProcessed = true; // Marca que o processamento foi bem-sucedido apenas após de commitar o offset
            console.log(`${this.constructor.name} Mensagem Consumida`);
            break;
            console.log("nao saiu do laço");
          } catch (error) {
            console.error(`Erro ao processar mensagem: ${error.message}`);
            retryCount++;
            console.log(`Tentando processar a mensagem novamente em 5 segundos...`);
            await new Promise(resolve => setTimeout(resolve, 5000)); // Espera 5 segundos antes de tentar novamente
          }
        }

        if (!messageProcessed) {
          console.error(`Falha ao processar a mensagem após ${maxRetries} tentativas.`);
          // Aqui você pode adicionar lógica adicional, como enviar a mensagem para uma fila de mensagens mortas (DLQ).
        }
      },
    });
  }




  async connect() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic });
    console.log(`${this.topic} Started to consume messages`);
    this.connected = true; // Define o estado de conexão como verdadeiro após a conexão bem-sucedida
  }

  async disconnect() {
    if (this.connected) {
      await this.consumer.disconnect(); // Desconecta o consumidor se estiver conectado
      console.log(`${this.topic} Disconnected`);
      this.connected = false; // Define o estado de conexão como falso após a desconexão
    } else {
      console.warn('O consumidor já está desconectado.');
    }
  }

  async retryIfThrowsException(cb) {
    const SLEEP_DURATION = 6000;
    let tries = 0;
    let lastError = null;

    while (tries < MAX_RETRIES) {
      try {
        await new Promise(resolve => setTimeout(resolve, SLEEP_DURATION));
        await cb();
        break;
      } catch (e) {
        console.error(`retryIfThrowsException(): tried ${tries + 1} times. Error: ${e.message}`);
        lastError = e;
        tries++;
      }
    }

    if (tries >= MAX_RETRIES) {
      throw lastError;
    } else {
      return true;
    }
  }

  async transformMessage(value) {
    console.log(`Transformando mensagem: ${value}`);
    return value;
  }

  async handleData(eventData) {
    console.log(`Manipulando dados: ${eventData}`);
    // Simulando um erro no processamento da mensagem
    /// throw new Error('Erro ao manipular dados');
  }

  persistError(error, options) {
    console.error('Erro persistente:', error, options);
  }
}

module.exports = {
  KafkaConsumer,
};
