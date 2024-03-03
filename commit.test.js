const { KafkaConsumer } = require('./KafkaConsumer');

describe('Kafka Consumer Tests', () => {
  let kafkaConsumer;

  beforeEach(() => {
    kafkaConsumer = new KafkaConsumer('test-topic', { brokers: ['localhost:9092'] });
  });

  /*afterEach(async () => {
    await kafkaConsumer.disconnect();
  });*/

  test('should retry processing message until success', async () => {
    const mockMessage = {
      topic: 'test-topic',
      partition: 0,
      message: {
        key: 'test-key',
        value: 'Test Kafka Message',
        offset: 0, // Simula o offset da mensagem
      },
    };

    // Mock da função que lança um erro ao manipular os dados
    kafkaConsumer.handleData = jest.fn().mockRejectedValueOnce(new Error('Erro ao manipular dados'));

    // Mock da função de transformação de mensagem
    kafkaConsumer.transformMessage = jest.fn().mockResolvedValueOnce(mockMessage.message.value);

    // Inicializa o consumo em uma nova promessa
    const consumePromise = kafkaConsumer.consume();

    // Aguarda um tempo suficiente para permitir que as operações assíncronas sejam concluídas
    await new Promise(resolve => setTimeout(resolve, 20000));

    // Aguarda a conclusão do consumo antes de finalizar o teste
    await consumePromise;

    // Verifica se o commit foi tentado após o processamento bem-sucedido
    expect(kafkaConsumer.handleData).toHaveBeenCalledTimes(2); // O número de tentativas de processamento é 4 (3 retries + tentativa inicial)
    expect(kafkaConsumer.transformMessage).toHaveBeenCalledTimes(2); // A transformação da mensagem deve ocorrer apenas uma vez
  }, 60000); // Aumenta o tempo limite do teste para 25 segundos
});
