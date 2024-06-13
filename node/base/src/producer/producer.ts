import { Kafka } from 'kafkajs';

const brokers = [`localhost:9092`]

const kafka = new Kafka({
  clientId: 'kafkajs-producer',
  brokers: brokers,
  ssl: false,
  // sasl: false,
  // brokers: string[] | BrokersFunction
  // ssl?: tls.ConnectionOptions | boolean
  // sasl?: SASLOptions | Mechanism
  // clientId?: string
  // connectionTimeout?: number
  // authenticationTimeout?: number
  // reauthenticationThreshold?: number
  // requestTimeout?: number
  // enforceRequestTimeout?: boolean
  // retry?: RetryOptions
  // socketFactory?: ISocketFactory
  // logLevel?: logLevel
  // logCreator?: logCreator
})

const producer = kafka.producer(
  // createPartitioner: (config: {
  //   topic: string
  //   partitionMetadata: PartitionMetadata[]
  //   message: Message
  // }) +> void,
  // retry?: RetryOptions
  // metadataMaxAge?: number
  // allowAutoTopicCreation?: boolean
  // idempotent?: boolean
  // transactionalId?: string
  // transactionTimeout?: number
  // maxInFlightRequests?: number
)

const run = async () => {
  // Producing
  await producer.connect()
  await producer.send({
    // topic: string
    // messages: Message[]
    // acks?: number
    // timeout?: number
    // compression?: CompressionTypes
    // test_unsubscribing_topic
    // test-unsubscribing-topic-group2
    // topic: 'test_unsubscribing_topic',
    topic: 'test-topic',
    /**
     * 0=서버 응답 기다리지 않음, 전송 보장 없음, 처리량 높아지겠지만 메시지 유실 괜찮은 경우만 사용
     * 1=파티션 리더에 저장되면 응답 받음. 리더 장애시 메시지 유실 가능(팔로워에 복제되지 않은 상태에서 에러날 경우)
     * all or -1= min.insync.replicas 리플리카에 저장되면 응답 받음, 
     */
    // acks: []; 
    messages: [
      { 
        value: 'Hello KafkaJS!', 
        // key: undefined, 
        // headers: {}, 
        // partition: 0, 
        // timestamp: 0,
    }],
    // compression: CompressionTypes.ZSTD,
  })
}

run().catch(console.error)