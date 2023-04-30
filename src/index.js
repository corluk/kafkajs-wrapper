"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaConsumerAuto = exports.KafkaProduceAuto = exports.Produce = exports.Consume = exports.createTopic = void 0;
const kafkajs_1 = require("kafkajs");
const createKafka = () => {
    const brokers = process.env.KAFKA_BROKERS.split(",");
    const kafka = new kafkajs_1.Kafka({
        clientId: process.env.KAFKA_CLIENTID,
        brokers: brokers,
        sasl: {
            mechanism: 'plain',
            username: process.env.KAFKA_USER,
            password: process.env.KAFKA_PASS
        }
    });
    return kafka;
};
const createTopic = (kafka, topic, numPartitions) => __awaiter(void 0, void 0, void 0, function* () {
    const admin = kafka.admin();
    yield admin.connect();
    const topics = yield admin.listTopics();
    if (!topics.includes(topic)) {
        yield admin.createTopics({
            topics: [{
                    topic: topic,
                    numPartitions: numPartitions
                }]
        });
    }
    yield admin.disconnect();
});
exports.createTopic = createTopic;
const Consume = (consumer, onMessage) => __awaiter(void 0, void 0, void 0, function* () {
    yield consumer.connect();
    consumer.run({
        eachMessage: (payload) => __awaiter(void 0, void 0, void 0, function* () {
            if (payload.message.value) {
                try {
                    const message = payload.message.value.toString();
                    const obj = JSON.parse(message);
                    onMessage(obj);
                }
                catch (err) {
                    console.error(err);
                }
            }
        })
    });
});
exports.Consume = Consume;
const Produce = (kafka, topic, message) => __awaiter(void 0, void 0, void 0, function* () {
    const messages = message.map(m => ({
        value: Buffer.from(JSON.stringify(m))
    }));
    const producer = kafka.producer();
    yield producer.connect();
    yield producer.send({
        topic: topic,
        messages: messages
    });
    yield producer.disconnect();
});
exports.Produce = Produce;
const KafkaProduceAuto = (topic, obj) => __awaiter(void 0, void 0, void 0, function* () {
    const numPartitions = parseInt(process.env.KAFKA_NUMPARTIONS) || 3;
    const kafka = createKafka();
    yield (0, exports.createTopic)(kafka, topic, 1);
    yield (0, exports.Produce)(kafka, topic, [obj]);
});
exports.KafkaProduceAuto = KafkaProduceAuto;
const KafkaConsumerAuto = (topic, onMessage) => __awaiter(void 0, void 0, void 0, function* () {
    const kafka = createKafka();
    const consumer = kafka.consumer({
        groupId: process.env.KAFKA_GROUPID,
    });
    yield consumer.subscribe({
        topics: [topic],
        fromBeginning: true
    });
    yield (0, exports.Consume)(consumer, onMessage);
});
exports.KafkaConsumerAuto = KafkaConsumerAuto;
exports.default = {
    Consume: exports.Consume,
    Produce: exports.Produce,
    KafkaConsumerAuto: exports.KafkaConsumerAuto,
    KafkaProduceAuto: exports.KafkaProduceAuto
};
