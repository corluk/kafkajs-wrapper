import { Kafka , Message, Partitioners , EachMessagePayload , Consumer} from "kafkajs"; 
import dotenv from "dotenv"


const createKafka = ()=>{
 
    const brokers  = (process.env.KAFKA_BROKERS as string).split(",")
    const kafka : Kafka = new Kafka({
        clientId : process.env.KAFKA_CLIENTID as string,
        brokers:brokers,
        
        sasl: {
          mechanism: 'plain', // scram-sha-256 or scram-sha-512
          username: process.env.KAFKA_USER as string,
          password: process.env.KAFKA_PASS as string 
        }
        
    })
    return kafka 

}
export const createTopic = async  (kafka : Kafka , topic:string , numPartitions : number  )=>{
    const admin = kafka.admin() 
    await admin.connect()
    const topics = await admin.listTopics()
 
    if(!topics.includes(topic)){
        await admin.createTopics({
            topics : [{
                topic: topic, 
                numPartitions: numPartitions
            }]
        })
    }
    
    await admin.disconnect()
}
export const Consume = async <T>(consumer : Consumer , onMessage : <T>(obj:T)=>void)=>{

     
    await consumer.connect() 
    
    consumer.run({
        eachMessage : async (payload:EachMessagePayload)=>{
            if(payload.message.value){
                try{
                    const message = payload.message.value.toString() 
                    const obj = JSON.parse(message) as unknown as T 
                    onMessage(obj)
                    
                }catch(err){
                    console.error(err)
                }
                
            }
             
        }
    })

}
export const Produce =async  <T>(kafka : Kafka , topic : string ,message : T[] )=>{
    
    const messages = message.map(m => ({
        
        value : Buffer.from(JSON.stringify(m))
    }))
    const producer = kafka.producer()
    await producer.connect()
    await producer.send({
        topic : topic,
        messages : messages
    })
    await producer.disconnect()
}

export const KafkaProduceAuto = async <T>(topic : string ,   obj : T )=>{

    const numPartitions = parseInt(process.env.KAFKA_NUMPARTIONS as string) || 3
    const kafka = createKafka() 
    await createTopic(kafka,topic,1)
    await Produce(kafka,topic,[obj])

}
export const KafkaConsumerAuto = async <T>(topic:string,onMessage : <T>(obj:T)=>void)=>{

    
    const kafka =  createKafka() 
    const consumer = kafka.consumer({
        groupId : process.env.KAFKA_GROUPID as string ,
        
    })
    await consumer.subscribe({
        topics : [topic],
        fromBeginning:true 
    })
    await  Consume(consumer,onMessage)
}

export default {
    Consume : Consume, 
    Produce : Produce,
    KafkaConsumerAuto: KafkaConsumerAuto,
    KafkaProduceAuto
}