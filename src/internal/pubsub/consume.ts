import amqp from 'amqplib';
import { type Channel } from 'amqplib'

export enum SimpleQueueType {
    Durable,
    Transient,
}

export async function declareAndBind(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
    const channel = await conn.createChannel();
    let options = {}
    if(queueType === SimpleQueueType.Durable){
        options = {
            durable: true,
            autoDelete: false,
            exclusive: false
        }
    }
    else if(queueType === SimpleQueueType.Transient){
        options = {
            durable: false,
            autoDelete: true,
            exclusive: true
        }
    }
    else{
        throw new Error("queue type incorrect")
    }
    const queue = await channel.assertQueue(queueName, options);
    await channel.bindQueue(queueName, exchange, key);
    return [channel, queue ]
}