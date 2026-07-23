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
    if (queueType === SimpleQueueType.Durable) {
        options = {
            durable: true,
            autoDelete: false,
            exclusive: false
        }
    }
    else if (queueType === SimpleQueueType.Transient) {
        options = {
            durable: false,
            autoDelete: true,
            exclusive: true
        }
    }
    else {
        throw new Error("queue type incorrect")
    }
    const queue = await channel.assertQueue(queueName, options);
    await channel.bindQueue(queueName, exchange, key);
    return [channel, queue]
}

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => void,
): Promise<void> {
    const decBind = await declareAndBind(conn, exchange, queueName, key, queueType);
    const channel = decBind[0];
    const queue = decBind[1];
    await channel.consume(queue.queue, (message: amqp.ConsumeMessage | null) => {
        if(!message){
            return
        }
        const parseMessage = JSON.parse(message.content.toString());
        handler(parseMessage);
        channel.ack(message);
    })
}