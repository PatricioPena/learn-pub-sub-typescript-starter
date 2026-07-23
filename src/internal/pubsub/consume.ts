import amqp from 'amqplib';
import { type Channel } from 'amqplib'
import { ExchangePerilDlx } from '../routing/routing.js';

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
            exclusive: false,
            arguments: {
                "x-dead-letter-exchange": ExchangePerilDlx
            }
        }
    }
    else if (queueType === SimpleQueueType.Transient) {
        options = {
            durable: false,
            autoDelete: true,
            exclusive: true,
            arguments: {
                "x-dead-letter-exchange": ExchangePerilDlx
            }
        }
    }
    else {
        throw new Error("queue type incorrect")
    }
    const queue = await channel.assertQueue(queueName, options);
    await channel.bindQueue(queueName, exchange, key);
    return [channel, queue]
}

export enum AckType {
    Ack,
    NackRequeue,
    NackDiscard,
}

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
    const decBind = await declareAndBind(conn, exchange, queueName, key, queueType);
    const channel = decBind[0];
    const queue = decBind[1];
    await channel.consume(queue.queue, async (message: amqp.ConsumeMessage | null) => {
        if(!message){
            return
        }
        const parseMessage = JSON.parse(message.content.toString());
        const result = await handler(parseMessage);
        if(result === AckType.Ack ){
            channel.ack(message);
            console.log("Ack action occured");
        }
        else if(result === AckType.NackRequeue){
            channel.nack(message, false, true);
            console.log("Nack Requeue action occured");
        }
        else if(result === AckType.NackDiscard){
            channel.nack(message, false, false);
            console.log("Nack Discard action occured");
        }
        
    })
}