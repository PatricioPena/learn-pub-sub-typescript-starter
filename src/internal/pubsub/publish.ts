import type { ConfirmChannel } from "amqplib";

export async function publishJSON<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T,
): Promise<void> {
    const strVal = JSON.stringify(value);
    const newVal = Buffer.from(strVal);
    ch.publish(exchange, routingKey, newVal, {contentType: "application/json"} );
    return;
}
