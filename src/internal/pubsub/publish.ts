import type { ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";

export async function publishJSON<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T,
): Promise<void> {
    const strVal = JSON.stringify(value);
    const newVal = Buffer.from(strVal);
    ch.publish(exchange, routingKey, newVal, { contentType: "application/json" });
    return;
}

export function publishMsgPack<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T,
): Promise<void> {
    const strVal = encode(value);
    const newVal = Buffer.from(strVal);
    return new Promise((resolve, reject) => {
        ch.publish(exchange, routingKey, newVal, { contentType: "application/x-msgpack" },
            (err) => {
                if (err !== null) {
                    reject(new Error("Message was NACKed by the broker"));
                }
                else {
                    resolve()
                }
            }
        );
    });
}