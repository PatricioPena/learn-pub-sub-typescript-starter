import amqp from "amqplib";
import process from 'node:process';
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection was successful");
  const confirmedChannel = await conn.createConfirmChannel();
  try{
    await publishJSON(confirmedChannel, ExchangePerilDirect, PauseKey, { isPaused: true });
  }catch(err){
    throw new Error(`Error found: ${err}`)
  }

  process.on('SIGINT', async () => {
    console.log(`Signal received, shutting down`);
    await conn.close();
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
