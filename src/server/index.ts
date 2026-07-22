import amqp from "amqplib";
import process from 'node:process';
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, GameLogSlug , ExchangePerilTopic, PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";

async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection was successful");
  const confirmedChannel = await conn.createConfirmChannel();
  printServerHelp();
  
  await confirmedChannel.assertQueue(GameLogSlug, {
    durable: true,
    autoDelete: false,
    exclusive: false
  })

  await confirmedChannel.bindQueue(GameLogSlug, ExchangePerilTopic, "game_logs.*");

  while (true) {
    const words = await getInput();
    if (words.length === 0) {
      continue;
    }
    if (words[0] === "resume") {
      console.log("sending a resume...")
      try {
        await publishJSON(confirmedChannel, ExchangePerilDirect, PauseKey, { isPaused: false });
      } catch (err) {
        console.log(`Error found: ${err}`)
      }
    }
    else if (words[0] === "pause") {
      console.log("sending a pause...")
      try {
        await publishJSON(confirmedChannel, ExchangePerilDirect, PauseKey, { isPaused: true });
      } catch (err) {
        console.log(`Error found: ${err}`)
      }
    }
    else if (words[0] === "quit") {
      console.log("sending a exit...");
      break;
    }
    else {
      console.log("Unknown command");
    }
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
