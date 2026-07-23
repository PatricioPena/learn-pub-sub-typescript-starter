import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import {  SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { handlerPause, handlerMove } from "./handlers.js";
import { ArmyMovesPrefix, ExchangePerilTopic } from "../internal/routing/routing.js";
import { publishJSON } from "../internal/pubsub/publish.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection was successful");
  const username = await clientWelcome();

  ["SIGINT", "SIGTERM"].forEach((signal) =>
  process.on(signal, async () => {
    try {
      await conn.close();
      console.log("RabbitMQ connection closed.");
    } catch (err) {
      console.error("Error closing RabbitMQ connection:", err);
    } finally {
      process.exit(0);
    }
  }),
);

  const newGame = new GameState(username);
  await subscribeJSON(conn, ExchangePerilDirect, `pause.${username}`, PauseKey, SimpleQueueType.Transient, handlerPause(newGame))
  await subscribeJSON(conn, ExchangePerilTopic, `${ArmyMovesPrefix}.${username}`, `${ArmyMovesPrefix}.*`, SimpleQueueType.Transient, handlerMove(newGame))
  const publishCh = await conn.createConfirmChannel();
  
  while (true) {
    const words = await getInput();
    if (words.length === 0) {
      continue;
    }
    if (words[0] === "spawn") {
      try {
        commandSpawn(newGame, words);
      } catch (err) {
        console.log((err as Error).message);
      }
    }
    else if (words[0] === "move") {
      try {
        const am = commandMove(newGame, words);
        await publishJSON(publishCh, ExchangePerilTopic, `${ArmyMovesPrefix}.${username}`, am )
        console.log("move published successfully")
      } catch (err) {
        console.log((err as Error).message);
      }
    }
    else if (words[0] === "status") {
      commandStatus(newGame);

    }
    else if (words[0] === "help") {
      printClientHelp();
    }
    else if (words[0] === "spam") {
      console.log("Spamming not allowed yet!");
    }
    else if (words[0] === "quit") {
      printQuit();
      process.exit(0);
    }
    else {
      console.log("Command not found")
    }
  }

}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
