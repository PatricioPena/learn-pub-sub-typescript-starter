import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";

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

  await declareAndBind(conn, ExchangePerilDirect, `pause.${username}`, PauseKey, SimpleQueueType.Transient);
  const newGame = new GameState(username);
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
        commandMove(newGame, words);
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
