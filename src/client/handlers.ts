import { type ArmyMove, type RecognitionOfWar } from './../internal/gamelogic/gamedata.js';
import { GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { AckType } from '../internal/pubsub/consume.js';
import type { ConfirmChannel } from 'amqplib';
import { publishJSON } from '../internal/pubsub/publish.js';
import * as routing from "../internal/routing/routing.js";
import { handleWar, WarOutcome } from '../internal/gamelogic/war.js';
import { publishGameLog } from './index.js';
import amqp from "amqplib";

export function handlerPause(gs: GameState): (ps: PlayingState) => Promise<AckType> {
    return async (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack
    }
}

export function handlerMove(gs: GameState, ch: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
    return async (move: ArmyMove) => {
        const result = handleMove(gs, move);
        if (result === MoveOutcome.Safe) {
            process.stdout.write("> ");
            return AckType.Ack;
        }
        else if (result === MoveOutcome.MakeWar) {
            try {
                const routingKey = `${routing.WarRecognitionsPrefix}.${gs.getPlayerSnap().username}`;
                const rw: RecognitionOfWar = {
                    attacker: move.player,
                    defender: gs.getPlayerSnap(),
                };
                await publishJSON(ch, routing.ExchangePerilTopic, routingKey, rw);
                process.stdout.write("> ");
                return AckType.Ack;
            } catch {
                return AckType.NackRequeue;
            }
        }
        else {
            process.stdout.write("> ");
            return AckType.NackDiscard;

        }
    }
}

export function handlerWar(gs: GameState, publishCh: amqp.ConfirmChannel): (rw: RecognitionOfWar) => Promise<AckType> {
    return async (rw: RecognitionOfWar) => {
        const result = handleWar(gs, rw);
        if (result.result === WarOutcome.NotInvolved) {
            process.stdout.write("> ");
            return AckType.NackRequeue;
        }
        else if (result.result === WarOutcome.NoUnits) {
            process.stdout.write("> ");
            return AckType.NackDiscard;
        }
        else if (result.result === WarOutcome.OpponentWon) {
            try {
                await publishGameLog(publishCh, result.winner, `${result.winner} won a war against ${result.loser}`);
            } catch (err) {
                process.stdout.write("> ");
                return AckType.NackRequeue;
            }
            process.stdout.write("> ");
            return AckType.Ack;
        }
        else if (result.result === WarOutcome.YouWon) {
            try {
                await publishGameLog(publishCh, result.winner, `${result.winner} won a war against ${result.loser}`);
            } catch (err) {
                process.stdout.write("> ");
                return AckType.NackRequeue;
            }
            process.stdout.write("> ");
            return AckType.Ack;
        }
        else if (result.result === WarOutcome.Draw) {
            try{
            await publishGameLog(publishCh, result.attacker, `A war between ${result.attacker} and ${result.defender} resulted in a draw`);
            }catch(err){
                process.stdout.write("> ");
                return AckType.NackRequeue;
            }
            process.stdout.write("> ");
            return AckType.Ack;
        }
        else {
            console.log("Error");
            process.stdout.write("> ");
            return AckType.NackDiscard;
        }
    }
}