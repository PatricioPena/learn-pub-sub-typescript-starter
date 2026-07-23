import { type ArmyMove } from './../internal/gamelogic/gamedata.js';
import { GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { AckType } from '../internal/pubsub/consume.js';

export function handlerPause(gs: GameState): (ps: PlayingState) => Promise<AckType> {
    return async (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack
    }
}

export function handlerMove(gs: GameState): (move: ArmyMove) => Promise<AckType>{
    return async (move: ArmyMove) => {
        const result = handleMove(gs, move);
        if(result === MoveOutcome.Safe || result === MoveOutcome.MakeWar){
            process.stdout.write("> ");
            return AckType.Ack;
        }
        else {
            process.stdout.write("> ");
            return AckType.NackDiscard;
            
        }
    }
}