package de.tu_berlin.dos.arm.khaos.common.utils;

public interface SequenceFSM<C, E extends Enum<E> & SequenceFSM<C, E>> {

    E runStage(C context);

    default void run(Class<E> definition, C context) {

        E[] stages = definition.getEnumConstants();
        E finalState = stages[stages.length - 1];
        E curState = stages[0];

        while (curState != finalState) {

            E prev = curState;
            curState = curState.runStage(context);
            //System.out.println("STATE-CHANGE: " + prev + " -> " + curState);
        }
    }
}
