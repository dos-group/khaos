package de.tu_berlin.dos.arm.khaos.utils;

import org.apache.log4j.Logger;

public interface SequenceFSM<C, E extends Enum<E> & SequenceFSM<C, E>> {

    Logger LOG = Logger.getLogger(SequenceFSM.class);

    E runStage(C context) throws Exception;

    default void run(Class<E> definition, C context) throws Exception {

        E[] stages = definition.getEnumConstants();
        E finalState = stages[stages.length - 1];
        E curState = stages[0];

        while (curState != finalState) {

            E prev = curState;
            curState = curState.runStage(context);
            LOG.info("STATE-CHANGE: " + prev + " -> " + curState);
        }
    }
}
