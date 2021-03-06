/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.odbogm.exceptions;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Marcelo D. Ré {@literal <marcelo.re@gmail.com>}
 */
public class InvalidObjectReference extends RuntimeException {
    private final static Logger LOGGER = Logger.getLogger(InvalidObjectReference.class .getName());
    static {
        LOGGER.setLevel(Level.INFO);
    }

    public InvalidObjectReference() {
        super("The NEW objects has been rolledback and does no longer exist");
    }

    public InvalidObjectReference(String message) {
        super(message);
    }
    
}
