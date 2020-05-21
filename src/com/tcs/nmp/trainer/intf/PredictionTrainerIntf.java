package com.tcs.nmp.trainer.intf;

import java.io.IOException;
import java.security.GeneralSecurityException;

public interface PredictionTrainerIntf {

	public void train() throws GeneralSecurityException, IOException, InterruptedException;
}
