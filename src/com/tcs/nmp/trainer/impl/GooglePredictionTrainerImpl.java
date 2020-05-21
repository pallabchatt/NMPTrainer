package com.tcs.nmp.trainer.impl;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.prediction.Prediction;
import com.google.api.services.prediction.PredictionScopes;
import com.google.api.services.prediction.model.Insert;
import com.google.api.services.prediction.model.Insert2;
import com.google.api.services.prediction.model.Update;
import com.google.api.services.storage.StorageScopes;
import com.tcs.nmp.trainer.intf.PredictionTrainerIntf;

/**
 * @author Shekhar Sarkar
 *
 */
class GooglePredictionTrainerImpl implements PredictionTrainerIntf {
	
	private static final Logger LOGGER =  LogManager.getLogger(GooglePredictionTrainerImpl.class);
	

	private static final String APPLICATION_NAME = "TestGoogleApi";
	
	static final String MODEL_ID = "NMPTrainer01122016";
	
	static final String PROJECT_ID = "symmetric-ray-147418";
	static final String SERVICE_ACCT_EMAIL = "testgoogleapi@symmetric-ray-147418.iam.gserviceaccount.com";
	static final String SERVICE_ACCT_KEYFILE = "/com/tcs/nmp/trainer/keys/TestGoogleApi-5cddc8f41a51.p12";
	
	static final String TRAINING_DATA_LOCATION = "C:\\Users\\Shekhar\\Downloads\\Hackathon\\TrainingFiles";
	
	static final String TRAINING_DATA_FILENAME =  "Prediction_Training_Data.txt";
	
	/** Global instance of the HTTP transport. */
	private static HttpTransport httpTransport;
	
	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = JacksonFactory
			.getDefaultInstance();
	
	
	@Override
	public void train() throws GeneralSecurityException, IOException, InterruptedException {
		httpTransport = GoogleNetHttpTransport.newTrustedTransport();

		Prediction prediction = getPrediction();
		trainPrediction(prediction);
		InputStream in = null;
		try{
			in = new FileInputStream(TRAINING_DATA_LOCATION+"\\"+ TRAINING_DATA_FILENAME);
		}catch(FileNotFoundException fe){
			LOGGER.debug("No Training Data Found");
		}
		if(in != null){
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line;
			int counter = 0;
			//int recordCounter = 1;
			while ((line = reader.readLine()) != null) {
				if (line.contains(",")) {
					String[] eachRow = line.split(",");
					if (eachRow.length > 1) {
						counter++;
						update(prediction, eachRow[1], eachRow[0]);
					}
				}
				if (counter == 100) {
					LOGGER.debug("********************Pausing training for 10 secs after 100 updates********************* ");
					/*Thread.sleep(10000 * (recordCounter++));
					counter = 0;
					if(recordCounter == 6){
						recordCounter = 0;
					}*/
					checkTrainingStatus(prediction);
				}
			}
			reader.close();
			checkTrainingStatus(prediction);
			if(renameTrainingDataFile()){
				LOGGER.debug("File rename completed");
			}
		}

	}
	



	/**
	 * @return
	 */
	private boolean renameTrainingDataFile() {
		boolean renameDone = false;
		File fileListDir = new File(TRAINING_DATA_LOCATION);
		File[] fileList = fileListDir.listFiles();
		if(fileList != null && fileList.length >0){
			for(File eachFile : fileList){
				if(eachFile.getName().equals(TRAINING_DATA_FILENAME)){					
					String []existingFileName = TRAINING_DATA_FILENAME.split("\\.");
					StringBuilder newFileName= new StringBuilder(existingFileName[0]);
					newFileName.append("_").append(new SimpleDateFormat("MM-dd-yyyy").format(new Date())).append(".").append(existingFileName[1]);
					renameDone = eachFile.renameTo(new File(TRAINING_DATA_LOCATION+"\\"+ newFileName.toString()));
					break;
				}
			}
		}
		return renameDone;
	}




	/** Authorizes the installed application to access user's protected data. 
	 * @throws IOException 
	 * @throws GeneralSecurityException */
	private static GoogleCredential authorize() throws GeneralSecurityException, IOException  {
		return new GoogleCredential.Builder()
				.setTransport(httpTransport)
				.setJsonFactory(JSON_FACTORY)
				.setServiceAccountId(SERVICE_ACCT_EMAIL)
				.setServiceAccountPrivateKeyFromP12File(
						new File(GooglePredictionTrainerImpl.class.getResource(
								 SERVICE_ACCT_KEYFILE).getFile()))
				.setServiceAccountScopes(
						Arrays.asList(PredictionScopes.PREDICTION,
								StorageScopes.DEVSTORAGE_READ_ONLY)).build();
	}



	public static Prediction getPrediction() throws GeneralSecurityException, IOException  {
		// authorization
		GoogleCredential credential = authorize();
		Prediction prediction = new Prediction.Builder(httpTransport,
				JSON_FACTORY, setHttpTimeout(credential)).setApplicationName(
				APPLICATION_NAME).build();
		return prediction;
	}

	private static void checkTrainingStatus(Prediction prediction)
			throws IOException {
		LOGGER.debug("Prediction waiting for traing status completion");
		int triesCounter = 0;
		Insert2 trainingModel;
		while (triesCounter < 10000) {
			// NOTE: if model not found, it will throw an HttpResponseException
			// with a 404 error
			try {
				HttpResponse response = prediction.trainedmodels()
						.get(PROJECT_ID, MODEL_ID).executeUnparsed();
				if (response.getStatusCode() == 200) {
					trainingModel = response.parseAs(Insert2.class);
					String trainingStatus = trainingModel.getTrainingStatus();
					if (trainingStatus.equals("DONE")) {
						LOGGER.debug("Training completed.");
						LOGGER.debug(trainingModel.getModelInfo());
						return;
					}
				}
				response.ignore();
			} catch (HttpResponseException e) {
			}

			try {
				// 5 seconds times the tries counter
				Thread.sleep(5000 * (triesCounter + 1));
			} catch (InterruptedException e) {
				break;
			}
			LOGGER.debug(".");
			System.out.flush();
			triesCounter++;
		}

	}

	private static void trainPrediction(Prediction prediction) throws IOException {
		Insert trainingData = new Insert();
		trainingData.setId(MODEL_ID);
		trainingData.setStorageDataLocation("");
		prediction.trainedmodels().insert(PROJECT_ID, trainingData).execute();
		LOGGER.debug("Training started.");
		LOGGER.debug("Waiting for training to complete");
		System.out.flush();

		int triesCounter = 0;
		Insert2 trainingModel;
		while (triesCounter < 100) {
			// NOTE: if model not found, it will throw an HttpResponseException
			// with a 404 error
			try {
				HttpResponse response = prediction.trainedmodels()
						.get(PROJECT_ID, MODEL_ID).executeUnparsed();
				if (response.getStatusCode() == 200) {
					trainingModel = response.parseAs(Insert2.class);
					String trainingStatus = trainingModel.getTrainingStatus();
					if (trainingStatus.equals("DONE")) {
						LOGGER.debug(trainingModel.getModelInfo());
						LOGGER.debug("Initial Training completed.");
						return;
					}
				}
				response.ignore();
			} catch (HttpResponseException e) {
			}

			try {
				// 5 seconds times the tries counter
				Thread.sleep(5000 * (triesCounter + 1));
			} catch (InterruptedException e) {
				break;
			}
			LOGGER.debug(".");
			System.out.flush();
			triesCounter++;
		}
		LOGGER.error("ERROR: training not completed.");
	}

	private static void update(Prediction prediction, String input,
			String output) throws GoogleJsonResponseException, IOException {
		Update trainingData = new Update();
		trainingData.setCsvInstance(Collections.<Object> singletonList(input));
		trainingData.setOutput(output);
		int triesCounter = 0;
		Update trainingModel = null;
		int retryCount = 0;
		do{			
			try {
				prediction.trainedmodels()
				.update(PROJECT_ID, MODEL_ID, trainingData).execute();
				break;
			} catch (GoogleJsonResponseException ge) {
				retryCount ++;
				LOGGER.debug("Retrying for  :" + ge.getMessage());
			}catch(SocketTimeoutException se){
				LOGGER.error(se);
				retryCount ++;
			}
			
		}while(retryCount <5);
		LOGGER.debug("Training Started.");
		LOGGER.debug("Waiting for training to complete");
		System.out.flush();

		while (triesCounter < 100) {
			// NOTE: if model not found, it will throw an HttpResponseException
			// with a 404 error
			try {
				HttpResponse response = prediction.trainedmodels()
						.get(PROJECT_ID, MODEL_ID).executeUnparsed();
				if (response.getStatusCode() == 200) {
					trainingModel = response.parseAs(Update.class);
					LOGGER.debug(trainingModel.getUnknownKeys());
					LOGGER.debug("Training DONE.");
					return;
				}
				response.ignore();
			} catch (GoogleJsonResponseException ge) {
				LOGGER.error(ge);
				return;
			} catch (HttpResponseException e) {
				LOGGER.error(e);
			}

			try {
				// 5 seconds times the tries counter
				Thread.sleep(5000 * (triesCounter + 1));
			} catch (InterruptedException e) {
				break;
			}
			LOGGER.debug(".");
			System.out.flush();
			triesCounter++;

		}
		LOGGER.error("ERROR: training not completed.");
	}

	private static HttpRequestInitializer setHttpTimeout(
			final HttpRequestInitializer requestInitializer) {
		return new HttpRequestInitializer() {
			@Override
			public void initialize(HttpRequest httpRequest) throws IOException {
				requestInitializer.initialize(httpRequest);
				httpRequest.setConnectTimeout(3 * 60000); // 3 minutes connect
															// timeout
				httpRequest.setReadTimeout(3 * 60000); // 3 minutes read timeout
			}
		};
	}

	/*private static void error(String errorMessage) {
		LOGGER.error(errorMessage);
		System.exit(1);
	}*/

	

	/*public static void main(String[] args) {

		// System.setProperty("javax.net.ssl.trustStore",
		// "C:/Apache22/keys/X509_certificate.cer");
		// System.setProperty("javax.net.ssl.trustStorePassword", "passsword");
		
		GooglePredictionTrainerImpl gp = new GooglePredictionTrainerImpl();
		try {
			gp.train();
			//check("");
			// success!
			return;
		} catch (GoogleJsonResponseException e) {
			
			LOGGER.error(e.getDetails());
		} catch (IOException e) {
			
			LOGGER.error(e.getMessage());
		} catch (Throwable t) {
			LOGGER.error(e.getMessage());
		}
		System.exit(1);
	}*/
	



}
