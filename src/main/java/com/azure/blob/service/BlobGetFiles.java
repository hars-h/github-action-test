package com.azure.blob.service;

import com.azure.eventhub.producer.EventHubProducer;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.json.JSONArray;
import org.json.JSONObject;
import static com.azureblob.service.constants.ReplayConstants.ACCOUNT_KEY;
import static com.azureblob.service.constants.ReplayConstants.ACCOUNT_NAME;
import static com.azureblob.service.constants.ReplayConstants.BLOB_CONTAINER;

/**
 * This class gets Event Hub capture files from Azure Storage Blob and replays
 * it to EventHub Topic.
 */
public class BlobGetFiles {

	/**
	 * Get Files from Storage blobs.
	 *
	 * @param args Unused. Arguments to the program.
	 * @throws IOException      If an I/O error occurs
	 * @throws RuntimeException If the downloaded data doesn't match the uploaded
	 *                          data
	 */
	public static void main(String[] args) throws IOException {

		/*
		 * From the Azure portal, get your Storage account's name and account key.
		 */

		FileReader reader = new FileReader("src/main/resources/blob.config");
		Properties property = new Properties();
		property.load(reader);

		String accountName = property.getProperty(ACCOUNT_NAME);
		String accountKey = property.getProperty(ACCOUNT_KEY);
		EventHubProducer ehproducer = new EventHubProducer();

		/*
		 * Use your Storage account's name and key to create a credential object; this
		 * is used to access your account.
		 */
		StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);

		/*
		 * From the Azure portal, get your Storage account blob service URL endpoint.
		 * The URL typically looks like this:
		 */
		String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);

		/*
		 * Create a BlobServiceClient object that wraps the service endpoint, credential
		 * and a request pipeline.
		 */
		BlobServiceClient storageClient = new BlobServiceClientBuilder().endpoint(endpoint).credential(credential)
				.buildClient();

		BlobContainerClient blobContainerClient = storageClient
				.getBlobContainerClient(property.getProperty(BLOB_CONTAINER));

		HashMap<String, Date> map = new HashMap<String, Date>();
		blobContainerClient.listBlobs().forEach(blobItem -> map.put(blobItem.getName().toString(),
				Date.from(blobItem.getProperties().getCreationTime().toInstant())));
		System.out.println("Size of Blob : " + map.size());

		for (Entry<String, Date> m : map.entrySet()) {
			String file = m.getKey();
			String fileDetails[] = file.split("/");
			String ehName = fileDetails[0];
			String topicName = fileDetails[1];

			Date d = m.getValue();
			BlockBlobClient blobClient = blobContainerClient.getBlobClient(file).getBlockBlobClient();
			String filename = topicName + "_tmp_" + d.toInstant().toEpochMilli() + ".avro";
			try {
				blobClient.downloadToFile(filename);
			} catch (Exception e) {
				System.out.println("ERROR");
			}

			DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(filename), datumReader);
			Schema schema = dataFileReader.getSchema();
			System.out.println(schema);

			GenericRecord record = null;
			while (dataFileReader.hasNext()) {
				record = dataFileReader.next();
				try {
					String s = new String(record.toString());
					System.out.println("RECORD: \n" + s);
					JSONObject json = new JSONObject(s);
					JSONObject prop = (JSONObject) json.get("Properties");
					// HashMap<String, Object> result = new ObjectMapper().readValue(prop, new
					// TypeReference<Map<String, Object>>(){});
					System.out.println("Prod.toString : " + prop.toString());
					HashMap<String, String> result = createHM(prop);
					// HashMap<String,String> result =new ObjectMapper().readValue(prop.toString(),
					// HashMap.class);
					JSONObject obj = (JSONObject) json.get("Body");
					Object data = new Object();
					data = new JSONArray(obj.get("bytes").toString());

					System.out.println("\n*********JSON*********\n" + data.toString());
					ehproducer.produce(data.toString(), result, topicName);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
			File fl = new File(filename);
			fl.delete();
		}

	}

	private static HashMap<String, String> createHM(JSONObject prop) {

		HashMap<String, String> headerMap = new HashMap<String, String>();
		Iterator<String> keys = prop.keys();

		while (keys.hasNext()) {
			String key = keys.next();
			String value = new String();
			JSONObject b = null;
			if (prop.get(key) instanceof JSONObject) {
				b = (JSONObject) prop.get(key);
			}
			value = b.get("bytes").toString();
			hm.put(key, value);
		}

		return hm;
	}
}