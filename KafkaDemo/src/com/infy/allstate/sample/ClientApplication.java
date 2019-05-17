package com.infy.allstate.sample;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.logging.SimpleFormatter;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.infy.allstate.sample.IKafkaConstants;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.infy.allstate.sample.ConsumerCreator;
import com.infy.allstate.sample.ProducerCreator;

import kafka.utils.json.JsonArray;
import kafka.utils.json.JsonObject;

public class ClientApplication {

	public static void main(String[] args) {

		runProducer();

		runConsumer();

	}

	static void runConsumer() {

		Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

		int noMessageFound = 0;

		while (true) {

			ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

			// 1000 is the time in milliseconds consumer will wait if no record is found at
			// broker.

			if (consumerRecords.count() == 0) {

				noMessageFound++;

				if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)

					// If no message found count is reached to threshold exit loop.
					break;

				else

					continue;

			}

			// print each record.

			List<EmployeeModel> cList = new ArrayList<EmployeeModel>();

			ObjectMapper mapper = new ObjectMapper();

			consumerRecords.forEach(record -> {
				/*
				 * if(null !=record.key()) { System.out.println("Record Key " + record.key()); }
				 */
				// JSONObject obj = new JSONObject();

				try {
					EmployeeModel employeeValue = mapper.readValue(record.value().toString(), EmployeeModel.class);
					cList.add(employeeValue);
				} catch (JsonParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JsonMappingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			});
//json conversion
			try {
				String newJsonData = mapper.writeValueAsString(cList);

				System.out.println("consumer data:" + newJsonData);

			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// commits the offset of record to broker.

			consumer.commitAsync();

		}

		consumer.close();

	}

	@SuppressWarnings("unchecked")
	static void runProducer() {

		Producer<Long, String> producer = ProducerCreator.createProducer();

		List<EmployeeModel> aList = new ArrayList<EmployeeModel>();
		ProducerRecord<Long, String> record = null;
		//EmployeeModel employeeModel = new EmployeeModel();
		try {
			String URL = "jdbc:sqlserver://vabufseze-04:1433;databaseName=ABUFSEZE1";
			String USERNAME = "root";
			String PASSWORD = "Welcome@2019";

			Connection conn;
			DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
			conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);

			String query = "SELECT * FROM DBS1.Employee1";
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);

			while (rs.next()) {
				EmployeeModel employeeModel = new EmployeeModel();
				SimpleDateFormat sf = new SimpleDateFormat("MM-dd-yyyy");
				String empName = rs.getString("EmpName");
				int age = rs.getInt("Age");
				String sex = rs.getString("Sex");
				String DOB = sf.format(rs.getDate("DOB"));
				String DOJ = sf.format(rs.getDate("DOJ"));
				String address = rs.getString("Address");
				String location = rs.getString("Location");
				String email = rs.getString("Email");
				String contactNo = rs.getString("ContactNo");
				employeeModel.setEmpName(empName);
				employeeModel.setAge(age);
				employeeModel.setSex(sex);
				employeeModel.setDOB(DOB);
				employeeModel.setDOJ(DOJ);
				employeeModel.setAddress(address);
				employeeModel.setLocation(location);
				employeeModel.setEmail(email);
				employeeModel.setContactNo(contactNo);
				aList.add(employeeModel);

				// }

				System.out.format("%s, %s, %s, %s, %s, %s,%s, %s, %s\n", empName, age, sex, DOB, DOJ, address, location,
						email, contactNo);
			}
			st.close();
		} catch (Exception e) {
			System.err.println("Got an exception! ");
			System.err.println(e.getMessage());
		}
		
		for (int index = 0; index < 100; index++) {
			
			int random = (int) (Math.random() * aList.size() + 1);
			
			//System.out.println("Random Number" + random);
			ObjectMapper mapper = new ObjectMapper();
			try {
				record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,
						"" + mapper.writeValueAsString(aList.get(random)));
			} catch (JsonProcessingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			try {

				RecordMetadata metadata = producer.send(record).get();
				//System.out.println("Record sent with key " + index);

			} catch (ExecutionException e) {

				System.out.println("Error in sending record");
				System.out.println(e);

			}

			catch (InterruptedException e) {

				System.out.println("Error in sending record");

				System.out.println(e);

			}

		}

	}

}
