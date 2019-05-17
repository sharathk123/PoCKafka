package com.infy.allstate.sample;

/*public class CustomObject {
	
	private int CustomerId = 0;
	public int getCustomerId() {
		return CustomerId;
	}
	public void setCustomerId(int customerId) {
		CustomerId = customerId;
	}
	public String getCustomerName() {
		return CustomerName;
	}
	public void setCustomerName(String customerName) {
		CustomerName = customerName;
	}
	public String getCustomerJobName() {
		return CustomerJobName;
	}
	public void setCustomerJobName(String customerJobName) {
		CustomerJobName = customerJobName;
	}
	private String CustomerName = null;
	private String CustomerJobName = null;
}
*/


import java.util.StringTokenizer;

public class CustomObject {

    private int contactId;
    private String firstName;
    private String lastName;

    public CustomObject(){

    }
    public CustomObject(int contactId, String firstName, String lastName) {
        this.contactId = contactId;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public void parseString(String csvStr){
        StringTokenizer st = new StringTokenizer(csvStr,",");
        contactId = Integer.parseInt(st.nextToken());
        firstName = st.nextToken();
        lastName = st.nextToken();
    }


    public int getContactId() {
        return contactId;
    }

    public void setContactId(int contactId) {
        this.contactId = contactId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Override
    public String toString() {
        return "Contact{" +
                "contactId=" + contactId +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                '}';
    }	
}