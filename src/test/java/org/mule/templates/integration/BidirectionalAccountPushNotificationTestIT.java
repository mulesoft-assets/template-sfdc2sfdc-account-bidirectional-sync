package org.mule.templates.integration;

import static org.mule.templates.builders.SfdcObjectBuilder.anAccount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.DefaultMuleMessage;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.construct.Flow;
import org.mule.processor.chain.InterceptingChainLifecycleWrapper;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;
import org.mule.templates.AbstractTemplatesTestCase;
import org.mule.templates.builders.SfdcObjectBuilder;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.mulesoft.module.batch.BatchTestHelper;
import com.sforce.soap.partner.SaveResult;

/**
 * The objective of this class is validating the correct behavior of the flows
 * for this Mule Anypoint Template
 * 
 */
@SuppressWarnings("unchecked")
public class BidirectionalAccountPushNotificationTestIT extends AbstractTemplatesTestCase {

	private static final String ANYPOINT_TEMPLATE_NAME = "sfdc2sfdc-bidirectional-account-sync";
	private static final String A_INBOUND_FLOW_NAME = "triggerSyncFromAFlow";
	private static final String B_INBOUND_FLOW_NAME = "triggerSyncFromBFlow";
	private static final int TIMEOUT_MILLIS = 60;

	private static List<String> accountsCreatedInA = new ArrayList<String>();
	private static List<String> accountsCreatedInB = new ArrayList<String>();
	private static SubflowInterceptingChainLifecycleWrapper deleteAccountFromAFlow;
	private static SubflowInterceptingChainLifecycleWrapper deleteAccountFromBFlow;
	
	private Flow triggerPushFlow;
	private SubflowInterceptingChainLifecycleWrapper createAccountInAFlow;
	private SubflowInterceptingChainLifecycleWrapper createAccountInBFlow;
	private InterceptingChainLifecycleWrapper queryAccountFromAFlow;
	private InterceptingChainLifecycleWrapper queryAccountFromBFlow;
	private BatchTestHelper batchTestHelper;

	@BeforeClass
	public static void beforeTestClass() {
		System.setProperty("page.size", "1000");

		// Set polling frequency to 10 seconds
		System.setProperty("polling.frequency", "10000");

		// Set default water-mark expression to current time
		System.clearProperty("watermark.default.expression");
		DateTime now = new DateTime(DateTimeZone.UTC);
		DateTimeFormatter dateFormat = DateTimeFormat
				.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		System.setProperty("watermark.default.expression",
				now.toString(dateFormat));
	}

	@Before
	public void setUp() throws MuleException {
		stopAutomaticPollTriggering();
		getAndInitializeFlows();
		
		batchTestHelper = new BatchTestHelper(muleContext);
	}

	@After
	public void tearDown() throws MuleException, Exception {
		cleanUpSandboxesByRemovingTestAccounts();
	}

	private void stopAutomaticPollTriggering() throws MuleException {
		stopFlowSchedulers(A_INBOUND_FLOW_NAME);
		stopFlowSchedulers(B_INBOUND_FLOW_NAME);
	}

  	private void getAndInitializeFlows() throws InitialisationException {
		// Flow for creating accounts in sfdc A instance
		triggerPushFlow = getFlow("triggerPushFlow");

		createAccountInAFlow = getSubFlow("createAccountInAFlow");
		createAccountInAFlow.initialise();

		// Flow for creating accounts in sfdc B instance
		createAccountInBFlow = getSubFlow("createAccountInBFlow");
		createAccountInBFlow.initialise();

		// Flow for deleting accounts in sfdc A instance
		deleteAccountFromAFlow = getSubFlow("deleteAccountFromAFlow");
		deleteAccountFromAFlow.initialise();

		// Flow for deleting accounts in sfdc B instance
		deleteAccountFromBFlow = getSubFlow("deleteAccountFromBFlow");
		deleteAccountFromBFlow.initialise();

		// Flow for querying the account in A instance
		queryAccountFromAFlow = getSubFlow("queryAccountFromAFlow");
		queryAccountFromAFlow.initialise();

		// Flow for querying the account in B instance
		queryAccountFromBFlow = getSubFlow("queryAccountFromBFlow");
		queryAccountFromBFlow.initialise();
	}

	private static void cleanUpSandboxesByRemovingTestAccounts()
			throws MuleException, Exception {
		final List<String> idList = new ArrayList<String>();
		for (String account : accountsCreatedInA) {
			idList.add(account);
		}
		deleteAccountFromAFlow.process(getTestEvent(idList,
				MessageExchangePattern.REQUEST_RESPONSE));
		idList.clear();
		for (String account : accountsCreatedInB) {
			idList.add(account);
		}
		deleteAccountFromBFlow.process(getTestEvent(idList,
				MessageExchangePattern.REQUEST_RESPONSE));
	}
	
	@Test
	public void whenUpdatingAnAccountInInstanceBTheBelongingAccountGetsUpdatedInInstanceA()
			throws MuleException, Exception {

		// Execution
		MuleMessage message = new DefaultMuleMessage(buildRequest(), muleContext);
		MuleEvent testEvent = getTestEvent(message, MessageExchangePattern.REQUEST_RESPONSE);
		testEvent.setFlowVariable("sourceSystem", "A");
		triggerPushFlow.process(testEvent);
		
		batchTestHelper.awaitJobTermination(TIMEOUT_MILLIS * 1000, 500);
		batchTestHelper.assertJobWasSuccessful();

		// Assertions
		HashMap<String, Object> account = new HashMap<String, Object>();
		account.put("Name", "Account bbbb");
		Map<String, String> retrievedAccountFromB = (Map<String, String>) queryAccount(
				account, queryAccountFromBFlow);

		Assert.assertNotNull(retrievedAccountFromB);
		Assert.assertEquals("Account Names should be equals", account.get("Name"), retrievedAccountFromB.get("Name"));
	}

	private Object queryAccount(Map<String, Object> account,
			InterceptingChainLifecycleWrapper queryAccountFlow)
			throws MuleException, Exception {
		return queryAccountFlow
				.process(
						getTestEvent(account,
								MessageExchangePattern.REQUEST_RESPONSE))
				.getMessage().getPayload();
	}

	private String createTestAccountsInSfdcSandbox(Map<String, Object> account,
			InterceptingChainLifecycleWrapper createAccountFlow)
			throws MuleException, Exception {
		List<Map<String, Object>> salesforceAccounts = new ArrayList<Map<String, Object>>();
		salesforceAccounts.add(account);

		final List<SaveResult> payloadAfterExecution = (List<SaveResult>) createAccountFlow
				.process(
						getTestEvent(salesforceAccounts,
								MessageExchangePattern.REQUEST_RESPONSE))
				.getMessage().getPayload();
		return payloadAfterExecution.get(0).getId();
	}

	private void executeWaitAndAssertBatchJob(String flowConstructName)
			throws Exception {

		// Execute synchronization
		runSchedulersOnce(flowConstructName);

		// Wait for the batch job execution to finish
		batchTestHelper.awaitJobTermination(TIMEOUT_MILLIS * 1000, 500);
		batchTestHelper.assertJobWasSuccessful();
	}

	private String buildRequest(){
		String req = "";
		req += "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
		req += "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">";
		req += " <soapenv:Body>";
		req += "  <notifications xmlns=\"http://soap.sforce.com/2005/09/outbound\">";
		req += "   <OrganizationId>00Dd0000000dtDqEAI</OrganizationId>";
		req += "   <ActionId>04kd0000000PCgvAAG</ActionId>";
		req += "   <SessionId xsi:nil=\"true\"/>";
		req += "   <EnterpriseUrl>https://na14.salesforce.com/services/Soap/c/30.0/00Dd0000000dtDq</EnterpriseUrl>";
		req += "   <PartnerUrl>https://na14.salesforce.com/services/Soap/u/30.0/00Dd0000000dtDq</PartnerUrl>";
		req += "   <Notification>";
		req += "    <Id>04ld000000TzMKpAAN</Id>";
		req += "    <sObject xsi:type=\"sf:Account\" xmlns:sf=\"urn:sobject.enterprise.soap.sforce.com\">";
		req += "     <sf:Id>001d000001XD5XKAA1</sf:Id>";
		req += "     <sf:AccountNumber>4564564</sf:AccountNumber>";
		req += "     <sf:AnnualRevenue>10000.0</sf:AnnualRevenue>";
		req += "     <sf:BillingCity>City</sf:BillingCity>";
		req += "     <sf:BillingCountry>Country</sf:BillingCountry>";
		req += "     <sf:BillingPostalCode>04001</sf:BillingPostalCode>";
		req += "     <sf:BillingState>State</sf:BillingState>";
		req += "     <sf:BillingStreet>Street</sf:BillingStreet>";
		req += "     <sf:CreatedById>005d0000000yYC7AAM</sf:CreatedById>";
		req += "     <sf:CreatedDate>2014-05-05T11:47:49.000Z</sf:CreatedDate>";
		req += "     <sf:CustomerPriority__c>High</sf:CustomerPriority__c>";
		req += "     <sf:Description>description ddddd</sf:Description>";
		req += "     <sf:Fax>+421995555</sf:Fax>";
		req += "     <sf:Industry>Apparel</sf:Industry>";
		req += "     <sf:IsDeleted>false</sf:IsDeleted>";
		req += "     <sf:LastModifiedById>005d0000000yYC7AAM</sf:LastModifiedById>";
		req += "     <sf:LastModifiedDate>2014-06-02T13:00:00.000Z</sf:LastModifiedDate>";
		req += "     <sf:LastReferencedDate>2014-05-19T11:02:14.000Z</sf:LastReferencedDate>";
		req += "     <sf:LastViewedDate>2014-05-19T11:02:14.000Z</sf:LastViewedDate>";
		req += "     <sf:Name>Account bbbb</sf:Name>";
		req += "     <sf:NumberOfEmployees>5000</sf:NumberOfEmployees>";
		req += "     <sf:OwnerId>005d0000000yYC7AAM</sf:OwnerId>";
		req += "     <sf:Ownership>Public</sf:Ownership>";
		req += "     <sf:Phone>+421995555</sf:Phone>";
		req += "     <sf:PhotoUrl>/services/images/photo/001d000001XD5XKAA1</sf:PhotoUrl>";
		req += "     <sf:Rating>Hot</sf:Rating>";
		req += "     <sf:SLA__c>Gold</sf:SLA__c>";
		req += "     <sf:ShippingCity>Shipping City</sf:ShippingCity>";
		req += "     <sf:ShippingCountry>Country</sf:ShippingCountry>";
		req += "     <sf:ShippingPostalCode>04001</sf:ShippingPostalCode>";
		req += "     <sf:ShippingState>Shipping State</sf:ShippingState>";
		req += "     <sf:ShippingStreet>Shipping street</sf:ShippingStreet>";
		req += "     <sf:Site>http://www.test.com</sf:Site>";
		req += "     <sf:SystemModstamp>2014-05-19T11:02:14.000Z</sf:SystemModstamp>";
		req += "     <sf:Type>Prospect</sf:Type>";
		req += "     <sf:Website>http://www.test.com</sf:Website>";
		req += "    </sObject>";
		req += "   </Notification>";
		req += "  </notifications>";
		req += " </soapenv:Body>";
		req += "</soapenv:Envelope>";
		return req;
	}
}
