package org.mule.templates.integration;

import static org.mule.templates.builders.SfdcObjectBuilder.aContact;

import java.util.ArrayList;
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
import org.mule.MessageExchangePattern;
import org.mule.api.MuleException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.processor.chain.InterceptingChainLifecycleWrapper;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;
import org.mule.templates.builders.SfdcObjectBuilder;
import org.mule.templates.test.util.AbstractTemplatesTestCase;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.mulesoft.module.batch.BatchTestHelper;
import com.sforce.soap.partner.SaveResult;

/**
 * The objective of this class is validating the correct behavior of the flows for this Mule Anypoint Template
 * 
 */
@SuppressWarnings("unchecked")
public class BidirectionalAccountSyncTestIT extends AbstractTemplatesTestCase {

	private static final String POLL_A_BATCH_JOB_NAME = "fromAToBBatch";
	private static final String POLL_B_BATCH_JOB_NAME = "fromBToABatch";
	private static final String ANYPOINT_TEMPLATE_NAME = "sfdc2sfdc-bidirectional-account-sync";
	private static final int TIMEOUT_MILLIS = 60;
	
	private static List<String> accountsCreatedInA = new ArrayList<String>();
	private static List<String> accountsCreatedInB = new ArrayList<String>();
	private static SubflowInterceptingChainLifecycleWrapper deleteContactFromAFlow;
	private static SubflowInterceptingChainLifecycleWrapper deleteContactFromBFlow;
	
	private SubflowInterceptingChainLifecycleWrapper createContactInAFlow;
	private SubflowInterceptingChainLifecycleWrapper createContactInBFlow;
	private InterceptingChainLifecycleWrapper queryContactFromAFlow;
	private InterceptingChainLifecycleWrapper queryContactFromBFlow;
	private BatchTestHelper batchTestHelper;
	
	
	@BeforeClass
	public static void beforeTestClass() {
		// Set polling frequency to 10 seconds
		System.setProperty("polling.frequency", "10000");
		
		// Set default water-mark expression to current time
		System.clearProperty("watermark.default.expression");
		DateTime now = new DateTime(DateTimeZone.UTC);
		DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		System.setProperty("watermark.default.expression", now.toString(dateFormat));
	}
	
	@Before
	public void setUp() throws MuleException {
		stopAutomaticPollTriggering();
		getAndInitializeFlows();
	}

	@After
	public void tearDown() throws MuleException, Exception {
		cleanUpSandboxesByRemovingTestContacts();
	}
	
	private void stopAutomaticPollTriggering() throws MuleException {
		stopFlowSchedulers(POLL_A_BATCH_JOB_NAME);
		stopFlowSchedulers(POLL_B_BATCH_JOB_NAME);
	}

	private void getAndInitializeFlows() throws InitialisationException {
		// Flow for creating contacts in sfdc A instance
		createContactInAFlow = getSubFlow("createContactInAFlow");
		createContactInAFlow.initialise();

		// Flow for creating contacts in sfdc B instance
		createContactInBFlow = getSubFlow("createContactInBFlow");
		createContactInBFlow.initialise();

		// Flow for deleting contacts in sfdc A instance
		deleteContactFromAFlow = getSubFlow("deleteContactFromAFlow");
		deleteContactFromAFlow.initialise();

		// Flow for deleting contacts in sfdc B instance
		deleteContactFromBFlow = getSubFlow("deleteContactFromBFlow");
		deleteContactFromBFlow.initialise();
		
		// Flow for querying the contact in source system
		queryContactFromAFlow = getSubFlow("queryContactFromAFlow");
		queryContactFromAFlow.initialise();

		// Flow for querying the contact in target system
		queryContactFromBFlow = getSubFlow("queryContactFromBFlow");
		queryContactFromBFlow.initialise();
	}
	
	private static void cleanUpSandboxesByRemovingTestContacts() throws MuleException, Exception {
		final List<String> idList = new ArrayList<String>();
		for (String contact : accountsCreatedInA) {
			idList.add(contact);
		}
		deleteContactFromAFlow.process(getTestEvent(idList, MessageExchangePattern.REQUEST_RESPONSE));
		idList.clear();
		for (String contact : accountsCreatedInB) {
			idList.add(contact);
		}
		deleteContactFromBFlow.process(getTestEvent(idList, MessageExchangePattern.REQUEST_RESPONSE));
	}
	
	@Test
	public void whenUpdatingAContactInInstanceBTheBelongingContactGetsUpdatedInInstanceA() throws MuleException, Exception {
		// Build test contacts
		SfdcObjectBuilder contact = aContact()
				.with("FirstName", "Manuel")
				.with("LastName", "Valadares")
				.with("MailingCountry", "US")
				.with("Email", ANYPOINT_TEMPLATE_NAME + "-" + System.currentTimeMillis() + "portuga@mail.com");

		SfdcObjectBuilder justCreatedContact = contact
				.with("Description", "Please enter your description here");
		SfdcObjectBuilder updatedContact = contact
				.with("Description", "Zeze's adoptive father");

		// Create contacts in sand-boxes and keep track of them for posterior cleaning up
		accountsCreatedInA.add(createTestContactsInSfdcSandbox(justCreatedContact.build(), createContactInAFlow));
		accountsCreatedInB.add(createTestContactsInSfdcSandbox(updatedContact.build(), createContactInBFlow));

		// Execution		
		executeWaitAndAssertBatchJob(POLL_A_BATCH_JOB_NAME);
		executeWaitAndAssertBatchJob(POLL_B_BATCH_JOB_NAME);
		
		// Assertions
		Map<String, String> retrievedContactFromA = (Map<String, String>) queryContact(contact.build(), queryContactFromAFlow);
		Map<String, String> retrievedContactFromB = (Map<String, String>) queryContact(contact.build(), queryContactFromBFlow);
		
		final MapDifference<String, String> mapsDifference = Maps.difference(retrievedContactFromA, retrievedContactFromB);
		Assert.assertTrue("Some contacts are not synchronized between systems. " + mapsDifference.toString(), mapsDifference.areEqual());
		
	}
	
	private Object queryContact(Map<String, Object> contact, InterceptingChainLifecycleWrapper queryContactFlow) throws MuleException, Exception {
		return queryContactFlow.process(getTestEvent(contact, MessageExchangePattern.REQUEST_RESPONSE)).getMessage().getPayload();
	}

	private String createTestContactsInSfdcSandbox(Map<String, Object> contact, InterceptingChainLifecycleWrapper createContactFlow) throws MuleException, Exception {
		List<Map<String, Object>> salesforceContacts = new ArrayList<Map<String, Object>>();
		salesforceContacts.add(contact);
		
		final List<SaveResult> payloadAfterExecution = (List<SaveResult>) createContactFlow.process(
				getTestEvent(salesforceContacts, MessageExchangePattern.REQUEST_RESPONSE)).getMessage().getPayload();
		return payloadAfterExecution.get(0).getId();
	}

	private void executeWaitAndAssertBatchJob(String flowConstructName) throws Exception {
		// Execute synchronization
		runSchedulersOnce(flowConstructName);

		// Wait for the batch job executed to finish
		batchTestHelper = new BatchTestHelper(muleContext);
		batchTestHelper.awaitJobTermination(TIMEOUT_MILLIS * 10000, 500);
		batchTestHelper.assertJobWasSuccessful();
	}
	
}
