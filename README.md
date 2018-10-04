
# Template: Salesforce Org to Org Account Bi-Directional Sync

Bi-directionally synchronizes account data between two Salesforce Orgs. This template makes it fast to configure the fields to be synchronized, how they map, and criteria on when to trigger the synchronization. 

This template can trigger either using the Mule polling mechanism or can be easily modified to work with Salesforce outbound messaging to better utilize Salesforce API calls. 

This template leverages watermarking functionality to ensure that only the most recent items are synchronized and batch to effectively process many records at a time.

![336f2d34-134c-4f0e-a99a-3dbda786faf1-image.png](https://exchange2-file-upload-service-kprod.s3.us-east-1.amazonaws.com:443/336f2d34-134c-4f0e-a99a-3dbda786faf1-image.png)

**Note:** Any references in the video to DataMapper have been updated in the template with DataWeave transformations.

[//]: # (![]\(https://www.youtube.com/embed/m77tpYnQrf4?wmode=transparent\))
[![YouTube Video](http://img.youtube.com/vi/m77tpYnQrf4/0.jpg)](https://www.youtube.com/watch?v=m77tpYnQrf4)

# License Agreement

This template is subject to the conditions of the [MuleSoft License Agreement](https://s3.amazonaws.com/templates-examples/AnypointTemplateLicense.pdf).

Review the terms of the license before downloading and using this template. You can use this template for free with the Mule Enterprise Edition, CloudHub, or as a trial in Anypoint Studio.

# Use Case

This template provdes an online bidirectional sync of accounts between two Salesforce instances with ability to specify filtering criteria.

The main behavior of this template is fetching data for changes (new or modified accounts) using the scheduler or the HTTP component that have occurred in any of the Salesforce instances during a certain defined period of time. For those accounts that both have not been updated yet, the integration triggers an upsert (update or create depending the case) taking the last modification as the one that should be applied.

# Considerations

To make this template run, there are certain preconditions that must be considered. All of them deal with the preparations in both, that must be made for all to run smoothly.

Failing to do so can lead to unexpected behavior of the template.

## Salesforce Considerations

Here's what you need to know about Salesforce to get this template to work.

### FAQ

- Where can I check that the field configuration for my Salesforce instance is the right one? See: [Salesforce: Checking Field Accessibility for a Particular Field](https://help.salesforce.com/HTViewHelpDoc?id=checking_field_accessibility_for_a_particular_field.htm&language=en_US "Salesforce: Checking Field Accessibility for a Particular Field")
- Can I modify the Field Access Settings? How? See: [Salesforce: Modifying Field Access Settings](https://help.salesforce.com/HTViewHelpDoc?id=modifying_field_access_settings.htm&language=en_US "Salesforce: Modifying Field Access Settings")

### As a Data Source

If the user who configured the template for the source system does not have at least _read only_ permissions for the fields that are fetched, then an _InvalidFieldFault_ API fault displays.

```
java.lang.RuntimeException: [InvalidFieldFault [ApiQueryFault 
[ApiFault  exceptionCode='INVALID_FIELD'
exceptionMessage='
Account.Phone, Account.Rating, Account.RecordTypeId, Account.ShippingCity
^
ERROR at Row:1:Column:486
No such column 'RecordTypeId' on entity 'Account'. If you are attempting 
to use a custom field, be sure to append the '__c' after the custom 
field name. Reference your WSDL or the describe call for the appropriate names.'
]
row='1'
column='486'
]
]
```

### As a Data Destination

There are no considerations with using Salesforce as a data destination.

# Run it!

Simple steps to get Salesforce to Salesforce Account Bidirectional Synchronization running.

To have your application up and running, you just need to complete two simple steps:

1. Configure the application properties.
2. Run it on premises or in CloudHub.

## Running On Premises

In this section we help you run your template on your computer.

### Where to Download Anypoint Studio and the Mule Runtime

If you are a newcomer to Mule, here is where to get the tools.

- [Download Anypoint Studio](https://www.mulesoft.com/platform/studio)
- [Download Mule runtime](https://www.mulesoft.com/lp/dl/mule-esb-enterprise)

### Importing a Template into Studio

In Studio, click the Exchange X icon in the upper left of the taskbar, log in with your

Anypoint Platform credentials, search for the template, and click **Open**.

### Running on Studio

After you import your template into Anypoint Studio, follow these steps to run it:

- Locate the properties file `mule.dev.properties`, in src/main/resources.
- Complete all the properties required as per the examples in the "Properties to Configure" section.
- Right click the template project folder.
- Hover your mouse over `Run as`
- Click `Mule Application (configure)`
- Inside the dialog, select Environment and set the variable `mule.env` to the value `dev`
- Click `Run`

### Running on Mule Standalone

Fill in all properties in one of the property files, for example in mule.dev.properties, and run your app with the corresponding environment variable to use it. To follow the example, this is `mule.env=dev`. 

## Running on CloudHub

While creating your application on CloudHub (or you can do it later as a next step), go to Runtime Manager > Manage Application > Properties to set the environment variables listed in "Properties to Configure" as well as the **mule.env**.

To create your application on CloudHub, go to Deployment > Advanced to set all environment variables detailed in "Properties to Configure" as well as the **mule.env**.

### Deploying your Anypoint Template on CloudHub

Studio provides an easy way to deploy your template directly to CloudHub, for the specific steps to do so check this

## Properties to Configure

To use this template, configure properties (credentials, configurations, etc.) in the properties file or in CloudHub from Runtime Manager > Manage Application > Properties. The sections that follow list example values.

### Application Configuration

- http.port `9090`
- scheduler.frequency `10000`  This is the milliseconds that runs between two different checks for updates in either Salesforce instance.
- scheduler.startDelay `0`
- watermark.default.expression `2018-02-25T11:00:00.000Z`  This property is an important one, as it configures what should be the start point of the synchronization. If the use case includes synchronization of every account created from the beginning of time, you should use a date previous to any account creation (perhaps `1900-01-01T08:00:00.000Z` is a good choice). If you want to synchronize the accounts created from now on, you should use a default value according to that requirement (for example, if today is April 21, 2018 and eleven o'clock in London, then you could use the following value `2018-04-21T11:00:00.000Z`).
- page.size `1000`
- trigger.policy `push` | `poll `**Note:** The property **trigger.policy** can take any of the three following values:
- **empty\_value**: If the property has no value assigned to it, the application does nothing to the account.
- **poll**: The Scheduler trigger flow to use.
- **push**: The Push Notification trigger flow to use.

#### SalesForce Connector Configuration for Company A

- sfdc.a.username `aunt.eater@example.com`
- sfdc.a.password `G0ttaF1ndTh3m`
- sfdc.a.securityToken `avsfwCUl7apQs56Xq2AKi3X`
- sfdc.a.integration.user.id `A0ed000BO9T`  **Note:** To find  the correct _sfdc.a.integration.user.id_ value, refer to the example project "Salesforce Data Retrieval" in Anypoint Exchange.

#### SalesForce Connector Configuration for Company B

- sfdc.b.username `polly.hedra@example.com`
- sfdc.b.password `WootWoot99^^`
- sfdc.b.securityToken `ces56arl7apQs56XTddf34X`
- sfdc.b.integration.user.id `B0ed000BO9T`  **Note:** To find the correct _sfdc.b.integration.user.id_ value, refer to the example project "Salesforce Data Retrieval" in Anypoint Exchange.

# API Calls

Not relevant for this use case.

# Customize It!

This brief guide intends to give a high level idea of how this template is built and how you can change it according to your needs.

As Mule applications are based on XML files, this page describes the XML files used with this template.

More files are available such as test classes and Mule application files, but to keep it simple, we focus on these XML files:

- config.xml
- businessLogic.xml
- endpoints.xml
- errorHandling.xml

## config.xml

Configuration for connectors and configuration properties are set in this file. Even change the configuration here, all parameters that can be modified are in properties file, which is the recommended place to make your changes. However if you want to do core changes to the logic, you need to modify this file.

In the Studio visual editor, the properties are on the _Global Element_ tab.

## businessLogic.xml

This file holds the functional aspect of the template. Its main component is a batch job, and it includes steps for both executing the synchronization from Salesforce "A" to Salesforce "B", and the other way around.

## endpoints.xml

This file contains each inbound and outbound endpoint for your integration app.

In this template, this file contains a scheduler endpoint that queries Salesforce "A" and Salesforce "B" for updates using the watermark and HTTP endpoint for push operation.

## errorHandling.xml

This file is for how to handle your integration, and reacts depending on the different exceptions.

This file provides error handling referenced by the scheduler and a push flow in the endpoints XML file.
