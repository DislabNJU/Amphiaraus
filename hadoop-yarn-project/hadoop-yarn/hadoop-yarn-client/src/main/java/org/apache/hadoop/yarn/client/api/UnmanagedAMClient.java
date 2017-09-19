package org.apache.hadoop.yarn.client.api;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.client.api.impl.UnmanagedYarnClientImpl;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

import com.sun.org.apache.regexp.internal.recompile;

import java.nio.*;

/**
 * The UnmanagedLauncher is a simple client that launches and unmanaged AM. An
 * unmanagedAM is an AM that is not launched and managed by the RM. The client
 * creates a new application on the RM and negotiates a new attempt id. Then it
 * waits for the RM app state to reach be YarnApplicationState.ACCEPTED after
 * which it spawns the AM in another process and passes it the container id via
 * env variable Environment.CONTAINER_ID. The AM can be in any
 * language. The AM can register with the RM using the attempt id obtained
 * from the container id and proceed as normal.
 * The client redirects app stdout and stderr to its own stdout and
 * stderr and waits for the AM process to exit. Then it waits for the RM to
 * report app completion.
 */
public class UnmanagedAMClient extends AbstractService{
  private static final Log LOG = LogFactory.getLog(UnmanagedAMClient.class);

  private Configuration conf;

  // Handle to talk to the Resource Manager/Applications Manager
  protected UnmanagedYarnClientImpl rmClient;

  // Application master specific info to register a new Application with RM/ASM
  private String appName = "";
  // App master priority
  private int amPriority = 0;
  // Queue for App master
  private String amQueue = "";
  // cmd to start AM
  private String amCmd = null;
  // set the classpath explicitly
  private String classpath = null;

  private volatile boolean amCompleted = false;
  
  private ApplicationId applicationId;
  
  private ApplicationAttemptId applicationAttemptId;
  
  private boolean registed;
  
  private boolean isWait;

  
  YarnApplicationAttemptState appState;
  
  private AMRMTokenIdentifier amrmTokenIdentifier;
  
  private int lastResponseId = 0;
  
  private AllocateResponse lastAllocateResponse = null;
  
  private AllocateRequest request;
  
  protected Resource clusterAvailableResources;
  protected int clusterNodeCount;
  
  protected String appHostName;
  protected int appHostPort;
  protected String appTrackingUrl;
  
  private boolean ready2regist;
  
  private Map<Integer, Long> expResponseTime = new HashMap<Integer, Long>();

  private static final long AM_STATE_WAIT_TIMEOUT_MS = 10000;

  public UnmanagedAMClient() throws Exception {

	  super(UnmanagedAMClient.class.getName());
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
	  LOG.info("UAM initing");
    RackResolver.init(conf);
    this.conf = conf;
    appState = YarnApplicationAttemptState.NEW;
    ready2regist = false;
    registed = false;
    isWait = getConfig().getBoolean("yarn.isWait", false);
    appName =  "UnmanagedAM";
    amPriority = 0;
    amQueue = "default";
    super.serviceInit(conf);
  }
  
  @Override
  protected void serviceStart() throws Exception {
	  
	  YarnConfiguration yarnConf = new YarnConfiguration(conf);
	  LOG.info("creating YarnClient");
	  rmClient = new UnmanagedYarnClientImpl();
	  rmClient.init(yarnConf);
	  LOG.info("init over");
	  super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
	  LOG.info("UAM stoping");
      rmClient.stop();
    super.serviceStop();
  }

  public void startUAM(){
      rmClient.start();
      try {  
        // Create launch context for app master
        LOG.info("Setting up application submission context for ASM");
        ApplicationSubmissionContext appContext = rmClient.createApplication()
            .getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        applicationId = appId;
        // set the application name
        appContext.setApplicationName(appHostName);
    
        // Set the priority for the application master
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(amPriority);
        appContext.setPriority(pri);
    
        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(amQueue);
    
        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records
            .newRecord(ContainerLaunchContext.class);
        appContext.setAMContainerSpec(amContainer);
    
        // unmanaged AM
        appContext.setUnmanagedAM(true);
        LOG.info("Setting unmanaged AM");
    
        // Submit the application to the applications manager
        LOG.info("Submitting application to ASM");
        rmClient.submitApplication(appContext);

        ApplicationReport appReport =
            monitorApplication(appId, EnumSet.of(YarnApplicationState.ACCEPTED,
              YarnApplicationState.KILLED, YarnApplicationState.FAILED,
              YarnApplicationState.FINISHED));

        if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
          // Monitor the application attempt to wait for launch state
          ApplicationAttemptReport attemptReport =
              monitorCurrentAppAttempt(appId, YarnApplicationAttemptState.LAUNCHED);
          LOG.info("attemptState of UAM: " + attemptReport.getYarnApplicationAttemptState().toString());
          ApplicationAttemptId attemptId =
              attemptReport.getApplicationAttemptId();
    	            
		    LOG.info("updata AMRMToken first time");
		    updateAMRMToken(appReport.getAMRMToken());
		    
          LOG.info("registUAM with application attempt id " + attemptId);

          registUAM(attemptId);
          
        }


      } catch (YarnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			LOG.info("launch over");
      }
  }
  
  public void registUAM(ApplicationAttemptId attemptId)
		  throws IOException, YarnException{
	  while(!ready2regist){
		  try {
			  LOG.info("waiting to registUAM");
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	  }
	  Credentials credentials = new Credentials();
	  Token<AMRMTokenIdentifier> token = 
			  rmClient.getAMRMToken(attemptId.getApplicationId());
	  amrmTokenIdentifier = token.decodeIdentifier();
	  applicationAttemptId = attemptId;
	  LOG.info("get Token success: " + amrmTokenIdentifier.toString() + 
			  " for ApplicationAttempt: " + applicationAttemptId);
	  rmClient.registerApplicationMaster(appHostName, appHostPort, appTrackingUrl);
	  registed = true;
	  // Service will be empty but that's okay, we are just passing down only
	  // AMRMToken down to the real AM which eventually sets the correct
	  // service-address.
	  credentials.addToken(token.getService(), token);
	  File tokenFile = File.createTempFile("unmanagedAMRMToken","", 
			  new File(System.getProperty("user.dir")));
	  try {
		  FileUtil.chmod(tokenFile.getAbsolutePath(), "600");
	  } catch (InterruptedException ex) {
		  throw new RuntimeException(ex);
	  }
	  tokenFile.deleteOnExit();
	  DataOutputStream os = new DataOutputStream(new FileOutputStream(tokenFile, 
			  true));
	  credentials.writeTokenStorageToStream(os);
	  os.close();
  }

  private ApplicationAttemptReport monitorCurrentAppAttempt(
      ApplicationId appId, YarnApplicationAttemptState attemptState)
      throws YarnException, IOException {
    long startTime = System.currentTimeMillis();
    ApplicationAttemptId attemptId = null;
    while (true) {
      if (attemptId == null) {
        attemptId =
            rmClient.getApplicationReport(appId)
              .getCurrentApplicationAttemptId();
      }
      ApplicationAttemptReport attemptReport = null;
      if (attemptId != null) {
        attemptReport = rmClient.getApplicationAttemptReport(attemptId);
        LOG.info("CurrentAppAttemptState: " + attemptReport.toString());
        if (attemptState.equals(attemptReport.getYarnApplicationAttemptState())) {
          return attemptReport;
        }
      }
      LOG.info("Current attempt state of " + appId + " is " + (attemptReport == null
            ? " N/A " : attemptReport.getYarnApplicationAttemptState())
                + ", waiting for current attempt to reach " + attemptState);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for current attempt of " + appId
            + " to reach " + attemptState);
      }
      if (System.currentTimeMillis() - startTime > AM_STATE_WAIT_TIMEOUT_MS) {
        String errmsg =
            "Timeout for waiting current attempt of " + appId + " to reach "
                + attemptState;
        LOG.error(errmsg);
        throw new RuntimeException(errmsg);
      }
    }
  }

  /**
   * Monitor the submitted application for completion. Kill application if time
   * expires.
   * 
   * @param appId
   *          Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnException
   * @throws IOException
   */
  private ApplicationReport monitorApplication(ApplicationId appId,
      Set<YarnApplicationState> finalState) throws YarnException,
      IOException {

    long foundAMCompletedTime = 0;
    StringBuilder expectedFinalState = new StringBuilder();
    boolean first = true;
    for (YarnApplicationState state : finalState) {
      if (first) {
        first = false;
        expectedFinalState.append(state.name());
      } else {
        expectedFinalState.append("," + state.name());
      }
    }

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      ApplicationReport report = rmClient.getApplicationReport(appId);
      
      LOG.info("Got application report from ASM for" + ", appId="
          + appId.getId() + ", appAttemptId="
          + report.getCurrentApplicationAttemptId() + ", clientToAMToken="
          + report.getClientToAMToken() + ", appDiagnostics="
          + report.getDiagnostics() + ", appMasterHost=" + report.getHost()
          + ", appQueue=" + report.getQueue() + ", appMasterRpcPort="
          + report.getRpcPort() + ", appStartTime=" + report.getStartTime()
          + ", yarnAppState=" + report.getYarnApplicationState().toString()
          + ", distributedFinalState="
          + report.getFinalApplicationStatus().toString() + ", appTrackingUrl="
          + report.getTrackingUrl() + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      if (finalState.contains(state)) {
        return report;
      }
    }
  }
  
  public ApplicationAttemptId getApplicationAttemptId(){
	  return this.applicationAttemptId;
  }
  
  private class AllocateThread extends Thread{
	  AllocateRequest alloRequest;
	    public AllocateThread(AllocateRequest newRequest) {
		      super("allocateThread");
	    	this.alloRequest = newRequest;
		    }
	    public void run() {

		try {
			synchronized(this){
				if(this.alloRequest.getResponseId() + 1 <= lastResponseId){
					LOG.info("request stale");
					return;
				}
				
				long start = System.currentTimeMillis();
				AllocateResponse allocateResponse = rmClient.allocate(this.alloRequest);
				long end = System.currentTimeMillis();
				if(allocateResponse != null){
					expResponseTime.put(allocateResponse.getResponseId(), end -start);
				}
				
				int lastRId = lastResponseId;
				if(lastAllocateResponse != null){
					lastRId = lastAllocateResponse.getResponseId();
				}
				if(allocateResponse != null && lastRId < allocateResponse.getResponseId()){
					lastAllocateResponse = allocateResponse;
					clusterNodeCount = allocateResponse.getNumClusterNodes();
					lastResponseId = allocateResponse.getResponseId();
					clusterAvailableResources = allocateResponse.getAvailableResources();

					LOG.info("getRemoteResponse: " + lastResponseId);
					if (allocateResponse.getAMRMToken() != null) {
					    LOG.info("updata AMRMToken");
						updateAMRMToken(allocateResponse.getAMRMToken());
					}
				}
		  	}
		} catch (YarnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	    }
	    
  }
  
  public AllocateResponse allocate(AllocateRequest request)  throws YarnException, IOException{
	  LOG.info("allocate in UnmanagedAMClient");
	  
	  request.setResponseId(lastResponseId);
	  
	  while(!registed){
		  LOG.info("UAM not launched yet");
		  try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	  }
	  
	  if(!isWait){
		  AllocateThread allocateThread = new AllocateThread(request);
		  allocateThread.start();
		  return lastAllocateResponse;
	  }else {
		  synchronized(this){
			  LOG.info("waiting");
			  AllocateResponse allocateResponse = rmClient.allocate(request);
			  if(allocateResponse != null && lastResponseId < allocateResponse.getResponseId()){
				  LOG.info("not null response");
				  clusterNodeCount = allocateResponse.getNumClusterNodes();
				  lastResponseId = allocateResponse.getResponseId();
				  clusterAvailableResources = allocateResponse.getAvailableResources();  
			  }
			  return allocateResponse;
		  }
	  }

  }
  
  public void killApplication()
	      throws YarnException, IOException {
	  rmClient.killApplication(applicationId);
  }
  
  public void setHostinfo(String host, int port, String trackingUrl){
	  this.appHostName = host;
	  this. appHostPort = port;
	  this.appTrackingUrl = trackingUrl;
	  this.ready2regist = true;
	  LOG.info("local:     host: " + appHostName + " port "+ appHostPort +  " url " + appTrackingUrl);
  }
  
  private void updateAMRMToken(org.apache.hadoop.yarn.api.records.Token token) throws IOException {
	    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken =
	        new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>(token
	          .getIdentifier().array(), token.getPassword().array(), new Text(
	          token.getKind()), new Text(token.getService()));

	    // Preserve the token service sent by the RM when adding the token
	    // to ensure we replace the previous token setup by the RM.
	    // Afterwards we can update the service address for the RPC layer.
	    UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
	    currentUGI.addToken(amrmToken);
	    amrmToken.setService(ClientRMProxy.getAMRMTokenServiceRemote(getConfig()));
	  }
  
  public void finishUAM() throws YarnException, IOException{
	  rmClient.finishApplicationMaster();
  }
  
  public void writeExpData() throws IOException{
	  LOG.info("save expDataUAM");
     /*  
      String filePath = "/home/zxd/expdata/allocateResponseTimeRemote.csv";
      File file = new File(filePath);
      FileOutputStream out = new FileOutputStream(file);
      OutputStreamWriter osw = new OutputStreamWriter(out, "UTF8");

      BufferedWriter bw = new BufferedWriter(osw);
      bw.append("responseId,responseTime(ms)\n");
 	  for(Map.Entry<Integer, Long> entry : this.expResponseTime.entrySet()){
 		  bw.append(String.valueOf(entry.getKey()) + "," + String.valueOf(entry.getValue()) + "\n");
 	  }

     bw.close();
     osw.close();
     out.close(); 
      */
	  LOG.info("appid: " + applicationId.toString());
	  LOG.info("atpid: " + applicationAttemptId.toString() );
     String fileName = "/home/zxd/expdata/" + applicationId.toString() + "/allocateResponseTimeRemote.csv";
     FileWriter writer = new FileWriter(fileName, true);
     writer.write("responseId,responseTime(ms)\n");
     for(Map.Entry<Integer, Long> entry : this.expResponseTime.entrySet()){
    	 writer.write(String.valueOf(entry.getKey()) + "," + String.valueOf(entry.getValue()) + "\n");
     }
     writer.close();

     
  }
  
}
