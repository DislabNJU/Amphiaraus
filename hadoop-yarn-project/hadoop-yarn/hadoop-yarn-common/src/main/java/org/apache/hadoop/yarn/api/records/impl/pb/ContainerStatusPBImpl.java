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

package org.apache.hadoop.yarn.api.records.impl.pb;


import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerDetails;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerDetailsProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerDetailsProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProtoOrBuilder;

import sun.net.www.content.text.plain;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class ContainerStatusPBImpl extends ContainerStatus {
  ContainerStatusProto proto = ContainerStatusProto.getDefaultInstance();
  ContainerStatusProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerId containerId = null;
  
  private ContainerDetails containerDetails = null;  //added by lxb;
  
  
  public ContainerStatusPBImpl() {
    builder = ContainerStatusProto.newBuilder();
  }

  public ContainerStatusPBImpl(ContainerStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized ContainerStatusProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" ContainerStatus: [");
    sb.append("ContainerId: ").append(getContainerId()).append(", ");
    sb.append("State: ").append(getState()).append(", ");
    
    sb.append("Details:(").append("Pmem:").append(getDetails().Pmem)
    .append("  Vmem:").append(getDetails().Vmem)
    .append(" MemUtilization").append(getDetails().MemUtilization)
    .append("  CpuTime:").append(getDetails().CpuTime)
    .append(" CpuUtilization").append(getDetails().CpuUtilization)
    .append("), ");
    
    sb.append("Diagnostics: ").append(getDiagnostics()).append(", ");
    sb.append("ExitStatus: ").append(getExitStatus()).append(", ");
    sb.append("]");
    return sb.toString();
  }

  private void mergeLocalToBuilder() {
    if (containerId != null) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ContainerStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public synchronized ContainerState getState() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public synchronized void setState(ContainerState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearState();
      return;
    }
    builder.setState(convertToProtoFormat(state));
  }
  @Override
  public synchronized ContainerId getContainerId() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId =  convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public synchronized void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) 
      builder.clearContainerId();
    this.containerId = containerId;
  }
  
  //added by lxb;
  @Override
  public synchronized ContainerDetails getDetails() {
	  ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
	  if(this.containerDetails != null){
		  return this.containerDetails;
	  }
	  if(!p.hasContainerDetails()){
		  return null;
	  }
	  this.containerDetails = convertFromProtoFormat(p.getContainerDetails());
	  return containerDetails;
  }

  @Override
  public synchronized void setDetails(ContainerDetails details) {
	  maybeInitBuilder();
	  if(details == null)
		  builder.clearContainerDetails();
	  this.containerDetails = details;
	  builder.setContainerDetails(convertToProtoFormat(details));
  }
  //added by lxb;
  
  @Override
  public synchronized int getExitStatus() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getExitStatus();
  }

  @Override
  public synchronized void setExitStatus(int exitStatus) {
    maybeInitBuilder();
    builder.setExitStatus(exitStatus);
  }

  @Override
  public synchronized String getDiagnostics() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getDiagnostics());    
  }

  @Override
  public synchronized void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    builder.setDiagnostics(diagnostics);
  }

  private ContainerStateProto convertToProtoFormat(ContainerState e) {
    return ProtoUtils.convertToProtoFormat(e);
  }

  private ContainerState convertFromProtoFormat(ContainerStateProto e) {
    return ProtoUtils.convertFromProtoFormat(e);
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }
  //added by lxb;
  private ContainerDetails convertFromProtoFormat(ContainerDetailsProto p){
	  ContainerDetails containerDetails =new ContainerDetails();
	  if(p.hasPmem())
		  containerDetails.Pmem = p.getPmem();
	  else
		  containerDetails.Pmem = -1;
	  if(p.hasVmem())
		  containerDetails.Vmem = p.getVmem();
	  else
		  containerDetails.Vmem = -1;
	  if(p.hasCpuTime())
		  containerDetails.CpuTime = p.getCpuTime();
	  else
		  containerDetails.CpuTime = -1;
	  if(p.hasMemUtilization())
		  containerDetails.MemUtilization = p.getMemUtilization();
	  else 
		  containerDetails.MemUtilization = -1;
	  if(p.hasCpuUtilization())
		  containerDetails.CpuUtilization = p.getCpuUtilization();
	  else 
		  containerDetails.CpuUtilization = -1;
	  
	  return containerDetails;
  }
  
  private ContainerDetailsProto convertToProtoFormat(ContainerDetails d){
	  return ContainerDetailsProto.newBuilder().setPmem(d.Pmem).setVmem(d.Vmem).setCpuTime(d.CpuTime).setMemUtilization(d.MemUtilization).setCpuUtilization(d.CpuUtilization).build();
  }



}  
