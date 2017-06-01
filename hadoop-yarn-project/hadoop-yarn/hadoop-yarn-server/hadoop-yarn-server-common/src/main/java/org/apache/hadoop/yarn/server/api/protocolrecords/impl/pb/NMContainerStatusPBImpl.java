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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.records.ContainerDetails;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerDetailsProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NMContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NMContainerStatusProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;

import com.google.protobuf.TextFormat;

public class NMContainerStatusPBImpl extends NMContainerStatus {

  NMContainerStatusProto proto = NMContainerStatusProto
    .getDefaultInstance();
  NMContainerStatusProto.Builder builder = null;
  boolean viaProto = false;

  private ContainerId containerId = null;
  private Resource resource = null;
  private Priority priority = null;
  private ContainerDetails containerDetails = null;

  public NMContainerStatusPBImpl() {
    builder = NMContainerStatusProto.newBuilder();
  }

  public NMContainerStatusPBImpl(NMContainerStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public NMContainerStatusProto getProto() {

    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return this.getProto().hashCode();
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
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public Resource getAllocatedResource() {
    if (this.resource != null) {
      return this.resource;
    }
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public ContainerId getContainerId() {
    if (this.containerId != null) {
      return this.containerId;
    }
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId = convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public String getDiagnostics() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnostics()) {
      return null;
    }
    return (p.getDiagnostics());
  }

  @Override
  public ContainerState getContainerState() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerState()) {
      return null;
    }
    return convertFromProtoFormat(p.getContainerState());
  }

  //added by lxb;
  @Override
  public ContainerDetails getContainerDetails(){
	  NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
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
  public void setContainerDetails(ContainerDetails containerDetails){
	  maybeInitBuilder();
	  if(containerDetails == null)
		  builder.clearContainerDetails();
	  this.containerDetails = containerDetails;
	  builder.setContainerDetails(convertToProtoFormat(containerDetails));
  }
  
  @Override
  public void setAllocatedResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null)
      builder.clearResource();
    this.resource = resource;
  }

  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null)
      builder.clearContainerId();
    this.containerId = containerId;
  }

  @Override
  public void setDiagnostics(String diagnosticsInfo) {
    maybeInitBuilder();
    if (diagnosticsInfo == null) {
      builder.clearDiagnostics();
      return;
    }
    builder.setDiagnostics(diagnosticsInfo);
  }

  @Override
  public void setContainerState(ContainerState containerState) {
    maybeInitBuilder();
    if (containerState == null) {
      builder.clearContainerState();
      return;
    }
    builder.setContainerState(convertToProtoFormat(containerState));
  }

  @Override
  public int getContainerExitStatus() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getContainerExitStatus();
  }

  @Override
  public void setContainerExitStatus(int containerExitStatus) {
    maybeInitBuilder();
    builder.setContainerExitStatus(containerExitStatus);
  }

  @Override
  public Priority getPriority() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.priority != null) {
      return this.priority;
    }
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }

  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null) 
      builder.clearPriority();
    this.priority = priority;
  }

  @Override
  public long getCreationTime() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getCreationTime();
  }

  @Override
  public void setCreationTime(long creationTime) {
    maybeInitBuilder();
    builder.setCreationTime(creationTime);
  }

  private void mergeLocalToBuilder() {
    if (this.containerId != null
        && !((ContainerIdPBImpl) containerId).getProto().equals(
          builder.getContainerId())) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }

    if (this.resource != null
        && !((ResourcePBImpl) this.resource).getProto().equals(
          builder.getResource())) {
      builder.setResource(convertToProtoFormat(this.resource));
    }

    if (this.priority != null) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NMContainerStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ((ResourcePBImpl) t).getProto();
  }

  private ContainerStateProto
      convertToProtoFormat(ContainerState containerState) {
    return ProtoUtils.convertToProtoFormat(containerState);
  }

  private ContainerState convertFromProtoFormat(
      ContainerStateProto containerState) {
    return ProtoUtils.convertFromProtoFormat(containerState);
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl)t).getProto();
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
