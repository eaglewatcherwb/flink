/*
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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deployment descriptor for a single input gate instance.
 *
 * <p>Each input gate consumes partitions of a single intermediate result. The consumed
 * subpartition index is the same for each consumed partition.
 *
 * @see SingleInputGate
 */
public class InputGateDeploymentDescriptor implements Serializable {

	private static final long serialVersionUID = -7143441863165366704L;
	/**
	 * The ID of the consumed intermediate result. Each input gate consumes partitions of the
	 * intermediate result specified by this ID. This ID also identifies the input gate at the
	 * consuming task.
	 */
	private final IntermediateDataSetID consumedResultId;

	/** The type of the partition the input gate is going to consume. */
	private final ResultPartitionType consumedPartitionType;

	/**
	 * The index of the consumed subpartition of each consumed partition. This index depends on the
	 * {@link DistributionPattern} and the subtask indices of the producing and consuming task.
	 */
	private final int[] consumedSubpartitionIndex;

	/** An input channel for each consumed subpartition. */
	private final InputChannelDeploymentDescriptor[] inputChannels;

	/**
	 * Input channel start index for each consumed subpartition index,
	 * for {@link DistributionPattern#ALL_TO_ALL} is always -1, i.e., all the {@link inputChannels}
	 * for {@link DistributionPattern#POINTWISE}, the channels for consumedSubpartitionIndex[i]
	 * is inputChannels[inputChannelIndex[i]..inputChannelIndex[i+1]].
	 */
	private final int[] inputChannelIndex;

	public InputGateDeploymentDescriptor(
		IntermediateDataSetID consumedResultId,
		ResultPartitionType consumedPartitionType,
		int consumedSubpartitionIndex,
		InputChannelDeploymentDescriptor[] inputChannels) {

		this(consumedResultId, consumedPartitionType, new int[]{consumedSubpartitionIndex}, inputChannels, new int[]{-1});
	}

	public InputGateDeploymentDescriptor(
			IntermediateDataSetID consumedResultId,
			ResultPartitionType consumedPartitionType,
			int[] consumedSubpartitionIndex,
			InputChannelDeploymentDescriptor[] inputChannels,
			int[] inputChannelIndex) {

		this.consumedResultId = checkNotNull(consumedResultId);
		this.consumedPartitionType = checkNotNull(consumedPartitionType);

		this.consumedSubpartitionIndex = checkNotNull(consumedSubpartitionIndex);

		this.inputChannels = checkNotNull(inputChannels);

		this.inputChannelIndex = checkNotNull(inputChannelIndex);

		Preconditions.checkState(consumedSubpartitionIndex.length == inputChannelIndex.length);
	}

	public IntermediateDataSetID getConsumedResultId() {
		return consumedResultId;
	}

	/**
	 * Returns the type of this input channel's consumed result partition.
	 *
	 * @return consumed result partition type
	 */
	public ResultPartitionType getConsumedPartitionType() {
		return consumedPartitionType;
	}

	public int[] getConsumedSubpartitionIndex() {
		return consumedSubpartitionIndex;
	}

	public int[] getInputChannelIndex() {
		return inputChannelIndex;
	}

	public InputChannelDeploymentDescriptor[] getInputChannelDeploymentDescriptors() {
		return inputChannels;
	}

	@Override
	public String toString() {
		return String.format("InputGateDeploymentDescriptor [result id: %s, " +
						"consumed subpartition index: %s, input channels: %s, channel index: %s]",
				consumedResultId.toString(), Arrays.toString(consumedSubpartitionIndex),
				Arrays.toString(inputChannels), Arrays.toString(inputChannelIndex));
	}
}
