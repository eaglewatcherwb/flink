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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.types.Either;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An {@code ExecutionJobVertex} is part of the {@link ExecutionGraph}, and the peer
 * to the {@link JobVertex}.
 *
 * <p>The {@code ExecutionJobVertex} corresponds to a parallelized operation. It
 * contains an {@link ExecutionVertex} for each parallel instance of that operation.
 */
public class ExecutionJobVertex implements AccessExecutionJobVertex, Archiveable<ArchivedExecutionJobVertex> {

	/** Use the same log for all ExecutionGraph classes. */
	private static final Logger LOG = ExecutionGraph.LOG;

	public static final int VALUE_NOT_SET = -1;

	private final Object stateMonitor = new Object();

	private final ExecutionGraph graph;

	private final JobVertex jobVertex;

	/**
	 * The IDs of all operators contained in this execution job vertex.
	 *
	 * <p>The ID's are stored depth-first post-order; for the forking chain below the ID's would be stored as [D, E, B, C, A].
	 *  A - B - D
	 *   \    \
	 *    C    E
	 * This is the same order that operators are stored in the {@code StreamTask}.
	 */
	private final List<OperatorID> operatorIDs;

	/**
	 * The alternative IDs of all operators contained in this execution job vertex.
	 *
	 * <p>The ID's are in the same order as {@link ExecutionJobVertex#operatorIDs}.
	 */
	private final List<OperatorID> userDefinedOperatorIds;

	private ExecutionVertex[] taskVertices;

	private final IntermediateResult[] producedDataSets;

	private final List<IntermediateResult> inputs;

	private int parallelism;

	private final SlotSharingGroup slotSharingGroup;

	private final CoLocationGroup coLocationGroup;

	private final InputSplit[] inputSplits;

	private final boolean maxParallelismConfigured;

	private int maxParallelism;

	private boolean enableAdaptiveParallelism;

	private boolean adaptiveParallelismComputed;

	private Map<Integer, List<Integer>> targetIndexes;
	private Map<Integer, Integer> originIndexToTarget;
	private Map<ExecutionJobVertex, DistributionPattern> outputJobVertex;
	private Map<ExecutionJobVertex, Integer> outputJobVertexIndex;
	private final ExecutionVertex[] originTaskVertices;
	private final int originParallelism;
	private long desiredInputSize;
	private boolean passiveComputeParallelism;

	/**
	 * Either store a serialized task information, which is for all sub tasks the same,
	 * or the permanent blob key of the offloaded task information BLOB containing
	 * the serialized task information.
	 */
	private Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInformationOrBlobKey = null;

	private InputSplitAssigner splitAssigner;

	/**
	 * Convenience constructor for testing.
	 */
	@VisibleForTesting
	ExecutionJobVertex(
		ExecutionGraph graph,
		JobVertex jobVertex,
		int defaultParallelism,
		Time timeout) throws JobException {

		this(graph, jobVertex, defaultParallelism, timeout, 1L, System.currentTimeMillis());
	}

	public ExecutionJobVertex(
			ExecutionGraph graph,
			JobVertex jobVertex,
			int defaultParallelism,
			Time timeout,
			long initialGlobalModVersion,
			long createTimestamp) throws JobException {

		if (graph == null || jobVertex == null) {
			throw new NullPointerException();
		}

		this.graph = graph;
		this.jobVertex = jobVertex;

		int vertexParallelism = jobVertex.getParallelism();
		int numTaskVertices = vertexParallelism > 0 ? vertexParallelism : defaultParallelism;

		final int configuredMaxParallelism = jobVertex.getMaxParallelism();

		this.maxParallelismConfigured = (VALUE_NOT_SET != configuredMaxParallelism);

		// if no max parallelism was configured by the user, we calculate and set a default
		setMaxParallelismInternal(maxParallelismConfigured ?
				configuredMaxParallelism : KeyGroupRangeAssignment.computeDefaultMaxParallelism(numTaskVertices));
		adaptiveParallelismComputed = false;

		// verify that our parallelism is not higher than the maximum parallelism
		if (numTaskVertices > maxParallelism) {
			throw new JobException(
				String.format("Vertex %s's parallelism (%s) is higher than the max parallelism (%s). Please lower the parallelism or increase the max parallelism.",
					jobVertex.getName(),
					numTaskVertices,
					maxParallelism));
		}

		this.parallelism = numTaskVertices;
		this.originParallelism = numTaskVertices;

		this.taskVertices = new ExecutionVertex[numTaskVertices];
		this.originTaskVertices = new ExecutionVertex[numTaskVertices];
		this.operatorIDs = Collections.unmodifiableList(jobVertex.getOperatorIDs());
		this.userDefinedOperatorIds = Collections.unmodifiableList(jobVertex.getUserDefinedOperatorIDs());

		this.inputs = new ArrayList<>(jobVertex.getInputs().size());

		// take the sharing group
		this.slotSharingGroup = jobVertex.getSlotSharingGroup();
		this.coLocationGroup = jobVertex.getCoLocationGroup();

		// setup the coLocation group
		if (coLocationGroup != null && slotSharingGroup == null) {
			throw new JobException("Vertex uses a co-location constraint without using slot sharing");
		}

		// create the intermediate results
		this.producedDataSets = new IntermediateResult[jobVertex.getNumberOfProducedIntermediateDataSets()];

		for (int i = 0; i < jobVertex.getProducedDataSets().size(); i++) {
			final IntermediateDataSet result = jobVertex.getProducedDataSets().get(i);

			this.producedDataSets[i] = new IntermediateResult(
					result.getId(),
					this,
					numTaskVertices,
					result.getResultType());
		}

		Configuration jobConfiguration = graph.getJobConfiguration();
		int maxPriorAttemptsHistoryLength = jobConfiguration != null ?
				jobConfiguration.getInteger(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE) :
				JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE.defaultValue();
		enableAdaptiveParallelism = jobConfiguration != null ?
			jobConfiguration.getBoolean(JobManagerOptions.ENABLE_ADAPTIVE_PARALLELISM) :
			JobManagerOptions.ENABLE_ADAPTIVE_PARALLELISM.defaultValue();
		desiredInputSize = jobConfiguration != null ?
			jobConfiguration.getLong(JobManagerOptions.ADAPTIVE_PARALLELISM_DESIREDINPUTSIZE) :
			JobManagerOptions.ADAPTIVE_PARALLELISM_DESIREDINPUTSIZE.defaultValue();
		passiveComputeParallelism = false;

		outputJobVertex = new HashMap<>();
		outputJobVertexIndex = new HashMap<>();

		// create all task vertices
		for (int i = 0; i < numTaskVertices; i++) {
			ExecutionVertex vertex = new ExecutionVertex(
					this,
					i,
					producedDataSets,
					timeout,
					initialGlobalModVersion,
					createTimestamp,
					maxPriorAttemptsHistoryLength);

			this.taskVertices[i] = vertex;
			this.originTaskVertices[i] = vertex;
		}

		// sanity check for the double referencing between intermediate result partitions and execution vertices
		for (IntermediateResult ir : this.producedDataSets) {
			if (ir.getNumberOfAssignedPartitions() != parallelism) {
				throw new RuntimeException("The intermediate result's partitions were not correctly assigned.");
			}
		}

		// set up the input splits, if the vertex has any
		try {
			@SuppressWarnings("unchecked")
			InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();

			if (splitSource != null) {
				Thread currentThread = Thread.currentThread();
				ClassLoader oldContextClassLoader = currentThread.getContextClassLoader();
				currentThread.setContextClassLoader(graph.getUserClassLoader());
				try {
					inputSplits = splitSource.createInputSplits(numTaskVertices);

					if (inputSplits != null) {
						splitAssigner = splitSource.getInputSplitAssigner(inputSplits);
					}
				} finally {
					currentThread.setContextClassLoader(oldContextClassLoader);
				}
			}
			else {
				inputSplits = null;
			}
		}
		catch (Throwable t) {
			throw new JobException("Creating the input splits caused an error: " + t.getMessage(), t);
		}
	}

	void computeAdaptiveParallelism() {
		if (!enableAdaptiveParallelism || adaptiveParallelismComputed) {
			return;
		}

		LOG.info("Compute adaptive parallelism {}", getName());

		int computedParallelism = computeRouting();
		computeAdaptiveParallelism(computedParallelism, false);
	}

	private boolean hasExecutionDeployed() {
		return Arrays.stream(taskVertices).map(ExecutionVertex::getCurrentExecutionAttempt).anyMatch(Execution::alreadyDeployed);
	}

	private boolean checkAdaptive() {
		if (!enableAdaptiveParallelism || adaptiveParallelismComputed || hasExecutionDeployed()) {
			return false;
		}

		for(Map.Entry<ExecutionJobVertex, DistributionPattern> entry : outputJobVertex.entrySet()) {
			if (DistributionPattern.POINTWISE.equals(entry.getValue())) {
				ExecutionJobVertex consumerVertex = entry.getKey();
				if (!consumerVertex.checkAdaptive()) {
					return false;
				}
			}
		}
		return true;
	}

	private void computeAdaptiveParallelism(int computedParallelism, boolean passiveCompute) {
		graph.assertRunningInJobMasterMainThread();

		if (computedParallelism >= parallelism || !checkAdaptive()) {
			return;
		}
		adaptiveParallelismComputed = true;
		passiveComputeParallelism = passiveCompute;

		for(Map.Entry<ExecutionJobVertex, DistributionPattern> entry : outputJobVertex.entrySet()) {
			if (DistributionPattern.POINTWISE.equals(entry.getValue())) {
				ExecutionJobVertex consumerVertex = entry.getKey();
				for (IntermediateResult ir : consumerVertex.getInputs()) {
					ExecutionJobVertex producerVertex = ir.getProducer();
					if (!producerVertex.equals(this)) {
						if (DistributionPattern.POINTWISE.equals(producerVertex.outputJobVertex.get(consumerVertex))) {
							producerVertex.adaptiveParallelismComputed = true;
						}
					}
				}
			}
		}

		// cancel task vertex
		Set<ExecutionVertex> cancelledVertices = new HashSet<>();
		for (int i = computedParallelism; i < taskVertices.length; i++) {
			taskVertices[i].markAdaptiveCancelled();
			cancelledVertices.add(taskVertices[i]);
		}
		adaptiveCancel(cancelledVertices);
		configureTargetMapping();

		// adjust POINTWISE downstream vertex
		for(Map.Entry<ExecutionJobVertex, DistributionPattern> entry : outputJobVertex.entrySet()) {
			if (DistributionPattern.POINTWISE.equals(entry.getValue())) {
				ExecutionJobVertex consumerVertex = entry.getKey();
				LOG.info("Adjust consumer vertex {}, due to POINTWISE producer vertex {}", consumerVertex.getName(), getName());
				Set<ExecutionEdge> cancelledExecutionEdge = new HashSet<>();
				for (int idx = parallelism; idx < originParallelism; idx++) {
					for (IntermediateResultPartition irp : originTaskVertices[idx].getProducedPartitions().values()) {
						cancelledExecutionEdge.addAll(irp.getConsumers().get(0));
					}
				}
				int consumerCancelledCnt = 0;
				for (ExecutionVertex consumerEV : consumerVertex.originTaskVertices) {
					Set<ExecutionEdge> inputEdges = new HashSet<>(Arrays.asList(consumerEV.getInputEdges(
						outputJobVertexIndex.get(consumerVertex))));
					for (ExecutionEdge ee : cancelledExecutionEdge) {
						inputEdges.remove(ee);
					}
					if (inputEdges.isEmpty()) {
						consumerCancelledCnt++;
					}
				}
				consumerVertex.computeAdaptiveParallelism(consumerVertex.originParallelism - consumerCancelledCnt, true);
			}
		}

		for (ExecutionVertex ev : cancelledVertices) {
			ev.finishAllBlockingPartitions();
		}
	}

	ExecutionVertex[] getOriginTaskVertices() {
		return originTaskVertices;
	}

	private int computeRouting() {
		BigInteger totalInputSize = BigInteger.ZERO;
		BigInteger totalInputRecord = BigInteger.ZERO;
		for (IntermediateResult ir : inputs) {
			for (ExecutionVertex ev : ir.getProducer().getTaskVertices()) {
				IOMetrics ioMetrics = ev.getCurrentExecutionAttempt().getIOMetrics();
				if (ioMetrics != null) {
					totalInputRecord = totalInputRecord.add(BigInteger.valueOf(ioMetrics.getNumRecordsOut()));
					totalInputSize = totalInputSize.add(BigInteger.valueOf(ioMetrics.getNumBytesOut()));
				}
			}
		}
		BigInteger totalSize = totalInputSize.compareTo(BigInteger.ONE) > 0 ? totalInputSize : totalInputRecord;
		int computeParallelism = totalSize.divide(BigInteger.valueOf(desiredInputSize)).intValue();
		computeParallelism = computeParallelism <= 0 ? 1 : computeParallelism;
		computeParallelism = computeParallelism > originParallelism ? originParallelism : computeParallelism;
		LOG.info("Total input size={}, total record={}, computeParallelism={}", totalInputSize.longValue(),
			totalInputRecord.longValue(), computeParallelism);
		return computeParallelism;
	}

	void adaptiveCancel(Set<ExecutionVertex> cancelledVertices) {
		this.parallelism -= cancelledVertices.size();
		taskVertices = new ExecutionVertex[parallelism];
		int count = 0;
		for (ExecutionVertex ev : originTaskVertices) {
			if (!cancelledVertices.contains(ev)) {
				taskVertices[count++] = ev;
			} else {
				LOG.debug("cancel execution vertex:{}", ev.getTaskNameWithSubtaskIndex());
			}
		}
		if (this.parallelism != count) {
			throw new RuntimeException("adaptiveCancel count mismatch,parallelism=" + this.parallelism + ", realCount="+ count);
		}
		graph.decreaseVerticesTotal(cancelledVertices.size());
	}

	private void configureTargetMapping() {
		targetIndexes = new HashMap<>();
		originIndexToTarget = new HashMap<>();
		int basePartitionRange = (int)Math.ceil(1.0 * originParallelism / parallelism);
		int remainderRangeForLastShuffler = originParallelism - basePartitionRange * (parallelism - 1);
		for (int idx = 0; idx < this.parallelism; ++idx) {
			int partitionRange = basePartitionRange;
			if (idx == (parallelism - 1)) {
				partitionRange = ((remainderRangeForLastShuffler > 0)
					? remainderRangeForLastShuffler : basePartitionRange);
			}
			// skip the basePartitionRange per destination task
			List<Integer> indices = createIndices(partitionRange, idx, basePartitionRange);
			targetIndexes.put(idx, indices);
			for (Integer origin : indices) {
				originIndexToTarget.put(origin, idx);
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("targetIdx[%s] to %s", idx, targetIndexes.get(idx)));
			}
		}
	}

	private static List<Integer> createIndices(int partitionRange, int taskIndex, int offSetPerTask) {
		int startIndex = taskIndex * offSetPerTask;
		List<Integer> indices = new ArrayList<>(partitionRange);
		for (int currentIndex = 0; currentIndex < partitionRange; ++currentIndex) {
			indices.add(startIndex + currentIndex);
		}
		return indices;
	}

	int getOriginIndexToTarget(int originIndex) {
		if (originIndexToTarget == null || !originIndexToTarget.containsKey(originIndex)) {
			return originIndex;
		}
		return originIndexToTarget.get(originIndex);
	}

	boolean isPassiveComputeParallelism() {
		return passiveComputeParallelism;
	}

	List<Integer> getTargetIndexes(int taskIndex) {
		if (targetIndexes == null || !targetIndexes.containsKey(taskIndex)) {
			List<Integer> indexes = new ArrayList<>(1);
			indexes.add(taskIndex);
			return indexes;
		}
		return targetIndexes.get(taskIndex);
	}

	/**
	 * Returns a list containing the IDs of all operators contained in this execution job vertex.
	 *
	 * @return list containing the IDs of all contained operators
	 */
	public List<OperatorID> getOperatorIDs() {
		return operatorIDs;
	}

	/**
	 * Returns a list containing the alternative IDs of all operators contained in this execution job vertex.
	 *
	 * @return list containing alternative the IDs of all contained operators
	 */
	public List<OperatorID> getUserDefinedOperatorIDs() {
		return userDefinedOperatorIds;
	}

	public void setMaxParallelism(int maxParallelismDerived) {

		Preconditions.checkState(!maxParallelismConfigured,
				"Attempt to override a configured max parallelism. Configured: " + this.maxParallelism
						+ ", argument: " + maxParallelismDerived);

		setMaxParallelismInternal(maxParallelismDerived);
	}

	private void setMaxParallelismInternal(int maxParallelism) {
		if (maxParallelism == ExecutionConfig.PARALLELISM_AUTO_MAX) {
			maxParallelism = KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM;
		}

		Preconditions.checkArgument(maxParallelism > 0
						&& maxParallelism <= KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM,
				"Overriding max parallelism is not in valid bounds (1..%s), found: %s",
				KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM, maxParallelism);

		this.maxParallelism = maxParallelism;
	}

	public ExecutionGraph getGraph() {
		return graph;
	}

	public JobVertex getJobVertex() {
		return jobVertex;
	}

	@Override
	public String getName() {
		return getJobVertex().getName();
	}

	@Override
	public int getParallelism() {
		return parallelism;
	}

	@Override
	public int getMaxParallelism() {
		return maxParallelism;
	}

	public boolean isMaxParallelismConfigured() {
		return maxParallelismConfigured;
	}

	public JobID getJobId() {
		return graph.getJobID();
	}

	@Override
	public JobVertexID getJobVertexId() {
		return jobVertex.getID();
	}

	@Override
	public ExecutionVertex[] getTaskVertices() {
		return taskVertices;
	}

	public IntermediateResult[] getProducedDataSets() {
		return producedDataSets;
	}

	public InputSplitAssigner getSplitAssigner() {
		return splitAssigner;
	}

	public SlotSharingGroup getSlotSharingGroup() {
		return slotSharingGroup;
	}

	public CoLocationGroup getCoLocationGroup() {
		return coLocationGroup;
	}

	public List<IntermediateResult> getInputs() {
		return inputs;
	}

	public InputDependencyConstraint getInputDependencyConstraint() {
		return getJobVertex().getInputDependencyConstraint();
	}

	public Either<SerializedValue<TaskInformation>, PermanentBlobKey> getTaskInformationOrBlobKey() throws IOException {
		// only one thread should offload the task information, so let's also let only one thread
		// serialize the task information!
		synchronized (stateMonitor) {
			if (taskInformationOrBlobKey == null) {
				final BlobWriter blobWriter = graph.getBlobWriter();

				final TaskInformation taskInformation = new TaskInformation(
					jobVertex.getID(),
					jobVertex.getName(),
					parallelism,
					maxParallelism,
					jobVertex.getInvokableClassName(),
					jobVertex.getConfiguration());

				taskInformationOrBlobKey = BlobWriter.serializeAndTryOffload(
					taskInformation,
					getJobId(),
					blobWriter);
			}

			return taskInformationOrBlobKey;
		}
	}

	@Override
	public ExecutionState getAggregateState() {
		int[] num = new int[ExecutionState.values().length];
		for (ExecutionVertex vertex : this.taskVertices) {
			num[vertex.getExecutionState().ordinal()]++;
		}

		return getAggregateJobVertexState(num, parallelism);
	}

	private String generateDebugString() {

		return "ExecutionJobVertex" +
				"(" + jobVertex.getName() + " | " + jobVertex.getID() + ")" +
				"{" +
				"parallelism=" + parallelism +
				", maxParallelism=" + getMaxParallelism() +
				", maxParallelismConfigured=" + maxParallelismConfigured +
				'}';
	}


	//---------------------------------------------------------------------------------------------

	public void connectToPredecessors(Map<IntermediateDataSetID, IntermediateResult> intermediateDataSets) throws JobException {

		List<JobEdge> inputs = jobVertex.getInputs();

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Connecting ExecutionJobVertex %s (%s) to %d predecessors.", jobVertex.getID(), jobVertex.getName(), inputs.size()));
		}

		if (this.enableAdaptiveParallelism && inputs.isEmpty()) {
			this.enableAdaptiveParallelism = false;
			LOG.debug("Disable adaptive parallelism of ExecutionJobVertex {}, since inputs is empty.", jobVertex.getName());
		}

		for (int num = 0; num < inputs.size(); num++) {
			JobEdge edge = inputs.get(num);

			if (LOG.isDebugEnabled()) {
				if (edge.getSource() == null) {
					LOG.debug(String.format("Connecting input %d of vertex %s (%s) to intermediate result referenced via ID %s.",
							num, jobVertex.getID(), jobVertex.getName(), edge.getSourceId()));
				} else {
					LOG.debug(String.format("Connecting input %d of vertex %s (%s) to intermediate result referenced via predecessor %s (%s).",
							num, jobVertex.getID(), jobVertex.getName(), edge.getSource().getProducer().getID(), edge.getSource().getProducer().getName()));
				}
			}

			// fetch the intermediate result via ID. if it does not exist, then it either has not been created, or the order
			// in which this method is called for the job vertices is not a topological order
			IntermediateResult ires = intermediateDataSets.get(edge.getSourceId());
			if (ires == null) {
				throw new JobException("Cannot connect this job graph to the previous graph. No previous intermediate result found for ID "
						+ edge.getSourceId());
			}

			this.inputs.add(ires);

			// adaptive parallelism only supports ResultPartitionType.BLOCKING
			if (this.enableAdaptiveParallelism && !ResultPartitionType.BLOCKING.equals(ires.getResultType())) {
				this.enableAdaptiveParallelism = false;
				LOG.debug("Disable adaptive parallelism of ExecutionJobVertex {}, since ResultPartitionType is not BLOCKING.", jobVertex.getName());
			}

			int consumerIndex = ires.registerConsumer();

			for (int i = 0; i < parallelism; i++) {
				ExecutionVertex ev = taskVertices[i];
				ev.connectSource(num, ires, edge, consumerIndex);
			}
		}
	}

	void checkPredecessorAdaptiveParallelism() {
		List<JobEdge> edges = jobVertex.getInputs();
		if (this.enableAdaptiveParallelism) {
			// check the source vertex parallelism and DistributionPattern
			boolean hasPointwise = false;
			boolean parallelismDivisive = false;
			int pointwiseSourceParallelism = -1;
			for (int i = 0; i < edges.size(); i++) {
				ExecutionJobVertex sourceJobVertex = graph.getJobVertex(edges.get(i).getSource().getProducer().getID());
				if (sourceJobVertex != null) {
					if (DistributionPattern.POINTWISE.equals(edges.get(i).getDistributionPattern())) {
						hasPointwise = true;

						if (pointwiseSourceParallelism == -1) {
							pointwiseSourceParallelism = sourceJobVertex.originParallelism;
						}
						if (pointwiseSourceParallelism != sourceJobVertex.originParallelism) {
							parallelismDivisive = true;
						}
					}

					sourceJobVertex.outputJobVertex.put(this, edges.get(i).getDistributionPattern());
					sourceJobVertex.outputJobVertexIndex.put(this, i);
				}
			}
			if (hasPointwise && parallelismDivisive) {
				for (JobEdge edge : jobVertex.getInputs()) {
					if (DistributionPattern.POINTWISE.equals(edge.getDistributionPattern())) {
						ExecutionJobVertex sourceJobVertex = graph.getJobVertex(edge.getSource().getProducer().getID());
						if (sourceJobVertex != null && sourceJobVertex.enableAdaptiveParallelism) {
							sourceJobVertex.enableAdaptiveParallelism = false;
							LOG.debug("Disable adaptive parallelism of ExecutionJobVertex {}, since the input edge of Output Vertex {} is POINTWISE divisive.",
								sourceJobVertex.getName(), jobVertex.getName());
						}
					}
				}
				this.enableAdaptiveParallelism = false;
			}
		} else {
			for (int i = 0; i < edges.size(); i++) {
				ExecutionJobVertex sourceJobVertex = graph.getJobVertex(edges.get(i).getSource().getProducer().getID());
				if (sourceJobVertex != null) {
					if (sourceJobVertex.enableAdaptiveParallelism && DistributionPattern.POINTWISE.equals(edges.get(i).getDistributionPattern())) {
						sourceJobVertex.enableAdaptiveParallelism = false;
						LOG.debug("Disable adaptive parallelism of ExecutionJobVertex {}, since Output Vertex {} is disabled and shuffle pattern is POINTWISE.",
							sourceJobVertex.getName(), jobVertex.getName());
					}
					sourceJobVertex.outputJobVertex.put(this, edges.get(i).getDistributionPattern());
					sourceJobVertex.outputJobVertexIndex.put(this, i);
				}
			}
		}
	}

	//---------------------------------------------------------------------------------------------
	//  Actions
	//---------------------------------------------------------------------------------------------

	/**
	 * Schedules all execution vertices of this ExecutionJobVertex.
	 *
	 * @param slotProvider to allocate the slots from
	 * @param queued if the allocations can be queued
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * @param allPreviousExecutionGraphAllocationIds set with all previous allocation ids in the job graph.
	 *                                                 Can be empty if the allocation ids are not required for scheduling.
	 * @return Future which is completed once all {@link Execution} could be deployed
	 */
	public CompletableFuture<Void> scheduleAll(
			SlotProvider slotProvider,
			boolean queued,
			LocationPreferenceConstraint locationPreferenceConstraint,
			@Nonnull Set<AllocationID> allPreviousExecutionGraphAllocationIds) {

		final ExecutionVertex[] vertices = this.taskVertices;

		final ArrayList<CompletableFuture<Void>> scheduleFutures = new ArrayList<>(vertices.length);

		// kick off the tasks
		for (ExecutionVertex ev : vertices) {
			scheduleFutures.add(ev.scheduleForExecution(
				slotProvider,
				queued,
				locationPreferenceConstraint,
				allPreviousExecutionGraphAllocationIds));
		}

		return FutureUtils.waitForAll(scheduleFutures);
	}

	/**
	 * Acquires a slot for all the execution vertices of this ExecutionJobVertex. The method returns
	 * pairs of the slots and execution attempts, to ease correlation between vertices and execution
	 * attempts.
	 *
	 * <p>If this method throws an exception, it makes sure to release all so far requested slots.
	 *
	 * @param resourceProvider The resource provider from whom the slots are requested.
	 * @param queued if the allocation can be queued
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * @param allPreviousExecutionGraphAllocationIds the allocation ids of all previous executions in the execution job graph.
	 * @param allocationTimeout timeout for allocating the individual slots
	 */
	public Collection<CompletableFuture<Execution>> allocateResourcesForAll(
			SlotProvider resourceProvider,
			boolean queued,
			LocationPreferenceConstraint locationPreferenceConstraint,
			@Nonnull Set<AllocationID> allPreviousExecutionGraphAllocationIds,
			Time allocationTimeout) {
		final ExecutionVertex[] vertices = this.taskVertices;

		@SuppressWarnings("unchecked")
		final CompletableFuture<Execution>[] slots = new CompletableFuture[vertices.length];

		// try to acquire a slot future for each execution.
		// we store the execution with the future just to be on the safe side
		for (int i = 0; i < vertices.length; i++) {
			// allocate the next slot (future)
			final Execution exec = vertices[i].getCurrentExecutionAttempt();
			final CompletableFuture<Execution> allocationFuture = exec.allocateAndAssignSlotForExecution(
				resourceProvider,
				queued,
				locationPreferenceConstraint,
				allPreviousExecutionGraphAllocationIds,
				allocationTimeout);
			slots[i] = allocationFuture;
		}

		// all good, we acquired all slots
		return Arrays.asList(slots);
	}

	/**
	 * Cancels all currently running vertex executions.
	 */
	public void cancel() {
		for (ExecutionVertex ev : getTaskVertices()) {
			ev.cancel();
		}
	}

	/**
	 * Cancels all currently running vertex executions.
	 *
	 * @return A future that is complete once all tasks have canceled.
	 */
	public CompletableFuture<Void> cancelWithFuture() {
		return FutureUtils.waitForAll(mapExecutionVertices(ExecutionVertex::cancel));
	}

	public CompletableFuture<Void> suspend() {
		return FutureUtils.waitForAll(mapExecutionVertices(ExecutionVertex::suspend));
	}

	@Nonnull
	private Collection<CompletableFuture<?>> mapExecutionVertices(final Function<ExecutionVertex, CompletableFuture<?>> mapFunction) {
		return Arrays.stream(getTaskVertices())
			.map(mapFunction)
			.collect(Collectors.toList());
	}

	public void fail(Throwable t) {
		for (ExecutionVertex ev : getTaskVertices()) {
			ev.fail(t);
		}
	}

	public void resetForNewExecution(final long timestamp, final long expectedGlobalModVersion)
			throws GlobalModVersionMismatch {

		synchronized (stateMonitor) {
			// check and reset the sharing groups with scheduler hints
			if (slotSharingGroup != null) {
				slotSharingGroup.clearTaskAssignment();
			}

			for (int i = 0; i < parallelism; i++) {
				taskVertices[i].resetForNewExecution(timestamp, expectedGlobalModVersion);
			}

			// set up the input splits again
			try {
				if (this.inputSplits != null) {
					// lazy assignment
					@SuppressWarnings("unchecked")
					InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();
					this.splitAssigner = splitSource.getInputSplitAssigner(this.inputSplits);
				}
			}
			catch (Throwable t) {
				throw new RuntimeException("Re-creating the input split assigner failed: " + t.getMessage(), t);
			}

			// Reset intermediate results
			for (IntermediateResult result : producedDataSets) {
				result.resetForNewExecution();
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Accumulators / Metrics
	// --------------------------------------------------------------------------------------------

	public StringifiedAccumulatorResult[] getAggregatedUserAccumulatorsStringified() {
		Map<String, OptionalFailure<Accumulator<?, ?>>> userAccumulators = new HashMap<>();

		for (ExecutionVertex vertex : taskVertices) {
			Map<String, Accumulator<?, ?>> next = vertex.getCurrentExecutionAttempt().getUserAccumulators();
			if (next != null) {
				AccumulatorHelper.mergeInto(userAccumulators, next);
			}
		}

		return StringifiedAccumulatorResult.stringifyAccumulatorResults(userAccumulators);
	}

	// --------------------------------------------------------------------------------------------
	//  Archiving
	// --------------------------------------------------------------------------------------------

	@Override
	public ArchivedExecutionJobVertex archive() {
		return new ArchivedExecutionJobVertex(this);
	}

	// ------------------------------------------------------------------------
	//  Static Utilities
	// ------------------------------------------------------------------------

	/**
	 * A utility function that computes an "aggregated" state for the vertex.
	 *
	 * <p>This state is not used anywhere in the  coordination, but can be used for display
	 * in dashboards to as a summary for how the particular parallel operation represented by
	 * this ExecutionJobVertex is currently behaving.
	 *
	 * <p>For example, if at least one parallel task is failed, the aggregate state is failed.
	 * If not, and at least one parallel task is cancelling (or cancelled), the aggregate state
	 * is cancelling (or cancelled). If all tasks are finished, the aggregate state is finished,
	 * and so on.
	 *
	 * @param verticesPerState The number of vertices in each state (indexed by the ordinal of
	 *                         the ExecutionState values).
	 * @param parallelism The parallelism of the ExecutionJobVertex
	 *
	 * @return The aggregate state of this ExecutionJobVertex.
	 */
	public static ExecutionState getAggregateJobVertexState(int[] verticesPerState, int parallelism) {
		if (verticesPerState == null || verticesPerState.length != ExecutionState.values().length) {
			throw new IllegalArgumentException("Must provide an array as large as there are execution states.");
		}

		if (verticesPerState[ExecutionState.FAILED.ordinal()] > 0) {
			return ExecutionState.FAILED;
		}
		if (verticesPerState[ExecutionState.CANCELING.ordinal()] > 0) {
			return ExecutionState.CANCELING;
		}
		else if (verticesPerState[ExecutionState.CANCELED.ordinal()] > 0) {
			return ExecutionState.CANCELED;
		}
		else if (verticesPerState[ExecutionState.RUNNING.ordinal()] > 0) {
			return ExecutionState.RUNNING;
		}
		else if (verticesPerState[ExecutionState.FINISHED.ordinal()] > 0) {
			return verticesPerState[ExecutionState.FINISHED.ordinal()] == parallelism ?
					ExecutionState.FINISHED : ExecutionState.RUNNING;
		}
		else {
			// all else collapses under created
			return ExecutionState.CREATED;
		}
	}

	public static Map<JobVertexID, ExecutionJobVertex> includeLegacyJobVertexIDs(
			Map<JobVertexID, ExecutionJobVertex> tasks) {

		Map<JobVertexID, ExecutionJobVertex> expanded = new HashMap<>(2 * tasks.size());
		// first include all new ids
		expanded.putAll(tasks);

		// now expand and add legacy ids
		for (ExecutionJobVertex executionJobVertex : tasks.values()) {
			if (null != executionJobVertex) {
				JobVertex jobVertex = executionJobVertex.getJobVertex();
				if (null != jobVertex) {
					List<JobVertexID> alternativeIds = jobVertex.getIdAlternatives();
					for (JobVertexID jobVertexID : alternativeIds) {
						ExecutionJobVertex old = expanded.put(jobVertexID, executionJobVertex);
						Preconditions.checkState(null == old || old.equals(executionJobVertex),
								"Ambiguous jobvertex id detected during expansion to legacy ids.");
					}
				}
			}
		}

		return expanded;
	}

	public static Map<OperatorID, ExecutionJobVertex> includeAlternativeOperatorIDs(
			Map<OperatorID, ExecutionJobVertex> operatorMapping) {

		Map<OperatorID, ExecutionJobVertex> expanded = new HashMap<>(2 * operatorMapping.size());
		// first include all existing ids
		expanded.putAll(operatorMapping);

		// now expand and add user-defined ids
		for (ExecutionJobVertex executionJobVertex : operatorMapping.values()) {
			if (executionJobVertex != null) {
				JobVertex jobVertex = executionJobVertex.getJobVertex();
				if (jobVertex != null) {
					for (OperatorID operatorID : jobVertex.getUserDefinedOperatorIDs()) {
						if (operatorID != null) {
							expanded.put(operatorID, executionJobVertex);
						}
					}
				}
			}
		}

		return expanded;
	}
}
