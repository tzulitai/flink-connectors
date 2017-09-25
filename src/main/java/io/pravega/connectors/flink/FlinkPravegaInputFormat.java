/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink;

import com.google.common.base.Preconditions;

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;

import org.apache.commons.lang3.RandomStringUtils;

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

import static io.pravega.connectors.flink.util.FlinkPravegaUtils.createPravegaReader;

/**
 * A Flink {@link InputFormat} that can be added as a source to read from Pravega in a Flink batch job.
 */
public class FlinkPravegaInputFormat<T> extends GenericInputFormat<T> implements InitializeOnMaster, FinalizeOnMaster {

	// The supplied event deserializer.
	private final DeserializationSchema<T> deserializationSchema;

	// The pravega controller endpoint.
	private final URI controllerURI;

	// The scope name of the destination stream.
	private final String scopeName;

	// The readergroup name to coordinate the parallel readers. This should be unique for a Flink job.
	private final String readerGroupName;

	// The names of Pravega streams to read
	private final Set<String> streamNames;

	// The configured start time for the reader
	private final long startTime;

	// The Pravega reader; a new reader will be opened for each input split
	private EventStreamReader<T> pravegaReader;

	// Read-ahead event; null indicates that end of input is reached
	private transient EventRead<T> lastReadAheadEvent;

	/**
	 *
	 * @param controllerURI
	 * @param scope
	 * @param streamNames
	 * @param startTime
	 * @param deserializationSchema
	 */
	public FlinkPravegaInputFormat(
			final URI controllerURI,
			final String scope,
			final Set<String> streamNames,
			final long startTime,
			final DeserializationSchema<T> deserializationSchema) {

		this(controllerURI, scope, streamNames, startTime, deserializationSchema, null);
	}

	public FlinkPravegaInputFormat(
			final URI controllerURI,
			final String scope,
			final Set<String> streamNames,
			final long startTime,
			final DeserializationSchema<T> deserializationSchema,
			final String readerName) {

		Preconditions.checkNotNull(controllerURI, "controllerURI");
		Preconditions.checkNotNull(scope, "scope");
		Preconditions.checkNotNull(streamNames, "streamNames");
		Preconditions.checkArgument(startTime >= 0, "start time must be >= 0");
		Preconditions.checkNotNull(deserializationSchema, "deserializationSchema");

		this.controllerURI = controllerURI;
		this.scopeName = scope;
		this.deserializationSchema = deserializationSchema;
		this.streamNames = streamNames;
		this.startTime = startTime;
		this.readerGroupName = "flink" + RandomStringUtils.randomAlphanumeric(20).toLowerCase();
	}

	// ------------------------------------------------------------------------
	//  Globol one-time initialization / cleanups
	// ------------------------------------------------------------------------

	@Override
	public void initializeGlobal(int i) throws IOException {
		ReaderGroupManager.withScope(scopeName, controllerURI)
				.createReaderGroup(this.readerGroupName, ReaderGroupConfig.builder().startingTime(startTime).build(),
						streamNames);
	}

	@Override
	public void finalizeGlobal(int i) throws IOException {
		ReaderGroupManager.withScope(scopeName, controllerURI)
				.deleteReaderGroup(readerGroupName);
	}

	// ------------------------------------------------------------------------
	//  Input split life cycle methods
	// ------------------------------------------------------------------------

	@Override
	public void open(GenericInputSplit split) throws IOException {
		super.open(split);

		// build a new reader for each input split
		this.pravegaReader = createPravegaReader(
				this.scopeName,
				this.controllerURI,
				getRuntimeContext().getTaskNameWithSubtasks(),
				this.readerGroupName,
				this.deserializationSchema,
				ReaderConfig.builder().build());

		try {
			this.lastReadAheadEvent = pravegaReader.readNextEvent(1000);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return lastReadAheadEvent.getEvent() == null;
	}

	@Override
	public T nextRecord(T t) throws IOException {
		try {
			final EventRead<T> nextEvent = lastReadAheadEvent;
			lastReadAheadEvent = pravegaReader.readNextEvent(1000);
			return nextEvent.getEvent();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void close() throws IOException {
		this.pravegaReader.close();
	}
}
