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

import io.pravega.connectors.flink.utils.SetupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FlinkPravegaInputFormatTest extends StreamingMultipleProgramsTestBase {

	/** Setup utility */
	private static final SetupUtils SETUP_UTILS = new SetupUtils();

	@Rule
	public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

	// ------------------------------------------------------------------------

	@BeforeClass
	public static void setupPravega() throws Exception {
		SETUP_UTILS.startAllServices();
	}

	@AfterClass
	public static void tearDownPravega() throws Exception {
		SETUP_UTILS.stopAllServices();
	}

	@Test
	public void testBatchEnds() throws Exception {

		// set up the stream
		final String streamName = RandomStringUtils.randomAlphabetic(20);
		SETUP_UTILS.createTestStream(streamName, 3);

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		env.createInput(new FlinkPravegaInputFormat<>(
				SETUP_UTILS.getControllerUri(),
				SETUP_UTILS.getScope(),
				Collections.singleton(streamName),
				0,
				new IntDeserializer())
		).print();

		env.execute();
	}

	private static class IntDeserializer extends AbstractDeserializationSchema<Integer> {

		@Override
		public Integer deserialize(byte[] message) throws IOException {
			return ByteBuffer.wrap(message).getInt();
		}

		@Override
		public boolean isEndOfStream(Integer nextElement) {
			return false;
		}
	}
}
