/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway.collector.flume;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @todo write javadoc
 */
public class NettyFlumeCollector extends FlumeCollector {

	private static final Logger LOG = LoggerFactory
			.getLogger(NettyFlumeCollector.class);

	private Server server; // the netty server

	@Override
	public void start() {

    LOG.debug("Starting up " + this);

    this.server = new NettyServer(
				new SpecificResponder(AvroSourceProtocol.class, this.flumeAdapter),
				new InetSocketAddress(this.getPort()));
		this.server.start();

    LOG.info("Collector '" + this.getName() + "' started on port " + port + ".");
  }

	@Override
	public void stop() {
		LOG.debug("Stopping " + this);
		try {
			this.server.close();
			this.server.join();
		} catch (InterruptedException e) {
			LOG.info("Received interrupt during join.");
		}
		LOG.debug("Stopped " + this);
	}
}
