package io.cdap.cdap.internal.tether;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TetherServerHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new GsonBuilder().create();
  private static TetherStore tetherStore;
  private static MessagingService messagingService;
  private static CConfiguration cConf;

  private NettyHttpService service;
  private ClientConfig config;

  @BeforeClass
  public static void setup() {
    tetherStore = new TetherStore(getInjector().getInstance(TransactionRunner.class));
    messagingService = getInjector().getInstance(MessagingService.class);
    cConf = getInjector().getInstance(CConfiguration.class);
  }

  @Before
  public void setUp() throws Exception {
    cConf.setInt(Constants.Tether.CONNECTION_TIMEOUT, 1);
    service = new CommonNettyHttpServiceBuilder(CConfiguration.create(), getClass().getSimpleName())
      .setHttpHandlers(new TetherServerHandler(cConf, tetherStore, messagingService)).build();
    service.start();
    config = ClientConfig.builder()
      .setConnectionConfig(
        ConnectionConfig.builder()
          .setHostname(service.getBindAddress().getHostName())
          .setPort(service.getBindAddress().getPort())
          .setSSLEnabled(false)
          .build()).build();
  }

  private void checkTetherStatus(Map<String, TetherConnectionStatus> expectedStatus) throws IOException {
    HttpRequest request = HttpRequest.builder(HttpMethod.GET, config.resolveURL("tethering/controlchannels"))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    Type type = new TypeToken<Map<String, TetherConnectionStatus>>() { }.getType();
    Map<String, TetherConnectionStatus> connectionStatus = GSON.fromJson(response.getResponseBodyAsString(), type);
    Assert.assertTrue(Maps.difference(expectedStatus, connectionStatus).areEqual());

  }

  @Test
  public void testCreateControlChannel() throws IOException, InterruptedException {
    checkTetherStatus(Collections.emptyMap());

    HttpRequest request = HttpRequest.builder(HttpMethod.PUT, config.resolveURL("tethering/controlchannels/xyz"))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    Type type = new TypeToken<List<TetherControlMessage>>() { }.getType();
    List<TetherControlMessage> expectedCommands = Collections.singletonList(
      new TetherControlMessage(TetherControlMessage.Type.KEEPALIVE, null));
    List<TetherControlMessage> commands = GSON.fromJson(response.getResponseBodyAsString(), type);
    Assert.assertEquals(expectedCommands, commands);

    Map<String, TetherConnectionStatus> expectedStatus = new HashMap<>();
    expectedStatus.put("xyz", TetherConnectionStatus.ACTIVE);
    checkTetherStatus(expectedStatus);
    Thread.sleep(cConf.getInt(Constants.Tether.CONNECTION_TIMEOUT)*1000);

    expectedStatus.put("xyz", TetherConnectionStatus.INACTIVE);
    checkTetherStatus(expectedStatus);
  }

  @Test
  public void testTether() throws IOException, TopicNotFoundException {
    List<NamespaceAllocation> allocations = ImmutableList.of(
      new NamespaceAllocation("testns", "10%", "20%"));
    TetherRequest tetherRequest = new TetherRequest("my-project", "us-west1", "xyz", null, allocations);
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/create"))
      .withBody(GSON.toJson(tetherRequest))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    request = HttpRequest.builder(HttpMethod.GET, config.resolveURL("tethering/requests"))
      .build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    Type type = new TypeToken<List<PeerInfo>>() { }.getType();
    List<PeerInfo> peers = GSON.fromJson(response.getResponseBodyAsString(), type);
    Assert.assertEquals(1, peers.size());
    PeerMetadata expectedPeerMetadata = new PeerMetadata(null, null, allocations);
    PeerInfo expectedPeerInfo = new PeerInfo("xyz", null, TetherStatus.PENDING, expectedPeerMetadata);
    Assert.assertEquals(expectedPeerInfo, peers.get(0));

    request = HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/requests/xyz/accept"))
      .build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    request = HttpRequest.builder(HttpMethod.PUT, config.resolveURL("tethering/controlchannels/xyz"))
      .build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    type = new TypeToken<List<TetherControlMessage>>() { }.getType();
    List<TetherControlMessage> responseMsgs = GSON.fromJson(response.getResponseBodyAsString(StandardCharsets.UTF_8),
                                                            type);
    Assert.assertEquals(1, responseMsgs.size());
    TetherControlMessage expectedMessage = new TetherControlMessage(TetherControlMessage.Type.TETHER_ACCEPTED, null);
    Assert.assertEquals(expectedMessage, responseMsgs.get(0));

    request = HttpRequest.builder(HttpMethod.DELETE, config.resolveURL("tethering/connections/xyz"))
      .build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
  }
}
