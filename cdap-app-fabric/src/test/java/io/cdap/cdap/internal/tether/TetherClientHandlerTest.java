package io.cdap.cdap.internal.tether;


import com.google.common.collect.ImmutableList;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
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
import java.util.List;
import java.util.Map;

public class TetherClientHandlerTest extends AppFabricTestBase {
  private static CConfiguration cConf;
  private static TetherStore tetherStore;

  private NettyHttpService serverService;
  private ClientConfig serverConfig;
  private NettyHttpService clientService;
  private ClientConfig clientConfig;

  @BeforeClass
  public static void setup() {
    cConf = getInjector().getInstance(CConfiguration.class);
    tetherStore = new TetherStore(getInjector().getInstance(TransactionRunner.class));
  }

  @Before
  public void setUp() throws Exception {
    CConfiguration conf = CConfiguration.create();
    serverService = new CommonNettyHttpServiceBuilder(conf, getClass().getSimpleName() + "_server")
      .setHttpHandlers(new MockTetherServerHandler()).build();
    serverService.start();
    serverConfig = ClientConfig.builder()
      .setConnectionConfig(
        ConnectionConfig.builder()
          .setHostname(serverService.getBindAddress().getHostName())
          .setPort(serverService.getBindAddress().getPort())
          .setSSLEnabled(false)
          .build()).build();

    cConf.setInt(Constants.Tether.CONNECT_INTERVAL, 1);
    cConf.setInt(Constants.Tether.CONNECTION_TIMEOUT, 5);

    clientService = new CommonNettyHttpServiceBuilder(conf, getClass().getSimpleName() + "_client")
      .setHttpHandlers(new TetherClientHandler(cConf, tetherStore)).build();
    clientService.start();
    clientConfig = ClientConfig.builder()
      .setConnectionConfig(
        ConnectionConfig.builder()
          .setHostname(clientService.getBindAddress().getHostName())
          .setPort(clientService.getBindAddress().getPort())
          .setSSLEnabled(false)
          .build()).build();
  }

  @Test
  public void testTether() throws IOException, InterruptedException {
    List<NamespaceAllocation> namespaces = ImmutableList.of(new NamespaceAllocation("ns1", "40%",
                                                                                    "40%"),
                                                            new NamespaceAllocation("ns2", "20%",
                                                                                    "30%"));
    TetherRequest tetherRequest = new TetherRequest("my-project,", "us-west1", "my-instance",
                                                    serverConfig.getConnectionConfig().getURI(), namespaces);

    HttpRequest request = HttpRequest.builder(HttpMethod.POST, clientConfig.resolveURL("tethering/create"))
      .withBody(GSON.toJson(tetherRequest))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    waitForStatus(tetherRequest.getInstance(), TetherConnectionStatus.ACTIVE);

    request = HttpRequest.builder(HttpMethod.GET, clientConfig.resolveURL("tethering/requests"))
      .build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    Type type = new TypeToken<List<PeerInfo>>() { }.getType();
    List<PeerInfo> peers = GSON.fromJson(response.getResponseBodyAsString(), type);
    Assert.assertEquals(1, peers.size());
    PeerMetadata expectedPeerMetadata = new PeerMetadata(tetherRequest.getProject(), tetherRequest.getLocation(),
                                                         tetherRequest.getNamespaces());
    PeerInfo expectedPeerInfo = new PeerInfo(tetherRequest.getInstance(), tetherRequest.getEndpoint().toString(),
                                             TetherStatus.PENDING, expectedPeerMetadata);
    Assert.assertEquals(expectedPeerInfo, peers.get(0));

    request = HttpRequest.builder(HttpMethod.DELETE, clientConfig.resolveURL("tethering/connections/my-instance"))
      .build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
  }

  private void waitForStatus(String instanceName, TetherConnectionStatus status) throws IOException, InterruptedException {
    HttpRequest request;
    HttpResponse response;
    Map<String, TetherConnectionStatus> channelStatus = null;
    for (int retry = 0; retry < 5; ++retry) {
      request = HttpRequest.builder(HttpMethod.GET, clientConfig.resolveURL("tethering/controlchannels")).build();
      response = HttpRequests.execute(request);
      Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
      Type type = new TypeToken<Map<String, TetherConnectionStatus>>() {
      }.getType();
      channelStatus = GSON.fromJson(response.getResponseBodyAsString(), type);
      if (channelStatus.get(instanceName) == status) {
        return;
      }
      Thread.sleep(500);
    }
    Assert.assertNotNull(channelStatus);
    Assert.assertEquals(1, channelStatus.size());
    Assert.assertEquals(TetherConnectionStatus.ACTIVE, channelStatus.get(instanceName));
  }
}
