package io.cdap.cdap.internal.tether;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Path(Constants.Gateway.API_VERSION_3)
public class MockTetherServerHandler extends AbstractHttpHandler {
  private static final Gson GSON = new Gson();

  @PUT
  @Path("/tethering/controlchannels/{peer}")
  public void getControlChannels(HttpRequest request, HttpResponder responder,
                                 @PathParam("peer") String peer) {
    Type type = new TypeToken<List<String>>() { }.getType();
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(
      Collections.singletonList(new TetherControlMessage(TetherControlMessage.Type.KEEPALIVE, null)), type));
  }

  @POST
  @Path("/tethering/create")
  public void createTether(FullHttpRequest request, HttpResponder responder) {
    String content = request.content().toString(StandardCharsets.UTF_8);
    TetherRequest tetherRequest = GSON.fromJson(content, TetherRequest.class);
    Assert.assertEquals("my-instance", tetherRequest.getInstance());
    Assert.assertEquals("us-west1", tetherRequest.getLocation());
    Assert.assertEquals(2, tetherRequest.getNamespaces().size());
    Assert.assertEquals(new NamespaceAllocation("ns1", "40%", "40%"), tetherRequest.getNamespaces().get(0));
    Assert.assertEquals(new NamespaceAllocation("ns2", "20%", "30%"), tetherRequest.getNamespaces().get(1));
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
