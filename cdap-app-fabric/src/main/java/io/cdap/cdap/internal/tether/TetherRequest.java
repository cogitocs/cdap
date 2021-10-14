package io.cdap.cdap.internal.tether;

import java.net.URI;
import java.util.List;

public class TetherRequest {
  private final String project;
  private final String location;
  private final String instance;
  // this field is not used on the server
  private final URI endpoint;
  private final List<NamespaceAllocation> namespaces;
  public TetherRequest(String project, String location, String instance, URI endpoint,
                       List<NamespaceAllocation> namespaces) {
    this.project = project;
    this.location = location;
    this.instance = instance;
    this.endpoint = endpoint;
    this.namespaces = namespaces;
  }

  public String getProject() {
    return project;
  }

  public String getLocation() {
    return location;
  }

  public String getInstance() {
    return instance;
  }

  public URI getEndpoint() {
    return endpoint;
  }

  public List<NamespaceAllocation> getNamespaces() {
    return namespaces;
  }
}
