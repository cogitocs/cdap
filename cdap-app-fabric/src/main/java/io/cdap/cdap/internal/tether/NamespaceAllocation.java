package io.cdap.cdap.internal.tether;

import java.util.Objects;

public class NamespaceAllocation {
  private final String namespace;
  private final String cpuLimit;
  private final String memoryLimit;

  public NamespaceAllocation(String namespace, String cpuLimit, String memoryLimit) {
    this.namespace = namespace;
    this.cpuLimit = cpuLimit;
    this.memoryLimit = memoryLimit;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    NamespaceAllocation that = (NamespaceAllocation) other;
    return Objects.equals(this.namespace, that.namespace) &&
      Objects.equals(this.cpuLimit, that.cpuLimit) &&
      Objects.equals(this.memoryLimit, that.memoryLimit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, cpuLimit, memoryLimit);
  }
}
