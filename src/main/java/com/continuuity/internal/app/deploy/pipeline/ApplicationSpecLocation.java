/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.filesystem.Location;

/**
 * This class carries information about ApplicationSpecification
 * and Location between stages.
 */
public class ApplicationSpecLocation {
  private final ApplicationSpecification specification;
  private final Location archive;

  public ApplicationSpecLocation(ApplicationSpecification specification, Location archive) {
    this.specification = specification;
    this.archive = archive;
  }

  /**
   * @return {@link ApplicationSpecification} sent to this stage.
   */
  public ApplicationSpecification getSpecification() {
    return specification;
  }

  /**
   * @return Location of archive to this stage.
   */
  public Location getArchive() {
    return archive;
  }
}
