package com.ox.bigdata.utils.log

import org.slf4j.LoggerFactory

trait LogSupport {
  protected val log = LoggerFactory.getLogger(this.getClass.getSimpleName)
}
