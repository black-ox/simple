package com.ox.bigdata.util.log

import org.slf4j.LoggerFactory

trait LogSupport {
  protected val LOG = LoggerFactory.getLogger(this.getClass.getSimpleName)
}
