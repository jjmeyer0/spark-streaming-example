package com.jj.log

import org.slf4j.{Logger, LoggerFactory}

// Based off of Spark's internal logging
trait Logging {
  @transient private var _log: Logger = null

  def log = {
    if (_log == null) _log = LoggerFactory.getLogger(this.getClass.getSimpleName.stripSuffix("$"))
    _log
  }
}
