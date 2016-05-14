package io.stat.util

import org.slf4j.LoggerFactory

/**
  * A mixin trait for easy logging primitives, backed by logback (technically slf4j).
  *
  * @author Ilya Ostrovskiy (https://github.com/iostat/)
  */
trait Logging {
  private final val _logger = LoggerFactory.getLogger(getClass)

  protected def logTrace(msg: => String) = {
    if (_logger.isTraceEnabled) {
      _logger.trace(msg)
    }
  }

  protected def logTrace(msg: => String, a1: => Any) = {
    if (_logger.isTraceEnabled) {
      a1 match {
        case throwable: Throwable => _logger.trace(msg, throwable)
        case other: Any           => _logger.trace(msg, other)
      }
    }
  }

  protected def logTrace(msg: => String, a1: => Any, a2: => Any) = {
    if (_logger.isTraceEnabled) {
      _logger.trace(msg, a1, a2)
    }
  }

  protected def logTrace(msg: => String, args: Any*) = {
    if (_logger.isTraceEnabled) {
      _logger.trace(msg, args)
    }
  }

  protected def logDebug(msg: => String) = {
    if (_logger.isDebugEnabled) {
      _logger.debug(msg)
    }
  }

  protected def logDebug(msg: => String, a1: => Any) = {
    if (_logger.isDebugEnabled) {
      a1 match {
        case throwable: Throwable => _logger.debug(msg, throwable)
        case other: Any           => _logger.debug(msg, other)
      }
    }
  }

  protected def logDebug(msg: => String, a1: => Any, a2: => Any) = {
    if (_logger.isDebugEnabled) {
      _logger.debug(msg, a1, a2)
    }
  }

  protected def logDebug(msg: => String, args: Any*) = {
    if (_logger.isDebugEnabled) {
      _logger.debug(msg, args)
    }
  }

  protected def logInfo(msg: => String) = {
    if (_logger.isInfoEnabled) {
      _logger.info(msg)
    }
  }

  protected def logInfo(msg: => String, a1: => Any) = {
    a1 match {
      case throwable: Throwable => _logger.info(msg, throwable)
      case other: Any           => _logger.info(msg, other)
    }
  }

  protected def logInfo(msg: => String, a1: => Any, a2: => Any) = {
    if (_logger.isInfoEnabled) {
      _logger.info(msg, a1, a2)
    }
  }

  protected def logInfo(msg: => String, args: Any*) = {
    if (_logger.isInfoEnabled) {
      _logger.info(msg, args)
    }
  }

  protected def logWarn(msg: => String) = {
    if (_logger.isWarnEnabled) {
      _logger.warn(msg)
    }
  }

  protected def logWarn(msg: => String, a1: => Any) = {
    if (_logger.isWarnEnabled) {
      a1 match {
        case throwable: Throwable => _logger.warn(msg, throwable)
        case other: Any           => _logger.warn(msg, other)
      }
    }
  }

  protected def logWarn(msg: => String, a1: => Any, a2: => Any) = {
    if (_logger.isWarnEnabled) {
      _logger.warn(msg, a1, a2)
    }
  }

  protected def logWarn(msg: => String, args: Any*) = {
    if (_logger.isWarnEnabled) {
      _logger.warn(msg, args)
    }
  }

  protected def logError(msg: => String) = {
    if (_logger.isErrorEnabled) {
      _logger.error(msg)
    }
  }

  protected def logError(msg: => String, a1: => Any) = {
    if (_logger.isErrorEnabled) {
      a1 match {
        case throwable: Throwable => _logger.error(msg, throwable)
        case other: Any           => _logger.error(msg, other)
      }
    }
  }

  protected def logError(msg: => String, a1: => Any, a2: => Any) = {
    if (_logger.isErrorEnabled) {
      _logger.error(msg, a1, a2)
    }
  }

  protected def logError(msg: => String, args: Any*) = {
    if (_logger.isErrorEnabled) {
      _logger.error(msg, args)
    }
  }
}
