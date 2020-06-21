package br.com.projscala2.config

import com.typesafe.config._

class ApplicationConfig(config: Config) {
  def this() {
    this(ConfigFactory.load())
    config.checkValid(ConfigFactory.defaultReference(), "application")
  }
  def loadConstants() : Config = {
    return config
  }
}
