package com.xiaoi.conf

import com.typesafe.config.ConfigFactory

/**
  * 配置管理组件
  * Created by josh.ye on 6/19/17.
  */
object ConfManager extends Serializable{
  private val config = ConfigFactory.load("xiaoi.properties")

  /**
    * Get the String type from config file
    */
  def getString(key: String): String = {
    return config.getString(key)
  }

  /**
    * Get the Integer type from config file
    */
  def getInt(key: String): Int = {
    return config.getInt(key)
  }

  /**
    * Get the Boolean value from config file
    * @param key
    */
  def getBoolean(key: String): Boolean = {
    return config.getBoolean(key)
  }

  /**
    * Get the Double value from config file
    * @param key
    */
  def getDouble(key: String): Double = {
    return config.getDouble(key)
  }

  /**
    * Get the Long value from config file
    * @param key
    */
  def getLong(key: String): Long = {
    return config.getLong(key)
  }

}

