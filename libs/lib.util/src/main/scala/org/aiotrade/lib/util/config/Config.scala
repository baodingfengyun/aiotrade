package org.aiotrade.lib.util.config

import com.typesafe.config.ConfigFactory
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.util.Locale
import java.util.Properties
import java.util.ResourceBundle
import java.util.logging.Logger

final case class ConfigurationException(message: String) extends RuntimeException(message)

/**
 * Loads up the configuration (from the app.conf file).
 */
object Config {
  private val log = Logger.getLogger(this.getClass.getName)

  val mode = System.getProperty("run.mode", "development")
  val version = "0.10"

  lazy val configDir: Option[String] = List("../conf", "../etc") find {x =>
    val f = new File(x)
    f.exists && f.isDirectory
  }

  private var _config: com.typesafe.config.Config = _

  def apply(fileName: String = null): com.typesafe.config.Config = {
    if (_config == null) {
      val classLoader = Thread.currentThread.getContextClassLoader

      _config = if (fileName != null) {
        try {
          val nbUserPath = System.getProperty("netbeans.user")
          log.info("Config loaded directly from [%s].".format(fileName))
          log.info("netbeans.user=" + nbUserPath)
          ConfigFactory.parseFile(new File(fileName))
        } catch {
          case ex: Throwable => throw new ConfigurationException(
              "The '" + fileName + " config file can not be found" +
              "\n\tdue to: " + ex.toString)
        }
      } else if (System.getProperty("config.resource", "") != "") {
        val configFile = System.getProperty("config.resource", "")
        try {
          log.info("Config loaded from -D" + "config.resource=%s".format(configFile))
          ConfigFactory.parseFile(new File(configFile))
        } catch {
          case ex: Throwable => throw new ConfigurationException(
              "Config could not be loaded from -D" + "run.config=" + configFile +
              "\n\tdue to: " + ex.toString)
        }
      } else if (configDir.isDefined) {
        try {
          val configFile = configDir.get + "/" + mode + ".conf"
          log.info("configDir is defined as [%s], config loaded from [%s].".format(configDir.get, configFile))
          ConfigFactory.parseFile(new File(configFile))
        } catch {
          case ex: Throwable => throw new ConfigurationException(
              "configDir is defined as [" + configDir.get + "] " +
              "\n\tbut the '" + mode + ".conf' config file can not be found at [" + configDir.get + "/" + mode + ".conf]," +
              "\n\tdue to: " + ex.toString)
        }
      } else if (classLoader.getResource(mode + ".conf") != null) {
        try {
          log.info("Config loaded from the application classpath [%s].".format(mode + ".conf"))
          ConfigFactory.parseResources(classLoader, mode + ".conf")
        } catch {
          case ex: Throwable => throw new ConfigurationException(
              "Can't load '" + mode + ".conf' config file from application classpath," +
              "\n\tdue to: " + ex.toString)
        }
      } else {
        log.warning(
          "\nCan't load '" + mode + ".conf'." +
          "\nOne of the three ways of locating the '" + mode + ".conf' file needs to be defined:" +
          "\n\t1. Define the '-D" + mode + ".config=...' system property option." +
          "\n\t2. Define './conf' directory." +
          "\n\t3. Put the '" + mode + ".conf' file on the classpath." +
          "\nI have no way of finding the '" + mode + ".conf' configuration file." +
          "\nUsing default values everywhere.")
        ConfigFactory.parseString("<" + mode + "></" + mode + ">") // default empty config
      }
    }

    _config
  }

  val startTime = System.currentTimeMillis
  def uptime = (System.currentTimeMillis - startTime) / 1000


  // --- todo for properties
  def loadProperties(fileName: String) {
    val props = new Properties
    val file = new File(fileName)
    if (file.exists) {
      try {
        val is = new FileInputStream(file)
        if (is != null) props.load(is)
        is.close
      } catch {case _: Throwable =>}
    }
  }

  private val SUFFIX = ".properties"
  def loadProperties($name: String, LOAD_AS_RESOURCE_BUNDLE: Boolean = false): Properties = {
    var name = $name
    if ($name.startsWith("/"))  name = $name.substring(1)
    
    if ($name.endsWith(SUFFIX)) name = $name.substring(0, $name.length - SUFFIX.length)
    
    val props = new Properties

    val loader = classLoader
    var in: InputStream = null
    try {
      if (LOAD_AS_RESOURCE_BUNDLE) {
        name = name.replace('/', '.')
        val rb = ResourceBundle.getBundle(name, Locale.getDefault, loader)
        val keys = rb.getKeys
        while (keys.hasMoreElements) {
          props.put(keys.nextElement.asInstanceOf[String], rb.getString(keys.nextElement.asInstanceOf[String]))
        }
      } else {
        name = name.replace('.', '/')
        if (!name.endsWith(SUFFIX)) name = name.concat(SUFFIX)
        in = loader.getResourceAsStream(name)
        if (in != null) {
          props.load(in) // can throw IOException
        }
      }
      in.close
    } catch {case _: Throwable =>}
    props
  }

  // ### Classloading

  def classLoader: ClassLoader = Thread.currentThread.getContextClassLoader

  def loadClass[C](name: String): Class[C] = Class.forName(name, true, classLoader).asInstanceOf[Class[C]]
}
